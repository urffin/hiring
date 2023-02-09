import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    maxThreads = Math.max(0, maxThreads);

    if (maxThreads == 0) {
        const mapQueue = new Map<number, Promise<void>>();
        for await (const task of queue) {
            const promise = mapQueue.get(task.targetId);
            if (!promise) {
                mapQueue.set(task.targetId, executor.executeTask(task));
                continue;
            }

            mapQueue.set(task.targetId, promise.then(() => executor.executeTask(task)));
        }
        return Promise.all(mapQueue.values())
    }

    const q: Promise<void>[] = Array(maxThreads).fill(Promise.resolve());
    // const qq: number[] = Array(maxThreads).fill(-1);

    const taskIterator = queue[Symbol.asyncIterator]();
    let { value: task, done } = await taskIterator.next();
    while (!done) {
        for (; !done; { value: task, done } = await taskIterator.next()) {
            await Promise.race(q);

            const t = task;
            const targetId = task.targetId;
            const backet = targetId % maxThreads
            // let backet = qq.findIndex(p => p == targetId);

            // if (backet == -1) {
            //     backet = qq.findIndex(p => p == -1);
            // }


            q[backet] = q[backet].then(() => {
                // qq[backet] = t.targetId;
                return executor.executeTask(t);
            }).then(() => {
                // qq[backet] = -1;
            });
        }

        await Promise.all(q);
        ({ value: task, done } = await taskIterator.next());
    }
}
