# ClickHouse Threading Reference

## Thread Pool Hierarchy

- **`GlobalThreadPool`** (`src/Common/ThreadPool.h`): Singleton managing all thread creation. Configured via `max_thread_pool_size`, `max_thread_pool_free_size`, `thread_pool_queue_size`.
- **`ThreadFromGlobalPool`**: Wraps `std::thread` but pulls from the global pool. Used throughout the codebase instead of raw `std::thread`.
- **`IOThreadPool`**: Separate pool for I/O-bound jobs to avoid starving query execution.
- **`BackupsIOThreadPool`**: Dedicated pool for backup operations.
- **`BackgroundSchedulePool`** (`src/Core/BackgroundSchedulePool.h`): Periodic task scheduler. Guarantees a task does not run simultaneously from multiple workers. Uses `TSA_GUARDED_BY(tasks_mutex)` extensively.
- **`MergeTreeBackgroundExecutor`** (`src/Storages/MergeTree/MergeTreeBackgroundExecutor.h`): Preemptable tasks for merges, mutations, fetches. Tasks implement `IExecutableTask` with `executeStep` method.

## Per-Thread State

- **`ThreadStatus`** (`src/Common/ThreadStatus.h`): Thread-local object holding thread ID, performance counters, memory tracker, query context.
- **`ThreadGroup`**: Logical grouping for threads in a single operation. Created via `createForQuery`, `createForMergeMutate`, etc.
- **`CurrentThread`** (`src/Common/CurrentThread.h`): Static accessor for per-thread state. No parameter passing needed.
- **`ConcurrencyControl`** (`src/Common/ConcurrencyControl.h`): Global CPU slot scheduling across competing queries.

## Common Thread Names (15-byte limit)

QueryPipelineEx, QueryPullPipeEx, QueryPushPipeEx, MergeMutate, MERGETREE_FETCH, MERGETREE_MOVE, BgSchPool, BgBufSchPool, BgDistSchPool, AsyncMetrics, ConfigReloader, ZooKeeperRecv, ZooKeeperSend, TCPHandler, HTTPHandler, MySQLHandler, AsyncInsertQue, AsyncLogger.

## Common Synchronization Patterns

- `std::mutex` + `TSA_GUARDED_BY` for field protection
- `SharedMutex` for read-heavy workloads
- `std::atomic` for lock-free counters and flags
- Per-thread access via `CurrentThread::get`

## TSA Macros (`base/base/defines.h`)

```cpp
TSA_GUARDED_BY(mutex)              // Field protected by mutex
TSA_PT_GUARDED_BY(mutex)           // Pointer-to-data protected by mutex
TSA_REQUIRES(mutex)                // Function requires exclusive lock
TSA_REQUIRES_SHARED(mutex)         // Function requires shared lock
TSA_NO_THREAD_SAFETY_ANALYSIS      // Suppress TSA checks (use sparingly!)
TSA_ACQUIRED_AFTER(mutex)          // Lock ordering constraint
TSA_CAPABILITY("mutex")            // Declare a capability type
```
