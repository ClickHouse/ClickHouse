---
slug: /en/operations/system-tables/asynchronous_loader
---
# asynchronous_loader

Contains information and status for recent asynchronous jobs (e.g. for tables loading). The table contains a row for every job. There is a tool for visualizing information from this table `utils/async_loader_graph`.

Example:

``` sql
SELECT *
FROM system.asynchronous_loader
FORMAT Vertical
LIMIT 1
```

``` text
```

Columns:

- `job` (`String`) - Job name (may be not unique).
- `job_id` (`UInt64`) - Unique ID of the job.
- `dependencies` (`Array(UInt64)`) - List of IDs of jobs that should be done before this job.
- `dependencies_left` (`UInt64`) - Current number of dependencies left to be done.
- `status` (`Enum`) - Current load status of a job:
    `PENDING`:  Load job is not started yet.
    `OK`: Load job executed and was successful.
    `FAILED`: Load job executed and failed.
    `CANCELED`: Load job is not going to be executed due to removal or dependency failure.

A pending job might be in one of the following states:
- `is_executing` (`UInt8`) - The job is currently being executed by a worker.
- `is_blocked` (`UInt8`) - The job waits for its dependencies to be done.
- `is_ready` (`UInt8`) - The job is ready to be executed and waits for a worker.
- `elapsed` (`Float64`) - Seconds elapsed since start of execution. Zero if job is not started. Total execution time if job finished.

Every job has a pool associated with it and is started in this pool. Each pool has a constant priority and a mutable maximum number of workers. Higher priority (lower `priority` value) jobs are run first. No job with lower priority is started while there is at least one higher priority job ready or executing. Job priority can be elevated (but cannot be lowered) by prioritizing it. For example jobs for a table loading and startup will be prioritized if incoming query required this table. It is possible prioritize a job during its execution, but job is not moved from its `execution_pool` to newly assigned `pool`. The job uses `pool` for creating new jobs to avoid priority inversion. Already started jobs are not preempted by higher priority jobs and always run to completion after start.
- `pool_id` (`UInt64`) - ID of a pool currently assigned to the job.
- `pool` (`String`) - Name of `pool_id` pool.
- `priority` (`Int64`) - Priority of `pool_id` pool.
- `execution_pool_id` (`UInt64`) - ID of a pool the job is executed in. Equals initially assigned pool before execution starts.
- `execution_pool` (`String`) - Name of `execution_pool_id` pool.
- `execution_priority` (`Int64`) - Priority of `execution_pool_id` pool.

- `ready_seqno` (`Nullable(UInt64)`) - Not null for ready jobs. Worker pulls the next job to be executed from a ready queue of its pool. If there are multiple ready jobs, then job with the lowest value of `ready_seqno` is picked.
- `waiters` (`UInt64`) - The number of threads waiting on this job.
- `exception` (`Nullable(String)`) - Not null for failed and canceled jobs. Holds error message raised during query execution or error leading to cancelling of this job along with dependency failure chain of job names.

Time instants during job lifetime:
- `schedule_time` (`DateTime64`) - Time when job was created and scheduled to be executed (usually with all its dependencies).
- `enqueue_time` (`Nullable(DateTime64)`) - Time when job became ready and was enqueued into a ready queue of its pool. Null if the job is not ready yet.
- `start_time` (`Nullable(DateTime64)`) - Time when worker dequeues the job from ready queue and start its execution. Null if the job is not started yet.
- `finish_time` (`Nullable(DateTime64)`) - Time when job execution is finished. Null if the job is not finished yet.
