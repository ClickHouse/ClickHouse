#pragma once

#include <Interpreters/Context_fwd.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <base/types.h>

namespace DB
{

struct DistributedQueryTaskBatch;

/// Sends `start_tasks` with the tasks of `batch`, all of the query `unique_query_id` and for this worker. `collectors` names
/// what the worker collects for them (the `collect` URL parameter); status replies carry only those payloads.
String sendTaskBatch(const String & endpoint_uri, const String & unique_query_id, const DistributedQueryTaskBatch & batch,
    const String & unique_temp_file_path, const TaskCollectors & collectors, const ContextPtr & context);

/// The statuses of `task_ids`, all on this worker, in order, after the worker waited up to `wait_for_ms` for one of them to end
/// or fail. `for_cleanup` uses short HTTP timeouts and no retries, so a best-effort cleanup does not wait for the normal
/// multi-minute request budget when a worker is slow or unreachable.
std::vector<DistributedQueryTaskStatusEntry> getTaskStatuses(const String & endpoint_uri, const Strings & task_ids, UInt32 wait_for_ms,
    const ContextPtr & context, bool for_cleanup = false);

void cancelTasks(const String & endpoint_uri, const Strings & task_ids, const ContextPtr & context);

void forgetTasks(const String & endpoint_uri, const Strings & task_ids, const ContextPtr & context);

}
