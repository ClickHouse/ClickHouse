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

/// Returns the statuses of `task_ids`, in order, once one of them ends or fails or after `wait_for_ms`. `for_cleanup`: short
/// timeouts and no retries.
std::vector<DistributedQueryTaskStatusEntry> getTaskStatuses(const String & endpoint_uri, const Strings & task_ids, UInt32 wait_for_ms,
    const ContextPtr & context, bool for_cleanup = false);

/// Cancels every task of the query on this worker; the worker then ignores the query's later batches.
void cancelQueryTasks(const String & endpoint_uri, const String & unique_query_id, const ContextPtr & context);

void forgetTasks(const String & endpoint_uri, const Strings & task_ids, const ContextPtr & context);

}
