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

/// `for_cleanup` uses short HTTP timeouts and no retries so best-effort cleanup cannot block for the
/// normal multi-minute request budget when a worker is slow or unreachable.
DistributedQueryTaskStatus getTaskStatus(const String & endpoint_uri, const String & task_id, UInt32 wait_for_ms, const ContextPtr & context, bool for_cleanup = false);

void cancelTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context);

void forgetTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context);

}
