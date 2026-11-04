#pragma once

#include <Interpreters/Context_fwd.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <base/types.h>

namespace DB
{

struct DistributedQueryTaskDescription;
struct DistributedQueryTask;
struct ExchangeStreamSources;

/// `collectors` names what the worker should collect for the task (the `collect` URL parameter of
/// the `start` request); the worker appends only their payloads to status replies.
String sendTask(const String & endpoint_uri, const String & unique_task_id, const DistributedQueryTaskDescription & task_description, const String & unique_temp_file_path, const TaskCollectors & collectors, const ContextPtr & context);

/// Registers a task on the worker without its description: the worker replies at once and the task waits for `sendTaskPayload`.
void createTask(const String & endpoint_uri, const String & unique_task_id, const DistributedQueryTask & task, const String & initial_query_id,
    const TaskCollectors & collectors, const ContextPtr & context);

/// Sends the description of a task registered with `createTask`; the worker then schedules it. `stage_description` holds the
/// fields shared by the tasks of a stage, `task` and `stream_sources` replace its own.
void sendTaskPayload(const String & endpoint_uri, const String & unique_task_id, const DistributedQueryTaskDescription & stage_description,
    const DistributedQueryTask & task, const ExchangeStreamSources & stream_sources, const String & unique_temp_file_path, const ContextPtr & context);

/// `for_cleanup` uses short HTTP timeouts and no retries so best-effort cleanup cannot block for the
/// normal multi-minute request budget when a worker is slow or unreachable.
DistributedQueryTaskStatus getTaskStatus(const String & endpoint_uri, const String & task_id, UInt32 wait_for_ms, const ContextPtr & context, bool for_cleanup = false);

void cancelTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context);

void forgetTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context);

}
