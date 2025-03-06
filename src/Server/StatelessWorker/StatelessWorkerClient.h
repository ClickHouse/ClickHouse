#pragma once

#include <Interpreters/Context_fwd.h>
#include <base/types.h>

namespace DB
{

struct DistributedQueryTask;

String sendTask(const String & endpoint_uri, const String & unique_task_id, const String & serialized_query_plan, const DistributedQueryTask & task, const String & unique_temp_file_path, const ContextPtr & context);

String getTaskStatus(const String & endpoint_uri, const String & task_id, UInt32 wait_for_ms, const ContextPtr & context);

void cancelTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context);

void forgetTask(const String & endpoint_uri, const String & task_id, const ContextPtr & context);

}
