#pragma once

#include <Processors/Chunk.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct DistributedQueryPlan;

void executeDistributedQuery(const DistributedQueryPlan & distributed_query_plan, ContextPtr context);

struct DistributedQueryTask;

/// Executes a task locally
void doExecuteTask(const String & serialized_query_plan, const DistributedQueryTask & task, ObjectStoragePtr object_storage, const String & object_storage_path, ContextPtr context);

}
