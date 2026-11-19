#pragma once

#include <base/types.h>

#include <functional>
#include <memory>

namespace DB
{

struct DistributedQueryTaskDescription;
struct DistributedQueryTaskBatch;
struct DistributedQueryTask;
struct ExchangeStreamSources;
class WriteBuffer;
class ReadBuffer;

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out);
/// Writes `task_description` with its `task` and `exchange_stream_sources` replaced by `task` and `stream_sources`.
void serializeTask(const DistributedQueryTaskDescription & task_description, const DistributedQueryTask & task,
    const ExchangeStreamSources & stream_sources, WriteBuffer & out);
void deserializeTask(DistributedQueryTaskDescription & task_description, ReadBuffer & in);

using TaskBatchPlanWriter = std::function<void(const std::shared_ptr<const String> & serialized_query_plan)>;

void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out);
/// Writes the same bytes, except that each stage plan goes to `write_plan` right after its length, so a caller can send
/// the plans without copying them.
void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out, const TaskBatchPlanWriter & write_plan);
/// Rejects a batch that a correct initiator never sends: no stages, a stage without tasks, a repeated stage or task id.
void deserializeTaskBatch(DistributedQueryTaskBatch & batch, ReadBuffer & in);

}
