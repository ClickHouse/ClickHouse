#pragma once

#include <base/types.h>

#include <functional>
#include <memory>

namespace DB
{

struct DistributedQueryTaskBatch;
class WriteBuffer;
class ReadBuffer;

using TaskBatchPlanWriter = std::function<void(const std::shared_ptr<const String> & serialized_query_plan)>;

void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out);
/// Writes the same bytes, except that each stage plan goes to `write_plan` right after its length, so a caller can send
/// the plans without copying them.
void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out, const TaskBatchPlanWriter & write_plan);
/// Rejects a batch that a correct initiator never sends: no stages, a stage without tasks, a repeated stage or task id.
void deserializeTaskBatch(DistributedQueryTaskBatch & batch, ReadBuffer & in);

}
