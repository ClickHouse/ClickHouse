#pragma once

namespace DB
{

struct DistributedQueryTaskDescription;
struct DistributedQueryTaskBatch;
class WriteBuffer;
class ReadBuffer;

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out);
void deserializeTask(DistributedQueryTaskDescription & task_description, ReadBuffer & in);

void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out);
/// Rejects a batch that a correct initiator never sends: no stages, a stage without tasks, a repeated stage or task id.
void deserializeTaskBatch(DistributedQueryTaskBatch & batch, ReadBuffer & in);

}
