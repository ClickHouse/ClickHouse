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
/// Rejects a batch of an unsupported version or with more stages or tasks than a real one has.
void deserializeTaskBatch(DistributedQueryTaskBatch & batch, ReadBuffer & in);

}
