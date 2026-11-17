#pragma once

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

void serializeTaskBatch(const DistributedQueryTaskBatch & batch, WriteBuffer & out);
/// Rejects a batch that a correct initiator never sends: no stages, a stage without tasks, a repeated stage or task id.
void deserializeTaskBatch(DistributedQueryTaskBatch & batch, ReadBuffer & in);

}
