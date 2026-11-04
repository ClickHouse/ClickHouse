#pragma once

namespace DB
{

struct DistributedQueryTaskDescription;
struct DistributedQueryTask;
struct ExchangeStreamSources;
class WriteBuffer;
class ReadBuffer;

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out);
/// Writes `task_description` with its `task` and `exchange_stream_sources` replaced by `task` and `stream_sources`.
void serializeTask(const DistributedQueryTaskDescription & task_description, const DistributedQueryTask & task,
    const ExchangeStreamSources & stream_sources, WriteBuffer & out);
void deserializeTask(DistributedQueryTaskDescription & task_description, ReadBuffer & in);

}
