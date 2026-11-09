#pragma once

namespace DB
{

struct DistributedQueryTaskDescription;
class WriteBuffer;
class ReadBuffer;

void serializeTask(const DistributedQueryTaskDescription & task_description, WriteBuffer & out);
void deserializeTask(DistributedQueryTaskDescription & task_description, ReadBuffer & in);

}
