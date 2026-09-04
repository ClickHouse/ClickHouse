#pragma once
#include <IO/Progress.h>
#include <base/types.h>
#include <Core/Block.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

struct DistributedQueryTaskStatus
{
    String status;
    String error_message;
    Progress progress;

    /// Log lines collected on the worker since the previous status poll, in the
    /// InternalTextLogsQueue block format. Empty when there is nothing to send or the
    /// negotiated version predates DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS.
    Block logs;

    void write(WriteBuffer & out, UInt64 version) const;
    void read(ReadBuffer & in, UInt64 version);
};

}
