#pragma once
#include <IO/Progress.h>
#include <base/types.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

struct DistributedQueryTaskStatus
{
    String status;
    String error_message;
    Progress progress;
    /// Error code of a failed task, 0 otherwise. Sent since task serialization version 3.
    Int32 error_code = 0;

    void write(WriteBuffer & out, UInt64 version) const;
    void read(ReadBuffer & in, UInt64 version);
};

}
