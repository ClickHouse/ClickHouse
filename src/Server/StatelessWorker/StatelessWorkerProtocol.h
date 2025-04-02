#pragma once
#include <IO/Progress.h>
#include "base/types.h"

namespace DB
{

class WriteBuffer;
class ReadBuffer;

struct DistributedQueryTaskStatus
{
    String status;
    String error_message;
    Progress progress;

    void write(WriteBuffer & out, UInt64 version) const;
    void read(ReadBuffer & in, UInt64 version);
};

}
