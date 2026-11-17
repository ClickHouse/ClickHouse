#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

void DistributedQueryTaskStatus::write(WriteBuffer & out, UInt64 version) const
{
    writeStringBinary(status, out);
    writeStringBinary(error_message, out);
    progress.write(out, version);
}

void DistributedQueryTaskStatus::read(ReadBuffer & in, UInt64 version)
{
    readStringBinary(status, in);
    readStringBinary(error_message, in);
    progress.read(in, version);
}

}
