#include <Server/DistributedQuery/StreamingExchangeProtocol.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace StreamingExchangeProtocol
{

UInt64 SourceHelloBody::readVersion(ReadBuffer & in)
{
    UInt64 version = 0;
    readIntBinary(version, in);
    return version;
}

void SourceHelloBody::readAfterVersion(ReadBuffer & in)
{
    readStringBinary(query_id, in);
    readStringBinary(stream_name, in);
}

void SourceHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(source_version, out);
    writeStringBinary(query_id, out);
    writeStringBinary(stream_name, out);
}

void SinkHelloBody::read(ReadBuffer & in)
{
    readIntBinary(sink_version, in);
}

void SinkHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(sink_version, out);
}

}
}
