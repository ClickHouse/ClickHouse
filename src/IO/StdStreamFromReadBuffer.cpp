#include <IO/StdStreamFromReadBuffer.h>

#include <IO/ReadBuffer.h>


namespace DB
{

StdIStreamFromReadBuffer::StdIStreamFromReadBuffer(std::unique_ptr<ReadBuffer> buf, size_t size)
    : Base(&stream_buf)
    , stream_buf(std::move(buf), size)
{
    /// ios::failbit isn't specified here because otherwise stream.read() will throw an exception on EOF.
    exceptions(std::ios::badbit);
}

StdIStreamFromReadBuffer::StdIStreamFromReadBuffer(ReadBuffer & buf, size_t size)
    : Base(&stream_buf)
    , stream_buf(buf, size)
{
    exceptions(std::ios::badbit);
}


StdStreamFromReadBuffer::StdStreamFromReadBuffer(std::unique_ptr<ReadBuffer> buf, size_t size)
    : Base(&stream_buf)
    , stream_buf(std::move(buf), size)
{
    /// ios::failbit isn't specified here because otherwise stream.read() will throw an exception on EOF.
    exceptions(std::ios::badbit);
}

StdStreamFromReadBuffer::StdStreamFromReadBuffer(ReadBuffer & buf, size_t size)
    : Base(&stream_buf)
    , stream_buf(buf, size)
{
    exceptions(std::ios::badbit);
}

}
