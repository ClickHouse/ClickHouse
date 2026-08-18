#pragma once

#include <IO/StdStreamBufFromReadBuffer.h>
#include <memory>


namespace DB
{
class ReadBuffer;

/// `std::istream`-compatible wrapper around a ReadBuffer.
class StdIStreamFromReadBuffer : public std::istream
{
public:
    using Base = std::istream;
    StdIStreamFromReadBuffer(std::unique_ptr<ReadBuffer> buf, size_t size) : Base(&stream_buf), stream_buf(std::move(buf), size) { }
    StdIStreamFromReadBuffer(ReadBuffer & buf, size_t size) : Base(&stream_buf), stream_buf(buf, size) { }
    StdStreamBufFromReadBuffer * rdbuf() const { return const_cast<StdStreamBufFromReadBuffer *>(&stream_buf); }

private:
    StdStreamBufFromReadBuffer stream_buf;
};


/// `std::iostream`-compatible wrapper around a ReadBuffer.
class StdStreamFromReadBuffer : public std::iostream
{
public:
    using Base = std::iostream;
    StdStreamFromReadBuffer(std::unique_ptr<ReadBuffer> buf, size_t size) : Base(&stream_buf), stream_buf(std::move(buf), size) { }
    StdStreamFromReadBuffer(ReadBuffer & buf, size_t size) : Base(&stream_buf), stream_buf(buf, size) { }
    StdStreamBufFromReadBuffer * rdbuf() const { return const_cast<StdStreamBufFromReadBuffer *>(&stream_buf); }

private:
    StdStreamBufFromReadBuffer stream_buf;
};

}
