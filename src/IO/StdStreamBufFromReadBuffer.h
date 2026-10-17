#pragma once

#include <memory>
#include <streambuf>


namespace DB
{
class ReadBuffer;
class SeekableReadBuffer;

/// `std::streambuf`-compatible wrapper around a ReadBuffer.
class StdStreamBufFromReadBuffer : public std::streambuf
{
public:
    using Base = std::streambuf;

    explicit StdStreamBufFromReadBuffer(std::unique_ptr<ReadBuffer> read_buffer_, size_t size_);
    explicit StdStreamBufFromReadBuffer(ReadBuffer & read_buffer_, size_t size_);
    ~StdStreamBufFromReadBuffer() override;

    /// The buffer this stream reads from. Lets code that has an `std::istream` at hand only
    /// because an interface demands one - the AWS SDK, for example - read from the buffer
    /// directly instead of copying the data through the stream.
    ReadBuffer & getReadBuffer() const { return *read_buffer; }
    std::unique_ptr<ReadBuffer> extractReadBuffer() { return std::move(read_buffer); }

private:
    int underflow() override;
    std::streamsize showmanyc() override;
    std::streamsize xsgetn(char* s, std::streamsize count) override;
    std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir, std::ios_base::openmode which) override;
    std::streampos seekpos(std::streampos pos, std::ios_base::openmode which) override;

    std::streamsize xsputn(const char* s, std::streamsize n) override;
    int overflow(int c) override;

    std::streampos getCurrentPosition() const;

    std::unique_ptr<ReadBuffer> read_buffer;
    SeekableReadBuffer * seekable_read_buffer = nullptr;
    size_t size;
};

}
