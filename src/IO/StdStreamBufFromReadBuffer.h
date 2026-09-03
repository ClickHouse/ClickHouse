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
