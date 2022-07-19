#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET
#include <Common/assert_cast.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <IO/PeekableReadBuffer.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>

#include <sys/stat.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & out_) : out{out_}, is_open{true}
{
}

arrow::Status ArrowBufferedOutputStream::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowBufferedOutputStream::Tell() const
{
    return arrow::Result<int64_t>(total_length);
}

arrow::Status ArrowBufferedOutputStream::Write(const void * data, int64_t length)
{
    out.write(reinterpret_cast<const char *>(data), length);
    total_length += length;
    return arrow::Status::OK();
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(ReadBuffer & in_, off_t file_size_)
    : in{in_}, seekable_in{dynamic_cast<SeekableReadBuffer &>(in_)}, file_size{file_size_}, is_open{true}
{
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(ReadBuffer & in_)
    : in{in_}, seekable_in{dynamic_cast<SeekableReadBuffer &>(in_)}, is_open{true}
{
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::GetSize()
{
    if (!file_size)
    {
        if (isBufferWithFileSize(in))
            file_size = getFileSizeFromReadBuffer(in);
    }
    return arrow::Result<int64_t>(*file_size);
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Tell() const
{
    return seekable_in.getPosition();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Seek(int64_t position)
{
    seekable_in.seek(position, SEEK_SET);
    return arrow::Status::OK();
}


ArrowInputStreamFromReadBuffer::ArrowInputStreamFromReadBuffer(ReadBuffer & in_) : in(in_), is_open{true}
{
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Status ArrowInputStreamFromReadBuffer::Abort()
{
    return arrow::Status();
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Tell() const
{
    return in.count();
}

arrow::Status ArrowInputStreamFromReadBuffer::Close()
{
    is_open = false;
    return arrow::Status();
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(
    ReadBuffer & in,
    const FormatSettings & settings,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes)
{
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size);
    }
    else if (dynamic_cast<SeekableReadBuffer *>(&in) && isBufferWithFileSize(in))
    {
        if (settings.seekable_read)
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(in);
    }

    // fallback to loading the entire file in memory
    std::string file_data;
    {
        PeekableReadBuffer buf(in);
        std::string magic_bytes_from_data;
        magic_bytes_from_data.resize(magic_bytes.size());
        bool read_magic_bytes = false;
        try
        {
            PeekableReadBufferCheckpoint checkpoint(buf, true);
            buf.readStrict(magic_bytes_from_data.data(), magic_bytes_from_data.size());
            read_magic_bytes = true;
        }
        catch (const Exception &) {}

        if (!read_magic_bytes || magic_bytes_from_data != magic_bytes)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Not a {} file", format_name);

        WriteBufferFromString file_buffer(file_data);
        copyData(buf, file_buffer, is_cancelled);
    }

    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

}

#endif
