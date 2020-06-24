#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/status.h>

#include <sys/stat.h>

namespace DB
{

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & out_) : out{out_}, is_open{true}
{
}

arrow::Status ArrowBufferedOutputStream::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Status ArrowBufferedOutputStream::Tell(int64_t * position) const
{
    *position = total_length;
    return arrow::Status::OK();
}

arrow::Status ArrowBufferedOutputStream::Write(const void * data, int64_t length)
{
    out.write(reinterpret_cast<const char *>(data), length);
    total_length += length;
    return arrow::Status::OK();
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_)
    : in{in_}, file_size{file_size_}, is_open{true}
{
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::GetSize(int64_t * size)
{
    *size = file_size;
    return arrow::Status::OK();
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Tell(int64_t * position) const
{
    *position = in.getPosition();
    return arrow::Status::OK();
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, int64_t * bytes_read, void * out)
{
    *bytes_read = in.readBig(reinterpret_cast<char *>(out), nbytes);
    return arrow::Status::OK();
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, std::shared_ptr<arrow::Buffer> * out)
{
    std::shared_ptr<arrow::Buffer> buf;
    ARROW_RETURN_NOT_OK(arrow::AllocateBuffer(nbytes, &buf));
    size_t n = in.readBig(reinterpret_cast<char *>(buf->mutable_data()), nbytes);
    *out = arrow::SliceBuffer(buf, 0, n);
    return arrow::Status::OK();
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Seek(int64_t position)
{
    in.seek(position, SEEK_SET);
    return arrow::Status::OK();
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(ReadBuffer & in)
{
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size);
    }

    // fallback to loading the entire file in memory
    std::string file_data;
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(in, file_buffer);
    }

    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

}

#endif
