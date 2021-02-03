#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/result.h>

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

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_)
    : in{in_}, file_size{file_size_}, is_open{true}
{
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::GetSize()
{
    return arrow::Result<int64_t>(file_size);
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Tell() const
{
    return arrow::Result<int64_t>(in.getPosition());
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, void * out)
{
    int64_t bytes_read = in.readBig(reinterpret_cast<char *>(out), nbytes);
    return arrow::Result<int64_t>(bytes_read);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes)
{
    auto buffer_status = arrow::AllocateBuffer(nbytes);
    ARROW_RETURN_NOT_OK(buffer_status);

    auto shared_buffer = std::shared_ptr<arrow::Buffer>(std::move(std::move(*buffer_status)));

    size_t n = in.readBig(reinterpret_cast<char *>(shared_buffer->mutable_data()), nbytes);

    auto read_buffer = arrow::SliceBuffer(shared_buffer, 0, n);
    return arrow::Result<std::shared_ptr<arrow::Buffer>>(shared_buffer);
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
