#pragma clang diagnostic ignored "-Wreserved-identifier"

#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET
#include <Common/assert_cast.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <IO/PeekableReadBuffer.h>
#include <arrow/buffer.h>
#include <arrow/util/future.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>
#include <Core/Settings.h>

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

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(ReadBuffer & in_, std::optional<off_t> file_size_, bool avoid_buffering_)
    : in{in_}, seekable_in{dynamic_cast<SeekableReadBuffer &>(in_)}, file_size{file_size_}, is_open{true}, avoid_buffering(avoid_buffering_)
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
    if (avoid_buffering)
        in.setReadUntilPosition(seekable_in.getPosition() + nbytes);
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

arrow::Future<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromSeekableReadBuffer::ReadAsync(const arrow::io::IOContext &, int64_t position, int64_t nbytes)
{
    /// Just a stub to to avoid using internal arrow thread pool
    return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(ReadAt(position, nbytes));
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Seek(int64_t position)
{
    if (avoid_buffering)
    {
        // Seeking to a position above a previous setReadUntilPosition() confuses some of the
        // ReadBuffer implementations.
        in.setReadUntilEnd();
    }
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

RandomAccessFileFromManyReadBuffers::RandomAccessFileFromManyReadBuffers(SeekableReadBufferFactory & factory) : buf_factory(factory) {}

arrow::Result<int64_t> RandomAccessFileFromManyReadBuffers::GetSize()
{
    return buf_factory.getFileSize();
}

arrow::Result<int64_t> RandomAccessFileFromManyReadBuffers::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    std::unique_lock lock(mutex);
    if (free_bufs.empty())
        free_bufs.push_back(buf_factory.getReader());
    auto buf = std::move(free_bufs.back());
    free_bufs.pop_back();
    lock.unlock();

    // To work well with this, ReadBuffer implementations need to respect setReadUntilPosition() and
    // not read above it. We often do very small reads here.
    // Also nice if they:
    //  * Make readBig() read directly into the provided memory, instead of copying from internal
    //    buffer.
    //  * Allocate the internal buffer (if any) lazily in first nextImpl() call. If all reads are
    //    tiny readBig() calls (as is typical here), it won't allocate an unnecessary 1 MB buffer.

    buf->seek(position, SEEK_SET);
    buf->setReadUntilPosition(position + nbytes);
    size_t bytes_read = buf->readBig(reinterpret_cast<char *>(out), nbytes);

    // Seeking to a position above a previous setReadUntilPosition() confuses some of the
    // ReadBuffer implementations. So we reset it before next seek.
    buf->setReadUntilEnd();

    lock.lock();
    free_bufs.push_back(std::move(buf));

    return static_cast<int64_t>(bytes_read);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromManyReadBuffers::ReadAt(int64_t position, int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(position, nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Future<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromManyReadBuffers::ReadAsync(const arrow::io::IOContext&, int64_t position, int64_t nbytes)
{
    return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(ReadAt(position, nbytes));
}

arrow::Status RandomAccessFileFromManyReadBuffers::Close()
{
    chassert(is_open);
    is_open = false;
    return arrow::Status::OK();
}

arrow::Status RandomAccessFileFromManyReadBuffers::Seek(int64_t) { return arrow::Status::NotImplemented(""); }
arrow::Result<int64_t> RandomAccessFileFromManyReadBuffers::Tell() const { return arrow::Status::NotImplemented(""); }
arrow::Result<int64_t> RandomAccessFileFromManyReadBuffers::Read(int64_t, void*) { return arrow::Status::NotImplemented(""); }
arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromManyReadBuffers::Read(int64_t) { return arrow::Status::NotImplemented(""); }

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(
    ReadBuffer & in,
    const FormatSettings & settings,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes,
    bool avoid_buffering)
{
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
            struct stat stat;
            auto res = ::fstat(fd_in->getFD(), &stat);
            // if fd is a regular file i.e. not stdin
            if (res == 0 && S_ISREG(stat.st_mode))
                return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size, avoid_buffering);
    }
    else if (auto * seekable_in = dynamic_cast<SeekableReadBuffer *>(&in);
             seekable_in && settings.seekable_read && isBufferWithFileSize(in) &&
             seekable_in->checkIfActuallySeekable())
    {
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(in, std::nullopt, avoid_buffering);
    }

    // fallback to loading the entire file in memory
    return asArrowFileLoadIntoMemory(in, is_cancelled, format_name, magic_bytes);
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFileLoadIntoMemory(
    ReadBuffer & in,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes)
{
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
