#pragma clang diagnostic ignored "-Wreserved-identifier"

#include "ArrowBufferedStreams.h"
#if USE_ARROW || USE_ORC || USE_PARQUET
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <arrow/buffer.h>
#include <arrow/util/future.h>
#include <arrow/io/memory.h>
#include <arrow/result.h>
#include <arrow/memory_pool_internal.h>
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
    try
    {
        out.write(reinterpret_cast<const char *>(data), length);
        total_length += length;
        return arrow::Status::OK();
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_ERROR(getLogger("ArrowBufferedOutputStream"), "Error while writing to arrow stream: {}", message);
        return arrow::Status::IOError(message);
    }
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
    try
    {
        if (avoid_buffering)
            in.setReadUntilPosition(seekable_in.getPosition() + nbytes);
        return in.readBig(reinterpret_cast<char *>(out), nbytes);
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_ERROR(getLogger("ArrowBufferedOutputStream"), "Error while reading from arrow stream: {}", message);
        return arrow::Status::IOError(message);
    }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, ArrowMemoryPool::instance()))
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
    try
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
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_ERROR(getLogger("ArrowBufferedOutputStream"), "Error while seeking arrow file: {}", message);
        return arrow::Status::IOError(message);
    }
}


ArrowInputStreamFromReadBuffer::ArrowInputStreamFromReadBuffer(ReadBuffer & in_) : in(in_), is_open{true}
{
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes, void * out)
{
    try
    {
        return in.readBig(reinterpret_cast<char *>(out), nbytes);
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_ERROR(getLogger("ArrowBufferedOutputStream"), "Error while reading from arrow stream: {}", message);
        return arrow::Status::IOError(message);
    }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, ArrowMemoryPool::instance()))
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

RandomAccessFileFromRandomAccessReadBuffer::RandomAccessFileFromRandomAccessReadBuffer(SeekableReadBuffer & in_, size_t file_size_, std::shared_ptr<ThreadPool> io_pool_) : in(in_), file_size(file_size_), io_pool(std::move(io_pool_))
{
    if (io_pool)
        async_runner = threadPoolCallbackRunnerUnsafe<void>(*io_pool, "ArrowFile");
}

arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::GetSize()
{
    return file_size;
}

arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    try
    {
        int64_t r = in.readBigAt(reinterpret_cast<char *>(out), nbytes, position, nullptr);
        return r;
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_ERROR(getLogger("ArrowBufferedOutputStream"), "Error while reading from arrow stream: {}", message);
        return arrow::Status::IOError(message);
    }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromRandomAccessReadBuffer::ReadAt(int64_t position, int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, ArrowMemoryPool::instance()))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(position, nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Future<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromRandomAccessReadBuffer::ReadAsync(const arrow::io::IOContext&, int64_t position, int64_t nbytes)
{
    auto future = arrow::Future<std::shared_ptr<arrow::Buffer>>::Make();
    if (io_pool)
    {
        async_runner([this, future, position, nbytes]() mutable { asyncThreadFunction(future, position, nbytes); }, {});
        return future.Then([=]() { return future.result(); });
    }
    asyncThreadFunction(future, position, nbytes);
    return future;
}

arrow::Status RandomAccessFileFromRandomAccessReadBuffer::Close()
{
    chassert(is_open);
    is_open = false;
    return arrow::Status::OK();
}

void RandomAccessFileFromRandomAccessReadBuffer::asyncThreadFunction(
    arrow::Future<std::shared_ptr<arrow::Buffer>> future, int64_t position, int64_t nbytes)
{
    auto buffer = ReadAt(position, nbytes);
    future.MarkFinished(buffer);
}

arrow::Status RandomAccessFileFromRandomAccessReadBuffer::Seek(int64_t) { return arrow::Status::NotImplemented(""); }
arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::Tell() const { return arrow::Status::NotImplemented(""); }
arrow::Result<int64_t> RandomAccessFileFromRandomAccessReadBuffer::Read(int64_t, void*) { return arrow::Status::NotImplemented(""); }
arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromRandomAccessReadBuffer::Read(int64_t) { return arrow::Status::NotImplemented(""); }

ArrowMemoryPool * ArrowMemoryPool::instance()
{
    static ArrowMemoryPool x;
    return &x;
}

arrow::Status ArrowMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t ** out)
{
    if (size == 0)
    {
        *out = arrow::memory_pool::internal::kZeroSizeArea;
        return arrow::Status::OK();
    }

    try // is arrow exception-safe? idk, let's avoid throwing, just in case
    {
        void * p = Allocator<false>().alloc(size_t(size), size_t(alignment));
        *out = reinterpret_cast<uint8_t*>(p);
    }
    catch (...)
    {
        return arrow::Status::OutOfMemory("allocation of size ", size, " failed");
    }

    return arrow::Status::OK();
}

arrow::Status ArrowMemoryPool::Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t ** ptr)
{
    if (old_size == 0)
    {
        chassert(*ptr == arrow::memory_pool::internal::kZeroSizeArea);
        return Allocate(new_size, alignment, ptr);
    }
    if (new_size == 0)
    {
        Free(*ptr, old_size, alignment);
        *ptr = arrow::memory_pool::internal::kZeroSizeArea;
        return arrow::Status::OK();
    }

    try
    {
        void * p = Allocator<false>().realloc(*ptr, size_t(old_size), size_t(new_size), size_t(alignment));
        *ptr = reinterpret_cast<uint8_t*>(p);
    }
    catch (...)
    {
        return arrow::Status::OutOfMemory("reallocation of size ", new_size, " failed");
    }

    return arrow::Status::OK();
}

void ArrowMemoryPool::Free(uint8_t * buffer, int64_t size, int64_t /*alignment*/)
{
    if (size == 0)
    {
        chassert(buffer == arrow::memory_pool::internal::kZeroSizeArea);
        return;
    }

    Allocator<false>().free(buffer, size_t(size));
}


std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(
    ReadBuffer & in,
    const FormatSettings & settings,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes,
    bool avoid_buffering,
    std::shared_ptr<ThreadPool> io_pool)
{
    bool has_file_size = isBufferWithFileSize(in);
    auto * seekable_in = dynamic_cast<SeekableReadBuffer *>(&in);

    if (has_file_size && seekable_in && settings.seekable_read)
    {
        if (avoid_buffering && seekable_in->supportsReadAt())
            return std::make_shared<RandomAccessFileFromRandomAccessReadBuffer>(*seekable_in, getFileSizeFromReadBuffer(in), io_pool);

        if (seekable_in->checkIfActuallySeekable())
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*seekable_in, std::nullopt, avoid_buffering);
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
    std::string file_data(magic_bytes.size(), '\0');

    /// Avoid loading the whole file if it doesn't seem to even be in the correct format.
    size_t bytes_read = in.read(file_data.data(), magic_bytes.size());
    if (bytes_read < magic_bytes.size() || file_data != magic_bytes)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a {} file", format_name);

    WriteBufferFromString file_buffer(file_data, AppendModeTag{});
    copyData(in, file_buffer, is_cancelled);
    file_buffer.finalize();

    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

}

#endif
