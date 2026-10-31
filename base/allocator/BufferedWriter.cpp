#include <allocator/BufferedWriter.h>

#include <cstring>

namespace jemalloc
{

/// jemalloc: buf_writer_assert
void BufferedWriter::checkInvariants() const
{
    ALLOCATOR_ASSERT(write_callback != nullptr);
    if (buf != nullptr)
    {
        ALLOCATOR_ASSERT(buf_size > 0);
    }
    else
    {
        ALLOCATOR_ASSERT(buf_size == 0);
        ALLOCATOR_ASSERT(internal_buf);
    }
    ALLOCATOR_ASSERT(buffer_end <= buf_size);
}

/// jemalloc: buf_writer_init
bool BufferedWriter::init(
    ThreadState * thread_state,
    WriteCallback * write_callback_,
    void * callback_argument_,
    char * buf_,
    size_t buf_len,
    const BufferAllocator * allocator_)
{
    write_callback = write_callback_ != nullptr ? write_callback_ : messageCallback();
    callback_argument = callback_argument_;
    allocator = allocator_;
    ALLOCATOR_ASSERT(buf_len >= 2);
    if (buf_ != nullptr)
    {
        buf = buf_;
        internal_buf = false;
    }
    else
    {
        /// jemalloc: buf_writer_allocate_internal_buf
        buf = allocator != nullptr ? static_cast<char *>(allocator->allocate(thread_state, buf_len)) : nullptr;
        internal_buf = true;
    }
    if (buf != nullptr)
        buf_size = buf_len - 1; /// Allowing for '\0'.
    else
        buf_size = 0;
    buffer_end = 0;
    checkInvariants();
    return buf == nullptr;
}

/// jemalloc: buf_writer_flush
void BufferedWriter::flush()
{
    checkInvariants();
    if (buf == nullptr)
        return;
    buf[buffer_end] = '\0';
    write_callback(callback_argument, buf);
    buffer_end = 0;
    checkInvariants();
}

/// jemalloc: buf_writer_cb
void BufferedWriter::write(const char * s)
{
    checkInvariants();
    if (buf == nullptr)
    {
        write_callback(callback_argument, s);
        return;
    }
    size_t i = 0;
    size_t segment_length = std::strlen(s);
    size_t n;
    for (; i < segment_length; i += n)
    {
        /// Flush only when the buffer is exactly full and more data arrives.
        if (buffer_end == buf_size)
            flush();
        size_t s_remain = segment_length - i;
        size_t buf_remain = buf_size - buffer_end;
        n = s_remain < buf_remain ? s_remain : buf_remain;
        std::memcpy(buf + buffer_end, s + i, n);
        buffer_end += n;
        checkInvariants();
    }
    ALLOCATOR_ASSERT(i == segment_length);
}

/// jemalloc: buf_writer_cb
void BufferedWriter::callback(void * buf_writer, const char * s)
{
    static_cast<BufferedWriter *>(buf_writer)->write(s);
}

/// jemalloc: buf_writer_terminate
void BufferedWriter::terminate(ThreadState * thread_state)
{
    checkInvariants();
    flush();
    if (internal_buf)
    {
        /// jemalloc: buf_writer_free_internal_buf
        if (buf != nullptr)
            allocator->deallocate(thread_state, buf);
    }
}

/// jemalloc: buf_writer_pipe
void BufferedWriter::pipe(ReadCallback * read_callback, void * read_callback_argument)
{
    /// A tiny local buffer in case the buffered writer failed to allocate at init.
    static constinit char backup_buf[16]{};
    static constinit BufferedWriter backup_buf_writer{};

    BufferedWriter * writer = this;
    checkInvariants();
    ALLOCATOR_ASSERT(read_callback != nullptr);
    if (writer->buf == nullptr)
    {
        backup_buf_writer.init(nullptr, write_callback, callback_argument, backup_buf, sizeof(backup_buf));
        writer = &backup_buf_writer;
    }
    ALLOCATOR_ASSERT(writer->buf != nullptr);
    ssize_t num_read = 0;
    do
    {
        writer->buffer_end += size_t(num_read);
        writer->checkInvariants();
        if (writer->buffer_end == writer->buf_size)
            writer->flush();
        num_read = read_callback(read_callback_argument, writer->buf + writer->buffer_end, writer->buf_size - writer->buffer_end);
    } while (num_read > 0);
    writer->flush();
}

}
