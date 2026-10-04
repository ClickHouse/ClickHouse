#include <allocator/BufferedWriter.h>

#include <cstring>

namespace jemalloc
{

/// jemalloc: buf_writer_assert
void BufferedWriter::checkInvariants() const
{
    JE_ASSERT(write_cb != nullptr);
    if (buf != nullptr)
    {
        JE_ASSERT(buf_size > 0);
    }
    else
    {
        JE_ASSERT(buf_size == 0);
        JE_ASSERT(internal_buf);
    }
    JE_ASSERT(buf_end <= buf_size);
}

/// jemalloc: buf_writer_init
bool BufferedWriter::init(
    ThreadState * tsdn, WriteCallback * write_cb_, void * cbopaque_, char * buf_, size_t buf_len, const BufferAllocator * allocator_)
{
    write_cb = write_cb_ != nullptr ? write_cb_ : messageCallback();
    cbopaque = cbopaque_;
    allocator = allocator_;
    JE_ASSERT(buf_len >= 2);
    if (buf_ != nullptr)
    {
        buf = buf_;
        internal_buf = false;
    }
    else
    {
        /// jemalloc: buf_writer_allocate_internal_buf
        buf = allocator != nullptr ? static_cast<char *>(allocator->allocate(tsdn, buf_len)) : nullptr;
        internal_buf = true;
    }
    if (buf != nullptr)
        buf_size = buf_len - 1; /// Allowing for '\0'.
    else
        buf_size = 0;
    buf_end = 0;
    checkInvariants();
    return buf == nullptr;
}

/// jemalloc: buf_writer_flush
void BufferedWriter::flush()
{
    checkInvariants();
    if (buf == nullptr)
        return;
    buf[buf_end] = '\0';
    write_cb(cbopaque, buf);
    buf_end = 0;
    checkInvariants();
}

/// jemalloc: buf_writer_cb
void BufferedWriter::write(const char * s)
{
    checkInvariants();
    if (buf == nullptr)
    {
        write_cb(cbopaque, s);
        return;
    }
    size_t i = 0;
    size_t slen = std::strlen(s);
    size_t n;
    for (; i < slen; i += n)
    {
        /// Flush only when the buffer is exactly full and more data arrives.
        if (buf_end == buf_size)
            flush();
        size_t s_remain = slen - i;
        size_t buf_remain = buf_size - buf_end;
        n = s_remain < buf_remain ? s_remain : buf_remain;
        std::memcpy(buf + buf_end, s + i, n);
        buf_end += n;
        checkInvariants();
    }
    JE_ASSERT(i == slen);
}

/// jemalloc: buf_writer_cb
void BufferedWriter::callback(void * buf_writer, const char * s)
{
    static_cast<BufferedWriter *>(buf_writer)->write(s);
}

/// jemalloc: buf_writer_terminate
void BufferedWriter::terminate(ThreadState * tsdn)
{
    checkInvariants();
    flush();
    if (internal_buf)
    {
        /// jemalloc: buf_writer_free_internal_buf
        if (buf != nullptr)
            allocator->deallocate(tsdn, buf);
    }
}

/// jemalloc: buf_writer_pipe
void BufferedWriter::pipe(ReadCallback * read_cb, void * read_cbopaque)
{
    /// A tiny local buffer in case the buffered writer failed to allocate at init.
    static constinit char backup_buf[16]{};
    static constinit BufferedWriter backup_buf_writer{};

    BufferedWriter * writer = this;
    checkInvariants();
    JE_ASSERT(read_cb != nullptr);
    if (writer->buf == nullptr)
    {
        backup_buf_writer.init(nullptr, write_cb, cbopaque, backup_buf, sizeof(backup_buf));
        writer = &backup_buf_writer;
    }
    JE_ASSERT(writer->buf != nullptr);
    ssize_t nread = 0;
    do
    {
        writer->buf_end += size_t(nread);
        writer->checkInvariants();
        if (writer->buf_end == writer->buf_size)
            writer->flush();
        nread = read_cb(read_cbopaque, writer->buf + writer->buf_end, writer->buf_size - writer->buf_end);
    } while (nread > 0);
    writer->flush();
}

}
