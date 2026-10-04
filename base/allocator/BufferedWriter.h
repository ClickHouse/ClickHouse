#pragma once

/// A buffered writer on top of a `WriteCallback`, used by stats printing and profile dumps.
/// jemalloc: `buf_writer.h`, `src/buf_writer.c`.
///
/// Note: `cbopaque` is passed to the callback only when the buffer is flushed.

#include <allocator/Format.h>

namespace jemalloc
{

class ThreadState;

/// How `BufferedWriter` allocates its buffer when the caller does not supply one. In jemalloc this is an internal
/// allocation in arena 0: `iallocztm(tsdn, len, sz_size2index(len), false, NULL, true, arena_get(tsdn, 0, false), true)`
/// and `idalloctm(tsdn, buf, NULL, NULL, true, true)`; the users of `BufferedWriter` provide functions doing exactly that.
struct BufferAllocator
{
    /// Returns nullptr on failure.
    void * (*allocate)(ThreadState * tsdn, size_t size);
    void (*deallocate)(ThreadState * tsdn, void * ptr);
};

/// `read_cb_t`: reads up to `limit` bytes into `buf`; returns the number of bytes read, 0 at the end, < 0 on error.
using ReadCallback = ssize_t(void * read_cbopaque, void * buf, size_t limit);

/// jemalloc: buf_writer_t
class BufferedWriter
{
public:
    /// If `buf` is nullptr, a buffer of `buf_len` bytes is obtained from `allocator` (if `allocator` is nullptr or
    /// fails, there is no buffer and every write goes directly to `write_cb`). `write_cb == nullptr` means
    /// `messageCallback()` (resolved once, here). `buf_len` must be >= 2.
    /// Returns true if there is no buffer.
    /// jemalloc: buf_writer_init
    bool init(
        ThreadState * tsdn, WriteCallback * write_cb, void * cbopaque, char * buf, size_t buf_len,
        const BufferAllocator * allocator = nullptr);

    /// jemalloc: buf_writer_flush
    void flush();

    /// jemalloc: buf_writer_cb (the instance method)
    void write(const char * s);

    /// Use as a `WriteCallback` with `this` as `cbopaque`.
    /// jemalloc: buf_writer_cb
    static void callback(void * buf_writer, const char * s);

    /// Flush and free the internal buffer.
    /// jemalloc: buf_writer_terminate
    void terminate(ThreadState * tsdn);

    /// Copy everything `read_cb` produces to the writer, then flush.
    /// jemalloc: buf_writer_pipe
    void pipe(ReadCallback * read_cb, void * read_cbopaque);

    bool hasBuffer() const { return buf != nullptr; }

private:
    /// jemalloc: buf_writer_assert
    void checkInvariants() const;

    WriteCallback * write_cb;
    void * cbopaque;
    char * buf;
    size_t buf_size;
    size_t buf_end;
    bool internal_buf;
    const BufferAllocator * allocator;
};

}
