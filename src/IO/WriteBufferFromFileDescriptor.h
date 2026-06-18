#pragma once

#include <functional>

#include <IO/WriteBufferFromFileBase.h>
#include <Common/IThrottler.h>


namespace DB
{

/** Use ready file descriptor. Does not open or close a file.
  */
class WriteBufferFromFileDescriptor : public WriteBufferFromFileBase
{
public:
    explicit WriteBufferFromFileDescriptor(
        int fd_ = -1,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        ThrottlerPtr throttler_ = {},
        size_t alignment = 0,
        std::string file_name_ = "",
        bool use_adaptive_buffer_size_ = false,
        size_t adaptive_buffer_initial_size = DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE);

    /** Could be used before initialization if needed 'fd' was not passed to constructor.
      * It's not possible to change 'fd' during work.
      */
    void setFD(int fd_)
    {
        fd = fd_;
    }

    int getFD() const
    {
        return fd;
    }

    void sync() override;

    /// clang-tidy wants these methods to be const, but
    /// they are not const semantically
    off_t seek(off_t offset, int whence); // NOLINT
    void truncate(off_t length); // NOLINT

    /// Name or some description of file.
    std::string getFileName() const override;

    off_t size() const;

    /// If set, the callback is consulted while writing the buffer: when it returns true, the
    /// buffered data is discarded instead of being written. To keep it responsive even when the
    /// sink blocks, writes to a descriptor that can block (a pipe, socket or terminal) are then
    /// done by waiting for writability with a timeout (checking the callback in between) and in
    /// bounded chunks, so that a single write() cannot block for long. This is used by the client
    /// to abort the output of a result set promptly on Ctrl+C even while a write to a slow sink
    /// (e.g. a slow terminal) would otherwise block. Passing an empty hook removes it.
    void setCancellationHook(std::function<bool()> cancellation_hook_);

protected:
    void nextImpl() override;

    int fd;
    ThrottlerPtr throttler;

    /// If file has name contains filename, otherwise contains string "(fd=...)"
    std::string file_name;

    /// If true, the size of internal buffer will be exponentially increased up to
    /// adaptive_buffer_max_size after each nextImpl call. It can be used to avoid
    /// large buffer allocation when actual size of written data is small.
    bool use_adaptive_buffer_size;
    size_t adaptive_max_buffer_size;

    /// See setCancellationHook.
    std::function<bool()> cancellation_hook;

    /// Whether a write to this descriptor can block (true for pipes, sockets and terminals; false
    /// for regular files, which never block on write). Computed when the cancellation hook is
    /// installed; only then the responsive bounded-chunk write path is used.
    bool cancellation_fd_can_block = false;

    void finalizeImpl() override;
};

}
