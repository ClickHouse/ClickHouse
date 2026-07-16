#pragma once

#include <functional>
#include <optional>
#include <string_view>

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

    ~WriteBufferFromFileDescriptor() override;

    /** Could be used before initialization if needed 'fd' was not passed to constructor.
      * It's not possible to change 'fd' during work.
      */
    void setFD(int fd_);

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
    /// a way that cannot sleep indefinitely in a single write() (see nextImpl). This is used by
    /// the client to abort the output of a result set promptly on Ctrl+C even while a write to a
    /// slow sink (e.g. a slow terminal) would otherwise block. Passing an empty hook removes it.
    void setCancellationHook(std::function<bool()> cancellation_hook_);

    /// Best-effort direct write of a small out-of-band message (e.g. an interactive diagnostic
    /// printed while cancelling a query), bypassing both the internal buffer and the cancellation
    /// hook. It never blocks longer than the given budget: if the sink stays unwritable (e.g. a
    /// terminal that stopped draining), the rest of the message is dropped - nothing is reading
    /// that sink anyway. It does not touch the internal buffer state, so it is safe to call while
    /// another thread writes through the buffer, but the message may then interleave with that
    /// output.
    void writeBestEffort(std::string_view data, UInt64 timeout_ms);

    /// While a budget is set, every flush of the internal buffer is written with the same bounded,
    /// never-throwing discipline as writeBestEffort: whatever the sink does not accept within the
    /// budget is dropped. It takes precedence over the cancellation hook, which would discard the
    /// data outright once cancellation is requested. Used for writes that are still wanted while
    /// cancellation may be fighting a stuck sink - e.g. clearing the progress indication on
    /// Ctrl+C, when the terminal may be exactly what is stuck. Passing std::nullopt removes the
    /// budget.
    void setBestEffortFlushBudget(std::optional<UInt64> timeout_ms)
    {
        best_effort_flush_budget_ms = timeout_ms;
    }

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
    /// installed; only then the responsive write path is used.
    bool cancellation_fd_can_block = false;

    /// Whether the descriptor is a socket. The responsive path then uses send(..., MSG_DONTWAIT),
    /// which is non-blocking per call without touching the open file description flags.
    bool cancellation_fd_is_socket = false;

    /// A private non-blocking descriptor for the same terminal, used by the responsive write path
    /// when the sink is a tty. O_NONBLOCK cannot simply be set on `fd`: the flag is a property of
    /// the open file description, which a terminal fd shares with fd 2 and the parent shell, so
    /// toggling it there leaks to unrelated writers (that broke the progress rendering once - see
    /// 3f8b12c2736). Re-opening the terminal by its path - recovered via /proc/self/fd (Linux),
    /// fcntl(F_GETPATH) (Darwin) or ttyname_r() (elsewhere, e.g. FreeBSD) - yields an independent
    /// open file description, so O_NONBLOCK on it affects nobody else. -1 when unavailable (not a
    /// terminal, or the re-open failed) - the responsive path then falls back to poll() + a
    /// blocking write capped at PIPE_BUF.
    int nonblocking_write_fd = -1;

    /// See setBestEffortFlushBudget.
    std::optional<UInt64> best_effort_flush_budget_ms;

    void finalizeImpl() override;
};

}
