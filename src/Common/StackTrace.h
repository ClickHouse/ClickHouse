#pragma once

#include <base/types.h>

#include <string>
#include <vector>
#include <array>
#include <optional>
#include <functional>
#include <csignal>
#include <csetjmp>

#ifdef OS_DARWIN
// ucontext is not available without _XOPEN_SOURCE
#   pragma clang diagnostic ignored "-Wreserved-id-macro"
#   define _XOPEN_SOURCE 700
#endif
#include <ucontext.h>

struct NoCapture
{
};

/// Tries to capture current stack trace using libunwind or signal context
/// NOTE: StackTrace calculation is signal safe only if updatePHDRCache() was called beforehand.
class StackTrace
{
public:
    struct Frame
    {
        const void * virtual_addr = nullptr;
        void * physical_addr = nullptr;
        std::optional<std::string> symbol;
        std::optional<std::string> object;
        std::optional<std::string> file;
        std::optional<UInt64> line;
    };

    /* NOTE: It cannot be larger right now, since otherwise it
     * will not fit into minimal PIPE_BUF (512) in TraceCollector.
     */
    static constexpr size_t capacity = 45;

    using FramePointers = std::array<void *, capacity>;
    using Frames = std::array<Frame, capacity>;

    /// Tries to capture stack trace
    StackTrace() { tryCapture(); }

    /// Tries to capture stack trace. Fallbacks on parsing caller address from
    /// signal context if no stack trace could be captured
    explicit StackTrace(const ucontext_t & signal_context);

    /// Creates empty object for deferred initialization
    explicit StackTrace(NoCapture) {}

    constexpr size_t getSize() const { return size; }
    constexpr size_t getOffset() const { return offset; }
    const FramePointers & getFramePointers() const { return frame_pointers; }
    std::string toString() const;

    static std::string toString(void * const * frame_pointers, size_t offset, size_t size);
    static void dropCache();

    /// @param fatal - if true, will process inline frames (slower)
    static void forEachFrame(
        const FramePointers & frame_pointers,
        size_t offset,
        size_t size,
        std::function<void(const Frame &)> callback,
        bool fatal);

    void toStringEveryLine(std::function<void(std::string_view)> callback) const;
    static void toStringEveryLine(const FramePointers & frame_pointers, std::function<void(std::string_view)> callback);
    static void toStringEveryLine(void ** frame_pointers_raw, size_t offset, size_t size, std::function<void(std::string_view)> callback);

    /// Displaying the addresses can be disabled for security reasons.
    /// If you turn off addresses, it will be more secure, but we will be unable to help you with debugging.
    /// Please note: addresses are also available in the system.stack_trace and system.trace_log tables.
    static void setShowAddresses(bool show);

protected:
    void tryCapture();

    size_t size = 0;
    size_t offset = 0;  /// How many frames to skip while displaying.
    FramePointers frame_pointers{};
};

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);

/// Special handling for errors during asynchronous stack unwinding,
/// Which is used in Query Profiler
extern thread_local bool asynchronous_stack_unwinding;
extern thread_local sigjmp_buf asynchronous_stack_unwinding_signal_jump_buffer;
