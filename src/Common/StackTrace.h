#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <base/MemorySanitizer.h>
#include <Common/FramePointers.h>

#include <string>
#include <array>
#include <exception>
#include <optional>
#include <functional>
#include <span>
/** A standalone build of the parser (see `utils/wasm-parser`) has no signals, no `setjmp` and no
  * way to walk its own stack, and it does not link `StackTrace.cpp`. Everything below that needs
  * those is left out rather than making the whole header unavailable, since `Common/Exception.h`
  * includes it and so does most of the tree.
  */
#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
#include <csignal>
#include <csetjmp>
#endif

#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
#ifdef OS_DARWIN
// ucontext is not available without _XOPEN_SOURCE
#   pragma clang diagnostic ignored "-Wreserved-id-macro"
#   define _XOPEN_SOURCE 700
#endif
#include <ucontext.h>
#endif

/** The stack trace of the throw that created an exception, recorded inside the `std::exception`
  * itself by ClickHouse's patched libc++. See `STD_EXCEPTION_HAS_STACK_TRACE` in `base/defines.h`:
  * these two functions are the only place that copes with a C++ standard library which records
  * nothing, where they report an empty trace and do nothing respectively.
  *
  * The frames are un-poisoned for MSan, which does not see libc++ writing them.
  */
inline std::span<void *> getStackTraceOfThrow([[maybe_unused]] const std::exception & e)
{
#if STD_EXCEPTION_HAS_STACK_TRACE
    void ** frames = e.get_stack_trace_frames();
    const size_t size = e.get_stack_trace_size();
    __msan_unpoison(frames, size * sizeof(frames[0]));
    return {frames, size};
#else
    return {};
#endif
}

/// Make the throw-site stack trace of `from` the throw-site stack trace of `to`.
inline void copyStackTraceOfThrow([[maybe_unused]] const std::exception & from, [[maybe_unused]] std::exception & to)
{
#if STD_EXCEPTION_HAS_STACK_TRACE
    const auto trace = getStackTraceOfThrow(from);
    to.set_stack_trace(trace.data(), static_cast<int>(trace.size()));
#endif
}

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
        std::optional<UInt64> column;
    };

    using Frames = std::array<Frame, FRAMEPOINTER_CAPACITY>;

    /// Tries to capture stack trace
    /// NO_INLINE to get correct line of StackTrace() caller in captured stack trace
    NO_INLINE StackTrace();

    /// Tries to capture stack trace. Fallbacks on parsing caller address from
    /// signal context if no stack trace could be captured
#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
    explicit StackTrace(const ucontext_t & signal_context);
#endif

    /// Creates empty object for deferred initialization
    explicit StackTrace(NoCapture) {}

    StackTrace(FramePointers frame_pointers_, size_t size_, size_t offset_ = 0);

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

#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);

std::optional<UInt64> getFaultAddress(int sig, const siginfo_t & info);
std::string getFaultMemoryAccessType(int sig, const ucontext_t & context);
#endif
std::string getSignalCodeDescription(int sig, int si_code);

/// Special handling for errors during asynchronous stack unwinding,
/// Which is used in Query Profiler
extern thread_local bool asynchronous_stack_unwinding;
#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
extern thread_local sigjmp_buf asynchronous_stack_unwinding_signal_jump_buffer;
#endif
