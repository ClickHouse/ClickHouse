#pragma once

#include <base/types.h>

#include <string>
#include <vector>
#include <array>
#include <optional>
#include <functional>
#include <csignal>

#ifdef OS_DARWIN
// ucontext is not available without _XOPEN_SOURCE
#   ifdef __clang__
#       pragma clang diagnostic ignored "-Wreserved-id-macro"
#   endif
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
    StackTrace();

    /// Tries to capture stack trace. Fallbacks on parsing caller address from
    /// signal context if no stack trace could be captured
    explicit StackTrace(const ucontext_t & signal_context);

    /// Creates empty object for deferred initialization
    explicit StackTrace(NoCapture);

    size_t getSize() const;
    size_t getOffset() const;
    const FramePointers & getFramePointers() const;
    std::string toString() const;

    static std::string toString(void ** frame_pointers, size_t offset, size_t size);
    static std::string toStringStatic(const FramePointers & frame_pointers, size_t offset, size_t size);
    static void dropCache();
    static void symbolize(const FramePointers & frame_pointers, size_t offset, size_t size, StackTrace::Frames & frames);

    void toStringEveryLine(std::function<void(const std::string &)> callback) const;

protected:
    void tryCapture();

    size_t size = 0;
    size_t offset = 0;  /// How many frames to skip while displaying.
    FramePointers frame_pointers{};
};

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);
