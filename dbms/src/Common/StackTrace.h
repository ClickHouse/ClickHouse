#pragma once

#include <string>
#include <vector>
#include <array>
#include <optional>
#include <functional>
#include <signal.h>

#ifdef __APPLE__
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
    static constexpr size_t capacity = 32;
    using Frames = std::array<void *, capacity>;

    /// Tries to capture stack trace
    StackTrace();

    /// Tries to capture stack trace. Fallbacks on parsing caller address from
    /// signal context if no stack trace could be captured
    StackTrace(const ucontext_t & signal_context);

    /// Creates empty object for deferred initialization
    StackTrace(NoCapture);

    size_t getSize() const;
    size_t getOffset() const;
    const Frames & getFrames() const;
    std::string toString() const;

    static std::string toString(void ** frames, size_t offset, size_t size);

    void toStringEveryLine(std::function<void(const std::string &)> callback) const;

protected:
    void tryCapture();

    size_t size = 0;
    size_t offset = 0;  /// How many frames to skip while displaying.
    Frames frames{};
};

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);
