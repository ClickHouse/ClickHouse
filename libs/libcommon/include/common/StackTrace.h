#pragma once

#include <string>
#include <vector>
#include <array>
#include <optional>
#include <signal.h>

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif
#include <ucontext.h>

struct NoCapture
{
};

/// Tries to capture current stack trace using libunwind or signal context
/// NOTE: All StackTrace constructors are signal safe
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

    /// Fills stack trace frames with provided sequence
    StackTrace(const std::vector<void *> & source_frames);

    size_t getSize() const;
    const Frames & getFrames() const;
    std::string toString() const;

protected:
    void tryCapture();
    static std::string toStringImpl(const Frames & frames, size_t size);

    size_t size = 0;
    Frames frames;
};

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);

void * getCallerAddress(const ucontext_t & context);
