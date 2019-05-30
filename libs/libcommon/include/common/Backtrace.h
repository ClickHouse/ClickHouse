#pragma once

#include <string>
#include <vector>
#include <array>
#include <optional>
#include <signal.h>

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif
#include <ucontext.h>

struct NoCapture
{
};

class Backtrace
{
public:
    static constexpr size_t capacity = 32;
    using Frames = std::array<void *, capacity>;

    Backtrace(NoCapture)
    {
    }

    Backtrace(const std::vector<void *>& sourceFrames);
    Backtrace(std::optional<ucontext_t> signal_context = std::nullopt);

    size_t getSize() const;
    const Frames& getFrames() const;
    std::string toString() const;

protected:
    static std::string toStringImpl(const Frames& frames, size_t size);

    size_t size;
    Frames frames;
};

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);

void * getCallerAddress(const ucontext_t & context);

