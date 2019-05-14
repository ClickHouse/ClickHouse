#pragma once

#include <string>
#include <vector>
#include <array>
#include <signal.h>

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif
#include <ucontext.h>


class Backtrace
{
public:
    static constexpr size_t capacity = 50;
    using Frames = std::array<void *, capacity>;

    Backtrace() = default;
    Backtrace(const std::vector<void *>& sourceFrames);
    Backtrace(const ucontext_t & signal_context);

    size_t getSize() const;
    const Frames& getFrames() const;
    std::string toString(const std::string & delimiter = "") const;

protected:
    size_t size;
    Frames frames;
};

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);

void * getCallerAddress(const ucontext_t & context);

