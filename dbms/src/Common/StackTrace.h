#pragma once

#include <string>
#include <array>

#define STACK_TRACE_MAX_DEPTH 32


/// Lets you get a stacktrace
class StackTrace
{
public:
    /// The stacktrace is captured when the object is created
    StackTrace();

    /// Print to string
    std::string toString() const;

private:
    using Frame = void*;
    using Frames = std::array<Frame, STACK_TRACE_MAX_DEPTH>;
    Frames frames;
    size_t frames_size;

    static std::string toStringImpl(const Frames & frames, size_t frames_size);
};
