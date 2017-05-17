#pragma once

#include <string>
#include <vector>

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
    Frame frames[STACK_TRACE_MAX_DEPTH];
    size_t frames_size;
};
