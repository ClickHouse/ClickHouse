#pragma once

#include <Interpreters/Context.h>

struct ContextHolder
{
    DB::SharedContextHolder shared_context;
    DB::ContextMutablePtr context;

    ContextHolder();

    ContextHolder(ContextHolder &&) = default;

    void destroy()
    {
        context->shutdown();
        context.reset();
        shared_context.reset();
    }
};

struct TestCommandLineOptions
{
    int argc = 0;
    const char * const * argv = nullptr;
};

const ContextHolder & getContext();

ContextHolder & getMutableContext();

// Command line with gtest's flags removed.
TestCommandLineOptions & getTestCommandLineOptions();
