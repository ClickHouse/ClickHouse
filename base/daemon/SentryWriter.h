#pragma once

#include <common/types.h>
#include <Common/StackTrace.h>

#include <string>

class SentryWriter
{
public:
    SentryWriter() = delete;

    static void initialize();
    static void shutdown();
    static void onFault(
        int sig,
        const siginfo_t & info,
        const ucontext_t & context,
        const StackTrace & stack_trace
    );
};
