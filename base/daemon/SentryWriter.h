#pragma once

#include <common/types.h>
#include <Common/StackTrace.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <string>

/// Sends crash reports to ClickHouse core developer team via https://sentry.io
class SentryWriter
{
public:
    SentryWriter() = delete;

    static void initialize(Poco::Util::LayeredConfiguration & config);
    static void shutdown();
    static void onFault(
        int sig,
        const siginfo_t & info,
        const ucontext_t & context,
        const StackTrace & stack_trace
    );
};
