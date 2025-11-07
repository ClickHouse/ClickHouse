#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>

#include "config.h"

#if USE_LIBFIU

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#  include <fiu.h>
#  include <fiu-control.h>
#pragma clang diagnostic pop

#else // USE_LIBFIU

// stubs from fiu-local.h
#define fiu_init(flags) 0
#define fiu_fail(name) 0
#define fiu_failinfo() NULL
#define fiu_do_on(name, action)
#define fiu_exit_on(name)
#define fiu_return_on(name, retval)

#endif // USE_LIBFIU

#include <unordered_map>


namespace DB
{

/// This is a simple named failpoint library inspired by https://github.com/pingcap/tiflash
/// The usage is simple:
/// 1. define failpoint with a 'failpoint_name' in FailPoint.cpp
/// 2. inject failpoint in normal code
///   2.1 use fiu_do_on which can inject any code blocks, when it is a regular-triggered / once-triggered failpoint
///   2.2 use pauseFailPoint when it is a pausable failpoint
/// 3. in test file, we can use system failpoint enable/disable 'failpoint_name'

class FailPointChannel;

class FailPointInjection
{
public:

    static void pauseFailPoint(const String & fail_point_name);

    static void enableFailPoint(const String & fail_point_name);

    static void enablePauseFailPoint(const String & fail_point_name, UInt64 time);

    static void disableFailPoint(const String & fail_point_name);

    static void wait(const String & fail_point_name);

private:
    static std::mutex mu;
    static std::unordered_map<String, std::shared_ptr<FailPointChannel>> fail_point_wait_channels;
};
}
