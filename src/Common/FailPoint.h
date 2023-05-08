#pragma once
#include "config.h"

#include <Common/Exception.h>
#include <Core/Types.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#endif

#include <fiu.h>
#include <fiu-control.h>

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include <any>
#include <unordered_map>

namespace DB
{

// When `fail_point` is enabled, wait till it is disabled
#define FAIL_POINT_PAUSE(fail_point) fiu_do_on(fail_point, FailPointHelper::wait(fail_point);)

class FailPointChannel;
class FailPointInjection
{
public:
    static void enableFailPoint(const String & fail_point_name);

    static void enablePauseFailPoint(const String & fail_point_name, UInt64 time);

    static void disableFailPoint(const String & fail_point_name);

    static void wait(const String & fail_point_name);

private:
    static std::mutex mu;
    static std::unordered_map<String, std::shared_ptr<FailPointChannel>> fail_point_wait_channels;
};
}
