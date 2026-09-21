#include <QueryPipeline/StreamLocalLimits.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsOverflowMode result_overflow_mode;
}

StreamLocalLimits StreamLocalLimits::forQueryResult(const Settings & settings)
{
    StreamLocalLimits limits;
    limits.mode = LimitsMode::LIMITS_CURRENT;
    limits.size_limits = SizeLimits(
        settings[Setting::max_result_rows],
        settings[Setting::max_result_bytes],
        settings[Setting::result_overflow_mode]);
    return limits;
}
}
