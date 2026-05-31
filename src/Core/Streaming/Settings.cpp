#include <Core/Streaming/Settings.h>

namespace DB
{

bool isIdleExpired(
    const std::chrono::steady_clock::time_point & now,
    const std::chrono::steady_clock::time_point & last_active,
    const WatermarkSettingsPtr & watermark)
{
    if (watermark->idle_timeout.count() <= 0)
        return false;

    return now > last_active + watermark->idle_timeout;
}

}
