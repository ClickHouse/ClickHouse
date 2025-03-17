#include "Formatters.h"

#include <fmt/format.h>

#include <cmath>

namespace DB
{
std::string formatKQLTimespan(const Int64 ticks)
{
    static constexpr Int64 TICKS_PER_SECOND = 10000000;
    static constexpr auto TICKS_PER_MINUTE = TICKS_PER_SECOND * 60;
    static constexpr auto TICKS_PER_HOUR = TICKS_PER_MINUTE * 60;
    static constexpr auto TICKS_PER_DAY = TICKS_PER_HOUR * 24;

    const auto abs_ticks = std::abs(ticks);
    std::string result = ticks < 0 ? "-" : "";
    if (abs_ticks >= TICKS_PER_DAY)
        result.append(fmt::format("{}.", abs_ticks / TICKS_PER_DAY));

    result.append(fmt::format(
        "{:02}:{:02}:{:02}", (abs_ticks / TICKS_PER_HOUR) % 24, (abs_ticks / TICKS_PER_MINUTE) % 60, (abs_ticks / TICKS_PER_SECOND) % 60));

    if (const auto fractional_second = abs_ticks % TICKS_PER_SECOND)
        result.append(fmt::format(".{:07}", fractional_second));

    return result;
}
}
