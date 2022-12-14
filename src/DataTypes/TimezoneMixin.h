#pragma once
#include <Core/Types.h>
#include <Common/DateLUT.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

class DateLUTImpl;

/** Mixin-class that manages timezone info for timezone-aware DateTime implementations
 *
 * Must be used as a (second) base for class implementing IDateType/ISerialization-interface.
 */
class TimezoneMixin
{
public:
    TimezoneMixin(const TimezoneMixin &) = default;

    explicit TimezoneMixin(const String & time_zone_name = "")
        : has_explicit_time_zone(!time_zone_name.empty())
        , time_zone(DateLUT::instance(checkTimezoneName(time_zone_name)))
        , utc_time_zone(DateLUT::instance("UTC"))
    {
    }

    const DateLUTImpl & getTimeZone() const { return time_zone; }
    bool hasExplicitTimeZone() const { return has_explicit_time_zone; }

protected:
    /// true if time zone name was provided in data type parameters, false if it's using default time zone.
    bool has_explicit_time_zone;

    const DateLUTImpl & time_zone;
    const DateLUTImpl & utc_time_zone;

private:
    static const String & checkTimezoneName(const String & timezone_name)
    {
        const char * forbidden_patterns[] = {"/", "../", "./", "~/"};
        for (const auto & pattern : forbidden_patterns)
        {
            if (timezone_name.starts_with(pattern))
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Timezone name cannot start with '{}'", pattern);
        }
        
        return timezone_name;
    }
};
