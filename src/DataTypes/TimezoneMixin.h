#pragma once

#include <Core/Types.h>
#include <Common/DateLUT.h>


/** Mixin-class that manages timezone info for timezone-aware DateTime implementations
  * Could be used as a (second) base for a class implementing IDateType/ISerialization-interface.
  *
  * Note that the UTC time zone is deliberately not kept here, even though the `best_effort` date/time parsers and
  * ISO output need it. Constructing a `DateLUTImpl` walks ~146k days through cctz, and the data types
  * (`DataTypeDateTime`, `DataTypeDateTime64`) are constructed just to name a column type - e.g. when building the
  * schemas of the system tables at startup - without ever touching UTC. Resolving UTC here made every such
  * construction build a second, always-unused lookup table. The serializations that do need it declare their own
  * `utc_time_zone` member instead, so the cost is paid once per formatter.
  */
class TimezoneMixin
{
public:
    TimezoneMixin(const TimezoneMixin &) = default;

    explicit TimezoneMixin(std::string_view time_zone_name = "")
        : has_explicit_time_zone(!time_zone_name.empty())
        , time_zone(DateLUT::instance(time_zone_name))
    {
    }

    const DateLUTImpl & getTimeZone() const { return time_zone; }
    bool hasExplicitTimeZone() const { return has_explicit_time_zone; }

protected:
    /// true if time zone name was provided in data type parameters, false if it's using default time zone.
    bool has_explicit_time_zone;

    const DateLUTImpl & time_zone;
};
