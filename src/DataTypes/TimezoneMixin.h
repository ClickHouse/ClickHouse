#pragma once

#include <Core/Types.h>
#include <Common/DateLUT.h>


/** Mixin-class that manages timezone info for timezone-aware DateTime implementations
  * Could be used as a (second) base for a class implementing IDateType/ISerialization-interface.
  *
  * The timezone identity is captured and validated at construction, but its calendar lookup table is
  * initialized only when `getTimeZone` is called. Naming a type does not require the table.
  * Serializations cache this table at construction and resolve the separate UTC table lazily for ISO output and `best_effort` parsing.
  */
class TimezoneMixin
{
public:
    TimezoneMixin(const TimezoneMixin &) = default;

    explicit TimezoneMixin(std::string_view time_zone_name = "")
        : has_explicit_time_zone(!time_zone_name.empty())
        , time_zone(DateLUT::getTimeZone(time_zone_name))
    {
    }

    const DateLUTImpl & getTimeZone() const { return time_zone.getLUT(); }
    const std::string & getTimeZoneName() const { return time_zone.getName(); }
    bool hasExplicitTimeZone() const { return has_explicit_time_zone; }

protected:
    /// true if time zone name was provided in data type parameters, false if it's using default time zone.
    bool has_explicit_time_zone;

    const DateLUT::TimeZone & time_zone;
};
