#pragma once

#include <base/DayNum.h>
#include <base/defines.h>
#include <base/types.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

class DateLUTImpl;


/// This class provides lazy initialization and lookup of singleton DateLUTImpl objects for a given timezone.
class DateLUT : private boost::noncopyable
{
public:
    class TimeZone
    {
    public:
        ~TimeZone();

        const std::string & getName() const { return name; }
        const DateLUTImpl & getLUT() const;

    private:
        friend class DateLUT;

        explicit TimeZone(std::string_view time_zone)
            : name(time_zone)
        {
        }

        const std::string name;
        mutable std::atomic<const DateLUTImpl *> impl{nullptr};
    };

    static const TimeZone & getTimeZone();

    static const TimeZone & getTimeZone(std::string_view time_zone)
    {
        if (time_zone.empty())
            return getTimeZone();

        return getInstance().getTimeZoneImpl(time_zone);
    }

    static const TimeZone & serverTimezone() { return *getInstance().default_time_zone.load(std::memory_order_acquire); }

    /// Return DateLUTImpl instance for session timezone.
    /// session_timezone is a session-level setting.
    /// If setting is not set, returns the server timezone.
    static const DateLUTImpl & instance();

    static ALWAYS_INLINE const DateLUTImpl & instance(std::string_view time_zone) { return getTimeZone(time_zone).getLUT(); }

    /// Return singleton DateLUTImpl for the server time zone.
    /// It may be set using 'timezone' server setting.
    static ALWAYS_INLINE const DateLUTImpl & serverTimezoneInstance() { return serverTimezone().getLUT(); }

    static ALWAYS_INLINE const DateLUTImpl & utcTimezoneInstance()
    {
        /// Cache the UTC table globally and lazily: it is immutable, independent of session/server timezones,
        /// and owned by the process-lifetime `DateLUT` singleton. Thread-safe static initialization lets all threads
        /// reuse the reference without a mutex and timezone-map lookup per value.
        static const auto & time_zone = instance("UTC");
        return time_zone;
    }

    static void setDefaultTimezone(std::string_view time_zone)
    {
        auto & date_lut = getInstance();
        const auto & selected_time_zone = date_lut.getTimeZoneImpl(time_zone);
        date_lut.default_time_zone.store(&selected_time_zone, std::memory_order_release);
    }

protected:
    DateLUT();

private:
    static DateLUT & getInstance();

    const TimeZone & getTimeZoneImpl(std::string_view time_zone) const;
    const DateLUTImpl & getImplementation(const TimeZone & time_zone) const;

    using TimeZonePtr = std::unique_ptr<TimeZone>;

    /// Time zone name -> validated identity and lazily initialized implementation.
    mutable std::unordered_map<std::string, TimeZonePtr> time_zones;
    mutable std::mutex mutex;

    std::atomic<const TimeZone *> default_time_zone;
};

inline const DateLUTImpl & DateLUT::TimeZone::getLUT() const
{
    if (const auto * initialized = impl.load(std::memory_order_acquire))
        return *initialized;

    return DateLUT::getInstance().getImplementation(*this);
}

inline UInt64 timeInMilliseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 timeInMicroseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 timeInSeconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 timeInNanoseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(timepoint.time_since_epoch()).count();
}

/// A few helper functions to avoid having to include DateLUTImpl.h in some heavy headers

ExtendedDayNum makeDayNum(const DateLUTImpl & date_lut, Int16 year, UInt8 month, UInt8 day_of_month, Int32 default_error_day_num = 0);
std::optional<ExtendedDayNum> tryToMakeDayNum(const DateLUTImpl & date_lut, Int16 year, UInt8 month, UInt8 day_of_month);

Int64 makeDate(const DateLUTImpl & date_lut, Int16 year, UInt8 month, UInt8 day_of_month);
Int64 makeDateTime(const DateLUTImpl & date_lut, Int16 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second);
std::optional<Int64> tryToMakeDateTime(const DateLUTImpl & date_lut, Int16 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second);

const std::string & getDateLUTTimeZone(const DateLUTImpl & date_lut);
UInt32 getDayNumOffsetEpoch();
