#pragma once

#include "DateLUTImpl.h"

#include <base/defines.h>

#include <boost/noncopyable.hpp>
#include "Common/CurrentThread.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>


/// This class provides lazy initialization and lookup of singleton DateLUTImpl objects for a given timezone.
class DateLUT : private boost::noncopyable
{
public:
    /// Return DateLUTImpl instance for session timezone.
    /// session_timezone is a session-level setting.
    /// If setting is not set, returns the server timezone.
    static ALWAYS_INLINE const DateLUTImpl & instance()
    {
        const auto & date_lut = getInstance();

        if (DB::CurrentThread::isInitialized())
        {
            std::string timezone_from_context;
            const DB::ContextPtr query_context = DB::CurrentThread::get().getQueryContext();

            if (query_context)
            {
                timezone_from_context = extractTimezoneFromContext(query_context);

                if (!timezone_from_context.empty())
                    return date_lut.getImplementation(timezone_from_context);
            }

            /// On the server side, timezone is passed in query_context,
            /// but on CH-client side we have no query context,
            /// and each time we modify client's global context
            const DB::ContextPtr global_context = DB::CurrentThread::get().getGlobalContext();
            if (global_context)
            {
                timezone_from_context = extractTimezoneFromContext(global_context);

                if (!timezone_from_context.empty())
                    return date_lut.getImplementation(timezone_from_context);
            }

        }
        return serverTimezoneInstance();
    }

    static ALWAYS_INLINE const DateLUTImpl & instance(const std::string & time_zone)
    {
        if (time_zone.empty())
            return instance();

        const auto & date_lut = getInstance();
        return date_lut.getImplementation(time_zone);
    }

    /// Return singleton DateLUTImpl for the server time zone.
    /// It may be set using 'timezone' server setting.
    static ALWAYS_INLINE const DateLUTImpl & serverTimezoneInstance()
    {
        const auto & date_lut = getInstance();
        return *date_lut.default_impl.load(std::memory_order_acquire);
    }

    static void setDefaultTimezone(const std::string & time_zone)
    {
        auto & date_lut = getInstance();
        const auto & impl = date_lut.getImplementation(time_zone);
        date_lut.default_impl.store(&impl, std::memory_order_release);
    }

protected:
    DateLUT();

private:
    static DateLUT & getInstance();

    static std::string extractTimezoneFromContext(DB::ContextPtr query_context);

    const DateLUTImpl & getImplementation(const std::string & time_zone) const;

    using DateLUTImplPtr = std::unique_ptr<DateLUTImpl>;

    /// Time zone name -> implementation.
    mutable std::unordered_map<std::string, DateLUTImplPtr> impls;
    mutable std::mutex mutex;

    std::atomic<const DateLUTImpl *> default_impl;
};

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
