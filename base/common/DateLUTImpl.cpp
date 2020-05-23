#include "DateLUTImpl.h"

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <cctz/zone_info_source.h>
#include <common/unaligned.h>
#include <Poco/Exception.h>

#include <dlfcn.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>


namespace
{

UInt8 getDayOfWeek(const cctz::civil_day & date)
{
    cctz::weekday day_of_week = cctz::get_weekday(date);
    switch (day_of_week)
    {
        case cctz::weekday::monday:     return 1;
        case cctz::weekday::tuesday:    return 2;
        case cctz::weekday::wednesday:  return 3;
        case cctz::weekday::thursday:   return 4;
        case cctz::weekday::friday:     return 5;
        case cctz::weekday::saturday:   return 6;
        case cctz::weekday::sunday:     return 7;
    }
    __builtin_unreachable();
}

}


__attribute__((__weak__)) extern bool inside_main;

DateLUTImpl::DateLUTImpl(const std::string & time_zone_)
    : time_zone(time_zone_)
{
    /// DateLUT should not be initialized in global constructors for the following reasons:
    /// 1. It is too heavy.
    if (&inside_main)
        assert(inside_main);

    size_t i = 0;
    time_t start_of_day = 0;

    cctz::time_zone cctz_time_zone;
    if (!cctz::load_time_zone(time_zone, &cctz_time_zone))
        throw Poco::Exception("Cannot load time zone " + time_zone_);

    cctz::time_zone::absolute_lookup start_of_epoch_lookup = cctz_time_zone.lookup(std::chrono::system_clock::from_time_t(start_of_day));
    offset_at_start_of_epoch = start_of_epoch_lookup.offset;
    offset_is_whole_number_of_hours_everytime = true;

    cctz::civil_day date{1970, 1, 1};

    do
    {
        cctz::time_zone::civil_lookup lookup = cctz_time_zone.lookup(date);

        start_of_day = std::chrono::system_clock::to_time_t(lookup.pre);    /// Ambiguity is possible.

        Values & values = lut[i];
        values.year = date.year();
        values.month = date.month();
        values.day_of_month = date.day();
        values.day_of_week = getDayOfWeek(date);
        values.date = start_of_day;

        assert(values.year >= DATE_LUT_MIN_YEAR && values.year <= DATE_LUT_MAX_YEAR);
        assert(values.month >= 1 && values.month <= 12);
        assert(values.day_of_month >= 1 && values.day_of_month <= 31);
        assert(values.day_of_week >= 1 && values.day_of_week <= 7);

        if (values.day_of_month == 1)
        {
            cctz::civil_month month(date);
            values.days_in_month = cctz::civil_day(month + 1) - cctz::civil_day(month);
        }
        else
            values.days_in_month = i != 0 ? lut[i - 1].days_in_month : 31;

        values.time_at_offset_change = 0;
        values.amount_of_offset_change = 0;

        if (start_of_day % 3600)
            offset_is_whole_number_of_hours_everytime = false;

        /// If UTC offset was changed in previous day.
        if (i != 0)
        {
            auto amount_of_offset_change_at_prev_day = 86400 - (lut[i].date - lut[i - 1].date);
            if (amount_of_offset_change_at_prev_day)
            {
                lut[i - 1].amount_of_offset_change = amount_of_offset_change_at_prev_day;

                const auto utc_offset_at_beginning_of_day = cctz_time_zone.lookup(std::chrono::system_clock::from_time_t(lut[i - 1].date)).offset;

                /// Find a time (timestamp offset from beginning of day),
                ///  when UTC offset was changed. Search is performed with 15-minute granularity, assuming it is enough.

                time_t time_at_offset_change = 900;
                while (time_at_offset_change < 86400)
                {
                    auto utc_offset_at_current_time = cctz_time_zone.lookup(std::chrono::system_clock::from_time_t(
                        lut[i - 1].date + time_at_offset_change)).offset;

                    if (utc_offset_at_current_time != utc_offset_at_beginning_of_day)
                        break;

                    time_at_offset_change += 900;
                }

                lut[i - 1].time_at_offset_change = time_at_offset_change;

                /// We doesn't support cases when time change results in switching to previous day.
                if (static_cast<int>(lut[i - 1].time_at_offset_change) + static_cast<int>(lut[i - 1].amount_of_offset_change) < 0)
                    lut[i - 1].time_at_offset_change = -lut[i - 1].amount_of_offset_change;
            }
        }

        /// Going to next day.
        ++date;
        ++i;
    }
    while (start_of_day <= DATE_LUT_MAX && i <= DATE_LUT_MAX_DAY_NUM);

    /// Fill excessive part of lookup table. This is needed only to simplify handling of overflow cases.
    while (i < DATE_LUT_SIZE)
    {
        lut[i] = lut[i - 1];
        ++i;
    }

    /// Fill lookup table for years and months.
    size_t year_months_lut_index = 0;
    size_t first_day_of_last_month = 0;

    for (size_t day = 0; day < DATE_LUT_SIZE; ++day)
    {
        const Values & values = lut[day];

        if (values.day_of_month == 1)
        {
            if (values.month == 1)
                years_lut[values.year - DATE_LUT_MIN_YEAR] = day;

            year_months_lut_index = (values.year - DATE_LUT_MIN_YEAR) * 12 + values.month - 1;
            years_months_lut[year_months_lut_index] = day;
            first_day_of_last_month = day;
        }
    }

    /// Fill the rest of lookup table with the same last month (2106-02-01).
    for (; year_months_lut_index < DATE_LUT_YEARS * 12; ++year_months_lut_index)
    {
        years_months_lut[year_months_lut_index] = first_day_of_last_month;
    }
}


#if !defined(ARCADIA_BUILD) /// Arcadia's variant of CCTZ already has the same implementation.

/// Prefer to load timezones from blobs linked to the binary.
/// The blobs are provided by "tzdata" library.
/// This allows to avoid dependency on system tzdata.
namespace cctz_extension
{
    namespace
    {
        class Source : public cctz::ZoneInfoSource
        {
        public:
            Source(const char * data_, size_t size_) : data(data_), size(size_) {}

            size_t Read(void * buf, size_t bytes) override
            {
                if (bytes > size)
                    bytes = size;
                memcpy(buf, data, bytes);
                data += bytes;
                size -= bytes;
                return bytes;
            }

            int Skip(size_t offset) override
            {
                if (offset <= size)
                {
                    data += offset;
                    size -= offset;
                    return 0;
                }
                else
                {
                    errno = EINVAL;
                    return -1;
                }
            }
        private:
            const char * data;
            size_t size;
        };

        std::unique_ptr<cctz::ZoneInfoSource> custom_factory(
            const std::string & name,
            const std::function<std::unique_ptr<cctz::ZoneInfoSource>(const std::string & name)> & fallback)
        {
            std::string name_replaced = name;
            std::replace(name_replaced.begin(), name_replaced.end(), '/', '_');
            std::replace(name_replaced.begin(), name_replaced.end(), '-', '_');

            /// These are the names that are generated by "ld -r -b binary"
            std::string symbol_name_data = "_binary_" + name_replaced + "_start";
            std::string symbol_name_size = "_binary_" + name_replaced + "_size";

            const void * sym_data = dlsym(RTLD_DEFAULT, symbol_name_data.c_str());
            const void * sym_size = dlsym(RTLD_DEFAULT, symbol_name_size.c_str());

            if (sym_data && sym_size)
                return std::make_unique<Source>(static_cast<const char *>(sym_data), unalignedLoad<size_t>(&sym_size));

            return fallback(name);
        }
    }

    ZoneInfoSourceFactory zone_info_source_factory = custom_factory;
}

#endif
