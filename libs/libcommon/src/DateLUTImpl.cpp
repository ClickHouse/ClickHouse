#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#if __clang__
	#pragma GCC diagnostic ignored "-Wdeprecated-register"
#endif
	#include <glib.h>
#pragma GCC diagnostic pop

#include <common/DateLUTImpl.h>
#include <Poco/Exception.h>

#include <memory>
#include <cstring>

#include <iostream>


namespace details
{
namespace
{

struct GTimeZoneUnref
{
	void operator()(GTimeZone * tz) const
	{
		g_time_zone_unref(tz);
	}
};

using GTimeZonePtr = std::unique_ptr<GTimeZone, GTimeZoneUnref>;

struct GDateTimeUnref
{
	void operator()(GDateTime * dt) const
	{
		g_date_time_unref(dt);
	}
};

using GDateTimePtr = std::unique_ptr<GDateTime, GDateTimeUnref>;


GTimeZonePtr createGTimeZone(const std::string & description)
{
	GTimeZone * tz = g_time_zone_new(description.c_str());
	if (tz == nullptr)
		throw Poco::Exception("Failed to create GTimeZone object.");

	return GTimeZonePtr(tz);
}


GDateTimePtr createGDateTimeUTC(time_t timestamp)
{
	GDateTime * dt = g_date_time_new_from_unix_utc(timestamp);
	if (dt == nullptr)
		throw Poco::Exception("Failed to create GDateTime object.");

	return GDateTimePtr(dt);
}


/// Create GDateTime object, interpreting passed unix timestamp in passed timezone.
GDateTimePtr createGDateTimeLocal(const GTimeZonePtr & p_tz, time_t timestamp)
{
	GDateTimePtr utc = createGDateTimeUTC(timestamp);
	GDateTime * local = g_date_time_to_timezone(utc.get(), p_tz.get());
	if (local == nullptr)
		throw Poco::Exception("Failed to create GDateTime object.");

	return GDateTimePtr(local);
}


/// Create GDateTime object, at beginning of same day in passed time zone.
GDateTimePtr createSameDayInDifferentTimeZone(const GTimeZonePtr & p_tz, const GDateTimePtr & p_dt)
{
	GDateTime * dt = p_dt.get();

	gint year;
	gint month;
	gint day;
	g_date_time_get_ymd(dt, &year, &month, &day);

	GDateTime * local_dt = g_date_time_new(p_tz.get(), year, month, day, 0, 0, 0);
	if (local_dt == nullptr)
		throw Poco::Exception("Failed to create GDateTime object.");

	return GDateTimePtr(local_dt);
}

GDateTimePtr toNextDay(const GTimeZonePtr & p_tz, const GDateTimePtr & p_dt)
{
	GDateTime * dt = p_dt.get();
	if (dt == nullptr)
		throw Poco::Exception("Null pointer.");

	dt = g_date_time_add_days(dt, 1);
	if (dt == nullptr)
		throw Poco::Exception("Failed to create GDateTime object.");

	GDateTimePtr p_next_dt = GDateTimePtr(dt);
	GDateTime * next_dt = p_next_dt.get();

	gint year;
	gint month;
	gint day;
	g_date_time_get_ymd(next_dt, &year, &month, &day);

	dt = g_date_time_new(p_tz.get(), year, month, day, 0, 0, 0);
	if (dt == nullptr)
		throw Poco::Exception("Failed to create GDateTime object.");

	return GDateTimePtr(dt);
}

}
}


DateLUTImpl::DateLUTImpl(const std::string & time_zone_)
	: time_zone(time_zone_)
{
	size_t i = 0;
	time_t start_of_day = DATE_LUT_MIN;

	details::GTimeZonePtr p_tz = details::createGTimeZone(time_zone);
	details::GDateTimePtr p_dt = details::createSameDayInDifferentTimeZone(p_tz, details::createGDateTimeUTC(start_of_day));

	do
	{
		if (i > DATE_LUT_MAX_DAY_NUM)
			throw Poco::Exception("Cannot create DateLUTImpl: i > DATE_LUT_MAX_DAY_NUM.");

		GDateTime * dt = p_dt.get();

		start_of_day = g_date_time_to_unix(dt);

		gint year;
		gint month;
		gint day;
		g_date_time_get_ymd(dt, &year, &month, &day);

		Values & values = lut[i];
		values.year = year;
		values.month = month;
		values.day_of_month = day;
		values.day_of_week = g_date_time_get_day_of_week(dt);
		values.date = start_of_day;

		values.time_at_offset_change = 0;
		values.amount_of_offset_change = 0;

		/// If UTC offset was changed in previous day.
		if (i != 0)
		{
			auto amount_of_offset_change_at_prev_day = 86400 - (lut[i].date - lut[i - 1].date);
			if (amount_of_offset_change_at_prev_day)
			{
				lut[i - 1].amount_of_offset_change = amount_of_offset_change_at_prev_day;

				const auto utc_offset_at_beginning_of_day = g_date_time_get_utc_offset(
					details::createGDateTimeLocal(p_tz, lut[i - 1].date).get());

				/// Find a time (timestamp offset from beginning of day),
				///  when UTC offset was changed. Search is performed with 15-minute granularity, assuming it is enough.

				time_t time_at_offset_change = 900;
				while (time_at_offset_change < 65536)
				{
					auto utc_offset_at_current_time = g_date_time_get_utc_offset(
						details::createGDateTimeLocal(p_tz, lut[i - 1].date + time_at_offset_change).get());

					if (utc_offset_at_current_time != utc_offset_at_beginning_of_day)
						break;

					time_at_offset_change += 900;
				}

				lut[i - 1].time_at_offset_change = time_at_offset_change >= 65536 ? 0 : time_at_offset_change;

/*				std::cerr << lut[i - 1].year << "-" << int(lut[i - 1].month) << "-" << int(lut[i - 1].day_of_month)
					<< " offset was changed at " << lut[i - 1].time_at_offset_change << " for " << lut[i - 1].amount_of_offset_change << " seconds.\n";*/
			}
		}

		/// Going to next day.
		p_dt = details::toNextDay(p_tz, p_dt);
		++i;
	}
	while (start_of_day <= DATE_LUT_MAX);

	/// Заполняем lookup таблицу для годов
	::memset(years_lut, 0, DATE_LUT_YEARS * sizeof(years_lut[0]));
	for (size_t day = 0; day < i && lut[day].year <= DATE_LUT_MAX_YEAR; ++day)
	{
		if (lut[day].month == 1 && lut[day].day_of_month == 1)
			years_lut[lut[day].year - DATE_LUT_MIN_YEAR] = day;
	}

	offset_at_start_of_epoch = g_time_zone_get_offset(p_tz.get(), g_time_zone_find_interval(p_tz.get(), G_TIME_TYPE_UNIVERSAL, 0));
}
