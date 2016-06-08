#include <common/DateLUTImpl.h>
#include <Poco/Exception.h>

#include <memory>
#include <cstring>
#include <glib.h>

namespace details { namespace {

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

GDateTimePtr createGDateTime(time_t timestamp)
{
	GDateTime * dt= g_date_time_new_from_unix_utc(timestamp);
	if (dt == nullptr)
		throw Poco::Exception("Failed to create GDateTime object.");

	return GDateTimePtr(dt);
}

GDateTimePtr createGDateTime(const GTimeZonePtr & p_tz, const GDateTimePtr & p_dt)
{
	GDateTime * dt = p_dt.get();
	if (dt == nullptr)
		throw Poco::Exception("Null pointer.");

	gint year;
	gint month;
	gint day;
	g_date_time_get_ymd(dt, &year, &month, &day);

	GDateTime * local_dt = g_date_time_new(p_tz.get(), year, month, day,
										   g_date_time_get_hour(dt),
										   g_date_time_get_minute(dt),
										   g_date_time_get_second(dt));
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

}}

DateLUTImpl::DateLUTImpl(const std::string & time_zone)
{
	details::GTimeZonePtr p_tz = details::createGTimeZone(time_zone);

	size_t i = 0;
	time_t start_of_day = DATE_LUT_MIN;

	details::GDateTimePtr p_dt = details::createGDateTime(start_of_day);

	p_dt = details::createGDateTime(p_tz, p_dt);

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

		/// Переходим на следующий день.
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
