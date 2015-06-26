#include <cstring>
#include <Yandex/DateLUTImpl.h>
#include <Poco/Exception.h>
#include <glib.h>
#include <memory>

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

	GDateTime * local_dt = g_date_time_new(p_tz.get(),
										   g_date_time_get_year(dt),
										   g_date_time_get_month(dt),
										   g_date_time_get_day_of_month(dt),
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
		throw Poco::Exception("Null pointer");

	dt = g_date_time_new(p_tz.get(),
						 g_date_time_get_year(dt),
						 g_date_time_get_month(dt),
						 g_date_time_get_day_of_month(dt),
						 0, 0, 0);
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

		Values & values = lut[i];

		values.year = g_date_time_get_year(dt);
		values.month = g_date_time_get_month(dt);
		values.day_of_week = g_date_time_get_day_of_week(dt);
		values.day_of_month = g_date_time_get_day_of_month(dt);
		values.date = start_of_day;

		/// Переходим на следующий день.
		p_dt = details::toNextDay(p_tz, p_dt);
		++i;
	}
	while (start_of_day <= DATE_LUT_MAX);

	/// Заполняем lookup таблицу для годов
	memset(years_lut, 0, DATE_LUT_YEARS * sizeof(years_lut[0]));
	for (size_t day = 0; day < i && lut[day].year <= DATE_LUT_MAX_YEAR; ++day)
	{
		if (lut[day].month == 1 && lut[day].day_of_month == 1)
			years_lut[lut[day].year - DATE_LUT_MIN_YEAR] = day;
	}

	offset_at_start_of_epoch = g_time_zone_get_offset(p_tz.get(), g_time_zone_find_interval(p_tz.get(), G_TIME_TYPE_UNIVERSAL, 0));
}
