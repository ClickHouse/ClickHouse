#include <Yandex/time2str.h>
#include <Yandex/DateLUT.h>
#include <Poco/Exception.h>


DateLUT::DateLUT()
{
	/** Дополнительный вызов Time2Date для случая, когда в 1981-1984 году в России,
	  * 1 апреля начиналось в час ночи, не в полночь.
	  */
	size_t i = 0;
	for (time_t t = Time2Date(DATE_LUT_MIN);
		t <= DATE_LUT_MAX;
		t = Time2Date(TimeDayShift(t)))
	{
		if (i > DATE_LUT_MAX_DAY_NUM)
			throw Poco::Exception("Cannot create DateLUT: i > DATE_LUT_MAX_DAY_NUM.");

		Values & values = lut[i];

		struct tm tm;
		localtime_r(&t, &tm);

		values.year = tm.tm_year + 1900;
		values.month = tm.tm_mon + 1;
		values.day_of_week = tm.tm_wday == 0 ? 7 : tm.tm_wday;
		values.day_of_month = tm.tm_mday;

		tm.tm_hour = 0;
		tm.tm_min = 0;
		tm.tm_sec = 0;
		tm.tm_isdst = -1;

		values.date = mktime(&tm);

		++i;
	}

	/// Заполняем lookup таблицу для годов
	memset(years_lut, 0, DATE_LUT_YEARS * sizeof(years_lut[0]));
	for (size_t day = 0; day < i && lut[day].year <= DATE_LUT_MAX_YEAR; ++day)
	{
		if (lut[day].month == 1 && lut[day].day_of_month == 1)
			years_lut[lut[day].year - DATE_LUT_MIN_YEAR] = day;
	}

	offset_at_start_of_epoch = 86400 - lut[findIndex(86400)].date;
}
