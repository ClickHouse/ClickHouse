#include <cstring>
#include <Yandex/DateLUT.h>
#include <Poco/Exception.h>


DateLUT::DateLUT()
{
	size_t i = 0;
	time_t start_of_day = DATE_LUT_MIN;

	do
	{
		if (i > DATE_LUT_MAX_DAY_NUM)
			throw Poco::Exception("Cannot create DateLUT: i > DATE_LUT_MAX_DAY_NUM.");

		tm time_descr;
		localtime_r(&start_of_day, &time_descr);

		time_descr.tm_hour = 0;
		time_descr.tm_min = 0;
		time_descr.tm_sec = 0;
		time_descr.tm_isdst = -1;

		start_of_day = mktime(&time_descr);

		Values & values = lut[i];

		values.year = time_descr.tm_year + 1900;
		values.month = time_descr.tm_mon + 1;
		values.day_of_week = time_descr.tm_wday == 0 ? 7 : time_descr.tm_wday;
		values.day_of_month = time_descr.tm_mday;

		values.date = start_of_day;

		/// Переходим на следующий день.
		++time_descr.tm_mday;

		/** Обратите внимание, что в 1981-1984 году в России,
		  * 1 апреля начиналось в час ночи, а не в полночь.
		  * Если здесь оставить час равным нулю, то прибавление единицы к дню, привело бы к 23 часам того же дня.
		  */
		time_descr.tm_hour = 12;
		start_of_day = mktime(&time_descr);

		++i;
	} while (start_of_day <= DATE_LUT_MAX);

	/// Заполняем lookup таблицу для годов
	memset(years_lut, 0, DATE_LUT_YEARS * sizeof(years_lut[0]));
	for (size_t day = 0; day < i && lut[day].year <= DATE_LUT_MAX_YEAR; ++day)
	{
		if (lut[day].month == 1 && lut[day].day_of_month == 1)
			years_lut[lut[day].year - DATE_LUT_MIN_YEAR] = day;
	}

	offset_at_start_of_epoch = 86400 - lut[findIndex(86400)].date;
}
