#include <Yandex/time2str.h>
#include <Yandex/DateLUT.h>


namespace Yandex
{
	DateLUT::DateLUT()
	{
		/** Дополнительный вызов Time2Date для случая, когда в 1981-1984 году в России,
		  * 1 апреля начиналось в час ночи, не в полночь.
		  */
		for (time_t t = Yandex::Time2Date(DATE_LUT_MIN);
			t <= DATE_LUT_MAX;
			t = Yandex::Time2Date(Yandex::TimeDayShift(t)))
		{
			Values values;

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

			lut.push_back(values);
		}
	}
}
