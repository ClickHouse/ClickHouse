#include <Yandex/time2str.h>
#include <Yandex/DateLUT.h>


namespace Yandex
{
	DateLUT::DateLUT()
	{
		time_t t, next_t;
		for (t = Yandex::Time2Date(DATE_LUT_MIN); t <= DATE_LUT_MAX; t = next_t)
		{
			/** Дополнительный вызов Time2Date для случая, когда в 1981-1984 году в России,
			  * 1 апреля начиналось в час ночи, не в полночь.
			  */
			next_t = Yandex::Time2Date(Yandex::TimeDayShift(t));
			Values & values = lut[next_t];

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
		}
	}

	time_t 		DateLUT::toDate(time_t t) 		const {	return lut.upper_bound(t)->second.date; }
	unsigned 	DateLUT::toMonth(time_t t) 		const {	return lut.upper_bound(t)->second.month; }
	unsigned 	DateLUT::toYear(time_t t) 		const {	return lut.upper_bound(t)->second.year; }
	unsigned 	DateLUT::toDayOfWeek(time_t t) 	const {	return lut.upper_bound(t)->second.day_of_week; }
	unsigned 	DateLUT::toDayOfMonth(time_t t) const {	return lut.upper_bound(t)->second.day_of_month; }
}
