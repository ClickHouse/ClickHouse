#include <stddef.h>


class DateLUTImpl;

namespace DB
{

class ReadBuffer;

/** This function will use the following assumptions:
  *
  * NNNNNNNNNN - 9..10 digits is a unix timestamp
  *
  * YYYYMMDDhhmmss - 14 numbers is always interpreted this way
  *
  * YYYYMMDD - 8 digits in a row
  * YYYY*MM*DD - or with any delimiters after first 4-digit year component
  *
  * DD/MM/YY
  * DD/MM/YYYY - when '/' separator is used, these are the only possible forms
  * Note that American style is not supported.
  *
  * hh:mm:ss - when ':' separator is used, it is always time
  * hh:mm - it can be specified without seconds
  *
  * YYYY - 4 digits is always year
  *
  * YYYYMM - 6 digits is a year, month if year was not already read
  * hhmmss - 6 digits is a time if year was already read
  *
  * .nnnnnnn - any number of digits after point is fractional part of second.
  *
  * T - means that time will follow
  *
  * Z - means zero UTC offset
  *
  * +hhmm
  * +hh:mm
  * +hh
  * -... - time zone offset
  *
  * single whitespace can be used as a separator
  *
  * AM/PM
  *
  * Jan/Feb/Mar/Apr/May/Jun/Jul/Aug/Sep/Oct/Nov/Dec
  * Mon/Tue/Wed/Thu/Fri/Sat/Sun
  *
  * AD/BC - not used
  */

void parseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
bool tryParseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);

}
