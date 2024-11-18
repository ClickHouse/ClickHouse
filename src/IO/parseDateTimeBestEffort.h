#pragma once
#include <stddef.h>
#include <time.h>

#include <Core/Types.h>

class DateLUTImpl;

namespace DB
{

class ReadBuffer;

/** https://xkcd.com/1179/
  *
  * The existence of this function is an example of bad practice
  *  and contradicts our development principles.
  *
  * This function will recognize the following patterns:
  *
  * NNNNNNNNNN - 9..10 digits is a unix timestamp
  *
  * YYYYMMDDhhmmss - 14 numbers is always interpreted this way
  *
  * YYYYMMDD - 8 digits in a row
  * YYYY*MM*DD - or with any delimiter after first 4-digit year component and after month.
  *
  * DD/MM/YY
  * DD/MM/YYYY - when '/' separator is used, these are the only possible forms
  *
  * hh:mm:ss - when ':' separator is used, it is always time
  * hh:mm - it can be specified without seconds
  *
  * YYYY - 4 digits is always year
  *
  * YYYYMM - 6 digits is a year, month if year was not already read
  * hhmmss - 6 digits is a time if year was already read
  *
  * .nnnnnnn - any number of digits after point is fractional part of second, if it is not YYYY.MM.DD or DD.MM.YYYY
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
  * AM/PM - AM means: subtract 12 hours if a value is 12 and PM means: add 12 hours if a value is less than 12.
  *
  * Jan/Feb/Mar/Apr/May/Jun/Jul/Aug/Sep/Oct/Nov/Dec - allowed to specify month
  * Mon/Tue/Wed/Thu/Fri/Sat/Sun - simply ignored.
  */

void parseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
bool tryParseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
void parseDateTimeBestEffortUS(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
bool tryParseDateTimeBestEffortUS(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
void parseDateTime64BestEffort(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
bool tryParseDateTime64BestEffort(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
void parseDateTime64BestEffortUS(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);
bool tryParseDateTime64BestEffortUS(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone);

/// More strict version of best effort parsing. Requires day, month and year to be present, checks for allowed
/// delimiters between date components, makes additional correctness checks. Used in schema inference if date times.
bool tryParseDateTimeBestEffortStrict(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters);
bool tryParseDateTimeBestEffortUSStrict(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters);
bool tryParseDateTime64BestEffortStrict(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters);
bool tryParseDateTime64BestEffortUSStrict(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters);

}
