#pragma once

#include <base/types.h>
#include <base/strong_typedef.h>

/** Represents number of days since 1970-01-01.
  * See DateLUTImpl for usage examples.
  */
STRONG_TYPEDEF(UInt16, DayNum)

/** Represent number of days since 1970-01-01 but in extended range,
 * for dates before 1970-01-01 and after 2105
 */
STRONG_TYPEDEF(Int32, ExtendedDayNum)
