#pragma once

#include <common/types.h>
#include <common/StrongTypedef.h>

/** Represents number of days since 1970-01-01.
  * See DateLUTImpl for usage examples.
  */
using DayNum = StrongTypedef<UInt16, struct DayNumTag>;

/** Represent number of days since 1970-01-01 but in extended range,
 * for dates before 1970-01-01 and after 2105
 */
using ExtendedDayNum = StrongTypedef<Int32, struct ExtendedDayNumTag>;
