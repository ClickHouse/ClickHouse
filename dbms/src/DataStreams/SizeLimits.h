#pragma once

#include <common/Types.h>


namespace DB
{

/// What to do if the limit is exceeded.
enum class OverflowMode
{
    THROW     = 0,    /// Throw exception.
    BREAK     = 1,    /// Abort query execution, return what is.

    /** Only for GROUP BY: do not add new rows to the set,
      * but continue to aggregate for keys that are already in the set.
      */
    ANY       = 2,
};


struct SizeLimits
{
    /// If it is zero, corresponding limit check isn't performed.
    UInt64 max_rows = 0;
    UInt64 max_bytes = 0;
    OverflowMode overflow_mode = OverflowMode::THROW;

    SizeLimits() {}
    SizeLimits(UInt64 max_rows, UInt64 max_bytes, OverflowMode overflow_mode)
        : max_rows(max_rows), max_bytes(max_bytes), overflow_mode(overflow_mode) {}

    /// Check limits. If exceeded, return false or throw an exception, depending on overflow_mode.
    bool check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const;
};

}
