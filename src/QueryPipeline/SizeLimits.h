#pragma once

#include <base/types.h>


namespace DB
{

/// What to do if the limit is exceeded.
enum class OverflowMode : uint8_t
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

    SizeLimits() = default;
    SizeLimits(UInt64 max_rows_, UInt64 max_bytes_, OverflowMode overflow_mode_)
        : max_rows(max_rows_), max_bytes(max_bytes_), overflow_mode(overflow_mode_) {}

    /// Check limits. If exceeded, return false or throw an exception, depending on overflow_mode.
    bool check(UInt64 rows, UInt64 bytes, const char * what, int too_many_rows_exception_code, int too_many_bytes_exception_code) const;
    bool check(UInt64 rows, UInt64 bytes, const char * what, int exception_code) const;

    /// Check limits. No exceptions.
    bool softCheck(UInt64 rows, UInt64 bytes) const;

    bool hasLimits() const { return max_rows || max_bytes; }
};

}
