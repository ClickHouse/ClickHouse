#pragma once

#include <Common/DateLUTImpl.h>

#include <limits>

namespace DB::ORC
{

/// The UTC offset as in the reference implementation: before the first transition record it is the oldest
/// standard time, not the local mean time (Asia/Shanghai +08:00, not +08:05:43). Shared by reader and writer.
class TimezoneOffset
{
public:
    explicit TimezoneOffset(const DateLUTImpl & lut_);

    Int64 operator()(Int64 t) const { return t < first_transition ? ancient_offset : lut.timezoneOffset(t); }

private:
    const DateLUTImpl & lut;
    Int64 first_transition = std::numeric_limits<Int64>::min();
    Int64 ancient_offset = 0;
};

}
