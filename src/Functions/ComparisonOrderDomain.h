#pragma once

#include <Core/Types.h>

namespace DB
{

/** Identifies a shared ordering used by comparison functions.
  * Equal valid domains guarantee that comparisons between any member types use the same
  * counterpart-independent ordering and do not require a partial or throwing conversion.
  */
struct ComparisonOrderDomain
{
    enum class Kind : UInt8
    {
        None,
        NativeNumber,
        String,
        Date,
        TimePoint,
    };

    Kind kind = Kind::None;
    /// Tick scale for `TimePoint`: `DateTime` uses scale 0 and `DateTime64(s)` uses scale `s`.
    UInt32 scale = 0;

    bool isValid() const
    {
        return kind != Kind::None;
    }
    bool operator==(const ComparisonOrderDomain &) const = default;
};

}
