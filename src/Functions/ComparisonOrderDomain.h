#pragma once

#include <Core/Types.h>

namespace DB
{

/** Identifies a shared ordering used by comparison functions
  * Equal valid domains guarantee that comparisons between any member types use the same
  * counterpart-independent ordering and do not require a partial or throwing conversion
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
        Decimal,
    };

    Kind kind = Kind::None;
    /// Tick scale for TimePoint (DateTime -> 0, DateTime64(s) -> s) and fraction scale for Decimal.
    /// Equal-scale values compare their underlying integers directly, without a throwing rescale.
    UInt32 scale = 0;

    bool isValid() const
    {
        return kind != Kind::None;
    }
    bool operator==(const ComparisonOrderDomain &) const = default;
};

}
