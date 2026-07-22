#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType_fwd.h>

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
        TimeOfDay,
        Decimal,
        /// The type accepts only its own exact type as a comparison counterpart, so its
        /// single per-type order is trivially counterpart-independent
        ExactType,
    };

    Kind kind = Kind::None;
    /// Tick scale for TimePoint (DateTime -> 0, DateTime64(s) -> s), TimeOfDay (Time -> 0,
    /// Time64(s) -> s) and fraction scale for Decimal. Equal-scale values compare their
    /// underlying integers directly, without a throwing rescale.
    UInt32 scale = 0;
    /// The single member type of an ExactType domain (UUID, a concrete Enum, a concrete
    /// FixedString width, ...); unset for the other kinds
    DataTypePtr exact_type = nullptr;

    bool isValid() const
    {
        return kind != Kind::None;
    }
    bool operator==(const ComparisonOrderDomain & other) const;
};

}
