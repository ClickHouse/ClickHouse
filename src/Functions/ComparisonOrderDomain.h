#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType_fwd.h>

namespace DB
{

/** Identifies a shared ordering used by comparison functions
  * Equal valid domains guarantee that comparisons between any member types use the same
  * counterpart-independent ordering and do not require a partial or throwing conversion.
  * One domain per type: types living in several orders (e.g. IPv4, comparable both
  * numerically and against IPv6) are conservatively left unchained.
  */
struct ComparisonOrderDomain
{
    enum class Kind : UInt8
    {
        None,
        Number,
        String,
        Date,
        TimePoint,
        TimeOfDay,
        Decimal,
        FixedString,
        /// Keyed by the type itself: matches only types equal after normalization, so a chain
        /// shares one canonical order (broader counterparts may compare, but never chain).
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
