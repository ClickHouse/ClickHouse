#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType_fwd.h>

namespace DB
{

/** Identifies a shared ordering used by comparison functions
  * Equal valid domains guarantee that comparisons between any member types use the same
  * counterpart-independent ordering and do not require a partial or throwing conversion.
  * The model is deliberately one domain per type: types that participate in several
  * orderings (e.g. IPv4, which compares both numerically and against IPv6) would need
  * domains-as-sets with intersection semantics and are conservatively left unchained.
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
        /// Keyed by the type itself: the domain matches only between types that are equal
        /// after normalization (`IDataType::equals`), so every node of a chain shares one
        /// type and its single canonical order. The type's comparisons may accept broader
        /// counterparts (e.g. a `Tuple` with different element names); those edges
        /// conservatively never join a chain.
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
