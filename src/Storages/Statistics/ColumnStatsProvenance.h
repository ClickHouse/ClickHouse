#pragma once

#include <base/types.h>

namespace DB
{

/// Source of a column fact before relational and expression transformations are applied.
enum class ColumnStatsOrigin : UInt8
{
    Unknown,
    PartStatistics,
    SyntheticFallback,
    /// Produced by the runtime-filter planner (row-count-derived NDV bound); not emitted here yet.
    ExactRowCount,
};

/// Facts accumulated while a column statistic is propagated to a consumer. These flags are
/// deliberately unordered: consumers ask whether a transformation happened, not when it did.
enum ColumnStatsTransformation : UInt16
{
    RowSubset = 1 << 0,
    NonUniformRowSubset = 1 << 1,
    ExactRowCountClamp = 1 << 2,
    EstimatedRowCountClamp = 1 << 3,
    NDVBoundExpression = 1 << 4,
    ValuePreservingExpression = 1 << 5,
    PartialPartCoverage = 1 << 6,
    Unsupported = 1 << 7,
};

struct ColumnStatsProvenance
{
    ColumnStatsOrigin origin{};
    UInt16 transformations = 0;

    void add(ColumnStatsTransformation transformation) { transformations |= transformation; }
    bool has(ColumnStatsTransformation transformation) const { return transformations & transformation; }
    bool hasAny(UInt16 mask) const { return transformations & mask; }
    bool hasOnly(UInt16 allowed) const { return (transformations & ~allowed) == 0; }

    String toString() const;
};

}
