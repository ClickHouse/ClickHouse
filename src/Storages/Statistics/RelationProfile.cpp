#include <Storages/Statistics/RelationProfile.h>

namespace DB
{

void ColumnStats::addTransformation(ColumnStatsTransformation transformation)
{
    ndv_provenance.add(transformation);
    range_provenance.add(transformation);
}

namespace
{

/// Whether facts of `origin` can give a guarantee. Test-only overrides stand in for measured statistics, see
/// `ColumnStatsOrigin::SyntheticOverride`.
bool isTrustedOrigin(ColumnStatsOrigin origin)
{
    return origin == ColumnStatsOrigin::MeasuredStatistics || origin == ColumnStatsOrigin::SyntheticOverride;
}

}

std::optional<UInt64> ColumnStats::distinctCount(DistinctCountGuarantee guarantee) const
{
    if (!isTrustedOrigin(ndv_provenance.origin))
        return {};

    UInt16 kept_by = 0;
    switch (guarantee)
    {
        case DistinctCountGuarantee::Exact: break;
        case DistinctCountGuarantee::UpperBound: kept_by = RowSubset | FilterOnOtherColumns | NDVBoundExpression; break;
    }
    if (!ndv_provenance.hasOnly(kept_by))
        return {};
    return num_distinct_values;
}

std::optional<ColumnValueRange> ColumnStats::valueRange(ValueRangeGuarantee guarantee) const
{
    if (!min_value || !max_value || !isTrustedOrigin(range_provenance.origin))
        return {};

    UInt16 kept_by = 0;
    switch (guarantee)
    {
        case ValueRangeGuarantee::Exact: break;
        case ValueRangeGuarantee::Superset: kept_by = RowSubset | FilterOnOtherColumns; break;
        /// With the assumption described at `ValueRangeGuarantee::Representative`.
        case ValueRangeGuarantee::Representative: kept_by = FilterOnOtherColumns; break;
    }
    if (!range_provenance.hasOnly(kept_by))
        return {};

    ColumnValueRange result{.min = *min_value, .max = *max_value, .null_fraction = null_fraction};
    if (guarantee == ValueRangeGuarantee::Superset)
        result.null_fraction.reset();
    return result;
}

}
