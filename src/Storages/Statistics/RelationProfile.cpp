#include <Storages/Statistics/RelationProfile.h>

namespace DB
{

void ColumnStats::addTransformation(ColumnStatsTransformation transformation)
{
    ndv_provenance.add(transformation);
    range_provenance.add(transformation);
}

std::optional<UInt64> ColumnStats::distinctCount(DistinctCountGuarantee guarantee) const
{
    if (ndv_provenance.origin != ColumnStatsOrigin::MeasuredStatistics && ndv_provenance.origin != ColumnStatsOrigin::SyntheticOverride)
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
    if (!min_value || !max_value || range_provenance.origin != ColumnStatsOrigin::MeasuredStatistics)
        return {};

    UInt16 kept_by = 0;
    switch (guarantee)
    {
        case ValueRangeGuarantee::Exact: break;
        case ValueRangeGuarantee::Superset: kept_by = RowSubset | FilterOnOtherColumns; break;
        /// Under the assumption that the column is independent of the columns the predicates read, NULLs included.
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
