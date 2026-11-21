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
    const auto origin = ndv_provenance.origin;
    const bool measured = origin == ColumnStatsOrigin::MeasuredStatistics || origin == ColumnStatsOrigin::SyntheticOverride;

    UInt16 kept_by = 0;
    switch (guarantee)
    {
        case DistinctCountGuarantee::Exact:
            /// An assumed distinct count deliberately errs upward, so it is not exact.
            if (!measured)
                return {};
            break;
        case DistinctCountGuarantee::UpperBound:
            /// Like a measured distinct count, an assumed one describes the rows it counts, see `ColumnStatsOrigin::AssumedDistinct`.
            if (!measured && origin != ColumnStatsOrigin::AssumedDistinct)
                return {};
            kept_by = RowSubset | NDVBoundExpression;
            break;
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
        case ValueRangeGuarantee::Exact:
        case ValueRangeGuarantee::Representative: break;
        case ValueRangeGuarantee::Superset: kept_by = RowSubset; break;
    }
    if (!range_provenance.hasOnly(kept_by))
        return {};

    ColumnValueRange result{.min = *min_value, .max = *max_value, .null_fraction = null_fraction};
    if (guarantee == ValueRangeGuarantee::Superset)
        result.null_fraction.reset();
    return result;
}

}
