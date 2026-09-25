#include <Storages/Statistics/ColumnStatsProvenance.h>

namespace DB
{

/// Stable spelling used by optimizer trace diagnostics and tests.
String ColumnStatsProvenance::toString() const
{
    String result;
    switch (origin)
    {
        case ColumnStatsOrigin::Unknown: result = "unknown"; break;
        case ColumnStatsOrigin::PartStatistics: result = "part-statistics"; break;
        case ColumnStatsOrigin::SyntheticFallback: result = "synthetic-fallback"; break;
        case ColumnStatsOrigin::SyntheticOverride: result = "synthetic-override"; break;
        case ColumnStatsOrigin::ExactRowCount: result = "exact-row-count"; break;
    }

    if (!transformations)
        return result;

    result += '[';
    bool first = true;
    auto append = [&](ColumnStatsTransformation transformation, const char * name)
    {
        if (!has(transformation))
            return;
        if (!first)
            result += ',';
        result += name;
        first = false;
    };
    append(RowSubset, "row-subset");
    append(NonUniformRowSubset, "non-uniform-row-subset");
    append(ExactRowCountClamp, "exact-row-clamp");
    append(EstimatedRowCountClamp, "estimated-row-clamp");
    append(NDVBoundExpression, "ndv-bound-expression");
    append(ValuePreservingExpression, "value-preserving-expression");
    append(PartialPartCoverage, "partial-part-coverage");
    append(Unsupported, "unsupported");
    result += ']';
    return result;
}

}
