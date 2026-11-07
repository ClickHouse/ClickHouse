#include <Storages/Statistics/ColumnStatsProvenance.h>

namespace DB
{

String ColumnStatsProvenance::toString() const
{
    String result;
    switch (origin)
    {
        case ColumnStatsOrigin::Unknown: result = "unknown"; break;
        case ColumnStatsOrigin::MeasuredStatistics: result = "measured-statistics"; break;
        case ColumnStatsOrigin::SyntheticFallback: result = "synthetic-fallback"; break;
        case ColumnStatsOrigin::SyntheticOverride: result = "synthetic-override"; break;
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
    append(EstimatedRowCountClamp, "estimated-row-clamp");
    append(NDVBoundExpression, "ndv-bound-expression");
    append(PartialStatisticsCoverage, "partial-statistics-coverage");
    append(Unmodeled, "unmodeled");
    result += ']';
    return result;
}

}
