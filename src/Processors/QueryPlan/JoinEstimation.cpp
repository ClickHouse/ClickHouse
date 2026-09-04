#include <Processors/QueryPlan/JoinEstimation.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>

#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Common/formatReadable.h>

#include <fmt/format.h>

namespace DB
{

String rowsEstimateToString(const std::optional<UInt64> & value)
{
    return value ? formatReadableQuantity(static_cast<double>(*value)) : String(missingValueText(MetricKey::RowsEstimated));
}

String costEstimateToString(const std::optional<double> & value)
{
    return value ? formatReadableQuantity(*value) : String(missingValueText(MetricKey::Estimated));
}

String selectivityEstimateToString(const std::optional<double> & value)
{
    return value ? fmt::format("{:.4g}", *value) : String(missingValueText(MetricKey::EstimatedNDV));
}

/// Uses the same group and metric names as the EXPLAIN ANALYZE report of the join,
/// so both explains stay in one format.
void describeJoinEstimation(const JoinEstimation & estimation, WriteBuffer & out, const String & prefix)
{
    out << prefix << toString(MetricGroupKey::Cost) << ": "
        << toString(MetricKey::Estimated) << ' ' << costEstimateToString(estimation.cost) << '\n';
    out << prefix << toString(MetricGroupKey::Selectivity) << ": "
        << toString(MetricKey::EstimatedNDV) << ' ' << selectivityEstimateToString(estimation.selectivity) << '\n';
    out << prefix << toString(MetricGroupKey::Output) << ": "
        << toString(MetricKey::Estimated) << ' ' << rowsEstimateToString(estimation.output_rows) << '\n';
    out << prefix << toString(MetricGroupKey::Left) << ": "
        << toString(MetricKey::RowsEstimated) << ' ' << rowsEstimateToString(estimation.left_rows) << '\n';
    out << prefix << toString(MetricGroupKey::Right) << ": "
        << toString(MetricKey::RowsEstimated) << ' ' << rowsEstimateToString(estimation.right_rows) << '\n';
}

}
