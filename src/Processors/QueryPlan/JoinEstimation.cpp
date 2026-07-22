#include <Processors/QueryPlan/JoinEstimation.h>

#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

#include <fmt/format.h>

namespace DB
{

namespace
{

String rowsToString(const std::optional<UInt64> & value)
{
    return value ? fmt::format("{}", *value) : String("missing stats");
}

String costToString(const std::optional<double> & value)
{
    return value ? fmt::format("{:g}", *value) : String("missing stats");
}

String selectivityToString(const std::optional<double> & value)
{
    return value ? fmt::format("{:.4g}", *value) : String("missing stats");
}

}

void describeJoinEstimation(const JoinEstimation & estimation, WriteBuffer & out, const String & prefix)
{
    out << prefix << "Estimated rows: output " << rowsToString(estimation.output_rows)
        << " · left " << rowsToString(estimation.left_rows)
        << " · right " << rowsToString(estimation.right_rows) << '\n';
    out << prefix << "Estimated cost: " << costToString(estimation.cost) << '\n';
    out << prefix << "Estimated selectivity (NDV): " << selectivityToString(estimation.selectivity) << '\n';
}

void describeJoinEstimation(const JoinEstimation & estimation, JSONBuilder::JSONMap & map)
{
    map.add("Estimated output rows", rowsToString(estimation.output_rows));
    map.add("Estimated left rows", rowsToString(estimation.left_rows));
    map.add("Estimated right rows", rowsToString(estimation.right_rows));
    map.add("Estimated cost", costToString(estimation.cost));
    map.add("Estimated selectivity (NDV)", selectivityToString(estimation.selectivity));
}

}
