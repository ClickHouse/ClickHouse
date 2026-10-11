#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <optional>


namespace DB
{

/// The range of the primary key `id` of the samples table and of the time ranges table covering all the time series of one metric.
/// If the identifiers are two-component tuples `Tuple(F, S)` and the id generator computes the first component from the metric name
/// alone (as the default generator does, see `TimeSeriesIDGenerator::getDefault`), the identifiers of the time series of one metric
/// occupy one continuous range: tuple(hash(metric_name), min_S) <= id <= tuple(hash(metric_name), max_S).
struct TimeSeriesIDRange
{
    String metric_name;
    ASTPtr first_component; /// hash(metric_name) as a constant expression, for example sipHash64('my_metric').
    ASTPtr min_second_component; /// The minimum and the maximum of the type of the second component.
    ASTPtr max_second_component;

    /// `<id> >= tuple(<first_component>, <min_S>) AND <id> <= tuple(<first_component>, <max_S>)`
    ASTPtr makeIDInRangeCondition(const ASTPtr & id) const;

    /// `tupleElement(<id>, 1) != <first_component>`: the identifier is outside the range.
    ASTPtr makeIDOutOfRangeCondition(const ASTPtr & id) const;
};

/// Makes the range of the identifiers of the metric selected by the matchers if the time series of the metric are known to occupy
/// one range (see TimeSeriesIDRange): the matchers must contain exactly one equality matcher on `__name__` with a non-empty value,
/// the id generator must be `tuple(<expression of the metric name only>, <anything>)`,
/// and the id type must be a two-component tuple whose second component has a type with known bounds.
std::optional<TimeSeriesIDRange> tryGetTimeSeriesIDRange(
    const PrometheusQueryTree::MatcherList & matchers, const ASTPtr & id_generator, const DataTypePtr & id_type);

}
