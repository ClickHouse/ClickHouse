#pragma once

#include <base/types.h>
#include <limits>
#include <memory>
#include <optional>
#include <unordered_map>

namespace DB
{

class WriteBuffer;

struct ExpressionColumnStatistics
{
    UInt64 number_of_distinct_values;
};

struct ExpressionStatistics
{
    /// Number of row that we estimated using probabilities, histograms, heurisitcs, etc.
    Float64 estimated_row_count;
    /// Proven minimum number of rows
    UInt64 min_row_count = 0;
    /// Proven maximum number of rows. E.g. after LIMIT step
    UInt64 max_row_count = std::numeric_limits<UInt64>::max();

    /// Statistics for output columns of the expression
    std::unordered_map<String, ExpressionColumnStatistics> column_statistics;

    void dump(WriteBuffer & out) const;
    String dump() const;
};

class IOptimizerStatistics
{
public:
    virtual ~IOptimizerStatistics() = default;
    virtual std::optional<UInt64> getNumberOfDistinctValues(const String & table_name, const String & column_name) const = 0;
};

using OptimizerStatisticsPtr = std::unique_ptr<IOptimizerStatistics>;

/// TODO: this is a temporary hack until table names are properly handled
String getUnqualifiedColumnName(const String & full_column_name);

OptimizerStatisticsPtr createEmptyStatistics();
OptimizerStatisticsPtr createTPCH100Statistics();
OptimizerStatisticsPtr createStatisticsFromHint(const String & statistics_hint_json);

}
