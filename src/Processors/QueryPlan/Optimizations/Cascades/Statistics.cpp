#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <IO/Operators.h>
#include <base/defines.h>
#include <boost/algorithm/string/split.hpp>
#include <mutex>
#include <optional>
#include <unordered_map>


namespace DB
{

void ExpressionStatistics::dump(WriteBuffer & out) const
{
    out << "estimated_rows: " << estimated_row_count
        << " min_rows: " << min_row_count
        << " max_rows: " << max_row_count
        << "\n";
    for (const auto & column : column_statistics)
        out << "`" << column.first << "` NDV : " << column.second.num_distinct_values << "\n";
}

String ExpressionStatistics::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

RelationStats getDummyStats(const String & dummy_stats_str, const String & table_name);

/// Statistics hint can be passed in JSON as query parameter:
///
/// SET param__internal_join_table_stat_hints = '{
///     "region": { "cardinality": 5, "distinct_keys": {
///         "r_regionkey" : 5,
///         "r_name" : 5,
///         "r_comment" : 5}
///     },
///     ...
/// }';
class StatisticsFromHint : public IOptimizerStatistics
{
public:
    explicit StatisticsFromHint(const String & statistics_hint_json_)
        : statistics_hint_json(statistics_hint_json_)
    {
    }

    std::optional<UInt64> getCardinality(const String & table_name) const override
    {
        std::lock_guard g(table_statistics_lock);
        fillParsedStatisticsIfNeeded(table_name);

        const auto & table_statistics = parsed_table_statistics[table_name];
        return table_statistics.estimated_rows;
    }

    std::optional<UInt64> getNumberOfDistinctValues(const String & table_name, const String & column_name) const override
    {
        std::lock_guard g(table_statistics_lock);
        fillParsedStatisticsIfNeeded(table_name);

        const auto & table_statistics = parsed_table_statistics[table_name];
        auto column_statistics = table_statistics.column_stats.find(column_name);
        if (column_statistics == table_statistics.column_stats.end())
            return std::nullopt;
        else
            return column_statistics->second.num_distinct_values;
    }

private:
    void fillParsedStatisticsIfNeeded(const String & table_name) const TSA_REQUIRES(table_statistics_lock)
    {
        if (!parsed_table_statistics.contains(table_name))
        {
            parsed_table_statistics[table_name] = getDummyStats(statistics_hint_json, table_name);
        }
    }

    mutable std::mutex table_statistics_lock;
    mutable std::unordered_map<String, RelationStats> parsed_table_statistics TSA_GUARDED_BY(table_statistics_lock);
    const String statistics_hint_json;
};

OptimizerStatisticsPtr createStatisticsFromHint(const String & statistics_hint_json)
{
    return std::make_unique<StatisticsFromHint>(statistics_hint_json);
}

class EmptyStatistics : public IOptimizerStatistics
{
public:
    std::optional<UInt64> getCardinality(const String & /*table_name*/) const override { return std::nullopt; }

    std::optional<UInt64> getNumberOfDistinctValues(const String & /*table_name*/, const String & /*column_name*/) const override { return std::nullopt; }
};

OptimizerStatisticsPtr createEmptyStatistics()
{
    return std::make_unique<EmptyStatistics>();
}

}
