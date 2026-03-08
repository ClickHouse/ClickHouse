#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <DataTypes/IDataType.h>
#include <IO/Operators.h>
#include <base/defines.h>
#include <boost/algorithm/string/split.hpp>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
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
        << " estimated_bytes_per_row: " << estimated_bytes_per_row
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

Float64 estimateRowWidthFromHeader(const Block & header)
{
    static constexpr Float64 DEFAULT_STRING_SIZE = 64.0;
    static constexpr Float64 DEFAULT_COMPLEX_TYPE_SIZE = 128.0;
    static constexpr Float64 MIN_ROW_WIDTH = 8.0;

    Float64 total = 0;
    for (const auto & col : header)
    {
        const auto & type = col.type;
        if (type->haveMaximumSizeOfValue())
            total += Float64(type->getMaximumSizeOfValueInMemory());
        else if (type->getTypeId() == TypeIndex::String)
            total += DEFAULT_STRING_SIZE;
        else
            total += DEFAULT_COMPLEX_TYPE_SIZE;
    }

    return std::max(total, MIN_ROW_WIDTH);
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

    std::optional<Float64> getAvgRowBytes(const String & table_name) const override
    {
        std::lock_guard g(table_statistics_lock);
        fillParsedStatisticsIfNeeded(table_name);

        auto it = parsed_avg_row_bytes.find(table_name);
        if (it != parsed_avg_row_bytes.end())
            return it->second;
        return std::nullopt;
    }

private:
    void fillParsedStatisticsIfNeeded(const String & table_name) const TSA_REQUIRES(table_statistics_lock)
    {
        if (!parsed_table_statistics.contains(table_name))
        {
            parsed_table_statistics[table_name] = getDummyStats(statistics_hint_json, table_name);

            /// Parse avg_row_bytes from the hint JSON
            try
            {
                Poco::JSON::Parser parser;
                auto result = parser.parse(statistics_hint_json);
                const auto & object = result.extract<Poco::JSON::Object::Ptr>();
                if (object && object->has(table_name))
                {
                    auto stat_object = object->getObject(table_name);
                    if (stat_object && stat_object->has("avg_row_bytes"))
                        parsed_avg_row_bytes[table_name] = stat_object->getValue<Float64>("avg_row_bytes");
                }
            }
            catch (const Poco::Exception &) {} // NOLINT
        }
    }

    mutable std::mutex table_statistics_lock;
    mutable std::unordered_map<String, RelationStats> parsed_table_statistics TSA_GUARDED_BY(table_statistics_lock);
    mutable std::unordered_map<String, Float64> parsed_avg_row_bytes TSA_GUARDED_BY(table_statistics_lock);
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

    std::optional<Float64> getAvgRowBytes(const String & /*table_name*/) const override { return std::nullopt; }
};

OptimizerStatisticsPtr createEmptyStatistics()
{
    return std::make_unique<EmptyStatistics>();
}


namespace QueryPlanOptimizations
{
RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter);
}

/// Estimate bytes per row for a ReadFromMergeTree step using storage column sizes or output header.
static Float64 estimateReadBytesPerRowFromStep(const ReadFromMergeTree & read_step)
{
    auto total_rows_opt = read_step.getStorageSnapshot()->storage.totalRows(nullptr);
    auto column_sizes = read_step.getStorageSnapshot()->storage.getColumnSizes();
    if (total_rows_opt && *total_rows_opt > 0 && !column_sizes.empty())
    {
        Float64 total_bytes = 0;
        for (const auto & col_name : read_step.getAllColumnNames())
        {
            auto it = column_sizes.find(col_name);
            if (it != column_sizes.end())
                total_bytes += Float64(it->second.data_uncompressed);
        }
        if (total_bytes > 0)
            return total_bytes / Float64(*total_rows_opt);
    }
    return estimateRowWidthFromHeader(*read_step.getOutputHeader());
}

std::optional<ExpressionStatistics> estimateStatistics(QueryPlan::Node & node)
{
    std::optional<ExpressionStatistics> stats;

    auto * read_step = typeid_cast<ReadFromMergeTree *>(node.step.get());

    /// Only for ReadFromMergeTree or Filter -> ReadFromMergeTree
    if (read_step ||
        (typeid_cast<FilterStep *>(node.step.get()) && node.children.size() == 1 && typeid_cast<ReadFromMergeTree *>(node.children[0]->step.get())))
    {
        if (!read_step)
            read_step = typeid_cast<ReadFromMergeTree *>(node.children[0]->step.get());

        /// For Filter -> ReadFromMergeTree estimateReadRowsCount will recursively call itself for the child ReadFromMergeTree with the filter's expression
        auto relation_stats = QueryPlanOptimizations::estimateReadRowsCount(node, nullptr);
        if (relation_stats.estimated_rows)
        {
            stats.emplace();
            stats->estimated_row_count = Float64(*relation_stats.estimated_rows);
            stats->column_statistics = relation_stats.column_stats;
            stats->estimated_bytes_per_row = relation_stats.avg_row_bytes
                ? *relation_stats.avg_row_bytes
                : estimateReadBytesPerRowFromStep(*read_step);
        }
    }

    return stats;
}

}
