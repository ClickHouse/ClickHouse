#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/Operators.h>
#include <base/defines.h>
#include <boost/algorithm/string/split.hpp>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
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
        out << "`" << column.first << "` NDV : " << column.second.num_distinct_values
            << " avg_bytes: " << column.second.avg_bytes << "\n";
}

String ExpressionStatistics::dump() const
{
    WriteBufferFromOwnString out;
    dump(out);
    return out.str();
}

/// Without a floor a zero-width row (e.g. a bare `count()`) would make exchanges look free.
static constexpr Float64 MIN_ROW_WIDTH = 1.0;

Float64 estimateColumnWidthFromType(const IDataType & type)
{
    static constexpr Float64 DEFAULT_STRING_SIZE = 64.0;
    static constexpr Float64 DEFAULT_COMPLEX_TYPE_SIZE = 128.0;

    if (type.haveMaximumSizeOfValue())
        return Float64(type.getMaximumSizeOfValueInMemory());
    if (const auto * agg_type = typeid_cast<const DataTypeAggregateFunction *>(&type))
        return Float64(agg_type->getFunction()->sizeOfData());
    if (type.getTypeId() == TypeIndex::String)
        return DEFAULT_STRING_SIZE;
    return DEFAULT_COMPLEX_TYPE_SIZE;
}

Float64 estimateRowWidthFromHeader(const Block & header)
{
    Float64 total = 0;
    for (const auto & column : header)
        total += estimateColumnWidthFromType(*column.type);

    return std::max(total, MIN_ROW_WIDTH);
}

Float64 estimateRowWidth(const Block & header, const std::unordered_map<String, ColumnStats> & column_statistics)
{
    Float64 total = 0;
    for (const auto & column : header)
    {
        auto it = column_statistics.find(column.name);
        if (it != column_statistics.end() && it->second.avg_bytes > 0)
            total += it->second.avg_bytes;
        else
            total += estimateColumnWidthFromType(*column.type);
    }

    return std::max(total, MIN_ROW_WIDTH);
}

RelationStats parseTableStatsHint(const String & dummy_stats_str, const String & table_name);

/// Statistics hint can be passed in JSON as query parameter. `avg_row_bytes` and `column_bytes`
/// (average bytes of one value per column) are optional:
///
/// SET param__internal_join_table_stat_hints = '{
///     "region": { "cardinality": 5, "distinct_keys": {
///         "r_regionkey" : 5,
///         "r_name" : 5,
///         "r_comment" : 5},
///       "avg_row_bytes": 120,
///       "column_bytes": { "r_name": 10 }
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

        return parsed_table_statistics[table_name].avg_row_bytes;
    }

    std::optional<Float64> getColumnAvgBytes(const String & table_name, const String & column_name) const override
    {
        std::lock_guard g(table_statistics_lock);
        fillParsedStatisticsIfNeeded(table_name);

        const auto & table_statistics = parsed_table_statistics[table_name];
        auto column_statistics = table_statistics.column_stats.find(column_name);
        if (column_statistics == table_statistics.column_stats.end() || column_statistics->second.avg_bytes == 0)
            return std::nullopt;
        return column_statistics->second.avg_bytes;
    }

private:
    void fillParsedStatisticsIfNeeded(const String & table_name) const TSA_REQUIRES(table_statistics_lock)
    {
        if (!parsed_table_statistics.contains(table_name))
            parsed_table_statistics[table_name] = parseTableStatsHint(statistics_hint_json, table_name);
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

    std::optional<Float64> getAvgRowBytes(const String & /*table_name*/) const override { return std::nullopt; }
};

OptimizerStatisticsPtr createEmptyStatistics()
{
    return std::make_unique<EmptyStatistics>();
}


std::unordered_map<String, Float64> estimateReadColumnWidths(const ReadFromMergeTree & read_step)
{
    const auto & storage = read_step.getStorageSnapshot()->storage;
    const auto total_rows_opt = storage.totalRows(nullptr);
    /// The named overload adds sizes of the requested subcolumns (`Map`/`JSON` reads).
    const auto column_sizes = storage.getColumnSizes(read_step.getAllColumnNames());
    const Float64 total_rows = (total_rows_opt && *total_rows_opt > 0) ? Float64(*total_rows_opt) : 0;

    /// Compact parts count in the row count and in the parts' column-data totals, but carry no
    /// per-column sizes. Sum only physical columns (a subcolumn's bytes are part of its parent's).
    Float64 measured_columns_bytes = 0;
    for (const auto & [_, size] : storage.getColumnSizes())
        measured_columns_bytes += Float64(size.data_uncompressed);
    /// Sum the parts' column-data sizes, which compact parts track as a total even though they
    /// carry no per-column split. `totalBytesUncompressed` would also count non-column files
    /// (e.g. statistics sketches), which can dwarf a small table's data and inflate its widths.
    Float64 total_bytes = 0;
    for (const auto & part : read_step.getMergeTreeData().getDataPartsVectorForInternalUsage())
        total_bytes += Float64(part->getTotalColumnsSize().data_uncompressed);

    /// With wide and compact parts mixed, dividing the wide-part-only column bytes by the full row
    /// count would understate every width; scale the measured widths up to the table total instead.
    Float64 measured_width_scale = 1.0;
    if (measured_columns_bytes > 0 && total_bytes > measured_columns_bytes)
        measured_width_scale = total_bytes / measured_columns_bytes;

    /// With no wide part at all, fall back to type-based estimates scaled so their sum matches the
    /// real row width.
    Float64 type_width_scale = 1.0;
    if (measured_columns_bytes == 0 && total_rows > 0 && total_bytes > 0)
    {
        Float64 type_width_sum = 0;
        for (const auto & column : read_step.getStorageSnapshot()->metadata->getColumns().getAllPhysical())
            type_width_sum += estimateColumnWidthFromType(*column.type);
        if (type_width_sum > 0)
            type_width_scale = (total_bytes / total_rows) / type_width_sum;
    }

    const auto & header = *read_step.getOutputHeader();
    std::unordered_map<String, Float64> widths;
    for (const auto & column_name : read_step.getAllColumnNames())
    {
        auto size_it = column_sizes.find(column_name);
        if (total_rows > 0 && size_it != column_sizes.end() && size_it->second.data_uncompressed > 0)
        {
            widths[column_name] = Float64(size_it->second.data_uncompressed) / total_rows * measured_width_scale;
            continue;
        }
        if (const auto * header_column = header.findByName(column_name))
            widths[column_name] = estimateColumnWidthFromType(*header_column->type) * type_width_scale;
    }
    return widths;
}

std::unordered_map<String, Float64> estimateReadColumnWidthsScaledToRow(const ReadFromMergeTree & read_step, Float64 row_bytes)
{
    Float64 type_width_sum = 0;
    for (const auto & column : read_step.getStorageSnapshot()->metadata->getColumns().getAllPhysical())
        type_width_sum += estimateColumnWidthFromType(*column.type);
    const Float64 scale = type_width_sum > 0 ? row_bytes / type_width_sum : 1.0;

    const auto & header = *read_step.getOutputHeader();
    std::unordered_map<String, Float64> widths;
    for (const auto & column_name : read_step.getAllColumnNames())
        if (const auto * header_column = header.findByName(column_name))
            widths[column_name] = estimateColumnWidthFromType(*header_column->type) * scale;
    return widths;
}

/// Estimate bytes per row of a read: the sum of the per-column widths, the output header as fallback.
Float64 estimateReadBytesPerRowFromStep(const ReadFromMergeTree & read_step)
{
    auto widths = estimateReadColumnWidths(read_step);
    if (widths.empty())
        return estimateRowWidthFromHeader(*read_step.getOutputHeader());

    Float64 total = 0;
    for (const auto & [_, width] : widths)
        total += width;
    return std::max(total, MIN_ROW_WIDTH);
}

namespace QueryPlanOptimizations
{

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr);

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

        /// estimateReadRowsCount handles FilterStep and PREWHERE sampling internally.
        auto relation_stats = QueryPlanOptimizations::estimateReadRowsCount(node);
        if (relation_stats.estimated_rows)
        {
            stats.emplace();
            stats->estimated_row_count = Float64(*relation_stats.estimated_rows);
            stats->column_statistics = relation_stats.column_stats;
            /// Hinted column widths are already in the stats; fill the rest so downstream width
            /// estimates (join, aggregation) know every column's size. A table-level width hint
            /// marks the parts as stand-ins, so it beats their real sizes.
            auto storage_widths = relation_stats.avg_row_bytes
                ? estimateReadColumnWidthsScaledToRow(*read_step, *relation_stats.avg_row_bytes)
                : estimateReadColumnWidths(*read_step);
            for (const auto & [column_name, width] : storage_widths)
            {
                auto & column_stats = stats->column_statistics[column_name];
                if (column_stats.avg_bytes == 0)
                    column_stats.avg_bytes = width;
            }
            stats->estimated_bytes_per_row = relation_stats.avg_row_bytes
                ? *relation_stats.avg_row_bytes
                : estimateRowWidth(*read_step->getOutputHeader(), stats->column_statistics);
            /// A read (with or without a filter) cannot emit more rows than the table holds. Stat
            /// hints can deliberately claim more rows than the table physically has (tiny tables
            /// standing in for big ones in tests), so never put the bound below the estimate.
            stats->max_row_count = std::max(stats->estimated_row_count,
                Float64(read_step->getStorageSnapshot()->storage.totalRows(nullptr)
                    .value_or(std::numeric_limits<UInt64>::max())));
        }
    }

    return stats;
}

}
