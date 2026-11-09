#include <memory>
#include <ranges>
#include <vector>

#include <Core/Block.h>
#include <Core/Joins.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsUtils.h>
#include <fmt/ranges.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

#include <Processors/QueryPlan/Optimizations/Cascades/CascadesParams.h>
#include <pcg_random.hpp>

/*
 * This file contains helper functions for debugging and testing join optimization.
 * These functions allow developers to:
 * - Hard-code specific join orders for testing alternative execution plans
 * - Override table statistics through query parameters for performance testing
 * - Generate randomized statistics for join reordering stress testing
 *
 * Note: These functions are intended for development and testing purposes only,
 * not for production use.
 */
namespace DB
{

String dumpRelationStatsForLogs(const RelationStats & stats)
{
    auto dump_column = [](const auto & column)
    {
        const auto & [name, column_stats] = column;
        /// No min/max: they can be values of rows hidden by a row policy, and this line reaches clients with `send_logs_level`.
        std::vector<String> fields;
        if (column_stats.null_fraction)
            fields.push_back(fmt::format("null {}", *column_stats.null_fraction));
        if (column_stats.avg_bytes != 0)
            fields.push_back(fmt::format("avg_bytes {}", column_stats.avg_bytes));
        /// The provenance goes last: tests match the NDV and the optional `[...]` fields right after the column name.
        const String provenance = fmt::format(
            "(ndv: {}, range: {})", column_stats.ndv_provenance.toString(), column_stats.range_provenance.toString());
        if (fields.empty())
            return fmt::format("{}: {} {}", name, column_stats.num_distinct_values, provenance);
        return fmt::format("{}: {} [{}] {}", name, column_stats.num_distinct_values, fmt::join(fields, ", "), provenance);
    };
    return fmt::format(
        "{}: {} rows, columns: [{}]",
        stats.table_name.empty() ? "<unknown>" : stats.table_name,
        stats.estimated_rows ? fmt::format("{}", stats.estimated_rows.value()) : "unknown",
        fmt::join(stats.column_stats | std::views::transform(dump_column), ", "));
}

/* Read a table statistics hint from the query parameter.
 * The parameter should be a JSON object with the following structure:
 * SET param__internal_join_table_stat_hints = '{
 *   "table_name": { "cardinality": 1000, "distinct_keys": { "column_name": 100, ... }, "key_ranges": { "column_name": [0, 999] } },
 *   ...
 * }';
 */
RelationStats parseTableStatsHint(const String & stats_hint_json, const String & table_name)
{
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(stats_hint_json);
        const auto & object = result.extract<Poco::JSON::Object::Ptr>();
        if (!object)
            return {};

        if (!object->has(table_name))
            return {};

        auto stat_object = object->getObject(table_name);
        if (!stat_object)
            return {};

        RelationStats stats;
        stats.table_name = table_name;
        stats.imprecise_estimate = true;
        stats.source = RowEstimateSource::Hint;

        if (stat_object->has("cardinality"))
            stats.estimated_rows = stat_object->getValue<UInt64>("cardinality");

        if (stat_object->has("avg_row_bytes"))
            stats.avg_row_bytes = stat_object->getValue<double>("avg_row_bytes");

        if (stat_object->isObject("distinct_keys"))
        {
            auto distinct_keys = stat_object->getObject("distinct_keys");
            for (const auto & [key, value] : *distinct_keys)
            {
                auto & column_stats = stats.column_stats[key];
                column_stats.num_distinct_values = value.convert<UInt64>();
                column_stats.ndv_provenance.origin = ColumnStatsOrigin::SyntheticOverride;
            }
        }

        if (stat_object->isObject("column_bytes"))
        {
            auto column_bytes = stat_object->getObject("column_bytes");
            for (const auto & [key, value] : *column_bytes)
                stats.column_stats[key].avg_bytes = value.convert<Float64>();
        }

        /// `"key_ranges": { "column_name": [min, max] }`
        if (stat_object->isObject("key_ranges"))
        {
            auto key_ranges = stat_object->getObject("key_ranges");
            for (const auto & [key, value] : *key_ranges)
            {
                const auto & range = value.extract<Poco::JSON::Array::Ptr>();
                if (!range || range->size() != 2)
                    continue;
                auto & column_stats = stats.column_stats[key];
                column_stats.min_value = Field(range->get(0).convert<Float64>());
                column_stats.max_value = Field(range->get(1).convert<Float64>());
                /// Unlike a hinted distinct count, a hinted range gives no guarantee, see `SyntheticOverride`.
                column_stats.range_provenance.origin = ColumnStatsOrigin::SyntheticOverride;
            }
        }
        LOG_DEBUG(
            getLogger("optimizeJoin"),
            "Got a table statistics hint for table '{}' from '{}' query parameter, it is meant for testing only, do not use it in "
            "production",
            table_name,
            CascadesParams::STAT_HINTS);
        return stats;
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(getLogger("optimizeJoin"), "Failed to parse '{}': {}", CascadesParams::STAT_HINTS, e.displayText());
        return {};
    }
}

RelationStats parseTableStatsHint(ContextPtr context, const String & table_name)
{
    const auto & query_params = context->getQueryParameters();
    if (auto it = query_params.find(CascadesParams::STAT_HINTS); it != query_params.end())
        return parseTableStatsHint(it->second, table_name);
    return {};
}

RelationStats getRandomizedStats(UInt64 seed, size_t relation_index, const String & table_name, const Block & header)
{
    SipHash hasher;
    hasher.update(seed);
    hasher.update(relation_index);
    hasher.update(table_name);
    UInt64 hash = hasher.get64();

    RelationStats stats;
    stats.table_name = table_name;
    stats.estimated_rows = 1 + (hash % 10'000'000);
    stats.imprecise_estimate = true;
    stats.source = RowEstimateSource::Randomized;

    pcg64 rng(hash);
    for (const auto & col : header)
    {
        UInt64 ndv = 1 + (rng() % stats.estimated_rows.value());
        auto & column_stats = stats.column_stats[col.name];
        column_stats.num_distinct_values = ndv;
        column_stats.ndv_provenance.origin = ColumnStatsOrigin::SyntheticOverride;
    }

    LOG_DEBUG(
        getLogger("optimizeJoin"),
        "Randomized statistics for '{}': rows={}, columns={}",
        table_name,
        *stats.estimated_rows,
        stats.column_stats.size());
    return stats;
}

}
