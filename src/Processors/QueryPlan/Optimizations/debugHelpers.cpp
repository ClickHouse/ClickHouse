#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Core/Joins.h>
#include <ranges>
#include <memory>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Common/SipHash.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>

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

constexpr auto DUMMY_JOIN_STATS_PARAM_NAME = "_internal_join_table_stat_hints";

/* Read dummy stats from query parameter
 * The parameter should be a JSON object with the following structure:
 * SET param__internal_join_table_stat_hints = '{
 *   "table_name": { "cardinality": 1000, "distinct_keys": { "column_name": 100, ... } },
 *   ...
 * }';
 */
RelationStats getDummyStats(const String & dummy_stats_str, const String & table_name)
{
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(dummy_stats_str);
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

        if (stat_object->has("cardinality"))
            stats.estimated_rows = stat_object->getValue<UInt64>("cardinality");

        if (stat_object->isObject("distinct_keys"))
        {
            auto distinct_keys = stat_object->getObject("distinct_keys");
            for (const auto & [key, value] : *distinct_keys)
                stats.column_stats[key].num_distinct_values = value.convert<UInt64>();
        }
        LOG_DEBUG(getLogger("optimizeJoin"),
            "Got dummy join stats for table '{}' from '{}' query parameter, it's supposed to be used only for testing, do not use it in production",
            table_name, DUMMY_JOIN_STATS_PARAM_NAME);
        return stats;
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(getLogger("optimizeJoin"), "Failed to parse '{}': {}", DUMMY_JOIN_STATS_PARAM_NAME, e.displayText());
        return {};
    }
}

RelationStats getDummyStats(ContextPtr context, const String & table_name)
{
    const auto & query_params = context->getQueryParameters();
    if (auto it = query_params.find(DUMMY_JOIN_STATS_PARAM_NAME); it != query_params.end())
        return getDummyStats(it->second, table_name);
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

    pcg64 rng(hash);
    for (const auto & col : header)
    {
        UInt64 ndv = 1 + (rng() % stats.estimated_rows.value());
        stats.column_stats[col.name] = ColumnStats{.num_distinct_values = ndv};
    }

    LOG_DEBUG(getLogger("optimizeJoin"), "Randomized statistics for '{}': rows={}, columns={}",
        table_name, *stats.estimated_rows, stats.column_stats.size());
    return stats;
}

}
