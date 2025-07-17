#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Core/Joins.h>
#include <ranges>
#include <memory>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

/*
 * This file contains helper functions for debugging and testing join optimization.
 * These functions allow developers to:
 * - Hard-code specific join orders for testing alternative execution plans
 * - Override table statistics through query parameters for performance testing
 *
 * Note: These functions are intended for development and testing purposes only,
 * not for production use.
 */
namespace DB
{

/* Read dummy stats from query parameter
 * The parameter should be a JSON object with the following structure:
 * SET param__internal_join_table_stat_hints = '{
 *   "table_name": { "cardinality": 1000, "distinct_keys": { "column_name": 100, ... } },
 *   ...
 * }';
 */
RelationStats getDummyStats(ContextPtr context, const String & table_name)
{
    constexpr auto param_name = "_internal_join_table_stat_hints";

    const auto & query_params = context->getQueryParameters();
    auto it = query_params.find(param_name);
    if (it == query_params.end())
        return {};

    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(it->second);
        auto object = result.extract<Poco::JSON::Object::Ptr>();
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
        LOG_WARNING(getLogger("optimizeJoin"),
            "Got dummy join stats for table '{}' from '{}' query parameter, it's supposed to be used only for testing, do not use it in production",
            table_name, param_name);
        return stats;
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(getLogger("optimizeJoin"), "Failed to parse '{}': {}", param_name, e.displayText());
        return {};
    }
}

}
