#include <Interpreters/Aggregator.h>

#include <Common/JSONBuilder.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>

#include <fmt/ranges.h>

/// Separate from Aggregator.cpp so that changes of the plan formatting headers do not recompile it.

namespace DB
{

void Aggregator::Params::explain(ExplainFormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    auto & out = settings.out;

    out << prefix << "Keys:";
    bool first = true;
    for (const auto & key : keys)
    {
        out << (first ? " " : ", ");
        first = false;
        out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(key, settings.pretty_names) : key);
    }
    out << '\n';

    if (!aggregates.empty())
    {
        if (settings.pretty)
        {
            out << prefix << "Aggregates:";
            first = true;
            for (const auto & aggregate : aggregates)
            {
                out << (first ? " " : ", ");
                first = false;
                aggregate.explainPretty(settings);
            }
            out << '\n';
        }
        else
        {
            out << prefix << "Aggregates:\n";
            for (const auto & aggregate : aggregates)
                aggregate.explain(out, prefix, 4);
        }
    }

    if (top_k)
        out << fmt::format(
            "{}Top-K: limit={}, columns={}, directions=[{}]\n", prefix, top_k->k, top_k->key_columns, fmt::join(top_k->directions, ","));
}

void Aggregator::Params::explain(JSONBuilder::JSONMap & map) const
{
    auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & key : keys)
        keys_array->add(key);

    map.add("Keys", std::move(keys_array));

    if (!aggregates.empty())
    {
        auto aggregates_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & aggregate : aggregates)
        {
            auto aggregate_map = std::make_unique<JSONBuilder::JSONMap>();
            aggregate.explain(*aggregate_map);
            aggregates_array->add(std::move(aggregate_map));
        }

        map.add("Aggregates", std::move(aggregates_array));
    }

    if (top_k)
    {
        auto top_k_map = std::make_unique<JSONBuilder::JSONMap>();
        top_k_map->add("Limit", top_k->k);
        top_k_map->add("Columns", top_k->key_columns);
        auto directions = std::make_unique<JSONBuilder::JSONArray>();
        for (int direction : top_k->directions)
            directions->add(direction);
        top_k_map->add("Directions", std::move(directions));
        map.add("Top-K", std::move(top_k_map));
    }
}

}
