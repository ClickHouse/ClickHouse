#include <Processors/QueryPlan/QueryPlanToJSON.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/StepStatsJSONPrinter.h>
#include <Processors/QueryPlan/StepStatsStorage.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>

#include <memory>
#include <vector>


namespace DB
{

namespace
{

/// What the step says about itself, as EXPLAIN ANALYZE prints it: `Filter column: ...`,
/// `Sort description: ...`, `Limit ...`. One array entry per line.
///
/// The structured form, describeActions(JSONBuilder::JSONMap &), is not an option: it rebuilds its
/// output from the step's ActionsDAG, and buildQueryPipeline has moved that out of every
/// ExpressionStep by the time an executed plan is serialized. Compact and pretty are pinned rather
/// than taken from the caller: they are the only settings under which the describe methods do not
/// read the DAG at all, and FilterStep would otherwise look its filter column up with
/// ActionsDAG::findInOutputs, which throws UNKNOWN_IDENTIFIER on the empty remains. Under pretty
/// the expressions come from the names captured while the DAGs were still intact.
void addStepDetails(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const PrettyNames * plan_pretty_names)
{
    PrettyNames empty_pretty_names;
    WriteBufferFromOwnString out;

    IQueryPlanStep::FormatSettings settings{
        .out = out,
        .header_prefix = "",
        .detail_prefix = "",
        .compact = true,
        .pretty = true,
        .pretty_names = plan_pretty_names ? plan_pretty_names->pretty_names : empty_pretty_names.pretty_names,
        .runtime_filter_names = plan_pretty_names ? plan_pretty_names->runtime_filter_names : empty_pretty_names.runtime_filter_names};

    step.describeActions(settings);

    auto details = std::make_unique<JSONBuilder::JSONArray>();

    const auto & text = out.str();
    size_t line_begin = 0;
    while (line_begin < text.size())
    {
        size_t line_end = text.find('\n', line_begin);
        if (line_end == String::npos)
            line_end = text.size();

        if (line_end > line_begin)
            details->add(text.substr(line_begin, line_end - line_begin));

        line_begin = line_end + 1;
    }

    map.add("Details", std::move(details));
}

/// The names are scoped per plan: a sub-plan is its own naming scope and has its own entry.
const PrettyNames * findPrettyNames(const PrettyNamesPerPlan * pretty_names, const QueryPlan * plan)
{
    if (!pretty_names)
        return nullptr;

    auto it = pretty_names->names.find(plan);
    return it == pretty_names->names.end() ? nullptr : &it->second;
}

std::unique_ptr<JSONBuilder::JSONMap> makeNode(
    const IQueryPlanStep & step,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNames * plan_pretty_names)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();

    /// `Node Type` and `Node Id` keep the names EXPLAIN json=1 gives them, so that a reader who
    /// knows one form recognises the other.
    map->add("Node Type", step.getName());
    map->add("Node Id", step.getUniqID());

    if (options.description)
    {
        std::string_view description = step.getStepDescription();

        /// Backs the view when trimming produces a new string, as the text renderer does.
        String pretty_description;
        if (options.pretty)
        {
            pretty_description = QueryPlanFormat::trimColumnIdentifier(description);
            description = pretty_description;
        }

        if (max_description_length)
            description = description.substr(0, max_description_length);

        if (!description.empty())
            map->add("Description", description);
    }

    if (options.actions)
        addStepDetails(step, *map, plan_pretty_names);

    if (options.indexes)
        step.describeIndexes(*map);

    if (options.projections)
        step.describeProjections(*map);

    if (steps_to_stats)
        map->add("Statistics", StepStatsJSONPrinter::toJSON(steps_to_stats->analyzeStep(&step)));

    return map;
}

}

JSONBuilder::ItemPtr queryPlanToJSON(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    struct Frame
    {
        const QueryPlan * plan = nullptr;
        QueryPlan::Node * node = nullptr;
    };

    auto nodes_array = std::make_unique<JSONBuilder::JSONArray>();

    std::vector<Frame> stack;
    stack.push_back({&plan, plan.getRootNode()});

    while (!stack.empty())
    {
        auto frame = stack.back();
        stack.pop_back();

        if (!frame.node)
            continue;

        auto & step = *frame.node->step;
        auto node_map = makeNode(
            step, options, max_description_length, steps_to_stats, findPrettyNames(pretty_names, frame.plan));

        auto children = std::make_unique<JSONBuilder::JSONArray>();

        for (auto * child : frame.node->children)
        {
            children->add(child->step->getUniqID());
            stack.push_back({frame.plan, child});
        }

        /// A sub-plan root is just another node here, referenced like any child. In the tree form
        /// it needed a second kind of nesting.
        for (auto * child_plan : step.getChildPlans())
        {
            if (!child_plan)
                continue;

            auto * child_root = child_plan->getRootNode();
            if (!child_root)
                continue;

            children->add(child_root->step->getUniqID());
            stack.push_back({child_plan, child_root});
        }

        node_map->add("Children", std::move(children));
        nodes_array->add(std::move(node_map));
    }

    auto result = std::make_unique<JSONBuilder::JSONMap>();
    result->add("Version", QUERY_PLAN_JSON_VERSION);
    result->add("Root", plan.getRootNode()->step->getUniqID());

    if (steps_to_stats)
        result->add("ExecutionTimeNs", steps_to_stats->getExecutionTimeNs());

    /// The columns the query produces. The text renderer prints these once above the tree rather
    /// than against a step, so they belong to the plan, not to any node.
    if (options.pretty)
    {
        auto output_array = std::make_unique<JSONBuilder::JSONArray>();

        const auto * root_pretty_names = findPrettyNames(pretty_names, &plan);
        PrettyNames empty_pretty_names;
        const auto & names
            = root_pretty_names ? root_pretty_names->pretty_names : empty_pretty_names.pretty_names;

        const auto & root_step = *plan.getRootNode()->step;
        if (root_step.hasOutputHeader() && root_step.getOutputHeader())
            for (const auto & column : *root_step.getOutputHeader())
                output_array->add(QueryPlanFormat::formatColumnPretty(column.name, names));

        result->add("Output", std::move(output_array));
    }

    result->add("Nodes", std::move(nodes_array));

    return result;
}

}
