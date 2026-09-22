#include <Processors/QueryPlan/QueryPlanToJSON.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/StepStatsJSONPrinter.h>
#include <Processors/QueryPlan/StepStatsStorage.h>
#include <IO/WriteBufferFromString.h>
#include <Common/StringUtils.h>
#include <base/types.h>

#include <memory>
#include <vector>


namespace DB
{

namespace
{

/// Adds description to the task. Like: `Filter column: ...`, `Sort description: ...`, `Limit ...`.
void addStepDetails(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const PrettyNames * plan_pretty_names)
{
    PrettyNames empty_pretty_names;
    WriteBufferFromOwnString out;

    IQueryPlanStep::FormatSettings settings{
        .out = out,
        .header_prefix = "",
        .detail_prefix = "",
        /// Compact and pretty are needed to avoid UNKNOWN_IDENTIFIER throw when using describeActions.
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
/// A miss is not a mistake: `ReadFromMerge` builds its child plans in `initializePipeline`, which
/// runs after `buildPrettyNamesPerPlan`, so a child plan reached here may have no entry.
const PrettyNames * findPrettyNames(const PrettyNamesPerPlan * pretty_names, const QueryPlan * plan)
{
    if (!pretty_names)
        return nullptr;

    auto it = pretty_names->names.find(plan);
    return it == pretty_names->names.end() ? nullptr : &it->second;
}

/// A serialized node, with the ids it needs to be cross-referenced: its own, and those of the
/// subqueries whose results its step consumes. Plain values -- nothing here points into the plan,
/// which the caller may drop before the document is assembled.
struct CollectedNode
{
    String id;
    std::vector<size_t> consumed_subquery_ids;
    std::unique_ptr<JSONBuilder::JSONMap> map;
};

CollectedNode makeNode(
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

    return {step.getUniqID(), step.getConsumedSubqueryIds(), std::move(map)};
}


/// Walks a plan and serializes every node, children included. Shared by the main document and by
/// the sub-plans that are serialized on their own.
std::vector<CollectedNode> collectNodes(
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

    std::vector<CollectedNode> collected;

    std::vector<Frame> stack;
    stack.push_back({&plan, plan.getRootNode()});

    while (!stack.empty())
    {
        auto frame = stack.back();
        stack.pop_back();

        if (!frame.node)
            continue;

        auto & step = *frame.node->step;
        auto collected_node = makeNode(
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

        collected_node.map->add("Children", std::move(children));
        collected.push_back(std::move(collected_node));
    }

    return collected;
}

}

std::string_view toString(SubPlanKind kind)
{
    switch (kind)
    {
        case SubPlanKind::Set: return "Set";
        case SubPlanKind::Scalar: return "Scalar";
    }
}

SerializedSubPlan serializeSubPlan(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    size_t subquery_id,
    SubPlanKind kind,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    SerializedSubPlan result;
    if (!plan.isInitialized() || !plan.getRootNode())
        return result;

    result.subquery_id = subquery_id;
    result.kind = kind;
    result.root_id = plan.getRootNode()->step->getUniqID();

    for (auto & node : collectNodes(plan, options, max_description_length, steps_to_stats, pretty_names))
    {
        // Says which subquery the node belongs to, so a reader walking the flat `Nodes` array can
        // tell it apart from the query's own steps without following `Children` from every root.
        node.map->add("SubPlanId", subquery_id);
        result.node_consumers.emplace_back(node.id, std::move(node.consumed_subquery_ids));
        result.nodes.push_back(std::move(node.map));
    }

    if (steps_to_stats)
    {
        result.execution_time_ns = steps_to_stats->getExecutionTimeNs();
        result.max_threads = steps_to_stats->getMaxThreads();
    }

    return result;
}


JSONBuilder::ItemPtr queryPlanToJSON(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsStorage * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names,
    std::vector<SerializedSubPlan> * sub_plans)
{
    /// Which nodes consume each subquery. Both ends carry the same assigned id, so this is a join
    /// over ids rather than a search for a name in rendered text: a step records the ids of the
    /// subqueries whose sets it reads (`IQueryPlanStep::getConsumedSubqueryIds`), and each captured
    /// sub-plan knows which subquery it is.
    std::unordered_map<size_t, std::vector<String>> consumers_by_subquery;

    const auto collect = [&](std::vector<CollectedNode> & nodes, JSONBuilder::JSONArray & into)
    {
        for (auto & node : nodes)
        {
            for (size_t id : node.consumed_subquery_ids)
                consumers_by_subquery[id].push_back(node.id);
            into.add(std::move(node.map));
        }
    };

    auto nodes_array = std::make_unique<JSONBuilder::JSONArray>();

    auto collected = collectNodes(plan, options, max_description_length, steps_to_stats, pretty_names);
    collect(collected, *nodes_array);

    /// Gathered before any entry is built, because a subquery can be consumed by a step in another
    /// sub-plan and the entries below read the finished map.
    if (sub_plans)
        for (const auto & sub_plan : *sub_plans)
            for (const auto & [node_id, ids] : sub_plan.node_consumers)
                for (size_t id : ids)
                    consumers_by_subquery[id].push_back(node_id);

    // Plans that ran for this query without being part of its tree, listed among the nodes so a
    // reader walks one array, and described at the root so their roots can be told from the main one.
    auto sub_plan_entries = std::make_unique<JSONBuilder::JSONArray>();
    if (sub_plans)
    {
        // Moved rather than copied: JSONBuilder items are not copyable, and `render` keeps its
        // result and releases the plan afterwards, so nothing serializes these a second time.
        for (auto & sub_plan : *sub_plans)
        {
            // Shaped like the root of the document, one level down: same keys, same meanings, for
            // the pipeline this subquery ran in. `ExecutionTimeNs` is that pipeline's own -- it ran
            // before the main one existed, so it is not part of the query's and the two do not sum.
            auto entry = std::make_unique<JSONBuilder::JSONMap>();
            entry->add("Id", sub_plan.subquery_id);
            entry->add("Kind", String(toString(sub_plan.kind)));
            entry->add("Root", sub_plan.root_id);

            /// Which steps use this subquery's result. Without it a reader sees a sub-plan that
            /// reads a large table and nothing saying what the query wanted it for. Absent when
            /// nothing in the document reads it -- a set used only by index analysis, say.
            if (const auto it = consumers_by_subquery.find(sub_plan.subquery_id); it != consumers_by_subquery.end())
            {
                auto consumers = std::make_unique<JSONBuilder::JSONArray>();
                for (const auto & node_id : it->second)
                    consumers->add(node_id);
                entry->add("ConsumedBy", std::move(consumers));
            }

            if (sub_plan.execution_time_ns)
                entry->add("ExecutionTimeNs", *sub_plan.execution_time_ns);
            if (sub_plan.max_threads)
                entry->add("MaxThreads", *sub_plan.max_threads);
            sub_plan_entries->add(std::move(entry));

            for (auto & node : sub_plan.nodes)
                nodes_array->add(std::move(node));
        }
    }

    auto result = std::make_unique<JSONBuilder::JSONMap>();
    result->add("Version", QUERY_PLAN_JSON_VERSION);
    result->add("Root", plan.getRootNode()->step->getUniqID());

    if (steps_to_stats)
    {
        result->add("ExecutionTimeNs", steps_to_stats->getExecutionTimeNs());
        result->add("MaxThreads", steps_to_stats->getMaxThreads());
    }

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

    if (sub_plans && !sub_plans->empty())
        result->add("SubPlans", std::move(sub_plan_entries));

    result->add("Nodes", std::move(nodes_array));

    return result;
}

}
