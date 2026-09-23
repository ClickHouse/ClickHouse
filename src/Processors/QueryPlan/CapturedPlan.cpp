#include <Processors/QueryPlan/CapturedPlan.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/StepStatsCollector.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>

#include <memory>
#include <vector>


namespace DB
{

namespace
{

/// The step's own account of itself, one string per line. Like: `Filter column: ...`,
/// `Sort description: ...`, `Limit ...`.
std::vector<String> stepDetails(const IQueryPlanStep & step, const PrettyNames * plan_pretty_names)
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

    std::vector<String> details;

    const auto & text = out.str();
    size_t line_begin = 0;
    while (line_begin < text.size())
    {
        size_t line_end = text.find('\n', line_begin);
        if (line_end == String::npos)
            line_end = text.size();

        if (line_end > line_begin)
            details.push_back(text.substr(line_begin, line_end - line_begin));

        line_begin = line_end + 1;
    }

    return details;
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

/// Everything the document will need about one step, read off the step and its pipeline now and
/// held as values. `children` is filled by the walk below, which is what knows the shape.
CapturedStep captureStep(
    const IQueryPlanStep & step,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsCollector * steps_to_stats,
    const PrettyNames * plan_pretty_names)
{
    CapturedStep captured;
    captured.id = step.getUniqID();
    captured.type = step.getName();
    captured.consumed_subquery_ids = step.getConsumedSubqueryIds();

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

        captured.description = description;
    }

    if (options.actions)
        captured.details = stepDetails(step, plan_pretty_names);

    /// Only a `ReadFromMergeTree` analyses indexes, and the analysis is data it already holds --
    /// `describeIndexes` is one rendering of it, this capture is another.
    if (const auto * read_from_merge_tree = dynamic_cast<const ReadFromMergeTree *>(&step))
    {
        if (options.indexes)
            captured.indexes = read_from_merge_tree->getIndexStats();
        if (options.projections)
            captured.projections = read_from_merge_tree->getProjectionStats();
    }

    if (steps_to_stats)
        captured.statistics = steps_to_stats->analyzeStep(&step);

    return captured;
}


/// Walks a plan and captures every node, children included. Shared by the query's own plan and by
/// the sub-plans that are captured on their own.
std::vector<CapturedStep> captureNodes(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsCollector * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    struct Frame
    {
        const QueryPlan * plan = nullptr;
        QueryPlan::Node * node = nullptr;
    };

    std::vector<CapturedStep> collected;

    std::vector<Frame> stack;
    stack.push_back({&plan, plan.getRootNode()});

    while (!stack.empty())
    {
        auto frame = stack.back();
        stack.pop_back();

        if (!frame.node)
            continue;

        auto & step = *frame.node->step;
        auto captured = captureStep(
            step, options, max_description_length, steps_to_stats, findPrettyNames(pretty_names, frame.plan));

        for (auto * child : frame.node->children)
        {
            captured.children.push_back(child->step->getUniqID());
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

            captured.children.push_back(child_root->step->getUniqID());
            stack.push_back({child_plan, child_root});
        }

        collected.push_back(std::move(captured));
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

CapturedSubPlan captureSubPlanData(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    size_t subquery_id,
    SubPlanKind kind,
    const StepStatsCollector * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    CapturedSubPlan result;
    if (!plan.isInitialized() || !plan.getRootNode())
        return result;

    result.subquery_id = subquery_id;
    result.kind = kind;
    result.root_id = plan.getRootNode()->step->getUniqID();
    result.nodes = captureNodes(plan, options, max_description_length, steps_to_stats, pretty_names);

    /// Says which subquery each node belongs to, so a reader walking the flat `Nodes` array can
    /// tell them apart from the query's own steps without following `Children` from every root.
    for (auto & node : result.nodes)
        node.sub_plan_id = subquery_id;

    if (steps_to_stats)
    {
        result.execution_time_ns = steps_to_stats->getExecutionTimeNs();
        result.max_threads = steps_to_stats->getMaxThreads();
    }

    return result;
}

CapturedPlan capturePlan(
    const QueryPlan & plan,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const StepStatsCollector * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    CapturedPlan result;
    if (!plan.isInitialized() || !plan.getRootNode())
        return result;

    result.root_id = plan.getRootNode()->step->getUniqID();
    result.nodes = captureNodes(plan, options, max_description_length, steps_to_stats, pretty_names);

    if (steps_to_stats)
    {
        result.execution_time_ns = steps_to_stats->getExecutionTimeNs();
        result.max_threads = steps_to_stats->getMaxThreads();
    }

    /// The columns the query produces. The text renderer prints these once above the tree rather
    /// than against a step, so they belong to the plan, not to any node.
    if (options.pretty)
    {
        const auto * root_pretty_names = findPrettyNames(pretty_names, &plan);
        PrettyNames empty_pretty_names;
        const auto & names
            = root_pretty_names ? root_pretty_names->pretty_names : empty_pretty_names.pretty_names;

        const auto & root_step = *plan.getRootNode()->step;
        if (root_step.hasOutputHeader() && root_step.getOutputHeader())
            for (const auto & column : *root_step.getOutputHeader())
                result.output.push_back(QueryPlanFormat::formatColumnPretty(column.name, names));
    }

    return result;
}

}
