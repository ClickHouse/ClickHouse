#include <iterator>
#include <type_traits>
#include <unordered_map>
#include <Processors/Port.h>
#include <Processors/QueryPlan/StepStatsStorage.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <Processors/QueryPlan/StepStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinBranchCosts.h>
#include <Processors/QueryPlan/JoinStatsAnalyzer.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/typeid_cast.h>
#include <Processors/StepWallClock.h>
#include <Processors/StepWallClockRegistry.h>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

StepStatsStorage::StepStatsStorage(const QueryPipeline & pipeline, const QueryPlan & plan, UInt64 execution_query_time_ns_)
: max_num_threads_per_query(pipeline.getNumThreads())
, execution_query_time_ns(execution_query_time_ns_)
{
    const auto & processors = pipeline.getProcessors();

    collectIOStats(processors);
    const auto elapsed_per_step_group = collectTimingStats(pipeline, processors);
    computeDistribution(elapsed_per_step_group);
    computeJoinBranchCosts(plan);
}

void StepStatsStorage::collectIOStats(const Processors & processors)
{
    /// A processor that belongs to no step has an empty id, which is how they compare equal to
    /// each other here, exactly as two null step pointers used to.
    auto crosses_step_boundary = [](const IProcessor & owner, const IProcessor & neighbour)
    {
        return owner.getStepUniqID() != neighbour.getStepUniqID();
    };

    for (const auto & proc : processors)
    {
        const auto & step_id = proc->getStepUniqID();

        if (step_id.empty())
            continue;

        auto & step_stats = stats_by_step[step_id];

        processors_by_step[step_id].push_back(proc.get());

        for (const auto & input_port : proc->getInputs())
        {
            if (!input_port.isConnected())
                continue;

            if (crosses_step_boundary(*proc, input_port.getOutputPort().getProcessor()))
            {
                const auto counters = proc->getPortDataCounters(input_port);
                step_stats.input_rows += counters.rows;
                step_stats.input_bytes += counters.bytes;
            }
        }

        for (const auto & output_port : proc->getOutputs())
        {
            if (!output_port.isConnected())
                continue;

            if (crosses_step_boundary(*proc, output_port.getInputPort().getProcessor()))
            {
                const auto counters = proc->getPortDataCounters(output_port);
                step_stats.output_rows += counters.rows;
                step_stats.output_bytes += counters.bytes;
            }
        }
    }
}

StepStatsStorage::ElapsedTimesPerStepGroup StepStatsStorage::collectTimingStats(const QueryPipeline & pipeline, const Processors & processors)
{
    ElapsedTimesPerStepGroup elapsed_per_step_group;

    for (const auto & proc : processors)
    {
        const auto & step_id = proc->getStepUniqID();

        if (step_id.empty())
            continue;

        const size_t group = proc->getQueryPlanStepGroup();
        const UInt64 group_elapsed = proc->getElapsedNs();
        if (group_elapsed == 0)
            continue;

        const auto step_group_key = std::make_pair(step_id, group);
        auto & group_stats = stats_by_step_group[step_group_key];
        group_stats.sum_elapsed_ns += group_elapsed;
        ++group_stats.total_num_processors;
        elapsed_per_step_group[step_group_key].insert(group_elapsed);

        if (group_stats.wall_clock_time_ns == 0)
        {
            if (auto * registry = pipeline.getStepClocks())
                if (const auto * clock = registry->find(step_id, group))
                    group_stats.wall_clock_time_ns = clock->getStepWallTime();
        }
    }

    return elapsed_per_step_group;
}

void StepStatsStorage::computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group)
{
    /// Compute the per-processor elapsed time distribution for each (step, group).
    /// The multiset is already sorted, so min/max are its bounds and the median is the middle element.
    for (const auto & [step_group_key, elapsed] : elapsed_per_step_group)
    {
        if (elapsed.empty())
            continue;

        auto & group_stats = stats_by_step_group[step_group_key];
        group_stats.min_elapsed_ns = *elapsed.begin();
        group_stats.max_elapsed_ns = *elapsed.rbegin();

        const size_t count = elapsed.size();
        const auto middle = std::next(elapsed.begin(), count / 2);
        group_stats.median_elapsed_ns = (count % 2 == 1)
            ? *middle
            : (*std::prev(middle) + *middle) / 2;
    }
}

void StepStatsStorage::computeJoinBranchCosts(const QueryPlan & plan)
{
    if (!plan.isInitialized())
        return;

    /// Walks the plan rather than the collected stats: those are keyed by the step's id and hold
    /// no pointer, and deciding whether a step is a join needs the object. The plan is here for
    /// exactly this reason -- a branch cost is a property of the tree, not of one step.
    std::vector<const IQueryPlanStep *> join_steps;
    std::vector<const QueryPlan::Node *> stack;
    stack.push_back(plan.getRootNode());

    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (!node)
            continue;

        join_steps.push_back(node->step.get());

        for (const auto * child : node->children)
            stack.push_back(child);
        for (const auto * child_plan : node->step->getChildPlans())
            if (child_plan)
                stack.push_back(child_plan->getRootNode());
    }

    CardinalityByJoinStep cardinality_by_join_step;
    for (const auto * step : join_steps)
    {
        const auto * join_step = typeid_cast<const JoinStep *>(step);
        if (!join_step || !join_step->getJoin())
            continue;

        const auto step_id = step->getUniqID();

        const auto io_stats_it = stats_by_step.find(step_id);
        if (io_stats_it == stats_by_step.end())
            continue;

        StepProcessors step_processors;
        if (const auto processors_it = processors_by_step.find(step_id); processors_it != processors_by_step.end())
            step_processors = processors_it->second;

        auto report = step->getAnalysisReport(step_processors);
        const auto & table_join = join_step->getJoin()->getTableJoin();
        cardinality_by_join_step[join_step]
            = joinMatchedOutputRows(report, io_stats_it->second.output_rows, table_join.kind(), table_join.strictness());

        join_raw_reports.emplace(step_id, std::move(report));
    }

    if (join_raw_reports.empty())
        return;

    const JoinBranchCosts join_branch_costs(plan, cardinality_by_join_step);
    for (const auto * step : join_steps)
    {
        const auto * join_step = typeid_cast<const JoinStep *>(step);
        if (!join_step)
            continue;

        const auto report_it = join_raw_reports.find(step->getUniqID());
        if (report_it == join_raw_reports.end())
            continue;

        MetricGroup cost_group{MetricGroupKey::Cost, {}};
        cost_group.metrics.emplace_back(MetricKey::Actual, optionalQuantity(join_branch_costs.getBranchCost(join_step)));
        report_it->second.push_back(std::move(cost_group));
    }
}

StepStatsContext StepStatsStorage::makeContext(const IQueryPlanStep * step) const
{
    StepStatsContext context;
    context.step = step;
    context.execution_query_time_ns = execution_query_time_ns;
    context.max_num_threads_per_query = max_num_threads_per_query;

    const auto step_id = step->getUniqID();

    if (const auto step_stats_it = stats_by_step.find(step_id); step_stats_it != stats_by_step.end())
        context.io = step_stats_it->second;

    for (size_t group : step->getStepGroups())
        if (const auto group_stats_it = stats_by_step_group.find(std::make_pair(step_id, group)); group_stats_it != stats_by_step_group.end())
            context.group_stats[group] = group_stats_it->second;

    return context;
}

AnalyzedStepData StepStatsStorage::analyzeStep(const IQueryPlanStep * step) const
{
    StepAnalysisReport raw_report;
    if (const auto report_it = join_raw_reports.find(step->getUniqID()); report_it != join_raw_reports.end())
    {
        raw_report = report_it->second;
    }
    else
    {
        StepProcessors step_processors;
        if (const auto processors_it = processors_by_step.find(step->getUniqID()); processors_it != processors_by_step.end())
            step_processors = processors_it->second;

        raw_report = step->getAnalysisReport(step_processors);
    }

    auto context_for_step = makeContext(step);
    StepStatsAnalyzer step_stats_generator = getStepStatsAnalyzer(step);

    /// Use the service of a generator, which takes the context (e.g. i/o, total time)
    /// some internal raw metrics, which are specific for each step,  that
    /// with the knowledge of the step will pre-process the metrics before printing
    return step_stats_generator(context_for_step, std::move(raw_report));
}
}
