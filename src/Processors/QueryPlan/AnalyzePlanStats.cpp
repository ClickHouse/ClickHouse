#include <algorithm>
#include <iterator>
#include <set>
#include <stack>
#include <unordered_map>
#include <Processors/Port.h>
#include <Processors/QueryPlan/AnalyzePlanStats.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/StepWallClock.h>
#include <Processors/StepWallClockRegistry.h>
#include <base/defines.h>
#include <base/types.h>

namespace DB
{

AnalyzeStepsStats::AnalyzeStepsStats(const QueryPipeline & pipeline, const QueryPlan & plan, UInt64 execution_query_time_ns_)
: max_num_threads_per_query(pipeline.getNumThreads())
, execution_query_time_ns(execution_query_time_ns_)
{
    const auto & processors = pipeline.getProcessors();

    collectIoStats(processors);
    const auto elapsed_per_step_group = collectTimingStats(pipeline, processors);
    computeDistribution(elapsed_per_step_group);
    computeStepTimes(plan);
}

void AnalyzeStepsStats::collectIoStats(const Processors & processors)
{
    auto crosses_step_boundary = [](const IProcessor & owner, const IProcessor & neighbour)
    {
        return owner.getQueryPlanStep() != neighbour.getQueryPlanStep();
    };

    for (const auto & proc : processors)
    {
        const auto * step = proc->getQueryPlanStep();

        if (!step)
            continue;

        auto & step_stats = stats_by_step[step];

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

AnalyzeStepsStats::ElapsedTimesPerStepGroup AnalyzeStepsStats::collectTimingStats(const QueryPipeline & pipeline, const Processors & processors)
{
    ElapsedTimesPerStepGroup elapsed_per_step_group;

    for (const auto & proc : processors)
    {
        const auto * step = proc->getQueryPlanStep();

        if (!step)
            continue;

        for (size_t group = 0; group < IProcessor::MAX_STEP_GROUPS; ++group)
        {
            const UInt64 group_elapsed = proc->getElapsedNs(group);
            if (group_elapsed == 0)
                continue;

            const auto step_group_key = std::make_pair(step, group);
            auto & group_stats = stats_by_step_group[step_group_key];
            group_stats.sum_elapsed_ns += group_elapsed;
            ++group_stats.total_num_processors;
            elapsed_per_step_group[step_group_key].insert(group_elapsed);

            if (group_stats.wall_clock_time_ns == 0)
            {
                if (const auto * registry = pipeline.getStepClocks())
                    if (const auto * clock = registry->find(step, group))
                        group_stats.wall_clock_time_ns = clock->getStepWallTime();
            }
        }
    }

    return elapsed_per_step_group;
}

void AnalyzeStepsStats::computeDistribution(const ElapsedTimesPerStepGroup & elapsed_per_step_group)
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

void AnalyzeStepsStats::computeStepTimes(const QueryPlan & plan)
{
    const auto * root_node = plan.getRootNode();
    if (!root_node)
        return;

    /// Walk the whole plan, descending into direct children and the root nodes of nested
    /// sub-plans (matching StepWallClockRegistry::populateFromPlan). For each step, the total
    /// time is the sum of its per-group wall-clock times.
    std::stack<const QueryPlan::Node *> stack;
    stack.push(root_node);

    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();

        const auto * step = node->step.get();

        UInt64 total_step_time_ns = 0;
        for (size_t group : step->getStepGroups())
            if (auto group_it = stats_by_step_group.find(std::make_pair(step, group)); group_it != stats_by_step_group.end())
                total_step_time_ns += group_it->second.wall_clock_time_ns;

        stats_by_step[step].total_step_time_ns = total_step_time_ns;

        for (const auto * child_node : node->children)
            stack.push(child_node);
        for (const auto * child_plan : node->step->getChildPlans())
            if (const auto * child_root = child_plan->getRootNode())
                stack.push(child_root);
    }
}

void AnalyzeStepsStats::printStepStats(const IQueryPlanStep * step, WriteBuffer & out, const std::string & prefix, bool processors_info) const
{
    if (!step)
        return ;

    StepStats step_stats;
    if (const auto step_it = stats_by_step.find(step); step_it != stats_by_step.end())
        step_stats = step_it->second;

    const bool empty_io = (step_stats.input_bytes == 0 && step_stats.output_bytes == 0);

    out << prefix << "Actual: rows "
        << formatReadableQuantity(static_cast<double>(step_stats.input_rows)) << " → "
        << formatReadableQuantity(static_cast<double>(step_stats.output_rows));

    if (step_stats.input_rows != step_stats.output_rows && step_stats.input_rows != 0)
    {
        const double selectivity = 100.0 * static_cast<double>(step_stats.output_rows) / static_cast<double>(step_stats.input_rows);
        out << fmt::format(" ({:.2f}%)", selectivity);
    }

    if (!empty_io)
        out << " · " << formatReadableSizeWithDecimalSuffix(static_cast<double>(step_stats.input_bytes))
            << " → " << formatReadableSizeWithDecimalSuffix(static_cast<double>(step_stats.output_bytes));

    out << "\n";

    for (size_t group : step->getStepGroups())
    {
        const auto group_it = stats_by_step_group.find(std::make_pair(step, group));
        if (group_it == stats_by_step_group.end())
            continue;

        const auto & group_stats = group_it->second;

        const double share_of_query_time = execution_query_time_ns != 0
            ? 100.0 * static_cast<double>(group_stats.wall_clock_time_ns) / static_cast<double>(execution_query_time_ns)
            : 0.0;
        const double parallelism = group_stats.wall_clock_time_ns
            ? static_cast<double>(group_stats.sum_elapsed_ns) / static_cast<double>(group_stats.wall_clock_time_ns)
            : 0.0;
        const UInt64 max_parallelism = std::min(max_num_threads_per_query, group_stats.total_num_processors);

        const std::string group_name = step->getStepGroupName(group);

        out << prefix << "  ";
        if (!group_name.empty())
            out << group_name << ": ";
        out << "time " << formatReadableTime(static_cast<double>(group_stats.wall_clock_time_ns))
            << fmt::format(" ({:.1f}%)", share_of_query_time) << " · parallelism "
            << (group_stats.wall_clock_time_ns ? fmt::format("{:.2f}/{}", parallelism, max_parallelism) : "Unknown")
            << "\n";

        if (processors_info)
            out << prefix << "    Time per processor (" << group_stats.total_num_processors << "): "
                << "min " << formatReadableTime(static_cast<double>(group_stats.min_elapsed_ns))
                << " · median " << formatReadableTime(static_cast<double>(group_stats.median_elapsed_ns))
                << " · max " << formatReadableTime(static_cast<double>(group_stats.max_elapsed_ns))
                << " · sum " << formatReadableTime(static_cast<double>(group_stats.sum_elapsed_ns)) << "\n";
    }

    if (step_stats.total_step_time_ns != 0)
    {
        const double share_of_query_time = execution_query_time_ns != 0
            ? 100.0 * static_cast<double>(step_stats.total_step_time_ns) / static_cast<double>(execution_query_time_ns)
            : 0.0;

        out << prefix << "  Total time: "
            << formatReadableTime(static_cast<double>(step_stats.total_step_time_ns))
            << fmt::format(" ({:.1f}%)", share_of_query_time)
            << "\n";
    }
}

};
