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

        processors_by_step[step].push_back(proc.get());

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
        const auto * step = proc->getQueryPlanStep();

        if (!step)
            continue;

        const size_t group = proc->getQueryPlanStepGroup();
        const UInt64 group_elapsed = proc->getElapsedNs();
        if (group_elapsed == 0)
            continue;

        const auto step_group_key = std::make_pair(step, group);
        auto & group_stats = stats_by_step_group[step_group_key];
        group_stats.sum_elapsed_ns += group_elapsed;
        ++group_stats.total_num_processors;
        elapsed_per_step_group[step_group_key].insert(group_elapsed);

        if (group_stats.wall_clock_time_ns == 0)
        {
            /// The registry stays keyed by id: it is looked up per task in the executor, from the
            /// id the processor already carries.
            if (auto * registry = pipeline.getStepClocks())
                if (const auto * clock = registry->find(proc->getStepUniqID(), group))
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
    CardinalityByJoinStep cardinality_by_join_step;
    for (const auto & [step, io_stats] : stats_by_step)
    {
        const auto * join_step = typeid_cast<const JoinStep *>(step);
        if (!join_step || !join_step->getJoin())
            continue;

        StepProcessors step_processors = processors_by_step.at(step);

        auto report = step->getAnalysisReport(step_processors);
        const auto & table_join = join_step->getJoin()->getTableJoin();
        cardinality_by_join_step[join_step] = joinMatchedOutputRows(report, io_stats.output_rows, table_join.kind(), table_join.strictness());

        join_raw_reports.emplace(step, std::move(report));
    }

    const JoinBranchCosts join_branch_costs(plan, cardinality_by_join_step);
    for (auto & [step, report] : join_raw_reports)
    {
        const auto * join_step = typeid_cast<const JoinStep *>(step);
        MetricGroup cost_group{MetricGroupKey::Cost, {}};
        cost_group.metrics.emplace_back(MetricKey::Actual, optionalQuantity(join_branch_costs.getBranchCost(join_step)));
        report.push_back(std::move(cost_group));
    }
}

StepStatsContext StepStatsStorage::makeContext(const IQueryPlanStep * step) const
{
    StepStatsContext context;
    context.step = step;
    context.execution_query_time_ns = execution_query_time_ns;
    context.max_num_threads_per_query = max_num_threads_per_query;

    if (const auto step_stats_it = stats_by_step.find(step); step_stats_it != stats_by_step.end())
        context.io = step_stats_it->second;

    for (size_t group : step->getStepGroups())
        if (const auto group_stats_it = stats_by_step_group.find(std::make_pair(step, group)); group_stats_it != stats_by_step_group.end())
            context.group_stats[group] = group_stats_it->second;

    return context;
}

AnalyzedStepData StepStatsStorage::analyzeStep(const IQueryPlanStep * step) const
{
    StepAnalysisReport raw_report;
    if (const auto report_it = join_raw_reports.find(step); report_it != join_raw_reports.end())
    {
        raw_report = report_it->second;
    }
    else
    {
        StepProcessors step_processors;
        if (const auto processors_it = processors_by_step.find(step); processors_it != processors_by_step.end())
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
