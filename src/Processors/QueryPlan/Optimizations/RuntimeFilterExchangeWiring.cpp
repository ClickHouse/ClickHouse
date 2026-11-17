#include <algorithm>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Columns/ColumnConst.h>
#include <DataTypes/IDataType.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/MergeRuntimeFiltersStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/Optimizations/RuntimeFilterExchangeWiring.h>
#include <Processors/QueryPlan/RuntimeFilterGeometry.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#if CLICKHOUSE_CLOUD
#include <Processors/QueryPlan/ReadFromMergeTreeAtWorker.h>
#endif
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

String taskBucketId(const DistributedQueryTask & task)
{
    return task.parameters.parameters.at("bucket_id").safeGet<String>();
}

/// True when `from_stage` already depends on `to_stage` (directly or through other stages).
/// Delivery would add the reverse edge and cycle.
bool stageDependsOnTransitively(
    const std::unordered_map<String, StageDependencies> & stage_depends_on,
    const String & from_stage,
    const String & to_stage)
{
    std::unordered_set<String> seen;
    std::vector<String> queue;
    queue.push_back(from_stage);
    seen.insert(from_stage);
    for (size_t i = 0; i < queue.size(); ++i)
    {
        auto it = stage_depends_on.find(queue[i]);
        if (it == stage_depends_on.end())
            continue;
        for (const auto & [dependency, _] : it->second)
        {
            if (dependency == to_stage)
                return true;
            if (seen.insert(dependency).second)
                queue.push_back(dependency);
        }
    }
    return false;
}

struct RuntimeFilterApplication
{
    String filter_key; /// Value of the label constant: the rendezvous key.
    String key_column; /// The probed input column; empty when the probed key is not a plain column.
};

/// Collects every `__applyFilter` call in the DAG. Only FUNCTION nodes count, so a leftover alias whose
/// computation was pushed further down is ignored. Matching uses the label's value, a random key that
/// cannot collide across plan builds.
void collectRuntimeFilterApplications(const ActionsDAG & dag, std::vector<RuntimeFilterApplication> & out)
{
    for (const auto & dag_node : dag.getNodes())
    {
        if (dag_node.type != ActionsDAG::ActionType::FUNCTION || !dag_node.function_base
            || dag_node.function_base->getName() != "__applyFilter" || dag_node.children.size() < 2)
            continue;

        const auto * key_argument = dag_node.children.front();
        if (!key_argument->column)
            continue;

        const auto * key_constant = typeid_cast<const ColumnConst *>(key_argument->column.get());
        if (!key_constant)
            continue;

        /// Unwrap the probed expression to its input column, looking through renames and the cast
        /// to the filter element type.
        const auto * probed = dag_node.children[1];
        while (true)
        {
            if (probed->type == ActionsDAG::ActionType::ALIAS && !probed->children.empty())
                probed = probed->children.front();
            else if (
                probed->type == ActionsDAG::ActionType::FUNCTION && probed->function_base
                && (probed->function_base->getName() == "CAST" || probed->function_base->getName() == "_CAST") && !probed->children.empty())
                probed = probed->children.front();
            else
                break;
        }
        out.push_back(
            {.filter_key = String(key_constant->getDataAt(0)),
             .key_column = probed->type == ActionsDAG::ActionType::INPUT ? probed->result_name : String{}});
    }
}

/// FilterStep predicates and scan PREWHEREs are where pushdown leaves the applications.
void collectStepApplications(const IQueryPlanStep & step, std::vector<RuntimeFilterApplication> & out)
{
    if (const auto * filter_step = typeid_cast<const FilterStep *>(&step))
        collectRuntimeFilterApplications(filter_step->getExpression(), out);
    else if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(&step))
    {
        if (const auto & prewhere_info = source->getPrewhereInfo(); prewhere_info)
            collectRuntimeFilterApplications(prewhere_info->prewhere_actions, out);
    }
#if CLICKHOUSE_CLOUD
    /// The worker scan is not a `SourceStepWithFilter` but carries the pushed-down PREWHERE.
    /// Without this branch its apply sites are not found and the filter is not transported.
    else if (const auto * worker_scan = typeid_cast<const ReadFromMergeTreeAtWorker *>(&step))
    {
        if (const auto & prewhere_info = worker_scan->getQueryInfo().prewhere_info; prewhere_info)
            collectRuntimeFilterApplications(prewhere_info->prewhere_actions, out);
    }
#endif
}

struct FilterProducer
{
    String key; /// The rendezvous key of the filter.
    BuildRuntimeFilterStep * step = nullptr;
    String stage;
};

struct FilterConsumerSite
{
    QueryPlan::Node * node = nullptr; /// site subtree root, admission estimates run from here
    String key_column;
};

/// Filter key -> stage -> apply sites in that stage.
using FilterConsumerSites = std::unordered_map<String, std::map<String, std::vector<FilterConsumerSite>>>;

/// Collects the filters that can be sent to other tasks and the sites that apply each filter. The
/// producers keep the plan traversal order: the filter keys are random, and ordering by them would
/// give different exchange names on each run.
void collectProducersAndSites(DistributedQueryPlan & distributed_plan, std::vector<FilterProducer> & producers, FilterConsumerSites & sites)
{
    std::unordered_map<String, size_t> producer_index;
    for (auto & [stage_name, stage] : distributed_plan.stages)
    {
        std::vector<QueryPlan::Node *> stack = {stage.query_plan_fragment.getRootNode()};
        while (!stack.empty())
        {
            auto * node = stack.back();
            stack.pop_back();
            for (auto * child : node->children)
                stack.push_back(child);

            /// Only over-approximating filters may cross tasks: an exclusion (anti-join) filter built
            /// from a subset of the keys would wrongly drop rows, and only the bloom-style state can
            /// be merged across tasks.
            auto * build = typeid_cast<BuildRuntimeFilterStep *>(node->step.get());
            if (build && !build->getFilterKey().empty() && build->allowsNotExactFilter()
                && AdaptiveSetRuntimeFilter::isDataTypeSupported(build->getFilterColumnType()))
            {
                FilterProducer producer{.key = build->getFilterKey(), .step = build, .stage = stage_name};
                auto [it, inserted] = producer_index.emplace(producer.key, producers.size());
                if (inserted)
                    producers.push_back(std::move(producer));
                else
                    producers[it->second] = std::move(producer);
            }

            std::vector<RuntimeFilterApplication> applications;
            collectStepApplications(*node->step, applications);
            /// Keep every site: the stage is admitted if any site admits. The filter is still
            /// delivered once per stage and read by all its sites.
            for (const auto & application : applications)
                sites[application.filter_key][stage_name].push_back(
                    FilterConsumerSite{.node = node, .key_column = application.key_column});
        }
    }
}

struct TransportBudget
{
    RuntimeFilterGeometry geometry;
    /// Estimated count of build-side rows. Without it no admission check runs.
    std::optional<UInt64> estimated_keys;
    /// `geometry.exact_bytes_limit` is above its settings value.
    bool is_upsized = false;
};

/// Estimated keys are the build-side rows, not the column NDV: HyperLogLog sketches undershoot, and
/// a low `exact_values_limit` would turn a complete key set into a bloom filter.
///
/// With an estimate, the exact-set byte limit grows to the estimated keys times the key width (a
/// variable-width key counts as an 8-byte hash), up to `MAX_RUNTIME_BLOOM_FILTER_BYTES`, and the row
/// limit grows to the estimate. Neither limit drops below its settings value. If the estimate is too
/// low, the partial changes to the bloom filter of the settings size.
TransportBudget makeTransportBudget(const BuildRuntimeFilterStep & step)
{
    TransportBudget budget{.geometry = step.getGeometry(), .estimated_keys = step.getEstimatedBuildRows()};
    if (!budget.estimated_keys)
        return budget;

    const UInt64 estimated_keys = *budget.estimated_keys;
    const auto & key_type = step.getFilterColumnType();
    const UInt64 key_width = key_type->haveMaximumSizeOfValue() ? key_type->getMaximumSizeOfValueInMemory() : sizeof(UInt64);
    const UInt64 exact_transport_bytes = estimated_keys > MAX_RUNTIME_BLOOM_FILTER_BYTES / key_width
        ? MAX_RUNTIME_BLOOM_FILTER_BYTES
        : estimated_keys * key_width;
    if (exact_transport_bytes > budget.geometry.exact_bytes_limit)
    {
        budget.geometry.exact_bytes_limit = exact_transport_bytes;
        budget.is_upsized = true;
    }
    budget.geometry.exact_values_limit = std::max(budget.geometry.exact_values_limit, estimated_keys);
    return budget;
}

/// True when sending the filter to this apply site can prune rows: the build key set must be
/// smaller than the site's key set and row count, and a raised budget needs a known site row count.
bool siteAdmitsRuntimeFilterTransport(
    const FilterConsumerSite & site, const BuildRuntimeFilterStep & producer, const TransportBudget & budget)
{
    const UInt64 estimated_keys = *budget.estimated_keys;
    /// The estimate runs on the cut fragment and cannot see across exchange boundaries;
    /// sites above a nested exchange (and Cloud worker-scan steps) yield no estimate and
    /// take the no-estimate admission path.
    auto site_stats = estimateReadRowsCount(*site.node);
    if (site_stats.estimated_rows && estimated_keys >= *site_stats.estimated_rows)
    {
        LOG_TRACE(
            getLogger("joinRuntimeFilter"),
            "Runtime-filter transport of '{}' refused at '{}': {} estimated build keys vs {} estimated site rows",
            producer.getFilterName(),
            site.node->step->getName(),
            estimated_keys,
            *site_stats.estimated_rows);
        return false;
    }
    if (auto it = site_stats.column_stats.find(site.key_column); !site.key_column.empty() && it != site_stats.column_stats.end()
        && it->second.num_distinct_values > 0 && estimated_keys >= it->second.num_distinct_values)
    {
        LOG_TRACE(
            getLogger("joinRuntimeFilter"),
            "Runtime-filter transport of '{}' refused at '{}': {} estimated build keys vs {} distinct values of '{}'",
            producer.getFilterName(),
            site.node->step->getName(),
            estimated_keys,
            it->second.num_distinct_values,
            site.key_column);
        return false;
    }
    if (budget.is_upsized && !site_stats.estimated_rows)
    {
        LOG_TRACE(
            getLogger("joinRuntimeFilter"),
            "Runtime-filter transport of '{}' refused at '{}': {} bytes upsized exact budget, but the site rows are "
            "unknown",
            producer.getFilterName(),
            site.node->step->getName(),
            budget.geometry.exact_bytes_limit);
        return false;
    }
    LOG_TRACE(
        getLogger("joinRuntimeFilter"),
        "Runtime-filter transport of '{}' admitted at '{}': {} estimated build keys, {} exact bytes budget, {} exact "
        "values limit",
        producer.getFilterName(),
        site.node->step->getName(),
        estimated_keys,
        budget.geometry.exact_bytes_limit,
        budget.geometry.exact_values_limit);
    return true;
}

struct ReceivingStage
{
    String name;
    /// The stage depends on the merge root, so the scheduler runs the chain before it.
    bool has_completion_edge = true;
};

/// The stages, other than the build stage, that get the filter. A stage gets it if any of its sites
/// admits it. A tiny or stats-less site must not veto a sibling that would prune.
std::vector<ReceivingStage> selectReceivingStages(
    const DistributedQueryPlan & distributed_plan,
    const FilterProducer & producer,
    const std::map<String, std::vector<FilterConsumerSite>> & sites_by_stage,
    const TransportBudget & budget,
    ExchangeDescription::Kind kind)
{
    std::vector<ReceivingStage> receivers;
    for (const auto & [stage_name, sites] : sites_by_stage)
    {
        /// The build stage reads the filter from the task's own lookup.
        if (stage_name == producer.stage)
            continue;

        auto admits = [&](const FilterConsumerSite & site) { return siteAdmitsRuntimeFilterTransport(site, *producer.step, budget); };
        if (budget.estimated_keys && std::ranges::none_of(sites, admits))
            continue;

        /// If the build stage already depends on this stage (a join that sends its filter to the
        /// scans below it), the completion edge would form a cycle. A streaming chain works without
        /// the edge: the stage applies the filter when it arrives. A persisted chain does not: the
        /// stage would read before the filter is written, so it gets no filter.
        ReceivingStage receiver{.name = stage_name};
        if (stageDependsOnTransitively(distributed_plan.stage_depends_on, producer.stage, stage_name))
        {
            if (kind == ExchangeDescription::Kind::Persisted)
            {
                LOG_TRACE(getLogger("joinRuntimeFilter"),
                    "Filter '{}' is not delivered to stage '{}': the exchange chain is persisted and "
                    "the required stage dependency would create a cycle",
                    producer.step->getFilterName(), stage_name);
                continue;
            }
            LOG_TRACE(getLogger("joinRuntimeFilter"),
                "Filter '{}' delivery to stage '{}' is wired without a stage dependency: "
                "the producing stage '{}' depends on that stage, so an entry would create a cycle",
                producer.step->getFilterName(), stage_name, producer.stage);
            receiver.has_completion_edge = false;
        }
        receivers.push_back(std::move(receiver));
    }
    return receivers;
}

/// Adds the merge stages of one filter and connects them. The build tasks send their partials to
/// the first level; each level has `RUNTIME_FILTER_MERGE_FAN_IN` times fewer tasks than the one
/// below, and the last level (the root) has one task, which sends the union to every task of every
/// receiving stage. One build task still gets a root merge stage, so that only a filter-only task
/// waits for slow or absent receivers.
class RuntimeFilterMergeTree
{
public:
    static constexpr size_t fan_in = RUNTIME_FILTER_MERGE_FAN_IN;

    RuntimeFilterMergeTree(
        DistributedQueryPlan & distributed_plan_,
        size_t & next_exchange_id,
        const FilterProducer & producer_,
        const RuntimeFilterGeometry & geometry_,
        ExchangeDescription::Kind kind_,
        std::vector<ReceivingStage> receivers_)
        : distributed_plan(distributed_plan_)
        , producer(producer_)
        , geometry(geometry_)
        , kind(kind_)
        , receivers(std::move(receivers_))
    {
        for (size_t tasks = buildTasks().size(); tasks > 1;)
        {
            tasks = (tasks + fan_in - 1) / fan_in;
            level_sizes.push_back(tasks);
        }
        if (level_sizes.empty())
            level_sizes.push_back(1);

        for (size_t level = 0; level < level_sizes.size(); ++level)
        {
            const String id = toString(next_exchange_id++);
            level_exchanges.push_back("exchange_" + id);
            /// The filter name in the stage and task names lets one string follow a filter
            /// through plans, logs and profiling tables.
            level_stages.push_back("rf_merge_" + id + "_" + producer.step->getFilterName());
        }
        for (size_t index = 0; index < receivers.size(); ++index)
            broadcast_exchanges.push_back("exchange_" + toString(next_exchange_id++));
    }

    void wire()
    {
        Strings child_buckets = connectBuildTasks();
        for (size_t level = 0; level < level_sizes.size(); ++level)
            child_buckets = addMergeLevel(level, child_buckets);
        connectReceivers();
    }

private:
    DistributedQueryPlan & distributed_plan;
    const FilterProducer & producer;
    const RuntimeFilterGeometry & geometry;
    const ExchangeDescription::Kind kind;
    const std::vector<ReceivingStage> receivers;

    std::vector<size_t> level_sizes;
    Strings level_exchanges; /// The input exchange of each level.
    Strings level_stages;
    Strings broadcast_exchanges; /// One per receiving stage.

    std::vector<DistributedQueryTask> & buildTasks() { return distributed_plan.stages.at(producer.stage).tasks; }

    void addExchange(const String & name, size_t source_bucket_count, size_t destination_bucket_count)
    {
        distributed_plan.exchange_descriptions[name] = ExchangeDescription{
            .name = name, .kind = kind, .source_bucket_count = source_bucket_count, .destination_bucket_count = destination_bucket_count};
    }

    /// Returns the buckets of the build tasks.
    Strings connectBuildTasks()
    {
        auto & tasks = buildTasks();
        Strings buckets;
        for (size_t index = 0; index < tasks.size(); ++index)
        {
            buckets.push_back(taskBucketId(tasks[index]));
            tasks[index].output_exchange_streams.emplace_back(
                level_exchanges[0], buckets.back(), toString(MergeRuntimeFiltersStep::parentIndex(index, fan_in)));
        }
        producer.step->setTreeExchange(level_exchanges[0], buckets, fan_in);
        return buckets;
    }

    /// Returns the buckets of the new level.
    Strings addMergeLevel(size_t level, const Strings & child_buckets)
    {
        const String & child_stage = level == 0 ? producer.stage : level_stages[level - 1];
        addExchange(level_exchanges[level], child_buckets.size(), level_sizes[level]);
        distributed_plan.stage_depends_on[level_stages[level]].push_back({child_stage, level_exchanges[level]});

        auto outputs = levelOutputs(level);
        DistributedQueryStage stage;
        stage.filter_only = true;
        stage.query_plan_fragment.addStep(std::make_unique<MergeRuntimeFiltersStep>(
            producer.step->getFilterName(),
            producer.step->getFilterColumnType(),
            geometry,
            level_exchanges[level],
            child_buckets,
            fan_in,
            outputs));

        Strings buckets;
        for (size_t index = 0; index < level_sizes[level]; ++index)
        {
            buckets.push_back(toString(index));
            stage.tasks.push_back(makeMergeTask(level, index, child_buckets, outputs));
        }
        distributed_plan.stages[level_stages[level]] = std::move(stage);
        return buckets;
    }

    /// An inner level sends to its parent level; the root sends to every receiving task.
    std::vector<MergeRuntimeFiltersStep::Output> levelOutputs(size_t level) const
    {
        if (level + 1 < level_sizes.size())
            return {{level_exchanges[level + 1], /*destination_buckets=*/{}}};

        chassert(level_sizes[level] == 1);
        std::vector<MergeRuntimeFiltersStep::Output> outputs;
        for (size_t index = 0; index < receivers.size(); ++index)
        {
            Strings destination_buckets;
            for (const auto & task : distributed_plan.stages.at(receivers[index].name).tasks)
                destination_buckets.push_back(taskBucketId(task));
            outputs.push_back({broadcast_exchanges[index], std::move(destination_buckets)});
        }
        return outputs;
    }

    DistributedQueryTask makeMergeTask(
        size_t level, size_t index, const Strings & child_buckets, const std::vector<MergeRuntimeFiltersStep::Output> & outputs) const
    {
        const String bucket = toString(index);
        DistributedQueryTask task;
        task.task_id = level_stages[level] + "_" + bucket;
        task.parameters.parameters["bucket_id"] = Field(bucket);

        auto inputs = MergeRuntimeFiltersStep::inputStreams(level_exchanges[level], child_buckets, index, fan_in);
        task.input_exchange_streams.assign(inputs.begin(), inputs.end());
        task.output_exchange_streams = MergeRuntimeFiltersStep::outputStreams(outputs, index, fan_in);
        return task;
    }

    void connectReceivers()
    {
        for (size_t index = 0; index < receivers.size(); ++index)
        {
            const auto & receiver = receivers[index];
            auto & tasks = distributed_plan.stages.at(receiver.name).tasks;
            addExchange(broadcast_exchanges[index], 1, tasks.size());
            for (auto & task : tasks)
            {
                ExchangeStreamId stream(broadcast_exchanges[index], "0", taskBucketId(task));
                task.runtime_filter_descriptors.push_back(
                    {producer.key, producer.step->getFilterName(), producer.step->getFilterColumnType(), geometry, stream});
                task.input_exchange_streams.push_back(std::move(stream));
            }
            if (receiver.has_completion_edge)
                distributed_plan.stage_depends_on[receiver.name].push_back({level_stages.back(), broadcast_exchanges[index]});
        }
    }
};

}

void wireRuntimeFilterExchangeTopology(DistributedQueryPlan & distributed_plan, size_t & next_exchange_id, ExchangeDescription::Kind kind)
{
    std::vector<FilterProducer> producers;
    FilterConsumerSites sites;
    collectProducersAndSites(distributed_plan, producers, sites);

    for (const auto & producer : producers)
    {
        auto stage_sites = sites.find(producer.key);
        if (stage_sites == sites.end())
            continue;

        auto budget = makeTransportBudget(*producer.step);
        auto receivers = selectReceivingStages(distributed_plan, producer, stage_sites->second, budget, kind);
        if (receivers.empty())
            continue;

        producer.step->setGeometry(budget.geometry);
        RuntimeFilterMergeTree(distributed_plan, next_exchange_id, producer, budget.geometry, kind, std::move(receivers)).wire();
    }
}

}
