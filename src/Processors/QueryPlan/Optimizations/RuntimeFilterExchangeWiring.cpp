#include <algorithm>
#include <limits>
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
#include <Processors/QueryPlan/Optimizations/RuntimeFilterExchangeWiring.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/RuntimeFilterGeometry.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

RelationStats
estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, bool for_runtime_filter_transport = false);

namespace
{

String taskBucketId(const DistributedQueryTask & task)
{
    return task.parameters.parameters.at("bucket_id").safeGet<String>();
}

/// True when `from_stage` already depends on `to_stage` (directly or through other stages).
/// Delivery would add the reverse edge and cycle.
bool stageDependsOnTransitively(
    const std::unordered_map<String, std::unordered_map<String, String>> & stage_depends_on,
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
    String filter_name; /// const result_name = structural id (`_runtime_filter_<hash>`)
    String filter_key; /// value of the `__applyFilter` label const = rendezvous key
    String key_column; /// probed INPUT column name; empty when the key is not a plain column
};

/// Every `__applyFilter` application in the DAG. Matching FUNCTION nodes by the label VALUE cannot
/// collide across plan builds and ignores a leftover alias whose computation was pushed further
/// down.
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
            {.filter_name = key_argument->result_name,
             .filter_key = String(key_constant->getDataAt(0)),
             .key_column = probed->type == ActionsDAG::ActionType::INPUT ? probed->result_name : String{}});
    }
}

/// FilterStep predicates and scan PREWHEREs are where pushdown leaves the applications
/// (SourceStepWithFilter covers ReadFromMergeTreeAtWorker as well).
void collectStepApplications(const IQueryPlanStep & step, std::vector<RuntimeFilterApplication> & out)
{
    if (const auto * filter_step = typeid_cast<const FilterStep *>(&step))
        collectRuntimeFilterApplications(filter_step->getExpression(), out);
    else if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(&step))
        if (const auto & prewhere_info = source->getPrewhereInfo(); prewhere_info)
            collectRuntimeFilterApplications(prewhere_info->prewhere_actions, out);
}

struct FilterProducer
{
    BuildRuntimeFilterStep * step = nullptr;
    String stage;
};

struct FilterConsumerSite
{
    QueryPlan::Node * node = nullptr; /// site subtree root, admission estimates run from here
    String key_column;
};

/// Same three admission gates as before, on one apply site. A build key set at least
/// as large as the site's key set (or its whole row count) is not expected to prune,
/// so shipping it only costs. `geometry` is the (possibly upsized) transport budget;
/// the step is updated only if a stage ships.
bool siteAdmitsRuntimeFilterTransport(
    const FilterConsumerSite & site,
    const BuildRuntimeFilterStep & producer,
    UInt64 estimated_keys,
    bool budget_is_upsized,
    const RuntimeFilterGeometry & geometry)
{
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
    if (budget_is_upsized && !site_stats.estimated_rows)
    {
        LOG_TRACE(
            getLogger("joinRuntimeFilter"),
            "Runtime-filter transport of '{}' refused at '{}': {} bytes upsized exact budget, but the site rows are "
            "unknown",
            producer.getFilterName(),
            site.node->step->getName(),
            geometry.exact_bytes_limit);
        return false;
    }
    LOG_TRACE(
        getLogger("joinRuntimeFilter"),
        "Runtime-filter transport of '{}' admitted at '{}': {} estimated build keys, {} exact bytes budget, {} exact "
        "values limit",
        producer.getFilterName(),
        site.node->step->getName(),
        estimated_keys,
        geometry.exact_bytes_limit,
        geometry.exact_values_limit);
    return true;
}

}

void restoreRuntimeFilterRendezvousKeys(QueryPlan & plan)
{
    auto * root = plan.getRootNode();
    if (!root)
        return;

    std::unordered_map<String, String> key_by_name;
    std::vector<BuildRuntimeFilterStep *> builds;
    std::vector<QueryPlan::Node *> stack{root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        for (auto * child : node->children)
            stack.push_back(child);

        if (auto * build = typeid_cast<BuildRuntimeFilterStep *>(node->step.get()))
            builds.push_back(build);

        std::vector<RuntimeFilterApplication> applications;
        collectStepApplications(*node->step, applications);
        for (const auto & application : applications)
            key_by_name.emplace(application.filter_name, application.filter_key);
    }

    for (auto * build : builds)
    {
        if (!build->getFilterKey().empty())
            continue;
        auto it = key_by_name.find(build->getFilterName());
        if (it != key_by_name.end())
            build->setFilterKey(it->second);
    }
}

void wireRuntimeFilterExchangeTopology(
    DistributedQueryPlan & distributed_plan, size_t & next_exchange_id, ExchangeDescription::Kind default_kind)
{
    std::unordered_map<String, FilterProducer> producers; /// by rendezvous key
    std::unordered_map<String, std::map<String, std::vector<FilterConsumerSite>>> consumers; /// key -> stage -> apply sites

    for (auto & [stage_name, stage] : distributed_plan.stages)
    {
        std::vector<QueryPlan::Node *> stack = {stage.query_plan_fragment.getRootNode()};
        while (!stack.empty())
        {
            auto * node = stack.back();
            stack.pop_back();
            for (auto * child : node->children)
                stack.push_back(child);

            if (auto * build = typeid_cast<BuildRuntimeFilterStep *>(node->step.get()))
            {
                /// Only over-approximating filters may cross tasks: an exclusion (anti-join) filter built
                /// from a subset of the keys would wrongly drop rows, and only the bloom-style state can
                /// be merged across tasks.
                if (!build->getFilterKey().empty() && build->allowsNotExactFilter()
                    && ApproximateRuntimeFilter::isDataTypeSupported(build->getFilterColumnType()))
                    producers[build->getFilterKey()] = FilterProducer{.step = build, .stage = stage_name};
            }

            std::vector<RuntimeFilterApplication> applications;
            collectStepApplications(*node->step, applications);
            for (const auto & application : applications)
            {
                /// Every apply site in the stage is kept for admission. Delivery is still once per
                /// stage: the descriptor registers once per task, and extra sites share the
                /// registered filter through the lookup.
                consumers[application.filter_key][stage_name].push_back(
                    FilterConsumerSite{.node = node, .key_column = application.key_column});
            }
        }
    }

    for (auto & [key, producer] : producers)
    {
        std::map<String, std::vector<FilterConsumerSite>> consuming_stages;
        if (auto consumers_it = consumers.find(key); consumers_it != consumers.end())
        {
            for (const auto & [stage_name, sites] : consumers_it->second)
            {
                /// Same-stage sites are not delivered over an exchange: a self-edge would cycle the
                /// scheduler. Local `__applyFilter` uses this task's lookup (transport mode registers
                /// the merged partial after serialize).
                if (stage_name == producer.stage)
                    continue;
                consuming_stages.emplace(stage_name, sites);
            }
        }
        if (consuming_stages.empty())
            continue;

        /// Estimated count of build-side rows. Do not tighten by column NDV: HyperLogLog
        /// sketches undershoot, and `exact_values_limit` is a hard cap. A low NDV would
        /// overflow a complete key set into a bloom filter. `exact_bytes_limit` still bounds
        /// the exact phase when there are fewer distinct keys than rows.
        std::optional<UInt64> estimated_keys = producer.step->getEstimatedBuildRows();

        /// Exact-phase budget = estimated keys * key width (variable-width keys counted as 8-byte hashes),
        /// capped at `MAX_RUNTIME_BLOOM_FILTER_BYTES`. Row cap raised to the same estimate.
        /// `exact_bytes_limit` is still the hard cap. Missed estimate -> degrade to the settings bloom;
        /// a degraded partial never exceeds that size on the wire. No estimate -> settings floor, unchanged.
        RuntimeFilterGeometry geometry = producer.step->getGeometry();
        bool budget_is_upsized = false;
        if (estimated_keys)
        {
            const auto & key_type = producer.step->getFilterColumnType();
            const UInt64 key_width = key_type->haveMaximumSizeOfValue() ? key_type->getMaximumSizeOfValueInMemory() : sizeof(UInt64);
            if (*estimated_keys > std::numeric_limits<UInt64>::max() / key_width)
                continue;
            const UInt64 exact_transport_bytes = std::min(*estimated_keys * key_width, MAX_RUNTIME_BLOOM_FILTER_BYTES);
            if (exact_transport_bytes > geometry.exact_bytes_limit)
            {
                geometry.exact_bytes_limit = exact_transport_bytes;
                budget_is_upsized = true;
            }
            geometry.exact_values_limit = std::max(geometry.exact_values_limit, *estimated_keys);
        }

        std::vector<String> remote_stages;
        for (const auto & [stage_name, sites] : consuming_stages)
        {
            /// Uniform discovery could otherwise create a dependency cycle the stage scheduler
            /// does not guard against. Skip delivery to this stage; if every consuming stage is
            /// skipped the filter stays fully local.
            if (stageDependsOnTransitively(distributed_plan.stage_depends_on, producer.stage, stage_name))
                continue;

            /// Ship the stage if any apply site passes. A tiny or stats-less first site must
            /// not veto a sibling that would prune. No estimates at all -> transport as before.
            if (estimated_keys)
            {
                bool admitted = false;
                for (const auto & site : sites)
                {
                    if (siteAdmitsRuntimeFilterTransport(site, *producer.step, *estimated_keys, budget_is_upsized, geometry))
                    {
                        admitted = true;
                        break;
                    }
                }
                if (!admitted)
                    continue;
            }

            remote_stages.push_back(stage_name);
        }
        if (remote_stages.empty())
            continue;

        producer.step->setGeometry(geometry);

        auto & send_tasks = distributed_plan.stages.at(producer.stage).tasks;
        Strings source_buckets;
        for (const auto & task : send_tasks)
            source_buckets.push_back(taskBucketId(task));

        if (send_tasks.size() == 1)
        {
            /// A single build task is itself the root of the merge tree: its complete partial is
            /// broadcast directly, one exchange per receiving stage. Unlike the shared chain of a
            /// real tree below, each of these edges is independent, so the exchange kind follows
            /// the per-pair rule (copy the kind of an existing data edge between the two stages).
            for (const auto & receive_stage : remote_stages)
            {
                auto & receive_tasks = distributed_plan.stages.at(receive_stage).tasks;
                Strings destination_buckets;
                for (const auto & task : receive_tasks)
                    destination_buckets.push_back(taskBucketId(task));

                ExchangeDescription exchange_description;
                exchange_description.name = "exchange_" + std::to_string(next_exchange_id);
                ++next_exchange_id;
                exchange_description.source_bucket_count = 1;
                exchange_description.destination_bucket_count = receive_tasks.size();

                /// Same kind as the data exchanges. Streaming data -> streaming filter (probe gets it while it runs).
                /// Persisted plan -> persisted filter (streaming transport is never started). A Persisted data
                /// edge between the same stages already waits for the whole build; a streaming filter sink would
                /// wait for a consumer that never starts.
                exchange_description.kind = default_kind;
                auto dependencies = distributed_plan.stage_depends_on.find(receive_stage);
                const bool edge_exists
                    = dependencies != distributed_plan.stage_depends_on.end() && dependencies->second.contains(producer.stage);
                if (edge_exists)
                {
                    const auto & data_exchange = dependencies->second.at(producer.stage);
                    exchange_description.kind = distributed_plan.exchange_descriptions.at(data_exchange).kind;
                }

                distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;
                producer.step->addExchange(exchange_description.name, destination_buckets);

                for (const String & destination_bucket : destination_buckets)
                    send_tasks.front().output_exchange_streams.emplace_back(
                        ExchangeStreamId(exchange_description.name, source_buckets.front(), destination_bucket));
                for (auto & task : receive_tasks)
                {
                    std::vector<ExchangeStreamId> streams;
                    for (const auto & source_bucket : source_buckets)
                        streams.emplace_back(exchange_description.name, source_bucket, taskBucketId(task));
                    task.runtime_filter_descriptors.push_back(
                        {key, producer.step->getFilterName(), producer.step->getFilterColumnType(), geometry, streams});
                    task.input_exchange_streams.insert(task.input_exchange_streams.end(), streams.begin(), streams.end());
                }

                if (!edge_exists)
                    distributed_plan.stage_depends_on[receive_stage][producer.stage] = exchange_description.name;
            }
            continue;
        }

        /// Several build tasks: a bounded fan-in merge tree instead of all-to-all delivery. Every
        /// build task sends its partial once to its parent merge task; each merge level combines
        /// complete child states; the single root task broadcasts the global union once per
        /// destination task of every receiving stage.
        const size_t fan_in = RUNTIME_FILTER_MERGE_FAN_IN;

        /// Whole chain uses one kind: the plan's data-exchange kind. Persisted data edge between
        /// build and receive -> persisted filter chain. Streaming here deadlocks: the build sink
        /// waits for a merge consumer that only starts (via the receive stage) after build completes.
        auto chain_kind = default_kind;
        for (const auto & receive_stage : remote_stages)
        {
            auto dependencies = distributed_plan.stage_depends_on.find(receive_stage);
            if (dependencies == distributed_plan.stage_depends_on.end())
                continue;
            auto edge = dependencies->second.find(producer.stage);
            if (edge != dependencies->second.end()
                && distributed_plan.exchange_descriptions.at(edge->second).kind == ExchangeDescription::Kind::Persisted)
                chain_kind = ExchangeDescription::Kind::Persisted;
        }

        std::vector<size_t> level_sizes;
        for (size_t tasks = send_tasks.size(); tasks > 1;)
        {
            tasks = (tasks + fan_in - 1) / fan_in;
            level_sizes.push_back(tasks);
        }

        std::vector<String> level_exchange(level_sizes.size());
        std::vector<String> level_stage(level_sizes.size());
        for (size_t level = 0; level < level_sizes.size(); ++level)
        {
            level_exchange[level] = "exchange_" + std::to_string(next_exchange_id);
            level_stage[level] = "rf_merge_" + std::to_string(next_exchange_id);
            ++next_exchange_id;
        }
        Strings broadcast_exchange(remote_stages.size());
        for (auto & name : broadcast_exchange)
        {
            name = "exchange_" + std::to_string(next_exchange_id);
            ++next_exchange_id;
        }

        {
            ExchangeDescription exchange_description;
            exchange_description.name = level_exchange[0];
            exchange_description.kind = chain_kind;
            exchange_description.source_bucket_count = send_tasks.size();
            exchange_description.destination_bucket_count = level_sizes[0];
            distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;

            for (size_t source = 0; source < send_tasks.size(); ++source)
                send_tasks[source].output_exchange_streams.emplace_back(
                    ExchangeStreamId(level_exchange[0], source_buckets[source], toString(source / fan_in)));

            producer.step->setTreeExchange(level_exchange[0], source_buckets, fan_in);
            distributed_plan.stage_depends_on[level_stage[0]][producer.stage] = level_exchange[0];
        }

        Strings child_buckets = source_buckets;
        for (size_t level = 0; level < level_sizes.size(); ++level)
        {
            const bool is_root = level + 1 == level_sizes.size();
            chassert(!is_root || level_sizes[level] == 1);

            std::vector<MergeRuntimeFiltersStep::Output> outputs;
            if (!is_root)
            {
                outputs.push_back({level_exchange[level + 1], /*destination_buckets=*/{}});
            }
            else
            {
                for (size_t receive_index = 0; receive_index < remote_stages.size(); ++receive_index)
                {
                    Strings destination_buckets;
                    for (const auto & task : distributed_plan.stages.at(remote_stages[receive_index]).tasks)
                        destination_buckets.push_back(taskBucketId(task));
                    outputs.push_back({broadcast_exchange[receive_index], std::move(destination_buckets)});
                }
            }

            Strings level_buckets;
            for (size_t task_index = 0; task_index < level_sizes[level]; ++task_index)
                level_buckets.push_back(toString(task_index));

            DistributedQueryStage stage;
            stage.query_plan_fragment.addStep(
                std::make_unique<MergeRuntimeFiltersStep>(
                    producer.step->getFilterName(),
                    producer.step->getFilterColumnType(),
                    producer.step->getGeometry(),
                    level_exchange[level],
                    child_buckets,
                    fan_in,
                    outputs));

            for (size_t task_index = 0; task_index < level_sizes[level]; ++task_index)
            {
                DistributedQueryTask task;
                task.task_id = level_stage[level] + "_" + level_buckets[task_index];
                task.parameters.parameters["bucket_id"] = Field(level_buckets[task_index]);

                const size_t children_begin = task_index * fan_in;
                const size_t children_end = std::min(children_begin + fan_in, child_buckets.size());
                for (size_t child = children_begin; child < children_end; ++child)
                    task.input_exchange_streams.emplace_back(
                        ExchangeStreamId(level_exchange[level], child_buckets[child], level_buckets[task_index]));

                if (!is_root)
                {
                    task.output_exchange_streams.emplace_back(
                        ExchangeStreamId(level_exchange[level + 1], level_buckets[task_index], toString(task_index / fan_in)));
                }
                else
                {
                    for (const auto & output : outputs)
                        for (const String & destination_bucket : output.destination_buckets)
                            task.output_exchange_streams.emplace_back(
                                ExchangeStreamId(output.exchange_id, level_buckets[task_index], destination_bucket));
                }

                stage.tasks.emplace_back(std::move(task));
            }

            distributed_plan.stages[level_stage[level]] = std::move(stage);

            if (!is_root)
            {
                ExchangeDescription exchange_description;
                exchange_description.name = level_exchange[level + 1];
                exchange_description.kind = chain_kind;
                exchange_description.source_bucket_count = level_sizes[level];
                exchange_description.destination_bucket_count = level_sizes[level + 1];
                distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;

                distributed_plan.stage_depends_on[level_stage[level + 1]][level_stage[level]] = level_exchange[level + 1];
            }

            child_buckets = std::move(level_buckets);
        }

        /// The root broadcasts the complete union to every task of every receiving stage. The
        /// receiving stages depend on the root (and, transitively, on the whole chain down to the
        /// build stage), so the scheduler orders the chain correctly for both kinds.
        const String & root_stage = level_stage.back();
        for (size_t receive_index = 0; receive_index < remote_stages.size(); ++receive_index)
        {
            const auto & receive_stage = remote_stages[receive_index];
            auto & receive_tasks = distributed_plan.stages.at(receive_stage).tasks;

            ExchangeDescription exchange_description;
            exchange_description.name = broadcast_exchange[receive_index];
            exchange_description.kind = chain_kind;
            exchange_description.source_bucket_count = 1;
            exchange_description.destination_bucket_count = receive_tasks.size();
            distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;

            for (auto & task : receive_tasks)
            {
                std::vector<ExchangeStreamId> streams;
                streams.emplace_back(broadcast_exchange[receive_index], "0", taskBucketId(task));
                task.runtime_filter_descriptors.push_back(
                    {key, producer.step->getFilterName(), producer.step->getFilterColumnType(), geometry, streams});
                task.input_exchange_streams.insert(task.input_exchange_streams.end(), streams.begin(), streams.end());
            }

            distributed_plan.stage_depends_on[receive_stage][root_stage] = broadcast_exchange[receive_index];
        }
    }
}

}
