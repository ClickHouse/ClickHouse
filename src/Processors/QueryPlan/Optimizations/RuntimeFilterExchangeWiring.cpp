#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/MergeRuntimeFiltersStep.h>
#include <Processors/QueryPlan/Optimizations/RuntimeFilterExchangeWiring.h>
#include <Processors/QueryPlan/ReceiveRuntimeFilterStep.h>
#include <Processors/QueryPlan/SendRuntimeFilterStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

String taskBucketId(const DistributedQueryTask & task)
{
    return task.parameters.parameters.at("bucket_id").safeGet<String>();
}

}

void wireRuntimeFilterExchangeTopology(
    DistributedQueryPlan & distributed_plan, size_t & next_exchange_id, ExchangeDescription::Kind default_kind)
{
    struct RuntimeFilterEndpoints
    {
        SendRuntimeFilterStep * send = nullptr;
        String send_stage;
        /// One filter can be applied in several probe-side stages, each with its own receive.
        std::vector<std::pair<ReceiveRuntimeFilterStep *, String>> receives;
    };
    std::unordered_map<String, RuntimeFilterEndpoints> filter_endpoints;

    for (auto & [stage_name, stage] : distributed_plan.stages)
    {
        std::vector<QueryPlan::Node *> stack = {stage.query_plan_fragment.getRootNode()};
        while (!stack.empty())
        {
            auto * node = stack.back();
            stack.pop_back();
            for (auto * child : node->children)
                stack.push_back(child);

            if (auto * send = typeid_cast<SendRuntimeFilterStep *>(node->step.get()))
            {
                auto & endpoints = filter_endpoints[send->getFilterKey()];
                endpoints.send = send;
                endpoints.send_stage = stage_name;
            }
            else if (auto * receive = typeid_cast<ReceiveRuntimeFilterStep *>(node->step.get()))
            {
                filter_endpoints[receive->getFilterKey()].receives.emplace_back(receive, stage_name);
            }
        }
    }

    for (auto & [_, endpoints] : filter_endpoints)
    {
        if (!endpoints.send)
            continue;

        /// A receive that ended up in the send's own stage keeps its empty exchange id and stays
        /// a passthrough: rows are simply not pre-filtered.
        std::vector<std::pair<ReceiveRuntimeFilterStep *, String>> remote_receives;
        for (const auto & receive : endpoints.receives)
            if (receive.second != endpoints.send_stage)
                remote_receives.push_back(receive);
        if (remote_receives.empty())
            continue;

        auto & send_tasks = distributed_plan.stages.at(endpoints.send_stage).tasks;
        Strings source_buckets;
        for (const auto & task : send_tasks)
            source_buckets.push_back(taskBucketId(task));

        if (send_tasks.size() == 1)
        {
            /// A single build task is itself the root of the merge tree: its complete partial is
            /// broadcast directly, one exchange per receiving stage. Unlike the shared chain of a
            /// real tree below, each of these edges is independent, so the exchange kind follows
            /// the per-pair rule (copy the kind of an existing data edge between the two stages).
            for (auto & [receive, receive_stage] : remote_receives)
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

                /// The plan's own kind, so probe tasks receive the filter while they run whenever
                /// the data exchanges stream too. A Persisted plan keeps persisted filters: its
                /// streaming transport is not even started. Additionally, when the scheduler
                /// already waits for the whole build stage before starting the receiving stage (a
                /// Persisted edge between the same stages), a streaming sink would wait for a
                /// consumer that never starts; a persisted filter follows the data and adds no
                /// ordering.
                exchange_description.kind = default_kind;
                auto dependencies = distributed_plan.stage_depends_on.find(receive_stage);
                const bool edge_exists
                    = dependencies != distributed_plan.stage_depends_on.end() && dependencies->second.contains(endpoints.send_stage);
                if (edge_exists)
                {
                    const auto & data_exchange = dependencies->second.at(endpoints.send_stage);
                    exchange_description.kind = distributed_plan.exchange_descriptions.at(data_exchange).kind;
                }

                distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;
                endpoints.send->addExchange(exchange_description.name, destination_buckets);
                receive->setExchange(exchange_description.name, source_buckets);

                for (const String & destination_bucket : destination_buckets)
                    send_tasks.front().output_exchange_streams.emplace_back(
                        ExchangeStreamId(exchange_description.name, source_buckets.front(), destination_bucket));
                for (size_t destination = 0; destination < receive_tasks.size(); ++destination)
                    receive_tasks[destination].input_exchange_streams.emplace_back(
                        ExchangeStreamId(exchange_description.name, source_buckets.front(), destination_buckets[destination]));

                if (!edge_exists)
                    distributed_plan.stage_depends_on[receive_stage][endpoints.send_stage] = exchange_description.name;
            }
            continue;
        }

        /// Several build tasks: a bounded fan-in merge tree instead of all-to-all delivery. Every
        /// build task sends its partial once to its parent merge task; each merge level combines
        /// complete child states; the single root task broadcasts the global union once per
        /// destination task of every receiving stage.
        const size_t fan_in = RUNTIME_FILTER_MERGE_FAN_IN;

        /// The whole chain shares one exchange kind: the plan's own kind, so probe tasks receive
        /// the filter while they run whenever the data exchanges stream too, and a Persisted plan
        /// (whose streaming transport is never started) keeps persisted filters. Additionally, when
        /// the scheduler already waits for the whole build
        /// stage before starting some receiving stage (a Persisted data edge between the two), a
        /// streaming chain would deadlock: the build task's filter sink would wait for a merge
        /// consumer that is only started -- transitively, through the receiving stage -- after the
        /// build stage completes. A persisted chain follows the same complete-before-consume order
        /// the scheduler already imposes.
        ///
        /// Checking the direct (send stage, receive stage) edges suffices only because exchange
        /// kinds are homogeneous today: the data edges are all Streaming unless
        /// `distributed_plan_force_exchange_kind` persists every one of them, so a persisted edge
        /// anywhere implies a persisted edge on the direct pair too (and implies the build stage
        /// has no streaming data sinks that could block its completion). If per-exchange kinds
        /// ever become mixed, this rule must consider transitive persisted ordering between the
        /// chain's stages instead.
        auto chain_kind = default_kind;
        for (const auto & [receive, receive_stage] : remote_receives)
        {
            auto dependencies = distributed_plan.stage_depends_on.find(receive_stage);
            if (dependencies == distributed_plan.stage_depends_on.end())
                continue;
            auto edge = dependencies->second.find(endpoints.send_stage);
            if (edge != dependencies->second.end()
                && distributed_plan.exchange_descriptions.at(edge->second).kind == ExchangeDescription::Kind::Persisted)
                chain_kind = ExchangeDescription::Kind::Persisted;
        }

        /// Merge level sizes: ceil(S / fan_in), then ceil of that, ... down to the single root.
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
        Strings broadcast_exchange(remote_receives.size());
        for (auto & name : broadcast_exchange)
        {
            name = "exchange_" + std::to_string(next_exchange_id);
            ++next_exchange_id;
        }

        /// Build tasks feed the first merge level.
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

            endpoints.send->setTreeExchange(level_exchange[0], source_buckets, fan_in);
            distributed_plan.stage_depends_on[level_stage[0]][endpoints.send_stage] = level_exchange[0];
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
                for (size_t receive_index = 0; receive_index < remote_receives.size(); ++receive_index)
                {
                    Strings destination_buckets;
                    for (const auto & task : distributed_plan.stages.at(remote_receives[receive_index].second).tasks)
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
                    endpoints.send->getFilterName(),
                    endpoints.send->getFilterColumnType(),
                    endpoints.send->getGeometry(),
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
        for (size_t receive_index = 0; receive_index < remote_receives.size(); ++receive_index)
        {
            auto & [receive, receive_stage] = remote_receives[receive_index];
            auto & receive_tasks = distributed_plan.stages.at(receive_stage).tasks;

            ExchangeDescription exchange_description;
            exchange_description.name = broadcast_exchange[receive_index];
            exchange_description.kind = chain_kind;
            exchange_description.source_bucket_count = 1;
            exchange_description.destination_bucket_count = receive_tasks.size();
            distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;

            receive->setExchange(broadcast_exchange[receive_index], /*source_buckets_=*/{"0"});
            for (auto & task : receive_tasks)
                task.input_exchange_streams.emplace_back(ExchangeStreamId(broadcast_exchange[receive_index], "0", taskBucketId(task)));

            distributed_plan.stage_depends_on[receive_stage][root_stage] = broadcast_exchange[receive_index];
        }
    }
}

}
