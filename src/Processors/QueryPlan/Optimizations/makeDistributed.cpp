#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace DB
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

/// Replaces LogicalJoin step with a subtree like this:
///
///   GatherExchange
///     LogicalJoin
///       ShuffleExchange by hash(join_key)
///         Expression: compute join key for right source
///         ...
///       ShuffleExchange by hash(join_key)
///         Expression: compute join key for left source
///         ...
void tryMakeDistributedJoin(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Is this a join step?
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return;

    /// Joining two sources?
    if (node.children.size() != 2)
        return;

    /// Check if join is possible to be distributed
    const auto & join_info = join_step->getJoinInfo();
    if (join_info.kind != JoinKind::Inner ||
        join_info.strictness != JoinStrictness::All ||
        (join_info.locality != JoinLocality::Unspecified && join_info.locality != JoinLocality::Global) ||
        !join_info.expression.disjunctive_conditions.empty())
    {
        return;
    }

    Names join_keys_a;
    Names join_keys_b;

    /// Only equi-join is supported
    const auto & join_condition = join_info.expression.condition;
    for (const auto & predicate : join_condition.predicates)
    {
        if (predicate.op != PredicateOperator::Equals)
            return;

        join_keys_a.push_back(predicate.left_node.getColumnName());
        join_keys_b.push_back(predicate.right_node.getColumnName());
    }

    QueryPlan::Node * source_a = node.children[0];
    QueryPlan::Node * source_b = node.children[1];

    /// Extract expressions for calculating join on keys
    /// Move them into separate nodes
    /// Replace pre-join actions in the join step with pass-through (no-op) actions
    {
        const auto & actions = join_step->getExpressionActions();

        /// Replaces the internals of ActionsDAG with no-op actions that just pass specified columns without any transformations
        /// This is done in-place because JoinActionRef-s store column names and pointers to ActionsDAG-s
        auto replace_with_pass_through_actions = [](ActionsDAG & actions_dag, const Block & header)
        {
            actions_dag = ActionsDAG(); /// Clear the actions DAG
            for (const auto & column : header.getColumnsWithTypeAndName())
                actions_dag.addOrReplaceInOutputs(actions_dag.addInput(column));
        };

        if (actions.left_pre_join_actions)
        {
            source_a = makeExpressionNodeOnTopOf(source_a, std::move(*actions.left_pre_join_actions), {}, nodes);
            replace_with_pass_through_actions(*actions.left_pre_join_actions, source_a->step->getOutputHeader());
            join_step->updateInputHeader(source_a->step->getOutputHeader(), 0);
        }

        if (actions.right_pre_join_actions)
        {
            source_b = makeExpressionNodeOnTopOf(source_b, std::move(*actions.right_pre_join_actions), {}, nodes);
            replace_with_pass_through_actions(*actions.right_pre_join_actions, source_b->step->getOutputHeader());
            join_step->updateInputHeader(source_b->step->getOutputHeader(), 1);
        }
    }

    const size_t bucket_count = optimization_settings.default_shuffle_join_bucket_count;    /// TODO: estimate number of buckets based on statistics and available nodes and memory

    /// Add shuffle exchange step above read from right source
    auto & exchange_shuffle_a_node = nodes.emplace_back();
    exchange_shuffle_a_node.step = std::make_unique<ShuffleExchangeStep>(source_a->step->getOutputHeader(), join_keys_a, bucket_count);
    exchange_shuffle_a_node.step->setStepDescription(fmt::format("by hash([{}])", fmt::join(join_keys_a, ", ")));
    exchange_shuffle_a_node.children = {source_a};

    /// Add shuffle exchange step above read from left source
    auto & exchange_shuffle_b_node = nodes.emplace_back();
    exchange_shuffle_b_node.step = std::make_unique<ShuffleExchangeStep>(source_b->step->getOutputHeader(), join_keys_b, bucket_count);
    exchange_shuffle_b_node.step->setStepDescription(fmt::format("by hash([{}])", fmt::join(join_keys_b, ", ")));
    exchange_shuffle_b_node.children = {source_b};

    /// Move join step to a new node
    auto & new_join_node = nodes.emplace_back();
    new_join_node.step = std::move(node.step);
    new_join_node.children = {&exchange_shuffle_a_node, &exchange_shuffle_b_node};

    /// Add gather exchange step above join
    QueryPlan::Node gather_node;
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_join_node.step->getOutputHeader());
    gather_node.step = std::move(exchange_gather_step);
    gather_node.children = {&new_join_node};

    /// Replace join node with gather node
    node = std::move(gather_node);
}


/// One way to parallelize aggregation is to split data into buckets by hash of aggregation keys.
/// Then results of aggregation of all buckets can just be united.
/// The other approach is to do partial aggregation on data into aggregation states regardless of how it is split and
/// then gather partial results and merge them finalizing aggregation states.
///
/// This function only implements the first approach. It replaces AggregatingStep step with a subtree like this:
///
///   GatherExchange
///     AggregatingStep
///       ShuffleExchange by hash(aggregation_keys)
void tryMakeDistributedAggregation(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Is this a aggregating step?
    auto * aggregating_step = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating_step)
        return;

    /// Only one source is expected for aggregation step
    if (node.children.size() != 1)
        return;
    QueryPlan::Node * source = node.children[0];

    Names aggregation_keys = aggregating_step->getParams().keys;
    /// Cannot partition if this is full aggregation
    if (aggregation_keys.empty())
        return;

    const size_t bucket_count = optimization_settings.default_shuffle_join_bucket_count;    /// TODO: estimate number of buckets based on statistics and available nodes and memory

    /// Add shuffle exchange step above source
    auto & exchange_shuffle_node = nodes.emplace_back();
    exchange_shuffle_node.step = std::make_unique<ShuffleExchangeStep>(source->step->getOutputHeader(), aggregation_keys, bucket_count);
    exchange_shuffle_node.step->setStepDescription(fmt::format("by hash([{}])", fmt::join(aggregation_keys, ", ")));
    exchange_shuffle_node.children = {source};

    /// Move aggregation step to a new node
    auto & new_aggregation_node = nodes.emplace_back();
    new_aggregation_node.step = std::move(node.step);
    new_aggregation_node.children = {&exchange_shuffle_node};

    /// Add gather exchange step above aggregation
    QueryPlan::Node gather_node;
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_aggregation_node.step->getOutputHeader());
    gather_node.step = std::move(exchange_gather_step);
    gather_node.children = {&new_aggregation_node};

    /// Replace aggregation node with gather node
    node = std::move(gather_node);
}

void tryMakeDistributedSorting(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Is this a sorting step?
    auto * sorting_step = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting_step)
        return;

    /// Only one source is expected for sorting step
    if (node.children.size() != 1)
        return;
    QueryPlan::Node * source = node.children[0];

    const size_t bucket_count = optimization_settings.distributed_plan_default_shuffle_join_bucket_count;    /// TODO: estimate number of buckets based on statistics and available nodes and memory
    auto sort_description = sorting_step->getSortDescription();

    /// Add "any" scatter exchange step above source. It will allow to optimize out unnecessary shuffle if the input is already parallelized in any way.
    /// TODO: need a special step with "any" partitioning?
    auto & exchange_scatter_node = nodes.emplace_back();
    exchange_scatter_node.step = std::make_unique<ScatterExchangeStep>(source->step->getOutputHeader(), Names{}, bucket_count);
    exchange_scatter_node.step->setStepDescription("any scatter");
    exchange_scatter_node.children = {source};

    /// Move sorting step to a new node
    auto & new_sorting_node = nodes.emplace_back();
    new_sorting_node.step = std::move(node.step);
    new_sorting_node.children = {&exchange_scatter_node};

    /// Add merge sorted gather exchange step above sorting
    QueryPlan::Node gather_node;
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_sorting_node.step->getOutputHeader(), sort_description);
    exchange_gather_step->setStepDescription(fmt::format("sorted by ({})", dumpSortDescription(sort_description)));
    gather_node.step = std::move(exchange_gather_step);
    gather_node.children = {&new_sorting_node};

    /// Replace sorting node with gather node
    node = std::move(gather_node);
}

/// Replaces ReadFromMergeTree step with a subtree like this:
///
///   GatherExchange
///     (Distributed)ReadFromMergeTree
void tryMakeDistributedRead(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    /// Is this a read from MergeTree step?
    auto * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(node.step.get());
    if (!read_from_merge_tree_step)
        return;

    /// Should not have children
    if (!node.children.empty())
        return;

    /// Move read step to a new node and set it to distributed read
    read_from_merge_tree_step->setDistributedRead();
    auto & new_read_node = nodes.emplace_back();
    new_read_node.step = std::move(node.step);

    /// Add gather exchange step above read
    QueryPlan::Node gather_node;
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_read_node.step->getOutputHeader());
    gather_node.step = std::move(exchange_gather_step);
    gather_node.children = {&new_read_node};

    /// Replace aggregation node with gather node
    node = std::move(gather_node);
}


/// 1. Moves exchanges where possible to parallelize more work. Example: if there is a Filter step on top of an GatherExchange step
/// then filter step can be moved below the exchange step to allow parallel processing.
/// 2. Removes unnecessary exchanges. Example: if there is a ShuffleExchange step on top of another exchange step then child
/// exchange step can be removed.
void optimizeExchanges(QueryPlan::Node & root)
{
    Stack stack;

    stack.push_back({.node = &root});
    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }
        else /// After all children were processed
        {
            /// Try to push up GatherExchange above Expression or Filter step
            if (frame.node->children.size() == 1 &&
                (typeid_cast<ExpressionStep *>(frame.node->step.get()) || typeid_cast<FilterStep *>(frame.node->step.get())))
            {
                auto & child_node = *frame.node->children[0];
                auto * gather_step = typeid_cast<GatherExchangeStep *>(child_node.step.get());
                if (gather_step)
                {
                    Header expression_header = frame.node->step->getOutputHeader();

                    /// If gather step has maintain_sort_description then we need to check that those columns are preserved
                    bool can_move_gather_up = true;
                    if (gather_step->getMaintainSortDescription())
                    {
                        const auto & sort_description = gather_step->getMaintainSortDescription().value();
                        for (const auto & column : sort_description)
                        {
                            if (!expression_header.has(column.column_name))
                                can_move_gather_up = false;
                        }
                    }

                    if (can_move_gather_up)
                    {
                        std::swap(frame.node->step, child_node.step);
                        frame.node->step->updateInputHeader(expression_header);
                    }
                }
            }

            /// If there is a Exchange step on top of another Exchange step then child Exchange step can be removed
            auto is_exchange_step = [](const IQueryPlanStep * step)
            {
                return dynamic_cast<const LogicalExchangeStep *>(step) != nullptr;
            };
            if (frame.node->children.size() == 1 && is_exchange_step(frame.node->step.get()) && is_exchange_step(frame.node->children[0]->step.get()))
            {
                frame.node->children = std::move(frame.node->children[0]->children);
            }
        }

        stack.pop_back();
    }
}


/// Tries to build list of possible shards for the read steps that can be processed in parallel.
Strings makeListOfShardsForReadStep(const IQueryPlanStep * read_step, const QueryPlanOptimizationSettings & optimization_settings)
{
    const auto * read_from_mt = dynamic_cast<const ReadFromMergeTree *>(read_step);
    if (read_from_mt)
        return read_from_mt->getShardsForDistributedRead(optimization_settings);

    return {"0"};   /// One shard by default if read step is not distributed
}

/// Builds distributed plan by splitting the query plan into multiple stages connected by exchanges.
/// Exchange step are split into ExchangeSink and ExchangeSource.
/// This allows to build a separate plan fragment (a part of the original full plan) for each stage.
DistributedQueryPlan makeDistributedPlan(QueryPlan::Nodes /*nodes*/, QueryPlan::Node * root, const QueryPlanOptimizationSettings & optimization_settings)
{
    size_t exchange_id = 0;

    DistributedQueryPlan distributed_plan;
    DistributedQueryTask main_task;

    QueryPlan plan_fragment;
    std::unordered_map<String, String> main_stage_depends_on;

    {
        struct Frame
        {
            QueryPlan::Node * node = nullptr;
            size_t next_child = 0;
            std::vector<std::unique_ptr<QueryPlan>> child_plans{};
            std::unordered_map<String, DistributedQueryTask> list_of_shards{};
            std::unordered_map<String, String> depends_on_stages{};
        };

        std::vector<Frame> stack;
        stack.push_back({.node = root});

        std::unique_ptr<QueryPlan> current_plan = std::make_unique<QueryPlan>();
        std::unordered_map<String, DistributedQueryTask> current_list_of_shards;     /// Tasks for shards that can be processed in parallel by the current_plan
        std::unordered_map<String, String> current_stage_depends_on;

        while (!stack.empty())
        {
            /// NOTE: frame cannot be safely used after stack was modified.
            auto & frame = stack.back();

            /// On entering the node.
            if (frame.next_child == 0)
            {
                /// Nothing to do
            }

            /// Returned from child
            if (frame.next_child > 0)
            {
                if (frame.next_child == 1)
                {
                    /// First child, take its list of shards
                    frame.list_of_shards = std::move(current_list_of_shards);
                    current_list_of_shards = {};
                }
                else
                {
                    /// Check that child plan has the same list of shards
                    if (frame.list_of_shards.size() != current_list_of_shards.size())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Different list of shards in child plans {} and {}",
                            frame.list_of_shards.size(), current_list_of_shards.size());

                    /// Add parameters and temporary files from the child plan
                    for (auto & [shard, task] : current_list_of_shards)
                    {
                        auto it = frame.list_of_shards.find(shard);
                        if (it == current_list_of_shards.end())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Shard {} is missing in the list of shards", shard);

                        it->second.parameters.parameters.insert(task.parameters.parameters.begin(), task.parameters.parameters.end());
                        it->second.input_exchange_streams.insert(it->second.input_exchange_streams.end(),
                            task.input_exchange_streams.begin(), task.input_exchange_streams.end());
                    }
                }

                frame.child_plans.emplace_back(std::move(current_plan));
                frame.depends_on_stages.insert(current_stage_depends_on.begin(), current_stage_depends_on.end());
                current_stage_depends_on.clear();
            }

            /// Traverse next child
            if (frame.next_child < frame.node->children.size())
            {
                auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
                ++frame.next_child;
                stack.push_back(std::move(next_frame));
                continue;
            }

            /// All children were traversed;
            assert(frame.next_child == frame.node->children.size());

            if (frame.child_plans.size() > 1)
            {
                /// Step has multiple inputs
                current_plan = std::make_unique<QueryPlan>();
                current_plan->unitePlans(std::move(frame.node->step), std::move(frame.child_plans));
            }
            else if (frame.child_plans.size() == 1)
            {
                /// Step has only one input
                current_plan = std::move(frame.child_plans.front());

                const auto * exchange_step = dynamic_cast<const LogicalExchangeStep *>(frame.node->step.get());

                if (exchange_step && !optimization_settings.distributed_plan_singe_stage)
                {
                    /// Make unique name for the exchange
                    const String stage_name = "stage_" + std::to_string(exchange_id);
                    ExchangeDescription exchange_description;
                    exchange_description.name = "exchange_" + std::to_string(exchange_id);
                    ++exchange_id;
                    exchange_description.kind = optimization_settings.force_exchange_kind == "Persisted" ?
                        ExchangeDescription::Kind::Persisted : ExchangeDescription::Kind::Streaming;
                    exchange_description.source_bucket_count = frame.list_of_shards.size();
                    exchange_description.destination_bucket_count = exchange_step->getResultBucketCount();

                    distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;

                    Strings source_shards;
                    for (auto & [source_shard, _] : frame.list_of_shards)
                        source_shards.push_back(source_shard);

                    auto send_and_receive_steps = exchange_step->createSinkAndSourcePair(exchange_description.name, source_shards);
                    send_and_receive_steps.first->setStepDescription(exchange_description.name);
                    send_and_receive_steps.second->setStepDescription(exchange_description.name);

                    Strings list_of_exchange_shards = shardsForShuffleBuckets(exchange_step->getResultBucketCount());

                    /// Finish current plan fragment with exchange sink
                    current_plan->addStep(std::move(send_and_receive_steps.first));
                    /// Create stage with the current plan fragment
                    {
                        DistributedQueryStage stage;

                        /// Create a task for each of the current shards
                        for (auto & [source_shard, source_task] : frame.list_of_shards)
                        {
                            source_task.task_id = stage_name + "_" + source_shard;

                            /// List of output streams for the exchange sink
                            for (const auto & destination_shard : list_of_exchange_shards)
                                source_task.output_exchange_streams.emplace_back(streamNameForExchange(exchange_description.name, source_shard, destination_shard));

                            /// Move source tasks to the source stage
                            stage.tasks.emplace_back(std::move(source_task));
                        }
                        stage.query_plan_fragment = std::move(*current_plan);
                        distributed_plan.stages[stage_name] = std::move(stage);
                        /// Add dependency from previous stages if any
                        distributed_plan.stage_depends_on[stage_name] = std::move(frame.depends_on_stages);
                        frame.depends_on_stages = {};
                    }

                    /// Prepare tasks for the next stage
                    std::unordered_map<String, DistributedQueryTask> destination_stage_tasks;
                    for (const auto & destination_shard : list_of_exchange_shards)
                    {
                        DistributedQueryTask destination_task;
                        destination_task.parameters.parameters["bucket_id"] = Field(destination_shard);
                        destination_task.parameters.parameters["total_buckets"] = Field(list_of_exchange_shards.size());

                        /// List of input streams for the exchange source
                        for (auto & [source_shard, source_task] : frame.list_of_shards)
                            destination_task.input_exchange_streams.emplace_back(streamNameForExchange(exchange_description.name, source_shard, destination_shard));

                        destination_stage_tasks[destination_shard] = std::move(destination_task);
                    }

                    /// Add previous stage to the current list of dependencies
                    frame.depends_on_stages.insert({stage_name, exchange_description.name});

                    /// And start a new plan fragment with exchange source
                    current_plan = std::make_unique<QueryPlan>();
                    current_plan->addStep(std::move(send_and_receive_steps.second));
                    frame.list_of_shards = std::move(destination_stage_tasks);
                }
                else
                {
                    /// Add current step on top of the current plan
                    current_plan->addStep(std::move(frame.node->step));
                }
            }
            else
            {
                /// No children, this means that this is a leaf step
                auto shards_for_read = makeListOfShardsForReadStep(frame.node->step.get(), optimization_settings);

                current_plan = std::make_unique<QueryPlan>();
                current_plan->addStep(std::move(frame.node->step));

                for (size_t bucket = 0; bucket < shards_for_read.size(); ++bucket)
                {
                    String shard_id = toString(bucket);
                    DistributedQueryTask task;
                    task.parameters.parameters["bucket_id"] = Field(shard_id);
                    task.parameters.parameters["bucket_description"] = Field(shards_for_read[bucket]);
                    task.parameters.parameters["total_buckets"] = Field(shards_for_read.size());
                    frame.list_of_shards[shard_id] = std::move(task);
                }
            }

            current_stage_depends_on = std::move(frame.depends_on_stages);
            current_list_of_shards = std::move(frame.list_of_shards);

            /// On leaving the last node.
            if (stack.size() == 1)
            {
                plan_fragment = std::move(*current_plan);
                main_stage_depends_on = std::move(current_stage_depends_on);
                current_stage_depends_on = {};
            }

            stack.pop_back();
        }

        assert(current_list_of_shards.size() == 1);
        main_task = std::move(current_list_of_shards.begin()->second);
    }

    /// Add last plan fragment as the main stage
    {
        DistributedQueryStage stage;
        stage.query_plan_fragment = std::move(plan_fragment);

        main_task.task_id = "main";
        stage.tasks.emplace_back(std::move(main_task));

        distributed_plan.stages["main"] = std::move(stage);
        distributed_plan.stage_depends_on["main"] = main_stage_depends_on;
    }

    return distributed_plan;
}

}

}
