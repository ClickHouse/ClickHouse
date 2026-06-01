#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/ScatterExchangeStep.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr);

/// Replaces LogicalJoin step with a subtree like this:
///
///   GatherExchange
///     LogicalJoin
///       ScatterExchange by hash(join_key)
///         Expression: compute join key for right source
///         ...
///       ScatterExchange by hash(join_key)
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

    /// Check if join is possible to be distributed.
    const auto & join_info = join_step->getJoinOperator();

    /// Must have a known locality.
    if (join_info.locality != JoinLocality::Unspecified && join_info.locality != JoinLocality::Global)
        return;

    /// Must have at least one equi-join predicate (required for shuffle partitioning).
    if (std::ranges::all_of(join_info.expression, [](const auto & expr) { return !expr.isFunction(JoinConditionOperator::Equals); }))
        return;

    QueryPlan::Node * source_a = node.children[0];
    QueryPlan::Node * source_b = node.children[1];

    auto row_count_b = estimateReadRowsCount(*source_b).estimated_rows;

    enum DistributedJoinStrategy
    {
        Shuffle,
        Broadcast
    } strategy = Shuffle;

    /// In a broadcast join, the right side is replicated to all workers and the
    /// left side is scattered.  For RIGHT and FULL joins the right side can
    /// produce unmatched output rows.  When the right side is replicated, every
    /// worker independently decides which right rows are unmatched based only on
    /// its local slice of the left side, producing duplicate unmatched rows.
    const bool broadcast_unsafe
        = join_info.kind == JoinKind::Right
        || join_info.kind == JoinKind::Full;

    /// Check if right table is small enough for broadcast
    if (!broadcast_unsafe && row_count_b && row_count_b <= optimization_settings.distributed_plan_max_rows_to_broadcast)
        strategy = Broadcast;

    QueryPlan::Node * exchange_scatter_a_node = nullptr;
    QueryPlan::Node * exchange_scatter_b_node = nullptr;

    size_t bucket_count = optimization_settings.distributed_plan_default_shuffle_join_bucket_count;

    if (strategy == Broadcast)
    {
        LOG_DEBUG(getLogger("tryMakeDistributedJoin"),
            "Estimated number of rows in right source: {}. Using broadcast join",
            row_count_b.transform(toString<UInt64>).value_or("unknown"));

        exchange_scatter_a_node = &nodes.emplace_back();
        exchange_scatter_b_node = &nodes.emplace_back();

        /// Add scatter exchange step above read from left source
        exchange_scatter_a_node->step = std::make_unique<ScatterExchangeStep>(source_a->step->getOutputHeader(), Names{}, bucket_count);
        exchange_scatter_a_node->step->setStepDescription("any scatter");

        /// Add broadcast exchange step above read from right source
        exchange_scatter_b_node->step = std::make_unique<BroadcastExchangeStep>(source_b->step->getOutputHeader(), bucket_count);
        exchange_scatter_b_node->step->setStepDescription("");
    }
    else
    {
        LOG_DEBUG(getLogger("tryMakeDistributedJoin"),
            "Estimated number of rows in right source: {}. Using {} buckets for shuffle join",
            row_count_b.transform(toString<UInt64>).value_or("unknown"),
            bucket_count);

        /// Extract expressions for calculating join on keys
        auto key_dags = join_step->preCalculateKeys(source_a->step->getOutputHeader(), source_b->step->getOutputHeader());
        if (!key_dags)
            return;

        auto get_node_name = [](const auto * e) { return e->result_name; };
        auto join_keys_a = std::ranges::to<Names>(key_dags->first.keys | std::views::transform(get_node_name));
        auto join_keys_b = std::ranges::to<Names>(key_dags->second.keys | std::views::transform(get_node_name));
        if (!isPassthroughActions(key_dags->first.actions_dag))
            makeExpressionNodeOnTopOf(*source_a, std::move(key_dags->first.actions_dag), nodes, makeDescription("Calculate left join keys"));
        if (!isPassthroughActions(key_dags->second.actions_dag))
            makeExpressionNodeOnTopOf(*source_b, std::move(key_dags->second.actions_dag), nodes, makeDescription("Calculate right join keys"));

        /// Add scatter exchange step above read from left source
        exchange_scatter_a_node = &nodes.emplace_back();
        exchange_scatter_a_node->step = std::make_unique<ScatterExchangeStep>(source_a->step->getOutputHeader(), join_keys_a, bucket_count);
        exchange_scatter_a_node->step->setStepDescription(fmt::format("by hash([{}])", fmt::join(join_keys_a, ", ")), optimization_settings.max_step_description_length);

        /// Add scatter exchange step above read from right source
        exchange_scatter_b_node = &nodes.emplace_back();
        exchange_scatter_b_node->step = std::make_unique<ScatterExchangeStep>(source_b->step->getOutputHeader(), join_keys_b, bucket_count);
        exchange_scatter_b_node->step->setStepDescription(fmt::format("by hash([{}])", fmt::join(join_keys_b, ", ")), optimization_settings.max_step_description_length);
    }

    exchange_scatter_a_node->children = {source_a};
    exchange_scatter_b_node->children = {source_b};

    /// Move join step to a new node
    auto & new_join_node = nodes.emplace_back();
    new_join_node.step = std::move(node.step);
    new_join_node.children = {exchange_scatter_a_node, exchange_scatter_b_node};

    /// Add gather exchange step above join
    QueryPlan::Node gather_node;
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_join_node.step->getOutputHeader(), bucket_count);
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
/// In the first approach the AggregatingStep is replaced with a subtree like this:
///
///   GatherExchange
///     AggregatingStep
///       ScatterExchange by hash(aggregation_keys)
///
/// In the second approach the AggregatingStep is replaced with a subtree like this:
///
///   MergingAggregated (merge)
///     GatherExchange
///       Aggregating (partial)
///         ScatterExchange (any)
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

    enum AggregationStrategy
    {
        PartialAggregation, /// Do partial aggregation and then merge aggregation states
        Shuffle,            /// Partition data by aggregation keys and do aggregation in disjoint buckets, then just unite the results
    } strategy = PartialAggregation;

    /// Choose Shuffle when the estimated number of groups is high.
    if (!aggregation_keys.empty())
    {
        auto input_stats = estimateReadRowsCount(*source);

        /// Use max NDV among GROUP BY keys as a lower-bound estimate for groups.
        std::optional<UInt64> estimated_groups;
        for (const auto & key : aggregation_keys)
        {
            auto it = input_stats.column_stats.find(key);
            if (it != input_stats.column_stats.end() && it->second.num_distinct_values > 0)
                estimated_groups = std::max(estimated_groups.value_or(0), it->second.num_distinct_values);
        }

        /// Fall back to input row count as an upper bound when NDV is unavailable.
        if (!estimated_groups && input_stats.estimated_rows)
            estimated_groups = input_stats.estimated_rows;

        if (estimated_groups && *estimated_groups > optimization_settings.distributed_plan_max_rows_to_broadcast)
            strategy = Shuffle;
        else if (!estimated_groups)
            /// No stats at all - default to Shuffle to be safe.
            strategy = Shuffle;
    }

    /// Fallback to Shuffle strategy for the cases when partial aggregation is not supported
    const bool can_use_partial_aggregation = !aggregating_step->inOrder() && !aggregating_step->explicitSortingRequired();
    if (!can_use_partial_aggregation)
        strategy = Shuffle;

    if (optimization_settings.distributed_plan_force_shuffle_aggregation && !aggregation_keys.empty())
        strategy = Shuffle;

    if (strategy == PartialAggregation)
    {
        const size_t bucket_count = optimization_settings.distributed_plan_default_shuffle_join_bucket_count;    /// TODO: estimate number of buckets based on statistics and available nodes and memory

        /// Add any-scatter
        auto & exchange_scatter_node = nodes.emplace_back();
        exchange_scatter_node.step = std::make_unique<ScatterExchangeStep>(source->step->getOutputHeader(), Names{}, bucket_count);
        exchange_scatter_node.step->setStepDescription("any");
        exchange_scatter_node.children = {source};

        /// Params will be used by merge step
        Aggregator::Params aggregator_params = aggregating_step->getParams();
        GroupingSetsParamsList grouping_sets_params = aggregating_step->getGroupingSetsParamsList();
        const bool has_grouping_sets = !grouping_sets_params.empty();

        const bool should_produce_results_in_order_of_bucket_number = aggregating_step->shouldProduceResultsInBucketOrder();
        const bool memory_bound_merging_of_aggregation_results_enabled = aggregating_step->usingMemoryBoundMerging();
        const bool original_step_was_final = aggregating_step->getFinal();   /// Save whether the original AggregatingStep was final or partial

        /// Convert Aggregation step to partial aggregation
        auto & partial_aggregation_node = nodes.emplace_back();
        partial_aggregation_node.step = aggregating_step->clone();
        typeid_cast<AggregatingStep *>(partial_aggregation_node.step.get())->setFinal(false);
        partial_aggregation_node.step->setStepDescription("partial");
        partial_aggregation_node.children = {&exchange_scatter_node};

        /// Add gather
        auto & gather_node = nodes.emplace_back();
        gather_node.step = std::make_unique<GatherExchangeStep>(partial_aggregation_node.step->getOutputHeader(), bucket_count);
        gather_node.children = {&partial_aggregation_node};

        /// Replace original aggregation step with MergingAggregated step
        aggregator_params.only_merge = true;    /// Merge partial aggregation results
        QueryPlanStepPtr final_aggregation_step = std::make_unique<MergingAggregatedStep>(
            gather_node.step->getOutputHeader(),
            aggregator_params,
            grouping_sets_params,
            /* final */ original_step_was_final,
            /// Grouping sets don't work with distributed_aggregation_memory_efficient enabled (#43989)
            optimization_settings.distributed_aggregation_memory_efficient && !has_grouping_sets,
            aggregating_step->getTemporaryDataMergeThreads(),
            should_produce_results_in_order_of_bucket_number,
            aggregating_step->getMaxBlockSize(),
            aggregating_step->getMaxBlockSizeForAggregationInOrder(),
            memory_bound_merging_of_aggregation_results_enabled);

        final_aggregation_step->setStepDescription("merge");
        node.step = std::move(final_aggregation_step);
        node.children = {&gather_node};
    }
    else if (strategy == Shuffle)
    {
        const size_t bucket_count = optimization_settings.distributed_plan_default_shuffle_join_bucket_count;    /// TODO: estimate number of buckets based on statistics and available nodes and memory

        /// Add scatter exchange step above source
        auto & exchange_scatter_node = nodes.emplace_back();
        exchange_scatter_node.step = std::make_unique<ScatterExchangeStep>(source->step->getOutputHeader(), aggregation_keys, bucket_count);
        exchange_scatter_node.step->setStepDescription(fmt::format("by hash([{}])", fmt::join(aggregation_keys, ", ")), optimization_settings.max_step_description_length);
        exchange_scatter_node.children = {source};

        /// Move aggregation step to a new node
        auto & new_aggregation_node = nodes.emplace_back();
        new_aggregation_node.step = std::move(node.step);
        new_aggregation_node.children = {&exchange_scatter_node};

        /// Add gather exchange step above aggregation
        QueryPlan::Node gather_node;
        QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_aggregation_node.step->getOutputHeader(), bucket_count);
        gather_node.step = std::move(exchange_gather_step);
        gather_node.children = {&new_aggregation_node};

        /// Replace aggregation node with gather node
        node = std::move(gather_node);
    }
}

/// Replaces SortingStep step with a subtree like this:
///
///   GatherExchange (merge sorted streams)
///     SortingStep
///       ScatterExchange (any partitioning)
///
/// NOTE: GatherExchange step is aware of sort descripiton and merges multiple sorted streams into one sorted stream.
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
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_sorting_node.step->getOutputHeader(), bucket_count, sort_description);
    exchange_gather_step->setStepDescription(fmt::format("sorted by ({})", dumpSortDescription(sort_description)), optimization_settings.max_step_description_length);
    gather_node.step = std::move(exchange_gather_step);
    gather_node.children = {&new_sorting_node};

    /// Replace sorting node with gather node
    node = std::move(gather_node);
}

/// Replaces ReadFromMergeTree step with a subtree like this:
///
///   GatherExchange
///     (Distributed)ReadFromMergeTree
void tryMakeDistributedRead(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Is this a read from MergeTree step?
    auto * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(node.step.get());
    auto * read_from_object_storage_step = typeid_cast<ReadFromObjectStorageStep *>(node.step.get());

    if (!read_from_merge_tree_step && !read_from_object_storage_step)
        return;

    /// Should not have children
    if (!node.children.empty())
        return;

    /// TODO: estimate number of buckets based on statistics and available nodes and memory
    const size_t bucket_count = optimization_settings.distributed_plan_default_reader_bucket_count;

    if (read_from_merge_tree_step)
    {
        /// Round-robin mark-range bucketing would split rows with the same sort key across buckets and
        /// break FINAL dedup on engines with specialized merging (Replacing, Collapsing, ...). Fall back
        /// to serial read until a correctness-preserving bucketing strategy exists.
        if (read_from_merge_tree_step->isQueryWithFinal() &&
            read_from_merge_tree_step->getMergeTreeData().merging_params.mode != MergeTreeData::MergingParams::Ordinary)
            return;

        /// Check if table is big enough for distributed read
        /// TODO: implement better logic for choosing number of parallel readers
        auto analysis_result = read_from_merge_tree_step->selectRangesToRead();
        if (analysis_result && analysis_result->selected_rows <= optimization_settings.distributed_plan_max_rows_to_broadcast)
            return;

        /// Move read step to a new node and set it to distributed read
        read_from_merge_tree_step->setDistributedRead(bucket_count);
    }
    else if (read_from_object_storage_step)
    {
        /// TODO: implement row-count-based decision for object storage reads;
        /// always enable distributed read for now.
        read_from_object_storage_step->setDistributedRead(bucket_count);
    }

    auto & new_read_node = nodes.emplace_back();
    new_read_node.step = std::move(node.step);

    /// Add gather exchange step above read
    QueryPlan::Node gather_node;
    QueryPlanStepPtr exchange_gather_step = std::make_unique<GatherExchangeStep>(new_read_node.step->getOutputHeader(), bucket_count);
    gather_node.step = std::move(exchange_gather_step);
    gather_node.children = {&new_read_node};

    /// Replace aggregation node with gather node
    node = std::move(gather_node);
}


/// If there is a Scatter step on top of Gather step then they can be replaced with Shuffle step that just
/// repartitions data from the source set of buckets to the destination set of buckets.
void tryReplaceScatterGatherWithShuffle(QueryPlan::Node * node)
{
    if (node->children.size() != 1)
        return;

    auto * scatter_step = typeid_cast<ScatterExchangeStep *>(node->step.get());
    if (!scatter_step)
        return;

    auto * gather_step = typeid_cast<GatherExchangeStep *>(node->children[0]->step.get());
    if (!gather_step)
        return;

    auto shuffle_step = std::make_unique<ShuffleExchangeStep>(node->children[0]->step->getOutputHeader(), scatter_step->getKeys(),
        gather_step->getSourceBucketCount(), scatter_step->getResultBucketCount());
    shuffle_step->setStepDescription(*scatter_step);
    node->step = std::move(shuffle_step);
    node->children = std::move(node->children[0]->children);
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
                (typeid_cast<ExpressionStep *>(frame.node->step.get()) ||
                typeid_cast<FilterStep *>(frame.node->step.get()) ||
                typeid_cast<BuildRuntimeFilterStep *>(frame.node->step.get())))
            {
                auto & child_node = *frame.node->children[0];
                auto * gather_step = typeid_cast<GatherExchangeStep *>(child_node.step.get());
                if (gather_step)
                {
                    SharedHeader expression_header = frame.node->step->getOutputHeader();

                    /// If Gather step has maintain_sort_description then we need to check that all those columns are present in Expression step results.
                    bool can_move_gather_up = true;
                    if (gather_step->getMaintainSortDescription())
                    {
                        const auto & sort_description = gather_step->getMaintainSortDescription().value();
                        for (const auto & column : sort_description)
                        {
                            if (!expression_header->has(column.column_name))
                            {
                                can_move_gather_up = false;
                                break;
                            }
                        }
                    }

                    if (can_move_gather_up)
                    {
                        std::swap(frame.node->step, child_node.step);
                        frame.node->step->updateInputHeader(expression_header);
                    }
                }
            }

            tryReplaceScatterGatherWithShuffle(frame.node);

            if (const auto * shuffle = dynamic_cast<const ShuffleExchangeStep *>(frame.node->step.get()))
            {
                /// Remove shuffle with empty keys as redundant
                if (shuffle->getKeys().empty() && shuffle->getResultBucketCount() == shuffle->getSourceBucketCount())
                {
                    frame.node->step = std::move(frame.node->children[0]->step);
                    frame.node->children = std::move(frame.node->children[0]->children);
                }
            }
        }

        stack.pop_back();
    }
}


/// Tries to build list of possible shards for the read steps that can be processed in parallel.
Strings makeListOfShardsForReadStep(const IQueryPlanStep * read_step)
{
    const auto * read_from_mt = dynamic_cast<const ReadFromMergeTree *>(read_step);
    if (read_from_mt)
        return read_from_mt->getShardsForDistributedRead();

    const auto * read_from_object_storage = dynamic_cast<const ReadFromObjectStorageStep *>(read_step);
    if (read_from_object_storage)
        return read_from_object_storage->getShardsForDistributedRead();

    return {"0"};   /// One shard by default if read step is not distributed
}

String dumpQueryPlanShort(const QueryPlan & query_plan)
{
    WriteBufferFromOwnString query_plan_buffer;
    query_plan.explainPlan(query_plan_buffer, ExplainPlanOptions{});

    return query_plan_buffer.str();
}


/// Builds distributed plan by splitting the query plan into multiple stages connected by exchanges.
/// Exchange steps are split into ExchangeSink and ExchangeSource.
/// This allows to build a separate plan fragment (a part of the original full plan) for each stage.
DistributedQueryPlan makeDistributedPlan(QueryPlan::Nodes /*nodes*/, QueryPlan::Node * root, const QueryPlanOptimizationSettings & optimization_settings)
{
    auto logger = getLogger("makeDistributedPlan");

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
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Different list of shards in child plans {} and {}, last child plan: \n{}",
                            frame.list_of_shards.size(), current_list_of_shards.size(),
                            dumpQueryPlanShort(*frame.child_plans.back()));

                    /// Add parameters and temporary files from the child plan
                    for (auto & [shard, task] : current_list_of_shards)
                    {
                        auto it = frame.list_of_shards.find(shard);
                        if (it == frame.list_of_shards.end())
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
            chassert(frame.next_child == frame.node->children.size());

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

                if (exchange_step && !optimization_settings.distributed_plan_single_stage)
                {
                    /// Make unique name for the exchange
                    const String stage_name = "stage_" + std::to_string(exchange_id);
                    ExchangeDescription exchange_description;
                    exchange_description.name = "exchange_" + std::to_string(exchange_id);
                    ++exchange_id;
                    exchange_description.kind = optimization_settings.distributed_plan_force_exchange_kind == "Persisted" ?
                        ExchangeDescription::Kind::Persisted : ExchangeDescription::Kind::Streaming;
                    exchange_description.source_bucket_count = frame.list_of_shards.size();
                    exchange_description.destination_bucket_count = exchange_step->getResultBucketCount();

                    distributed_plan.exchange_descriptions[exchange_description.name] = exchange_description;

                    Strings source_shards;
                    for (auto & [source_shard, _] : frame.list_of_shards)
                        source_shards.push_back(source_shard);

                    auto send_and_receive_steps = exchange_step->createSinkAndSourcePair(exchange_description.name, source_shards);
                    send_and_receive_steps.first->setStepDescription(exchange_description.name, optimization_settings.max_step_description_length);
                    send_and_receive_steps.second->setStepDescription(exchange_description.name, optimization_settings.max_step_description_length);

                    Strings list_of_exchange_shards;
                    list_of_exchange_shards.reserve(exchange_description.destination_bucket_count);
                    for (size_t bucket = 0; bucket < exchange_description.destination_bucket_count; ++bucket)
                        list_of_exchange_shards.push_back(toString(bucket));

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
                                source_task.output_exchange_streams.emplace_back(ExchangeStreamId(exchange_description.name, source_shard, destination_shard));

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
                            destination_task.input_exchange_streams.emplace_back(ExchangeStreamId(exchange_description.name, source_shard, destination_shard));

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
                /// No children, this means that this is a leaf step.
                /// Use ReadFromMergeTree with catalog access for distributed reads on public master.
                auto shards_for_read = makeListOfShardsForReadStep(frame.node->step.get());

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

            LOG_TEST(logger, "Current plan:\n{}\nshard count: {}\n",
                dumpQueryPlanShort(*current_plan), current_list_of_shards.size());

            /// On leaving the last node.
            if (stack.size() == 1)
            {
                plan_fragment = std::move(*current_plan);
                main_stage_depends_on = std::move(current_stage_depends_on);
                current_stage_depends_on = {};
            }

            stack.pop_back();
        }

        chassert(current_list_of_shards.size() == 1);
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
