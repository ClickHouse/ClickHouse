#include <QueryCoordination/Fragments/PlanFragmentBuilder.h>

#include <QueryCoordination/Fragments/PlanFragment.h>

#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/ScanStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryCoordination/Interpreters/InDepthPlanNodeVisitor.h>

namespace DB
{

bool needPushDownChild(QueryPlanStepPtr step)
{
    if (dynamic_cast<FilterStep *>(step.get()))
    {
        return true;
    }

    if (dynamic_cast<ExpressionStep *>(step.get()))
    {
        return step->getStepDescription() != "Before LIMIT BY";
    }

    return false;
}

bool isLimitRelated(QueryPlanStepPtr step)
{
    if (dynamic_cast<ExpressionStep *>(step.get()))
    {
        return step->getStepDescription() == "Before LIMIT BY";
    }
    else if (dynamic_cast<LimitByStep *>(step.get()))
    {
        return true;
    }
    else if (dynamic_cast<LimitStep *>(step.get()))
    {
        return true;
    }

    return false;
}

bool isExtremesOrTotalsOrCubeOrRollup(QueryPlanStepPtr step)
{
    return dynamic_cast<ExtremesStep *>(step.get()) || dynamic_cast<RollupStep *>(step.get())
        || dynamic_cast<CubeStep *>(step.get()) || dynamic_cast<TotalsHavingStep *>(step.get());
}



std::shared_ptr<LimitStep> createPreLimit(const DataStream & input_stream, LimitStep * limit_step, const Settings & settings)
{
    auto limit_length = limit_step->getLimit();
    auto limit_offset = limit_step->getOffset();

    if (limit_length > std::numeric_limits<UInt64>::max() - limit_offset)
        return {};

    limit_length += limit_offset;
    limit_offset = 0;

//    LOG_DEBUG(&Poco::Logger::get("PlanFragmentBuilder"), "limit_length {}, limit_offset {}", limit_length, limit_offset);
    auto limit = std::make_shared<LimitStep>(input_stream, limit_length, limit_offset, settings.exact_rows_before_limit);
    limit->setStepDescription("preliminary LIMIT (without OFFSET)");
    return limit;
}


std::shared_ptr<LimitByStep> createLimitBy(const DataStream & input_streamm, LimitByStep * limit_by_step)
{
    return std::make_shared<LimitByStep>(input_streamm, limit_by_step->getGroupLength(), limit_by_step->getGroupOffset(), limit_by_step->getColumns());
}


void PlanFragmentBuilder::pushDownLimitRelated(QueryPlanStepPtr step, PlanFragmentPtr child_fragment)
{
    child_fragment->addStep(step);

    if (child_fragment->getChildren().empty())
        return;

    if (!child_fragment->getChildren()[0]->isPartitioned())
        return;

    auto fragment = child_fragment->getChildren()[0];
    if (auto * expression_step = dynamic_cast<ExpressionStep *>(step.get()))
    {
        auto actions_dag = expression_step->getExpression()->clone();

        auto clone_expression_step = std::make_shared<ExpressionStep>(fragment->getCurrentDataStream(), actions_dag);

        clone_expression_step->setStepDescription("Before LIMIT BY");
        fragment->addStep(std::move(clone_expression_step));
    }
    else if (auto * limit_by_step = dynamic_cast<LimitByStep *>(step.get()))
    {
        fragment->addStep(createLimitBy(fragment->getCurrentDataStream(), limit_by_step));
    }
    else if (auto * limit_step = dynamic_cast<LimitStep *>(step.get()))
    {
        auto limit = createPreLimit(fragment->getCurrentDataStream(), limit_step, context->getSettingsRef());
        if (limit)
            fragment->addStep(limit);
    }
    else
        throw;
}


PlanFragmentPtrs PlanFragmentBuilder::build()
{
    createPlanFragments(query_plan, *query_plan.getRootNode());
    return all_fragments;
}


/* all extends IQueryPlanStep
AggregatingProjectionStep (DB)
CreatingSetsStep (DB)
IntersectOrExceptStep (DB)
ISourceStep (DB)
    ReadFromParallelRemoteReplicasStep (DB)
    ReadFromPart (DB)
    ReadFromPreparedSource (DB)
        ReadFromStorageStep (DB)
    ReadFromRemote (DB)
    ReadNothingStep (DB)
    SourceStepWithFilter (DB)
        ReadFromDummy (DB)
        ReadFromMemoryStorageStep (DB)
        ReadFromMerge (DB)
        ReadFromMergeTree (DB)
        ReadFromSystemZooKeeper (DB)
ITransformingStep (DB)
    AggregatingStep (DB)
    ArrayJoinStep (DB)
    CreateSetAndFilterOnTheFlyStep (DB)
    CreatingSetStep (DB)
    CubeStep (DB)
    DistinctStep (DB)
    ExpressionStep (DB)
    ExtremesStep (DB)
    FilledJoinStep (DB)
    FillingStep (DB)
    FilterStep (DB)
    LimitByStep (DB)
    LimitStep (DB)
    MergingAggregatedStep (DB)
    OffsetStep (DB)
    RollupStep (DB)
    SortingStep (DB)
    TotalsHavingStep (DB)
    WindowStep (DB)
JoinStep (DB)
UnionStep (DB)
 *
 * */
PlanFragmentPtr PlanFragmentBuilder::createPlanFragments(const QueryPlan & single_plan, Node & root_node)
{

    /// TODO test
//    PlanFragmentVisitor::Data data({.context = context, .all_fragments = all_fragments});
//    PlanFragmentVisitor visitor(data);
//    visitor.visit(root_node);

    PlanFragmentPtrs child_fragments;
    for (Node * child : root_node.children)
    {
        child_fragments.emplace_back(createPlanFragments(single_plan, *child));
    }

    if (child_fragments.size() != root_node.children.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Child fragments size {} not same as QueryPlan children node size {}",
            child_fragments.size(),
            root_node.children.size());

    PlanFragmentPtr result;
    /// ReadFromMergeTree
    if (dynamic_cast<ReadFromMergeTree *>(root_node.step.get()))
    {
        result = createScanFragment(root_node.step);
    }
    else if (dynamic_cast<AggregatingStep *>(root_node.step.get()))
    {
        /// push down partial aggregating to lower level fragment
        result = createAggregationFragment(root_node.step, child_fragments[0]);
    }
    else if (needPushDownChild(root_node.step))
    {
        /// not Projection ExpressionStep push it to child_fragments[0]

        child_fragments[0]->addStep(root_node.step);
        result = child_fragments[0];
    }
    else if (dynamic_cast<SortingStep *>(root_node.step.get()))
    {
        result = createOrderByFragment(root_node.step, child_fragments[0]);
    }
    else if (isLimitRelated(root_node.step))
    {
        pushDownLimitRelated(root_node.step, child_fragments[0]);
        result = child_fragments[0];
    }
    else if (dynamic_cast<JoinStep *>(root_node.step.get()))
    {
        if (child_fragments.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join step children fragment size {}", child_fragments.size());

        result = createJoinFragment(root_node.step, child_fragments[0], child_fragments[1]);
    }
    else if (dynamic_cast<UnionStep *>(root_node.step.get()))
    {
        result = createUnionFragment(root_node.step, child_fragments);
        all_fragments.emplace_back(result);
    }
    else if (dynamic_cast<CreatingSetStep *>(root_node.step.get()))
    {
        /// Do noting, add to brother fragment
        result = child_fragments[0];
    }
    else if (dynamic_cast<CreatingSetsStep *>(root_node.step.get()))
    {
        /// CreatingSetsStep need push to child_fragments[0], connect child_fragments[0] to child_fragments[1-N]
        result = createCreatingSetsFragment(root_node, child_fragments);
    }
    else if (isExtremesOrTotalsOrCubeOrRollup(root_node.step))
    {
        result = createUnpartitionedFragment(root_node.step, child_fragments[0]);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot create plan fragment for this node type {}", root_node.step->getName());
    }

    if (single_plan.getRootNode() == &root_node) /// is root node
    {
        if (result->isPartitioned())
        {
            DataPartition partition{.type = PartitionType::UNPARTITIONED};

            result = createParentFragment(child_fragments[0], partition);
            all_fragments.emplace_back(result);
        }
    }

    return result;

}

PlanFragmentPtr PlanFragmentBuilder::createUnionFragment(QueryPlanStepPtr step, PlanFragmentPtrs child_fragments)
{
    DataPartition partition{.type = PartitionType::UNPARTITIONED};
    PlanFragmentPtr parent_fragment = std::make_shared<PlanFragment>(context->getFragmentID(), partition, context);
    parent_fragment->unitePlanFragments(step, child_fragments);
    return parent_fragment;
}

PlanFragmentPtr PlanFragmentBuilder::createOrderByFragment(QueryPlanStepPtr step, PlanFragmentPtr child_fragment)
{
    child_fragment->addStep(step);
    if (!child_fragment->isPartitioned())
    {
        return child_fragment;
    }

    auto * sort_step = dynamic_cast<SortingStep *>(step.get());

    /// Create a new fragment for a sort-merging exchange.
    PlanFragmentPtr merge_fragment = createParentFragment(child_fragment, DataPartition{.type = PartitionType::UNPARTITIONED});
    auto * exchange_node = merge_fragment->getRootNode(); /// exchange node

    const SortDescription & sort_description = sort_step->getSortDescription();
    const UInt64 limit = sort_step->getLimit();
    const auto max_block_size = context->getSettingsRef().max_block_size;
    const auto exact_rows_before_limit = context->getSettingsRef().exact_rows_before_limit;

    ExchangeDataStep::SortInfo sort_info{
        .max_block_size = max_block_size,
        .always_read_till_end = exact_rows_before_limit,
        .limit = limit,
        .result_description = sort_description};

    auto * exchange_step = dynamic_cast<ExchangeDataStep *>(exchange_node->step.get());
    exchange_step->setSortInfo(sort_info);

    all_fragments.emplace_back(merge_fragment);

    return merge_fragment;
}

PlanFragmentPtr PlanFragmentBuilder::createScanFragment(QueryPlanStepPtr step)
{
    DataPartition partition;
    partition.type = PartitionType::RANDOM;

    auto fragment = std::make_shared<PlanFragment>(context->getFragmentID(), partition, context);
    fragment->addStep(std::move(step));

    fragment->setCluster(context->getCluster("test_two_shards"));

    all_fragments.emplace_back(fragment);
    return fragment;
}

PlanFragmentPtr PlanFragmentBuilder::createAggregationFragment(QueryPlanStepPtr step, PlanFragmentPtr child_fragment)
{
    // check size
    //    if (child_fragment.getPlanRoot().getNumInstances() <= 1) {
    //        child_fragment.addPlanRoot(node);
    //        return child_fragment;
    //    }

    auto * aggregate_step = dynamic_cast<AggregatingStep *>(step.get());

    auto aggregating_step = aggregate_step->clone(false);

    child_fragment->addStep(aggregating_step);

    DataPartition partition;
    //    if (expressions.group_by_elements_actions.empty())
    if (!aggregating_step->getParams().keys_size || aggregating_step->withTotalsOrCubeOrRollup())
    {
        partition.type = PartitionType::UNPARTITIONED;
    }
    else
    {
        partition.type = PartitionType::HASH_PARTITIONED;
        partition.keys = aggregating_step->getParams().keys;
        partition.keys_size = aggregating_step->getParams().keys_size;
        partition.partition_by_bucket_num = true;
    }

    // place a merge aggregation step in a new fragment
    PlanFragmentPtr merge_fragment = createParentFragment(child_fragment, partition);

    const Settings & settings = context->getSettingsRef();
    std::shared_ptr<MergingAggregatedStep> merge_agg_node = aggregating_step->makeMergingAggregatedStep(merge_fragment->getCurrentDataStream(), settings);

    merge_fragment->addStep(std::move(merge_agg_node));

    all_fragments.emplace_back(merge_fragment);

    return merge_fragment;
}

PlanFragmentPtr PlanFragmentBuilder::createParentFragment(PlanFragmentPtr child_fragment, const DataPartition & partition)
{
    PlanFragmentPtr parent_fragment = std::make_shared<PlanFragment>(context->getFragmentID(), partition, context);

    std::shared_ptr<ExchangeDataStep> exchange_step
        = std::make_shared<ExchangeDataStep>(parent_fragment->getFragmentID(), child_fragment->getCurrentDataStream(), storage_limits);
    parent_fragment->addStep(exchange_step);
    auto * exchange_node = parent_fragment->getRootNode();

    parent_fragment->setCluster(context->getCluster("test_two_shards"));

    exchange_step->setPlanID(exchange_node->plan_id);
    exchange_node->children.emplace_back(child_fragment->getRootNode());

    child_fragment->setDestination(exchange_node, parent_fragment);
    child_fragment->setOutputPartition(partition);
    return parent_fragment;
}

/*
ConcurrentHashJoin (DB)
DirectKeyValueJoin (DB)
FullSortingMergeJoin (DB)
GraceHashJoin (DB)
HashJoin (DB)
JoinSwitcher (DB)
MergeJoin (DB)
 */

PlanFragmentPtr PlanFragmentBuilder::createJoinFragment(QueryPlanStepPtr step, PlanFragmentPtr left_child_fragment, PlanFragmentPtr right_child_fragment)
{
    auto * join_step = dynamic_cast<JoinStep *>(step.get());

    JoinPtr join = join_step->getJoin();
    const TableJoin & table_join = join->getTableJoin();

    if (table_join.getClauses().size() != 1 || table_join.strictness() == JoinStrictness::Asof) /// broadcast join. Asof support != condition
    {
        /// join_step push down left fragment, right fragment as left fragment child
        left_child_fragment->addChildPlanFragments(step, {right_child_fragment});
        right_child_fragment->setOutputPartition(DataPartition{.type = PartitionType::UNPARTITIONED});
        return left_child_fragment;
    }
    else
    {
        auto join_clause = table_join.getOnlyClause(); /// Definitely equals condition
        DataPartition lhs_join_partition{.type = HASH_PARTITIONED, .keys = join_clause.key_names_left, .keys_size = join_clause.key_names_left.size(), .partition_by_bucket_num = false};

        DataPartition rhs_join_partition{.type = HASH_PARTITIONED, .keys = join_clause.key_names_right, .keys_size = join_clause.key_names_right.size(), .partition_by_bucket_num = false};

        PlanFragmentPtr parent_fragment = std::make_shared<PlanFragment>(context->getFragmentID(), lhs_join_partition, context);

        PlanFragmentPtrs left_right_fragments;
        left_right_fragments.emplace_back(left_child_fragment);
        left_right_fragments.emplace_back(right_child_fragment);

        parent_fragment->unitePlanFragments(step, left_right_fragments, storage_limits);

        left_child_fragment->setOutputPartition(lhs_join_partition);
        right_child_fragment->setOutputPartition(rhs_join_partition);

        all_fragments.emplace_back(parent_fragment);

        return parent_fragment;
    }
}

PlanFragmentPtr PlanFragmentBuilder::createCreatingSetsFragment(Node & root_node, PlanFragmentPtrs child_fragments)
{
    std::vector<QueryPlanStepPtr> child_steps;
    for (size_t i = 1; i < root_node.children.size(); ++i)
    {
        if (dynamic_cast<CreatingSetStep *>(root_node.children[i]->step.get()))
        {
            child_steps.emplace_back(root_node.children[i]->step);
        }
        else
            throw;
    }

    PlanFragmentPtrs to_child_fragments;
    to_child_fragments.insert(to_child_fragments.end(), child_fragments.begin() + 1, child_fragments.end());
    child_fragments[0]->unitePlanFragments(root_node.step, child_steps, to_child_fragments, storage_limits);

    for (auto & fragment : to_child_fragments)
    {
        fragment->setOutputPartition(DataPartition{.type = PartitionType::UNPARTITIONED});
    }

    return child_fragments[0];
}

PlanFragmentPtr PlanFragmentBuilder::createUnpartitionedFragment(QueryPlanStepPtr step, PlanFragmentPtr child_fragment)
{
    if (child_fragment->isPartitioned())
    {
        DataPartition partition{.type = PartitionType::UNPARTITIONED};
        auto result = createParentFragment(child_fragment, partition);
        result->addStep(step);

        all_fragments.emplace_back(result);
        return result;
    }
    else
    {
        child_fragment->addStep(step);
        return child_fragment;
    }
}

}
