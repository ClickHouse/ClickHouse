#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StorageLayout.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>
#include <algorithm>
#include <iterator>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool isTopNSort(const IQueryPlanStep & step)
{
    const auto * sorting_step = typeid_cast<const SortingStep *>(&step);
    /// A partitioned bounded sort keeps one stream per hash partition instead of merging to
    /// a global top-N, so the top-N rules (which promise one sorted stream) must not take it.
    return sorting_step != nullptr && sorting_step->getType() == SortingStep::Type::Full
        && sorting_step->getLimit() > 0 && !sorting_step->hasPartitions();
}

bool isDistributionPassthroughStep(const IQueryPlanStep & step)
{
    /// A preliminary `DISTINCT` deduplicates each stream on its own, so it is correct on any split of its
    /// input and keeps the order within a stream.
    if (const auto * distinct = typeid_cast<const DistinctStep *>(&step))
        return distinct->isPreliminary();
    return typeid_cast<const ExpressionStep *>(&step) != nullptr
        || typeid_cast<const FilterStep *>(&step) != nullptr
        || typeid_cast<const BuildRuntimeFilterStep *>(&step) != nullptr;
}

GroupExpressionPtr tryMakeBucketedReadVariant(
    const GroupExpressionPtr & expression,
    size_t node_count,
    size_t target_buckets,
    ImplementationStrategyPtr strategy,
    const char * description_prefix,
    bool is_replicated,
    size_t & actual_buckets)
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());

    auto bucketed_read_step_ptr = cloneStepAs(*read_step);
    auto * bucketed_read_step = bucketed_read_step_ptr.get();

    actual_buckets = bucketed_read_step->setupDistributedReadBuckets(target_buckets, ReadFromMergeTree::max_distributed_read_buckets);
    if (actual_buckets != target_buckets)
        return nullptr;
    bucketed_read_step->setStepDescription(fmt::format("{} {}", description_prefix, read_step->getStepDescription()), 200);

    GroupExpressionPtr bucketed_read_expression = std::make_shared<GroupExpression>(*expression);
    bucketed_read_expression->plan_step = std::move(bucketed_read_step_ptr);
    bucketed_read_expression->strategy = std::move(strategy);

    ExpressionProperties output_properties;
    output_properties.distribution.node_count = node_count;
    output_properties.distribution.is_replicated = is_replicated;
    bucketed_read_expression->properties = output_properties;
    return bucketed_read_expression;
}

bool joinPreservesLeftSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

bool joinPreservesRightSide(JoinKind kind)
{
    return kind == JoinKind::Inner || kind == JoinKind::Right || kind == JoinKind::Cross || kind == JoinKind::Comma;
}

/// The pairs (left column, right column) that the join compares for equality with no cast.
static JoinEqualities getJoinEqualities(const JoinStepLogical & join_step)
{
    JoinEqualities equalities;
    for (const auto & predicate : join_step.getJoinOperator().expression)
    {
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;
        if (left_node.fromRight() && right_node.fromLeft())
            std::swap(left_node, right_node);
        else if (!left_node.fromLeft() || !right_node.fromRight())
            continue;
        if (left_node.getType()->equals(*right_node.getType()))
            equalities.emplace_back(left_node.getColumnName(), right_node.getColumnName());
    }
    return equalities;
}

/// The layouts that the result of a join can have: the layouts of each preserved input. In the rows of an
/// inner join the two columns of an equality are equal, so an argument column also gets the name of the
/// column on the other side. This lets the next join or an aggregation use the layout by either name.
static StorageLayouts getJoinStorageLayouts(
    const JoinStepLogical & join_step, const StorageLayouts & left_layouts, const StorageLayouts & right_layouts)
{
    const auto kind = join_step.getJoinOperator().kind;
    const auto & output_header = *join_step.getOutputHeader();

    /// Pairs of equal columns, only for an inner join: an outer join extends one side with defaults.
    JoinEqualities equal_columns;
    if (kind == JoinKind::Inner)
        equal_columns = getJoinEqualities(join_step);

    StorageLayouts result;
    auto add_layouts_of_input = [&](const StorageLayouts & input_layouts)
    {
        for (const auto & input_layout : input_layouts)
        {
            StorageLayout layout;
            layout.function = input_layout.function;

            bool all_arguments_reach_output = true;
            for (const auto & input_argument : input_layout.argument_columns)
            {
                NameSet argument = input_argument;
                for (const auto & [left_column, right_column] : equal_columns)
                {
                    if (input_argument.contains(left_column))
                        argument.insert(right_column);
                    if (input_argument.contains(right_column))
                        argument.insert(left_column);
                }
                std::erase_if(argument, [&](const String & name) { return !output_header.has(name); });
                if (argument.empty())
                {
                    all_arguments_reach_output = false;
                    break;
                }
                layout.argument_columns.push_back(std::move(argument));
            }
            if (!all_arguments_reach_output)
                continue;

            /// The two inputs of a co-located join give the same layout.
            const bool is_known = std::ranges::any_of(result, [&](const StorageLayout & known)
            {
                return known.function->equals(*layout.function) && known.argument_columns == layout.argument_columns;
            });
            if (!is_known)
                result.push_back(std::move(layout));
        }
    };

    if (joinPreservesLeftSide(kind))
        add_layouts_of_input(left_layouts);
    if (joinPreservesRightSide(kind))
        add_layouts_of_input(right_layouts);
    return result;
}

const StorageLayouts & getDeliveredStorageLayouts(Memo & memo, GroupId group_id)
{
    auto group = memo.getGroup(group_id);
    if (group->storage_layouts)
        return *group->storage_layouts;

    StorageLayouts layouts;
    if (memo.getContext().distributed_plan_use_partition_key_layout && !group->logical_expressions.empty())
    {
        /// All logical expressions of a group give the same result, so the first one is enough.
        const auto & expression = group->logical_expressions.front();
        const IQueryPlanStep * step = expression->getQueryPlanStep();

        if (const auto * read_step = typeid_cast<const ReadFromMergeTree *>(step))
        {
            layouts = getStorageLayouts(read_step->getStorageMetadata()->getPartitionKey());
        }
        else if (isDistributionPassthroughStep(*step) && expression->inputs.size() == 1)
        {
            /// Such a step keeps every row on its node. It can rename the argument columns or drop them.
            const auto & input_layouts = getDeliveredStorageLayouts(memo, expression->inputs.front().group_id);
            if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
                layouts = remapStorageLayouts(input_layouts, expression_step->getExpression());
            else if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
                layouts = remapStorageLayouts(input_layouts, filter_step->getExpression());
            else
                layouts = input_layouts;
        }
        else if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && expression->inputs.size() == 2)
        {
            /// A candidate only: the join delivers the layout if it keeps the rows of that input on their nodes,
            /// which the search checks when a consumer requires the layout from this group.
            layouts = getJoinStorageLayouts(
                *join_step,
                getDeliveredStorageLayouts(memo, expression->inputs[0].group_id),
                getDeliveredStorageLayouts(memo, expression->inputs[1].group_id));
        }
    }

    group->storage_layouts = std::move(layouts);
    return *group->storage_layouts;
}

size_t getCommonKeyRangePrefix(
    const KeyRangeLayoutCandidate & left, const KeyRangeLayoutCandidate & right, const JoinEqualities & equalities)
{
    if (left.in_reverse_order != right.in_reverse_order)
        return 0;

    size_t size = 0;
    for (; size < left.key_columns.size() && size < right.key_columns.size(); ++size)
    {
        if (!left.key_types[size]->equals(*right.key_types[size]))
            break;

        const bool columns_are_equal = std::ranges::any_of(equalities, [&](const auto & equality)
        {
            return left.key_columns[size].contains(equality.first) && right.key_columns[size].contains(equality.second);
        });
        if (!columns_are_equal)
            break;
    }
    return size;
}

/// The candidates of the result of a join. When the join compares the key columns of a candidate of each
/// input, the two inputs can be divided at the same borders: this gives one candidate for the reads of the
/// two inputs. A candidate of one preserved input stays a candidate too, for a join that keeps the rows of
/// that input on their nodes.
static KeyRangeLayoutCandidates getJoinKeyRangeLayoutCandidates(
    const JoinStepLogical & join_step, const KeyRangeLayoutCandidates & left_candidates, const KeyRangeLayoutCandidates & right_candidates)
{
    const auto kind = join_step.getJoinOperator().kind;
    const auto & output_header = *join_step.getOutputHeader();
    const bool left_preserved = joinPreservesLeftSide(kind);
    const bool right_preserved = joinPreservesRightSide(kind);
    const auto equalities = getJoinEqualities(join_step);

    KeyRangeLayoutCandidates result;
    auto add_candidate = [&](KeyRangeLayoutCandidate candidate)
    {
        size_t size = 0;
        for (; size < candidate.key_columns.size(); ++size)
        {
            auto & names = candidate.key_columns[size];
            /// In the rows of an inner join the two columns of an equality are equal.
            if (kind == JoinKind::Inner)
            {
                for (const auto & [left_column, right_column] : equalities)
                {
                    if (names.contains(left_column))
                        names.insert(right_column);
                    else if (names.contains(right_column))
                        names.insert(left_column);
                }
            }
            std::erase_if(names, [&](const String & name) { return !output_header.has(name); });
            if (names.empty())
                break;
        }

        if (size == 0)
            return;
        candidate.resize(size);
        result.push_back(std::move(candidate));
    };

    VectorWithMemoryTracking<UInt8> left_is_merged(left_candidates.size());
    VectorWithMemoryTracking<UInt8> right_is_merged(right_candidates.size());
    for (size_t left_index = 0; left_index < left_candidates.size(); ++left_index)
    {
        for (size_t right_index = 0; right_index < right_candidates.size(); ++right_index)
        {
            const auto & left = left_candidates[left_index];
            const auto & right = right_candidates[right_index];
            const size_t common_size = getCommonKeyRangePrefix(left, right, equalities);
            if (common_size == 0)
                continue;
            left_is_merged[left_index] = true;
            right_is_merged[right_index] = true;

            KeyRangeLayoutCandidate merged;
            merged.in_reverse_order = left.in_reverse_order;
            for (size_t i = 0; i < common_size; ++i)
            {
                NameSet names;
                if (left_preserved)
                    names.insert(left.key_columns[i].begin(), left.key_columns[i].end());
                if (right_preserved)
                    names.insert(right.key_columns[i].begin(), right.key_columns[i].end());
                merged.key_columns.push_back(std::move(names));
                merged.key_types.push_back(left.key_types[i]);
            }
            std::ranges::set_union(left.read_groups, right.read_groups, std::back_inserter(merged.read_groups));
            add_candidate(std::move(merged));
        }
    }

    for (size_t left_index = 0; left_preserved && left_index < left_candidates.size(); ++left_index)
    {
        if (!left_is_merged[left_index])
            add_candidate(left_candidates[left_index]);
    }
    for (size_t right_index = 0; right_preserved && right_index < right_candidates.size(); ++right_index)
    {
        if (!right_is_merged[right_index])
            add_candidate(right_candidates[right_index]);
    }
    return result;
}

const KeyRangeLayoutCandidates & getKeyRangeLayoutCandidates(Memo & memo, GroupId group_id)
{
    auto group = memo.getGroup(group_id);
    if (group->key_range_layout_candidates)
        return *group->key_range_layout_candidates;

    KeyRangeLayoutCandidates candidates;
    if (memo.getContext().distributed_plan_use_primary_key_range_layout && !group->logical_expressions.empty())
    {
        /// All logical expressions of a group give the same result, so the first one is enough.
        const auto & expression = group->logical_expressions.front();
        const IQueryPlanStep * step = expression->getQueryPlanStep();

        if (const auto * read_step = typeid_cast<const ReadFromMergeTree *>(step))
        {
            const auto & metadata = read_step->getStorageMetadata();
            auto candidate = getKeyRangeLayoutCandidate(
                metadata->getPrimaryKey(), metadata->getSortingKey(), *read_step->getOutputHeader(), group_id);
            if (candidate)
                candidates.push_back(std::move(*candidate));
        }
        else if (isDistributionPassthroughStep(*step) && expression->inputs.size() == 1)
        {
            const auto & input_candidates = getKeyRangeLayoutCandidates(memo, expression->inputs.front().group_id);
            if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
                candidates = remapKeyRangeLayoutCandidates(input_candidates, expression_step->getExpression());
            else if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
                candidates = remapKeyRangeLayoutCandidates(input_candidates, filter_step->getExpression());
            else
                candidates = input_candidates;
        }
        else if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && expression->inputs.size() == 2)
        {
            candidates = getJoinKeyRangeLayoutCandidates(
                *join_step,
                getKeyRangeLayoutCandidates(memo, expression->inputs[0].group_id),
                getKeyRangeLayoutCandidates(memo, expression->inputs[1].group_id));
        }
        else if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step);
            aggregating_step && aggregating_step->getFinal() && !aggregating_step->isGroupingSets() && expression->inputs.size() == 1)
        {
            /// A candidate only: the aggregation delivers the layout if it runs on the nodes of its input,
            /// which its rule checks when a consumer requires the layout from this group.
            candidates = restrictKeyRangeLayoutCandidates(
                getKeyRangeLayoutCandidates(memo, expression->inputs.front().group_id), aggregating_step->getParams().keys);
        }
        else if (const auto * distinct_step = typeid_cast<const DistinctStep *>(step);
            distinct_step && !distinct_step->isPreliminary() && expression->inputs.size() == 1)
        {
            candidates = restrictKeyRangeLayoutCandidates(
                getKeyRangeLayoutCandidates(memo, expression->inputs.front().group_id), distinct_step->getColumnNames());
        }
    }

    group->key_range_layout_candidates = std::move(candidates);
    return *group->key_range_layout_candidates;
}

StorageLayoutFunctionPtr getKeyRangeLayoutFunction(
    Memo & memo, const VectorWithMemoryTracking<GroupId> & read_groups, const DataTypes & key_types, bool in_reverse_order, size_t node_count)
{
    Memo::KeyRangeLayoutFunctionKey key{.read_groups = read_groups, .key_size = key_types.size(), .node_count = node_count};
    if (auto it = memo.key_range_layout_functions.find(key); it != memo.key_range_layout_functions.end())
        return it->second;

    /// The borders must balance the rows of all the reads together, so the marks of all of them go into one list.
    RangesInDataParts all_parts;
    for (GroupId read_group : read_groups)
    {
        const auto * read_step = typeid_cast<const ReadFromMergeTree *>(
            memo.getGroup(read_group)->logical_expressions.front()->getQueryPlanStep());
        if (!read_step)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Group {} of a key range layout is not a table read", read_group);

        auto analysis = read_step->getAnalyzedResult();
        if (!analysis)
            analysis = read_step->selectRangesToRead();
        if (analysis)
            all_parts.insert(all_parts.end(), analysis->parts_with_ranges.begin(), analysis->parts_with_ranges.end());
    }

    StorageLayoutFunctionPtr function;
    if (!all_parts.empty())
    {
        auto split = splitIntersectingPartsRangesIntoLayers(
            std::move(all_parts), node_count, key_types.size(), in_reverse_order, getLogger("KeyRangeLayout"));
        function = StorageLayoutFunction::createKeyRanges(key_types, in_reverse_order, std::move(split.borders));
    }

    memo.key_range_layout_functions.emplace(std::move(key), function);
    return function;
}

std::optional<size_t> findPartitionKeyComponent(const ReadFromMergeTree & read_step, const DistributionDescription & distribution)
{
    if (!distribution.layout_function)
        return std::nullopt;

    const auto & partition_key = read_step.getStorageMetadata()->getPartitionKey();
    for (size_t component_index = 0; component_index < partition_key.column_names.size(); ++component_index)
    {
        auto function = StorageLayoutFunction::tryCreate(partition_key, component_index);
        if (!function || !function->equals(*distribution.layout_function))
            continue;

        /// The required columns must be the argument columns of this table, in the order of the function.
        const auto & argument_names = function->tryGetPartitionExpression()->argument_names;
        if (argument_names.size() != distribution.columns.size())
            continue;
        bool arguments_match = true;
        for (size_t i = 0; i < argument_names.size() && arguments_match; ++i)
            arguments_match = distribution.columns[i].contains(argument_names[i]);
        if (arguments_match)
            return component_index;
    }
    return std::nullopt;
}

/// Wraps a read whose buckets a layout function made into a physical expression that delivers `distribution`.
static GroupExpressionPtr makePartitionedReadExpression(
    const GroupExpressionPtr & expression, std::unique_ptr<ReadFromMergeTree> partitioned_read_step, const DistributionDescription & distribution)
{
    GroupExpressionPtr partitioned_read_expression = std::make_shared<GroupExpression>(*expression);
    partitioned_read_expression->plan_step = std::move(partitioned_read_step);
    partitioned_read_expression->strategy = strategySingleton<PartitionedReadStrategy>();
    partitioned_read_expression->properties = ExpressionProperties{};
    partitioned_read_expression->properties.distribution = distribution;
    return partitioned_read_expression;
}

GroupExpressionPtr tryMakePartitionedReadVariant(
    const GroupExpressionPtr & expression, size_t component_index, const DistributionDescription & distribution)
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());

    auto partitioned_read_step = cloneStepAs(*read_step);
    if (partitioned_read_step->setupDistributedReadBucketsByPartitionValue(component_index, distribution.node_count) != distribution.node_count)
        return nullptr;
    partitioned_read_step->setStepDescription(fmt::format("PartitionedRead {}", read_step->getStepDescription()), 200);

    return makePartitionedReadExpression(expression, std::move(partitioned_read_step), distribution);
}

GroupExpressionPtr tryMakeKeyRangeReadVariant(const GroupExpressionPtr & expression, const DistributionDescription & distribution)
{
    const auto * read_step = typeid_cast<const ReadFromMergeTree *>(expression->getQueryPlanStep());
    const auto * key_ranges = distribution.layout_function->tryGetKeyRanges();
    const auto & key_types = distribution.layout_function->getArgumentTypes();

    /// The required columns must be the first key columns of this table, with the types and the direction of the function.
    const auto & metadata = read_step->getStorageMetadata();
    auto candidate = getKeyRangeLayoutCandidate(
        metadata->getPrimaryKey(), metadata->getSortingKey(), *read_step->getOutputHeader(), expression->group_id);
    if (!candidate || candidate->in_reverse_order != key_ranges->in_reverse_order
        || candidate->key_columns.size() < key_types.size() || distribution.columns.size() != key_types.size())
        return nullptr;
    for (size_t i = 0; i < key_types.size(); ++i)
    {
        const String & key_column = *candidate->key_columns[i].begin();
        if (!candidate->key_types[i]->equals(*key_types[i]) || !distribution.columns[i].contains(key_column))
            return nullptr;
    }

    auto partitioned_read_step = cloneStepAs(*read_step);
    if (partitioned_read_step->setupDistributedReadBucketsByKeyRanges(key_ranges->borders, key_ranges->in_reverse_order, distribution.node_count)
        != distribution.node_count)
        return nullptr;
    partitioned_read_step->setStepDescription(fmt::format("RangePartitionedRead {}", read_step->getStepDescription()), 200);

    return makePartitionedReadExpression(expression, std::move(partitioned_read_step), distribution);
}

GroupExpressionPtr makeEnforcerExpression(
    const GroupExpressionPtr & source,
    QueryPlanStepPtr step,
    ExpressionProperties input_required,
    ExpressionProperties output_properties,
    const ExpressionProperties & required_properties)
{
    auto enforcer_expression = std::make_shared<GroupExpression>(std::move(step));
    enforcer_expression->group_id = source->group_id;
    enforcer_expression->inputs.push_back({.group_id = source->group_id, .required_properties = std::move(input_required)});
    enforcer_expression->properties = std::move(output_properties);
    enforcer_expression->enforced_for.push_back(required_properties);
    return enforcer_expression;
}

}
