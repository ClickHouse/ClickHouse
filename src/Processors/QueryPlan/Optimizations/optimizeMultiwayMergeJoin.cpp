#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Common/StackWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/MultiwayMergeJoinStep.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// A join of the group, see `optimizeMultiwayMergeJoin`.
const FullSortingMergeJoin * getMemberJoin(const QueryPlan::Node & node)
{
    const auto * join_step = typeid_cast<const JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return nullptr;

    const auto & join = join_step->getJoin();
    const auto * merge_join = typeid_cast<const FullSortingMergeJoin *>(join.get());
    if (!merge_join || merge_join->isParallel())
        return nullptr;

    const auto & table_join = join->getTableJoin();
    if (table_join.kind() != JoinKind::Inner || table_join.strictness() != JoinStrictness::All
        || !table_join.oneDisjunct() || table_join.getMixedJoinExpression())
        return nullptr;

    const auto & clause = table_join.getOnlyClause();
    if (clause.on_filter_condition_left || clause.on_filter_condition_right
        || !clause.analyzer_left_filter_condition_column_name.empty() || !clause.analyzer_right_filter_condition_column_name.empty()
        || clause.key_names_left.empty() || clause.key_names_left.size() != clause.key_names_right.size())
        return nullptr;

    if (join_step->swap_streams || !join_step->primary_key_sharding.empty() || !join->getTotals().empty())
        return nullptr;

    for (size_t side = 0; side < 2; ++side)
    {
        const auto & keys = side == 0 ? clause.key_names_left : clause.key_names_right;
        const auto & sort_node = *node.children[side];
        const auto * sorting = typeid_cast<const SortingStep *>(sort_node.step.get());
        if (!sorting || !sorting->isSortingForMergeJoin() || sorting->hasPartitions() || sorting->getScatterPartitions() != 0)
            return nullptr;

        const auto & description = sorting->getSortDescription();
        if (description.size() != keys.size())
            return nullptr;
        for (size_t i = 0; i < keys.size(); ++i)
        {
            if (description[i].column_name != keys[i] || description[i].direction != 1
                || description[i].nulls_direction != merge_join->getNullDirection() || description[i].collator)
                return nullptr;
        }

        /// These sets filter one side of the join by the keys of the other.
        if (sort_node.children.size() != 1 || typeid_cast<const CreateSetAndFilterOnTheFlyStep *>(sort_node.children.front()->step.get()))
            return nullptr;
    }

    return merge_join;
}

bool isProjection(const QueryPlan::Node & node)
{
    const auto * expression = typeid_cast<const ExpressionStep *>(node.step.get());
    /// An `arrayJoin` changes the number of rows even when its result is not an output.
    if (!expression || node.children.size() != 1 || expression->getExpression().hasArrayJoin())
        return false;

    for (const auto * output : expression->getExpression().getOutputs())
        if (output->type != ActionsDAG::ActionType::INPUT)
            return false;
    return true;
}

struct Leaf
{
    QueryPlan::Node * node;
    Names keys;
};

struct Group
{
    VectorWithMemoryTracking<Leaf> leaves;
    /// The key tuples of both sides of every join.
    VectorWithMemoryTracking<std::pair<Names, Names>> equalities;
};

/// Collects the joins of the group rooted at `root` and the inputs of the group, from left to right.
/// A side of a join is absorbed when it is a join of the group under the merge-join sort and projections.
Group collectGroup(QueryPlan::Node & root, int null_direction)
{
    struct Side
    {
        QueryPlan::Node * sort_node;
        Names keys;
    };

    Group group;
    StackWithMemoryTracking<Side> stack;

    auto push_join = [&](QueryPlan::Node & node)
    {
        const auto & clause = typeid_cast<const JoinStep &>(*node.step).getJoin()->getTableJoin().getOnlyClause();
        group.equalities.emplace_back(clause.key_names_left, clause.key_names_right);
        stack.push({node.children[1], clause.key_names_right});
        stack.push({node.children[0], clause.key_names_left});
    };

    push_join(root);
    while (!stack.empty())
    {
        auto side = std::move(stack.top());
        stack.pop();

        QueryPlan::Node * node = side.sort_node->children.front();
        while (isProjection(*node))
            node = node->children.front();

        const auto * member = getMemberJoin(*node);
        bool has_keys = member && member->getNullDirection() == null_direction;
        for (size_t i = 0; has_keys && i < side.keys.size(); ++i)
            has_keys = node->step->getOutputHeader()->has(side.keys[i]);

        if (has_keys)
            push_join(*node);
        else
            group.leaves.push_back({side.sort_node, std::move(side.keys)});
    }

    return group;
}

/// Checks that every output row of the group combines rows of all leaves with equal key tuples, and that
/// every column of the output comes from one leaf.
bool isValidGroup(const Group & group, const Block & output_header)
{
    if (group.leaves.size() < 3)
        return false;

    UnorderedMapWithMemoryTracking<String, size_t> leaf_of_column;
    for (size_t i = 0; i < group.leaves.size(); ++i)
        for (const auto & column : *group.leaves[i].node->step->getOutputHeader())
            if (!leaf_of_column.emplace(column.name, i).second)
                return false;

    auto find_leaf = [&](const Names & keys) -> std::optional<size_t>
    {
        auto it = leaf_of_column.find(keys.front());
        if (it == leaf_of_column.end() || group.leaves[it->second].keys != keys)
            return {};
        return it->second;
    };

    /// The joins form a tree over the leaves, so equal key tuples on every join make all leaf tuples equal.
    for (const auto & [left_keys, right_keys] : group.equalities)
    {
        auto left = find_leaf(left_keys);
        auto right = find_leaf(right_keys);
        if (!left || !right || *left == *right)
            return false;
    }

    const auto & first = group.leaves.front();
    const auto & first_header = *first.node->step->getOutputHeader();
    for (const auto & leaf : group.leaves)
    {
        if (leaf.keys.size() != first.keys.size())
            return false;

        const auto & header = *leaf.node->step->getOutputHeader();
        for (size_t k = 0; k < leaf.keys.size(); ++k)
        {
            auto type = removeNullable(recursiveRemoveLowCardinality(header.getByName(leaf.keys[k]).type));
            auto first_type = removeNullable(recursiveRemoveLowCardinality(first_header.getByName(first.keys[k]).type));
            if (!type->equals(*first_type))
                return false;
        }
    }

    for (const auto & column : output_header)
    {
        auto it = leaf_of_column.find(column.name);
        if (it == leaf_of_column.end())
            return false;
        if (!group.leaves[it->second].node->step->getOutputHeader()->getByName(column.name).type->equals(*column.type))
            return false;
    }

    return true;
}

}

/// Replaces a subtree of `full_sorting_merge` INNER ALL joins whose keys are all equal to each other by one
/// `MultiwayMergeJoinStep` over the inputs of the subtree. The joins are connected through their merge-join
/// sorts and pure projections, which are dropped; every input keeps its own sort.
void optimizeMultiwayMergeJoin(QueryPlan::Node & root)
{
    StackWithMemoryTracking<QueryPlan::Node *> stack;
    stack.push(&root);

    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (const auto * merge_join = getMemberJoin(*node))
        {
            const int null_direction = merge_join->getNullDirection();
            auto group = collectGroup(*node, null_direction);
            const auto & join_step = typeid_cast<const JoinStep &>(*node->step);
            if (isValidGroup(group, *join_step.getOutputHeader()))
            {
                SharedHeaders input_headers;
                VectorWithMemoryTracking<Names> key_names;
                VectorWithMemoryTracking<QueryPlan::Node *> children;
                for (auto & leaf : group.leaves)
                {
                    input_headers.push_back(leaf.node->step->getOutputHeader());
                    key_names.push_back(std::move(leaf.keys));
                    children.push_back(leaf.node);
                }

                auto multiway_join = std::make_unique<MultiwayMergeJoinStep>(
                    std::move(input_headers),
                    std::move(key_names),
                    join_step.getOutputHeader(),
                    join_step.getMaxBlockSize(),
                    join_step.getMaxStreams(),
                    null_direction);
                node->step = std::move(multiway_join);
                node->children = std::move(children);
            }
        }

        for (auto * child : node->children)
            stack.push(child);
    }
}

}
