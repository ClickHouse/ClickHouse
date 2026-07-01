#include <Processors/QueryPlan/Optimizations/joinEquivalentSets.h>

#include <Processors/QueryPlan/JoinStepLogical.h>

namespace DB::QueryPlanOptimizations
{

JoinActionRef EquivalentJoinKeySet::findOrAdd(JoinActionRef ref)
{
    auto it = parent.find(ref);
    if (it == parent.end())
    {
        parent.emplace(ref, ref);
        return ref;
    }

    if (it->second == ref)
        return ref;

    JoinActionRef root = findOrAdd(it->second);
    parent.insert_or_assign(ref, root);
    return root;
}

JoinActionRef EquivalentJoinKeySet::unite(JoinActionRef a, JoinActionRef b)
{
    JoinActionRef root_a = findOrAdd(a);
    JoinActionRef root_b = findOrAdd(b);

    if (root_a == root_b)
        return root_a;

    size_t rank_a = rank[root_a];
    size_t rank_b = rank[root_b];

    if (rank_a < rank_b)
        std::swap(root_a, root_b);

    parent.insert_or_assign(root_b, root_a);

    if (rank_a == rank_b)
        ++rank[root_a];

    return root_a;
}

bool EquivalentJoinKeySet::connected(JoinActionRef a, JoinActionRef b)
{
    return findOrAdd(a) == findOrAdd(b);
}

std::unordered_map<JoinActionRef, std::vector<JoinActionRef>> EquivalentJoinKeySet::getClasses()
{
    std::unordered_map<JoinActionRef, std::vector<JoinActionRef>> classes;
    for (auto & [ref, _] : parent)
        classes[findOrAdd(ref)].push_back(ref);
    return classes;
}

std::vector<JoinActionRef> EquivalentJoinKeySet::getClass(JoinActionRef ref)
{
    std::vector<JoinActionRef> res;
    JoinActionRef root = findOrAdd(ref);
    for (auto & [other_ref, _] : parent)
    {
        if (findOrAdd(other_ref) == root)
            res.push_back(other_ref);
    }
    return res;
}

std::vector<JoinActionRefPair> getJoiningKeysForJoinStep(const JoinOperator & join_operator)
{
    std::vector<JoinActionRefPair> joining_keys;
    for (const auto & predicate : join_operator.expression)
    {
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
            continue;

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;

        auto left_column = lhs.getColumn();
        auto right_column = rhs.getColumn();
        if (!left_column.type->equals(*right_column.type))
            continue;
        joining_keys.emplace_back(lhs, rhs);
    }
    return joining_keys;
}

std::vector<JoinActionRefPair> buildEquialentSetsForJoinStepLogical(
    EquivalentJoinKeySet & equivalent_sets,
    const JoinStepLogical * join_step,
    const std::vector<QueryPlan::Node *> & child_nodes,
    int lookup_depth)
{
    auto join_inputs = join_step->getInputActions();
    auto join_input_it = join_inputs.begin();

    for (const auto * child_node : child_nodes)
    {
        if (lookup_depth >= 32)
            break;

        const auto * child_join = typeid_cast<const JoinStepLogical *>(child_node->step.get());
        if (!child_join)
            continue;
        auto join_strictness = child_join->getJoinOperator().strictness;
        auto join_kind = child_join->getJoinOperator().kind;
        if (join_kind != JoinKind::Inner || (join_strictness != JoinStrictness::All && join_strictness != JoinStrictness::Any))
            continue;

        for (const auto & output : child_join->getOutputActions())
        {
            while (join_input_it != join_inputs.end() && output.getColumnName() != join_input_it->getColumnName())
                ++join_input_it;
            if (join_input_it == join_inputs.end())
                break;
            equivalent_sets.unite(*join_input_it, output);
        }
        buildEquialentSetsForJoinStepLogical(equivalent_sets, child_join, child_node->children, lookup_depth + 1);
    }

    auto joining_keys = getJoiningKeysForJoinStep(join_step->getJoinOperator());
    for (const auto & [lhs, rhs] : joining_keys)
        equivalent_sets.unite(lhs, rhs);
    return joining_keys;
}

}
