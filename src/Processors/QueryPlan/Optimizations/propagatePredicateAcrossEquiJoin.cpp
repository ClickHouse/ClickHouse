#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Functions/FunctionsComparison.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <optional>
#include <string>

namespace DB::QueryPlanOptimizations
{

/// Copies filter conjuncts across equi-join keys so the other side can prune its primary key.
/// Limited to `Expression`/`Filter` chains, see `isTransparentForPropagation`

/// Defined in partialJoinFilterPushDown.cpp
void addFilterOnTop(QueryPlan::Node & join_node, size_t child_idx, QueryPlan::Nodes & nodes, ActionsDAG filter_dag);

namespace
{

using SubstitutionMap = std::unordered_map<std::string, ColumnWithTypeAndName>;

/// Equi-key pairs, left first. Differing types are skipped: the JOIN casts them itself
std::vector<std::pair<JoinActionRef, JoinActionRef>> getEquiJoinKeyPairs(const JoinOperator & join_operator)
{
    std::vector<std::pair<JoinActionRef, JoinActionRef>> pairs;
    for (const auto & predicate : join_operator.expression)
    {
        auto [predicate_op, lhs, rhs] = predicate.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals && predicate_op != JoinConditionOperator::NullSafeEquals)
            continue;

        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        else if (!lhs.fromLeft() || !rhs.fromRight())
            continue;

        if (!lhs.getColumn().type->equals(*rhs.getColumn().type))
            continue;
        pairs.emplace_back(lhs, rhs);
    }
    return pairs;
}

/// Keep in sync with what `optimizePrimaryKeyConditionAndLimit` walks up through, or the copy never prunes
bool isTransparentForPropagation(const IQueryPlanStep * step)
{
    if (const auto * expr = typeid_cast<const ExpressionStep *>(step))
        return !expr->getExpression().hasArrayJoin();
    return typeid_cast<const FilterStep *>(step) != nullptr;
}

/// Walk down transparent steps until `predicate` matches or the chain ends
template <typename Predicate>
const QueryPlan::Node * walkDown(const QueryPlan::Node * node, Predicate && predicate)
{
    while (node)
    {
        if (predicate(node))
            return node;
        if (!isTransparentForPropagation(node->step.get()) || node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    return nullptr;
}

std::optional<std::string> resolveDown(const QueryPlan::Node * node, std::string name, bool stop_at_filter);

const FilterStep * findFilterBelow(const QueryPlan::Node * node)
{
    const auto * found = walkDown(node, [](const auto * n)
    {
        return typeid_cast<const FilterStep *>(n->step.get()) != nullptr;
    });
    return found ? typeid_cast<const FilterStep *>(found->step.get()) : nullptr;
}

/// Primary key of the MergeTree the target reads from, empty when there is none
NameSet getTargetPrimaryKeyColumns(const QueryPlan::Node * target_root)
{
    const auto * read = walkDown(target_root, [](const auto * n)
    {
        return typeid_cast<const ReadFromMergeTree *>(n->step.get()) != nullptr;
    });
    if (!read)
        return {};
    const auto & primary_key = typeid_cast<const ReadFromMergeTree &>(*read->step).getStorageMetadata()->getPrimaryKey();
    return NameSet(primary_key.column_names.begin(), primary_key.column_names.end());
}

/// Worth copying only when every substituted key is in the target's primary key
bool atomCanUseTargetPrimaryKey(
    const QueryPlan::Node * target_root,
    const NameSet & primary_key_columns,
    const ActionsDAG::Node * atom,
    const SubstitutionMap & substitution)
{
    for (const auto * child : atom->children)
    {
        if (child->type != ActionsDAG::ActionType::INPUT)
            continue;
        const auto it = substitution.find(child->result_name);
        if (it == substitution.end())
            return false;
        const auto target_column = resolveDown(target_root, it->second.name, /*stop_at_filter=*/false);
        if (!target_column || !primary_key_columns.contains(*target_column))
            return false;
    }
    return true;
}

/// result_names of atoms the target filter already has, for dedup
std::unordered_set<std::string> collectTargetAtoms(const QueryPlan::Node * target_root)
{
    std::unordered_set<std::string> result;
    const auto * target_filter = findFilterBelow(target_root);
    if (!target_filter)
        return result;
    const auto & dag = target_filter->getExpression();
    const auto * filter_root = dag.tryFindInOutputs(target_filter->getFilterColumnName());
    if (!filter_root)
        return result;
    for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_root))
        result.insert(atom->result_name);
    return result;
}

/// Shapes `KeyCondition` can turn into a key atom. `IN` with a constant set gives ranges just like
/// a comparison does
bool atomSafelySubstitutable(const ActionsDAG::Node * node, const SubstitutionMap & sub)
{
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return false;

    const auto & name = node->function_base->getName();
    const bool is_set_check = name == "in";
    const bool is_comparison = name == "equals" || name == "notEquals"
        || name == "less" || name == "greater" || name == "lessOrEquals" || name == "greaterOrEquals";
    if ((!is_comparison && !is_set_check) || node->children.size() != 2)
        return false;

    /// The atom will also run on target rows the source never saw, so it must not throw on them:
    /// a comparison across type domains parses one side per row and can fail there
    if (is_comparison && comparisonCanThrow(node->children[0]->result_type, node->children[1]->result_type))
        return false;
    if (!node->function_base->isDeterministic())
        return false;

    size_t substituted_keys = 0;
    for (const auto * child : node->children)
    {
        if (child->type == ActionsDAG::ActionType::INPUT)
        {
            const auto it = sub.find(child->result_name);
            if (it == sub.end())
                return false;
            /// Substituting a column of another type would change the atom, and `FunctionIn`
            /// rejects a left argument that does not match the type the set was built with
            if (!it->second.type->equals(*child->result_type))
                return false;
            ++substituted_keys;
        }
        else if (child->type != ActionsDAG::ActionType::COLUMN)
            return false;
    }
    /// `KeyCondition` only builds `key <op> const`, so `k1 = k2` would be a full scan filter
    return substituted_keys == 1;
}

/// Follow the ALIAS chain to the first INPUT. A computed name (`sipHash64(k) AS k`) resolves to nothing
std::optional<std::string> resolveInsideDAG(const ActionsDAG & dag, const std::string & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    /// Not an output here, so this step does not rename it
    if (!node)
        return name;
    while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};
    return node->result_name;
}

/// Undo renames, `__tableX.orderkey` -> `orderkey`. The source stops at its filter, the target goes to the read
std::optional<std::string> resolveDown(const QueryPlan::Node * node, std::string name, bool stop_at_filter)
{
    while (node)
    {
        const auto * filter = typeid_cast<const FilterStep *>(node->step.get());
        if (filter && stop_at_filter)
            return resolveInsideDAG(filter->getExpression(), name);
        if (!isTransparentForPropagation(node->step.get()) || node->children.size() != 1)
            return name;
        const auto * expr = typeid_cast<const ExpressionStep *>(node->step.get());
        if (filter || expr)
        {
            auto resolved = resolveInsideDAG(filter ? filter->getExpression() : expr->getExpression(), name);
            if (!resolved)
                return {};
            name = std::move(*resolved);
        }
        node = node->children.front();
    }
    return name;
}

size_t tryPropagateToSide(
    QueryPlan::Node * join_node,
    size_t target_idx,
    QueryPlan::Node * source_root,
    const FilterStep * source_filter,
    const SubstitutionMap & substitution,
    QueryPlan::Nodes & nodes)
{
    auto * target_root = join_node->children[target_idx];
    if (!source_filter)
        return 0;
    /// Only helps when the target feeds a MergeTree primary key
    const auto primary_key_columns = getTargetPrimaryKeyColumns(target_root);
    if (primary_key_columns.empty())
        return 0;

    SubstitutionMap filter_level_sub;
    for (const auto & [join_name, target_col] : substitution)
    {
        if (auto filter_name = resolveDown(source_root, join_name, /*stop_at_filter=*/true))
            filter_level_sub[*filter_name] = target_col;
    }

    const auto & src_dag = source_filter->getExpression();
    const auto * filter_root = src_dag.tryFindInOutputs(source_filter->getFilterColumnName());
    if (!filter_root)
        return 0;

    ActionsDAG::NodeRawConstPtrs propagatable;
    for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_root))
    {
        if (atomSafelySubstitutable(atom, filter_level_sub)
            && atomCanUseTargetPrimaryKey(target_root, primary_key_columns, atom, filter_level_sub))
            propagatable.push_back(atom);
    }
    if (propagatable.empty())
        return 0;

    auto propagated_dag = ActionsDAG::buildFilterActionsDAG(propagatable, filter_level_sub, /*single_output_condition_node=*/true);
    if (!propagated_dag)
        return 0;
    propagated_dag->deduplicateSubtrees();
    /// addFilterOnTop requires exactly one output (filter column)
    if (propagated_dag->getOutputs().size() != 1)
        return 0;

    /// Drop conjuncts the target already has: equal result_name means structurally equal atom
    auto existing_target_atoms = collectTargetAtoms(target_root);
    if (!existing_target_atoms.empty())
    {
        const auto * propagated_root = propagated_dag->getOutputs().front();
        ActionsDAG::NodeRawConstPtrs novel_atoms;
        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(propagated_root))
        {
            if (!existing_target_atoms.contains(atom->result_name))
                novel_atoms.push_back(atom);
        }
        if (novel_atoms.empty())
            return 0;
        auto novel_dag = ActionsDAG::buildFilterActionsDAG(novel_atoms, /*node_name_to_input_node_column=*/{}, /*single_output_condition_node=*/true);
        if (!novel_dag || novel_dag->getOutputs().size() != 1)
            return 0;
        propagated_dag = std::move(novel_dag);
    }

    addFilterOnTop(*join_node, target_idx, nodes, std::move(*propagated_dag));
    join_node->children[target_idx]->step->setStepDescription("Propagated equi-join filter");
    return 1;
}

}

size_t tryPropagatePredicateAcrossEquiJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    auto * join = typeid_cast<JoinStepLogical *>(parent_node->step.get());
    if (!join || parent_node->children.size() != 2)
        return 0;

    const auto & op = join->getJoinOperator();
    /// The copied atom is a function of the key alone, so it takes whole key groups: it cannot
    /// change which row `Any` picks, nor whether `Semi` finds a match. `any_join_distinct_right_table_keys`
    /// rewrites `ANY` into `Semi` / `RightAny`, so those shapes arrive here as well
    if (op.strictness != JoinStrictness::All && op.strictness != JoinStrictness::Any
        && op.strictness != JoinStrictness::Semi && op.strictness != JoinStrictness::RightAny)
        return 0;
    if (op.kind == JoinKind::Full || op.kind == JoinKind::Paste)
        return 0;

    /// Only this join's keys: pushdown has already sunk any usable filter below a nested join
    auto equi_pairs = getEquiJoinKeyPairs(op);
    if (equi_pairs.empty())
        return 0;

    /// join_use_nulls makes the JOIN-side type nullable, which would not match the target input
    const bool changes_left  = join->typeChangingSides().contains(JoinTableSide::Left);
    const bool changes_right = join->typeChangingSides().contains(JoinTableSide::Right);

    SubstitutionMap l_to_r;
    SubstitutionMap r_to_l;
    /// The substituted column must be in the child's header; for `ON l.k = r.k + 1` it is not
    const auto & left_header  = *parent_node->children[0]->step->getOutputHeader();
    const auto & right_header = *parent_node->children[1]->step->getOutputHeader();
    for (const auto & [lhs, rhs] : equi_pairs)
    {
        if (!changes_right && right_header.has(rhs.getColumn().name))
            l_to_r[lhs.getColumnName()] = rhs.getColumn();
        if (!changes_left && left_header.has(lhs.getColumn().name))
            r_to_l[rhs.getColumnName()] = lhs.getColumn();
    }

    /// LEFT keeps unmatched left rows, so only L->R is safe there; mirrored for RIGHT, both for INNER
    const bool can_l_to_r = (op.kind == JoinKind::Inner || op.kind == JoinKind::Left)  && !l_to_r.empty();
    const bool can_r_to_l = (op.kind == JoinKind::Inner || op.kind == JoinKind::Right) && !r_to_l.empty();

    /// Snapshot the sources before the first copy mutates the tree
    QueryPlan::Node * left_root = parent_node->children[0];
    QueryPlan::Node * right_root = parent_node->children[1];
    const FilterStep * left_filter  = findFilterBelow(left_root);
    const FilterStep * right_filter = findFilterBelow(right_root);

    size_t propagated = 0;
    if (can_l_to_r)
        propagated += tryPropagateToSide(parent_node, 1, left_root, left_filter, l_to_r, nodes);
    if (can_r_to_l)
        propagated += tryPropagateToSide(parent_node, 0, right_root, right_filter, r_to_l, nodes);
    return propagated;
}

}
