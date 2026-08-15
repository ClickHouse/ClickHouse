#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/joinEquivalentSets.h>

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <string>

namespace DB::QueryPlanOptimizations
{

/// Defined in partialJoinFilterPushDown.cpp
void addFilterOnTop(QueryPlan::Node & join_node, size_t child_idx, QueryPlan::Nodes & nodes, ActionsDAG filter_dag);

namespace
{

using SubstitutionMap = std::unordered_map<std::string, ColumnWithTypeAndName>;

/// Walk down a single-child chain of Expression/Filter steps until `predicate` matches or
/// the chain ends. Returns the matching node or nullptr
template <typename Predicate>
const QueryPlan::Node * walkDown(const QueryPlan::Node * node, Predicate && predicate)
{
    while (node)
    {
        if (predicate(node))
            return node;
        const bool passthrough = typeid_cast<const ExpressionStep *>(node->step.get())
            || typeid_cast<const FilterStep *>(node->step.get());
        if (!passthrough || node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    return nullptr;
}

std::string resolveToFilterInput(const QueryPlan::Node * node, std::string name);

const FilterStep * findFilterBelow(const QueryPlan::Node * node)
{
    const auto * found = walkDown(node, [](const auto * n)
    {
        return typeid_cast<const FilterStep *>(n->step.get()) != nullptr;
    });
    return found ? typeid_cast<const FilterStep *>(found->step.get()) : nullptr;
}

/// Lifting only helps when the target side eventually feeds a MergeTree primary key
bool targetReachesIndexedSource(const QueryPlan::Node * node)
{
    return walkDown(node, [](const auto * n)
    {
        return typeid_cast<const ReadFromMergeTree *>(n->step.get()) != nullptr;
    }) != nullptr;
}

/// A lifted predicate is useful only when every substituted key that it uses belongs to the
/// target's primary key. This avoids adding a full-scan target filter for joins on columns that
/// are unrelated to `ORDER BY`. Secondary-index propagation can be added independently once it
/// can use the same per-part applicability analysis as `ReadFromMergeTree`.
bool atomCanUseTargetPrimaryKey(
    const QueryPlan::Node * target_root,
    const ActionsDAG::Node * atom,
    const SubstitutionMap & substitution)
{
    const auto * read = walkDown(target_root, [](const auto * n)
    {
        return typeid_cast<const ReadFromMergeTree *>(n->step.get()) != nullptr;
    });
    const auto * mt = read ? typeid_cast<const ReadFromMergeTree *>(read->step.get()) : nullptr;
    if (!mt)
        return false;

    const auto & primary_key = mt->getStorageMetadata()->getPrimaryKey().column_names;
    NameSet primary_key_columns(primary_key.begin(), primary_key.end());
    for (const auto * child : atom->children)
    {
        if (child->type != ActionsDAG::ActionType::INPUT)
            continue;
        const auto it = substitution.find(child->result_name);
        if (it == substitution.end()
            || !primary_key_columns.contains(resolveToFilterInput(target_root, it->second.name)))
            return false;
    }
    return true;
}

/// result_names of conjunct atoms already present in target filter, for dedup against lift candidates
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

/// Moving an atom to the other join side makes it execute on rows that the source-side
/// predicate did not see. Restrict the pass to predicates that cannot raise an exception
/// for those additional values. In particular, do not propagate arbitrary deterministic
/// functions such as `intDiv` or casts.
bool atomSafelySubstitutable(const ActionsDAG::Node * node, const SubstitutionMap & sub)
{
    if (!node || node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return false;

    const auto & name = node->function_base->getName();
    const bool is_null_check = name == "isNull" || name == "isNotNull";
    const bool is_comparison = name == "equals" || name == "notEquals"
        || name == "less" || name == "greater" || name == "lessOrEquals" || name == "greaterOrEquals";
    if ((!is_null_check && !is_comparison) || node->children.size() != (is_null_check ? 1 : 2))
        return false;

    for (const auto * child : node->children)
    {
        if (child->type == ActionsDAG::ActionType::INPUT)
        {
            if (!sub.contains(child->result_name))
                return false;
        }
        else if (child->type != ActionsDAG::ActionType::COLUMN)
            return false;
    }
    return true;
}

/// Follow ALIAS chain from a DAG output to its first INPUT, returning INPUT's name
std::string resolveInsideDAG(const ActionsDAG & dag, const std::string & name)
{
    const auto * node = dag.tryFindInOutputs(name);
    if (!node)
        return name;
    while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return name;
    return node->result_name;
}

/// Undo ExpressionStep renames, JOIN-level `__tableX.orderkey` -> filter-level `orderkey`
std::string resolveToFilterInput(const QueryPlan::Node * node, std::string name)
{
    while (node)
    {
        if (const auto * filter = typeid_cast<const FilterStep *>(node->step.get()))
            return resolveInsideDAG(filter->getExpression(), name);
        const auto * expr = typeid_cast<const ExpressionStep *>(node->step.get());
        if (!expr || node->children.size() != 1)
            return name;
        name = resolveInsideDAG(expr->getExpression(), name);
        node = node->children.front();
    }
    return name;
}

size_t tryLiftSide(
    QueryPlan::Node * join_node,
    size_t target_idx,
    QueryPlan::Node * source_root,
    const FilterStep * source_filter,
    const SubstitutionMap & substitution,
    QueryPlan::Nodes & nodes)
{
    auto * target_root = join_node->children[target_idx];
    if (!targetReachesIndexedSource(target_root))
        return 0;
    if (!source_filter)
        return 0;

    SubstitutionMap filter_level_sub;
    for (const auto & [join_name, target_col] : substitution)
        filter_level_sub[resolveToFilterInput(source_root, join_name)] = target_col;

    const auto & src_dag = source_filter->getExpression();
    const auto * filter_root = src_dag.tryFindInOutputs(source_filter->getFilterColumnName());
    if (!filter_root)
        return 0;

    ActionsDAG::NodeRawConstPtrs liftable;
    for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_root))
    {
        if (atomSafelySubstitutable(atom, filter_level_sub) && atomCanUseTargetPrimaryKey(target_root, atom, filter_level_sub))
            liftable.push_back(atom);
    }
    if (liftable.empty())
        return 0;

    auto lifted_dag = ActionsDAG::buildFilterActionsDAG(liftable, filter_level_sub, /*single_output_condition_node=*/true);
    if (!lifted_dag)
        return 0;
    lifted_dag->deduplicateSubtrees();
    /// addFilterOnTop requires exactly one output (filter column)
    if (lifted_dag->getOutputs().size() != 1)
        return 0;

    /// drop conjuncts the target already has. result_name is computed deterministically from structure, so structurally equivalent atoms match
    auto existing_target_atoms = collectTargetAtoms(target_root);
    if (!existing_target_atoms.empty())
    {
        const auto * lifted_root = lifted_dag->getOutputs().front();
        ActionsDAG::NodeRawConstPtrs novel_atoms;
        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(lifted_root))
        {
            if (!existing_target_atoms.contains(atom->result_name))
                novel_atoms.push_back(atom);
        }
        if (novel_atoms.empty())
            return 0;
        auto novel_dag = ActionsDAG::buildFilterActionsDAG(novel_atoms, /*node_name_to_input_node_column=*/{}, /*single_output_condition_node=*/true);
        if (!novel_dag || novel_dag->getOutputs().size() != 1)
            return 0;
        lifted_dag = std::move(novel_dag);
    }

    addFilterOnTop(*join_node, target_idx, nodes, std::move(*lifted_dag));
    join_node->children[target_idx]->step->setStepDescription("Lifted equi-join filter");
    return 1;
}

}

size_t tryLiftPredicateAcrossEquiJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings &)
{
    auto * join = typeid_cast<JoinStepLogical *>(parent_node->step.get());
    if (!join || parent_node->children.size() != 2)
        return 0;

    const auto & op = join->getJoinOperator();
    if (op.strictness != JoinStrictness::All)
        return 0;
    if (op.kind == JoinKind::Full || op.kind == JoinKind::Paste)
        return 0;

    EquivalentJoinKeySet equi_set;
    auto equi_pairs = buildEquialentSetsForJoinStepLogical(equi_set, join, parent_node->children);
    if (equi_pairs.empty())
        return 0;

    /// join_use_nulls makes JOIN-side type nullable, so substituted column would not match target input
    const bool changes_left  = join->typeChangingSides().contains(JoinTableSide::Left);
    const bool changes_right = join->typeChangingSides().contains(JoinTableSide::Right);

    SubstitutionMap l_to_r;
    SubstitutionMap r_to_l;
    /// The substituted column ends up as an INPUT of the lifted FilterStep that sits above the
    /// target child, so its name must exist in the child's output header. For computed equi-keys
    /// like `ON l.k = r.k + 1` the rhs name is `plus(...)` which is not in the right child
    const auto & left_header  = *parent_node->children[0]->step->getOutputHeader();
    const auto & right_header = *parent_node->children[1]->step->getOutputHeader();
    for (const auto & [lhs, rhs] : equi_pairs)
    {
        if (!changes_right && right_header.has(rhs.getColumn().name))
        {
            l_to_r[lhs.getColumnName()] = rhs.getColumn();
            /// `buildEquialentSetsForJoinStepLogical` proves transitive key equivalences through
            /// nested INNER joins, so a filter written on an inner-child key equivalent to `lhs`
            /// can be lifted to the right side as well
            for (const auto & eq_expr : equi_set.getClass(lhs))
            {
                if (eq_expr.isFromSameActions(lhs) && eq_expr.fromLeft()
                    && eq_expr.getColumn().type->equals(*rhs.getColumn().type))
                    l_to_r.emplace(eq_expr.getColumnName(), rhs.getColumn());
            }
        }
        if (!changes_left && left_header.has(lhs.getColumn().name))
        {
            r_to_l[rhs.getColumnName()] = lhs.getColumn();
            for (const auto & eq_expr : equi_set.getClass(rhs))
            {
                if (eq_expr.isFromSameActions(rhs) && eq_expr.fromRight()
                    && eq_expr.getColumn().type->equals(*lhs.getColumn().type))
                    r_to_l.emplace(eq_expr.getColumnName(), lhs.getColumn());
            }
        }
    }

    /// LEFT keeps unmatched left rows, so only L->R is safe/ mirror for RIGHT/ INNER allows both
    const bool can_l_to_r = (op.kind == JoinKind::Inner || op.kind == JoinKind::Left)  && !l_to_r.empty();
    const bool can_r_to_l = (op.kind == JoinKind::Inner || op.kind == JoinKind::Right) && !r_to_l.empty();

    /// Snapshot source-side roots and filters before any lift mutates the tree
    QueryPlan::Node * left_root = parent_node->children[0];
    QueryPlan::Node * right_root = parent_node->children[1];
    const FilterStep * left_filter  = findFilterBelow(left_root);
    const FilterStep * right_filter = findFilterBelow(right_root);

    size_t lifts = 0;
    if (can_l_to_r)
        lifts += tryLiftSide(parent_node, 1, left_root, left_filter, l_to_r, nodes);
    if (can_r_to_l)
        lifts += tryLiftSide(parent_node, 0, right_root, right_filter, r_to_l, nodes);
    return lifts;
}

}
