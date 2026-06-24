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

bool atomSubstitutable(const ActionsDAG::Node * node, const SubstitutionMap & sub)
{
    if (!node || node->type == ActionsDAG::ActionType::ARRAY_JOIN)
        return false;
    if (!node->isDeterministic())
        return false;
    /// Stateful functions (aiEmbed, timeSeriesStoreTags) are deterministic but carry side effects or external calls
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->isStateful())
        return false;
    if (node->type == ActionsDAG::ActionType::INPUT)
        return sub.contains(node->result_name);
    for (const auto * child : node->children)
        if (!atomSubstitutable(child, sub))
            return false;
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
    size_t source_idx,
    size_t target_idx,
    const SubstitutionMap & substitution,
    QueryPlan::Nodes & nodes)
{
    auto * source_root = join_node->children[source_idx];
    auto * target_root = join_node->children[target_idx];

    if (!targetReachesIndexedSource(target_root))
        return 0;

    const auto * source_filter = findFilterBelow(source_root);
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
        if (atomSubstitutable(atom, filter_level_sub))
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
    for (const auto & [lhs, rhs] : equi_pairs)
    {
        if (!changes_right)
            l_to_r[lhs.getColumnName()] = rhs.getColumn();
        if (!changes_left)
            r_to_l[rhs.getColumnName()] = lhs.getColumn();
    }

    /// LEFT keeps unmatched left rows, so only L->R is safe/ mirror for RIGHT/ INNER allows both
    const bool can_l_to_r = (op.kind == JoinKind::Inner || op.kind == JoinKind::Left)  && !l_to_r.empty();
    const bool can_r_to_l = (op.kind == JoinKind::Inner || op.kind == JoinKind::Right) && !r_to_l.empty();

    size_t lifts = 0;
    if (can_l_to_r)
        lifts += tryLiftSide(parent_node, 0, 1, l_to_r, nodes);
    if (can_r_to_l)
        lifts += tryLiftSide(parent_node, 1, 0, r_to_l, nodes);
    return lifts;
}

}
