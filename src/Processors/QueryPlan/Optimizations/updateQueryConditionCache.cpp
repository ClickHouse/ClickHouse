#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/functional/hash.hpp>

#include <algorithm>
#include <string_view>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Like `VirtualColumnUtils::isDeterministic`, but treats `__topKFilter` as deterministic.
///
/// `__topKFilter` is the only "officially" non-deterministic function we expect to appear in
/// `query_info.filter_actions_dag` once `tryOptimizeTopK` has chosen the read step for TopK
/// dynamic filtering: filter pushdown collects the PREWHERE `__topKFilter` together with the
/// WHERE predicate. The non-determinism is bounded — for a fixed plan and data, the threshold
/// trajectory only tightens, so any row whose sort-column value lies in the final top-N is
/// kept by `__topKFilter` at every point during execution. Consequently a chunk that the
/// outer `WHERE` reduces to zero rows is one whose granule has no row that could have
/// reached the final result, regardless of the threshold's exact path through the run. The
/// cache key is also salted with the TopK plan parameters, so cached granule decisions can
/// only be reused under the same TopK plan that produced them.
bool isDeterministicAllowingTopKFilter(const ActionsDAG::Node * node)
{
    for (const auto * child : node->children)
        if (!isDeterministicAllowingTopKFilter(child))
            return false;

    if (node->type == ActionsDAG::ActionType::COLUMN)
        return node->isDeterministic();

    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return true;

    if (!node->function_base->isDeterministic())
        return node->function_base->getName() == "__topKFilter";

    return true;
}

/// Unwrap leading ALIAS nodes so `extractConjunctionAtoms` can see a top-level `and(...)` behind an
/// alias. Mirrors `optimizeUseNormalProjection`.
const ActionsDAG::Node * unwrapAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();
    return node;
}

/// Strip a leading analyzer `__tableN.` qualifier from a column identifier so the storage-domain name
/// `v` compares equal to the analyzer-domain name `__table1.v`. Same helper as `optimizeUseNormalProjection`.
std::string_view stripTableQualifier(std::string_view name)
{
    static constexpr std::string_view prefix = "__table";
    if (!name.starts_with(prefix))
        return name;

    size_t pos = prefix.size();
    while (pos < name.size() && isdigit(static_cast<unsigned char>(name[pos])))
        ++pos;

    if (pos > prefix.size() && pos < name.size() && name[pos] == '.')
        return name.substr(pos + 1);

    return name;
}

/// Name-independent structural equality of two ActionsDAG expression nodes. Ignores `result_name`,
/// aliases, and `__tableN.` qualifiers — the analyzer decorates the same predicate with different names
/// in the FilterStep (analyzer domain) vs the read step's `filter_actions_dag` (storage domain), so a
/// raw `Node::getHash` comparison (which folds `result_name`) would not match. Adapted from the
/// DAG-to-AST `matchDAGNodeToAST` used by projection implication matching.
bool nodesStructurallyEqual(const ActionsDAG::Node * lhs, const ActionsDAG::Node * rhs)
{
    lhs = unwrapAliases(lhs);
    rhs = unwrapAliases(rhs);

    if (lhs->type != rhs->type)
        return false;

    switch (lhs->type)
    {
        case ActionsDAG::ActionType::FUNCTION:
        {
            if (!lhs->function_base || !rhs->function_base)
                return false;
            if (lhs->function_base->getName() != rhs->function_base->getName())
                return false;
            /// We compare inputs by unqualified name, so a name-sensitive function (formatRowNoNewline,
            /// toTypeName) with differently-aliased args could match spuriously; require name-insensitivity.
            if (!lhs->function_base->isNameInsensitive())
                return false;
            if (!lhs->result_type || !rhs->result_type || !lhs->result_type->equals(*rhs->result_type))
                return false;
            if (lhs->children.size() != rhs->children.size())
                return false;
            for (size_t i = 0; i < lhs->children.size(); ++i)
                if (!nodesStructurallyEqual(lhs->children[i], rhs->children[i]))
                    return false;
            return true;
        }
        case ActionsDAG::ActionType::INPUT:
            return stripTableQualifier(lhs->result_name) == stripTableQualifier(rhs->result_name);
        case ActionsDAG::ActionType::COLUMN:
        {
            /// Only match deterministic constants. A non-deterministic constant (e.g. `now()`) must never
            /// be treated as equal — the read side already rejects those via `isDeterministicAllowingTopKFilter`.
            if (!lhs->is_deterministic_constant || !rhs->is_deterministic_constant)
                return false;
            const auto * lhs_const = typeid_cast<const ColumnConst *>(lhs->column.get());
            const auto * rhs_const = typeid_cast<const ColumnConst *>(rhs->column.get());
            if (!lhs_const || !rhs_const || !lhs->result_type || !rhs->result_type)
                return false;
            if (!lhs->result_type->equals(*rhs->result_type))
                return false;
            /// Reject dummy constants (ColumnSet for IN, ColumnFunction for lambdas): their value is not
            /// Field-representable, so distinct sets would compare equal (mirrors `hasDummyInside` in
            /// ActionsDAG.cpp). Otherwise compare the inner data column value.
            const IColumn & lhs_inner = lhs_const->getDataColumn();
            const IColumn & rhs_inner = rhs_const->getDataColumn();
            if (lhs_inner.isDummy() || rhs_inner.isDummy())
                return false;
            return lhs_inner.compareAt(0, 0, rhs_inner, /* nan_direction_hint */ 1) == 0;
        }
        default:
            /// ALIAS is unwrapped above; other node types (ARRAY_JOIN, etc.) are not expected in a filter
            /// predicate. Be conservative: treat as not-equal so we skip (miss the optimization) rather
            /// than risk a false match.
            return false;
    }
}

/// Attributing the read condition to a `FilterStep` is sound only if every mark the step drops is also
/// dropped by the read condition, i.e. the step predicate is implied by it. Conservative sufficient
/// check: every conjunction atom of the step predicate must structurally match a read-condition atom.
/// A false negative only skips the write (missed optimization); a false positive returns wrong results,
/// so the comparison errs conservative.
bool filterStepConditionIsImpliedByReadCondition(
    const ActionsDAG::Node * filter_step_condition, const ActionsDAG::Node * read_condition)
{
    const auto read_atoms = ActionsDAG::extractConjunctionAtoms(unwrapAliases(read_condition));
    const auto filter_atoms = ActionsDAG::extractConjunctionAtoms(unwrapAliases(filter_step_condition));

    for (const auto * filter_atom : filter_atoms)
    {
        const bool implied = std::any_of(
            read_atoms.begin(),
            read_atoms.end(),
            [&](const auto * read_atom) { return nodesStructurallyEqual(filter_atom, read_atom); });
        if (!implied)
            return false;
    }

    return true;
}

}

/// This is not really an optimization. The purpose of this function is to extract and hash the filter condition of WHERE or PREWHERE
/// filters. These correspond to these steps:
///
///   [...]
///     ^
///     |
///     |
///   FilterStep
///     ^
///     |
///     |
///   ReadFromMergeTree
///
/// Later on, the hashed filter condition will be used as a key in the query condition cache.
///
void updateQueryConditionCache(const Stack & stack, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (!optimization_settings.use_query_condition_cache)
        return;

    const auto & frame = stack.back();

    auto * read_from_merge_tree = dynamic_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree)
        return;

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto & filter_actions_dag = query_info.filter_actions_dag;
    if (!filter_actions_dag || query_info.isFinal())
        return;

    const auto & outputs = filter_actions_dag->getOutputs();

    /// Restrict to the case that ActionsDAG has a single output. This isn't technically necessary but de-risks
    /// the implementation a lot while not losing much usefulness.
    if (outputs.size() != 1)
        return;

    /// Issues #81506 and #84508.
    for (const auto * output : outputs)
    {
        if (!isDeterministicAllowingTopKFilter(output))
            return;
    }

    /// Expression DAGs of `ExpressionStep`s encountered between the read step and the `FilterStep`,
    /// bottom-to-top. The `FilterStep` predicate is composed through them so its column identifiers
    /// resolve into the read step's naming domain before it is compared against `filter_actions_dag`.
    /// Same technique as `optimizePrimaryKeyConditionAndLimit`.
    std::vector<const ActionsDAG *> expression_dags;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            /// Compose the step predicate through the intervening `ExpressionStep`s so its
            /// identifiers resolve into the read domain, then attribute only if it is deterministic
            /// (skip filters carrying e.g. `__applyFilter`) and implied by the read condition
            /// (`filterStepConditionIsImpliedByReadCondition`).
            auto filter_dag = filter_step->getExpression().clone();
            for (auto it = expression_dags.rbegin(); it != expression_dags.rend(); ++it)
                filter_dag = ActionsDAG::merge((*it)->clone(), std::move(filter_dag));

            const auto * filter_node = filter_dag.tryFindInOutputs(filter_step->getFilterColumnName());
            if (!filter_node
                || !isDeterministicAllowingTopKFilter(filter_node)
                || !filterStepConditionIsImpliedByReadCondition(filter_node, filter_actions_dag->getOutputs()[0]))
                return;

            /// `size_t` (not `UInt64`) so `boost::hash_combine` binds on platforms where
            /// they differ (e.g. Apple, where `size_t` is `unsigned long` but `UInt64` is `unsigned long long`).
            size_t condition_hash = filter_actions_dag->getOutputs()[0]->getHash();

            /// `ORDER BY ... LIMIT N` may drop granules during reading, so the result of the WHERE
            /// filter is no longer "applies to every granule of every part" — it applies only to
            /// the granules that the TopK filter decided to keep. To keep the QCC entry sound, we
            /// fold the deterministic part of the TopK plan into the cache key. Same query + same
            /// part set + same TopK params → cache hit; different LIMIT or sort column → fresh
            /// entry, never reusing a row-set computed under different TopK conditions.
            if (const auto & top_k_filter_info = read_from_merge_tree->getTopKFilterInfo())
                boost::hash_combine(condition_hash, top_k_filter_info->condition_hash);

            String condition = filter_actions_dag->getNames()[0];
            filter_step->setConditionForQueryConditionCache(condition_hash, condition);
            return;
        }

        if (auto * expression_step = typeid_cast<ExpressionStep *>(iter->node->step.get()))
        {
            /// `arrayJoin` changes row cardinality, so composing a filter through it is unsound; stop
            /// walking and skip attribution (same guard as `optimizePrimaryKeyConditionAndLimit`).
            if (expression_step->getExpression().hasArrayJoin())
                return;
            expression_dags.push_back(&expression_step->getExpression());
            continue;
        }

        /// Any other step between the read step and the filter (e.g. cardinality-changing) makes the
        /// attribution unsound to reason about; be conservative and stop.
        return;
    }
}

}
