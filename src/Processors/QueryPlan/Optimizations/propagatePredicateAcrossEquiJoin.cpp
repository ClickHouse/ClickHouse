#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
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

/// Copies filter conjuncts across equi-join keys. A copy on a primary key column prunes granules;
/// on any other column it still shrinks the target's join input. The two run at different stages,
/// see `PredicatePropagationStage`. Limited to `Expression`/`Filter` chains, see `isTransparentForPropagation`

/// Defined in partialJoinFilterPushDown.cpp
void addFilterOnTop(QueryPlan::Node & join_node, size_t child_idx, QueryPlan::Nodes & nodes, ActionsDAG filter_dag);

namespace
{

using SubstitutionMap = UnorderedMapWithMemoryTracking<std::string, ColumnWithTypeAndName>;
using AddedFilters = std::unordered_set<const QueryPlan::Node *>;

/// Equi-key pairs, left first. Differing types are skipped: the JOIN casts them itself
VectorWithMemoryTracking<std::pair<JoinActionRef, JoinActionRef>> getEquiJoinKeyPairs(const JoinOperator & join_operator)
{
    VectorWithMemoryTracking<std::pair<JoinActionRef, JoinActionRef>> pairs;
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

std::optional<std::string> resolveDown(const QueryPlan::Node * node, std::string name, const QueryPlan::Node * stop_at);

/// The first filter below `node` that this pass did not add: a copy from the earlier stage would hide the source's own filter
const QueryPlan::Node * findSourceFilter(const QueryPlan::Node * node, const AddedFilters & added_filters)
{
    return walkDown(node, [&](const auto * n)
    {
        return typeid_cast<const FilterStep *>(n->step.get()) != nullptr && !added_filters.contains(n);
    });
}

/// The `MergeTree` read below the target's `Expression`/`Filter` chain, `nullptr` when the chain ends
/// elsewhere (a remote read, an aggregation, a nested join): the pass does not copy onto such targets
ReadFromMergeTree * findTargetMergeTreeRead(const QueryPlan::Node * target_root)
{
    const auto * read = walkDown(target_root, [](const auto * n)
    {
        return typeid_cast<const ReadFromMergeTree *>(n->step.get()) != nullptr;
    });
    return read ? typeid_cast<ReadFromMergeTree *>(read->step.get()) : nullptr;
}

/// Bytes a comparison reads from this argument per row, `std::nullopt` when they are not bounded
std::optional<size_t> comparedWidth(const ActionsDAG::Node * node)
{
    const auto type = removeNullable(removeLowCardinality(node->result_type));
    if (type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        return type->getSizeOfValueInMemory();
    /// a `String` constant against a `FixedString` key is compared up to its own length
    if (node->type == ActionsDAG::ActionType::COLUMN && isString(type) && node->column && isColumnConst(*node->column))
        return node->column->getDataAt(0).size();
    return {};
}

/// A comparison of fixed-size values of at most 32 bytes is cheap next to the per-row work of any
/// join algorithm. A wider argument or a set lookup is not, so it has to earn its place by pruning
/// the primary key instead of being evaluated over the whole target
bool atomIsCheapEnoughForAnyTarget(const ActionsDAG::Node * atom)
{
    if (atom->function_base->getName() == "in")
        return false;
    /// The constant counts too: a `FixedString` key is compared against all of a wider constant
    for (const auto * child : atom->children)
    {
        const auto width = comparedWidth(child);
        if (!width || *width > 32)
            return false;
    }
    return true;
}

/// Every substituted key has to resolve to a column the target reads. `Pruning` takes the atoms whose keys are
/// all prunable, `Shrinking` the cheap atoms among the others
bool atomWorthCopying(
    const QueryPlan::Node * target_root,
    const NameSet & prunable_columns,
    const ActionsDAG::Node * atom,
    const SubstitutionMap & substitution,
    PredicatePropagationStage stage)
{
    bool prunes = true;
    for (const auto * child : atom->children)
    {
        if (child->type != ActionsDAG::ActionType::INPUT)
            continue;
        const auto it = substitution.find(child->result_name);
        if (it == substitution.end())
            return false;
        const auto target_column = resolveDown(target_root, it->second.name, /*stop_at=*/nullptr);
        if (!target_column)
            return false;
        prunes = prunes && prunable_columns.contains(*target_column);
    }
    if (stage == PredicatePropagationStage::Pruning)
        return prunes;
    return !prunes && atomIsCheapEnoughForAnyTarget(atom);
}

/// result_names of atoms the target's filters already have, for dedup
UnorderedSetWithMemoryTracking<std::string> collectTargetAtoms(const QueryPlan::Node * target_root)
{
    UnorderedSetWithMemoryTracking<std::string> result;
    walkDown(target_root, [&](const auto * n)
    {
        const auto * filter = typeid_cast<const FilterStep *>(n->step.get());
        if (!filter)
            return false;
        if (const auto * filter_root = filter->getExpression().tryFindInOutputs(filter->getFilterColumnName()))
        {
            for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_root))
                result.insert(atom->result_name);
        }
        return false;
    });
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

/// Undo renames, `__tableX.orderkey` -> `orderkey`. The source stops at its filter `stop_at`, the target goes to the read
std::optional<std::string> resolveDown(const QueryPlan::Node * node, std::string name, const QueryPlan::Node * stop_at)
{
    while (node)
    {
        const auto * filter = typeid_cast<const FilterStep *>(node->step.get());
        if (filter && node == stop_at)
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

/// `optimizeJoinLogical` keys the hash table statistics of the build side by the input it saw. A copy made after it
/// changes that input, so the size the build records must not go under the key of the unfiltered input.
/// The key is zero before `optimizeJoinLogical` and with the statistics off
void rekeyBuildSideStatistics(JoinStepLogical & join, const QueryPlan::Node & build_root)
{
    if (!join.getRightHashTableCacheKey())
        return;
    std::unordered_map<const QueryPlan::Node *, UInt64> cache_keys;
    std::unordered_map<const QueryPlan::Node *, UInt64> raw_hashes;
    calculateHashTableCacheKeys(build_root, cache_keys, raw_hashes);
    join.setRightHashTableCacheKey(raw_hashes.at(&build_root) ^ calculateJoinStepCacheKeyContribution(join, JoinTableSide::Right));
}

size_t tryPropagateToSide(
    QueryPlan::Node * join_node,
    size_t target_idx,
    QueryPlan::Node * source_root,
    const QueryPlan::Node * source_filter_node,
    const SubstitutionMap & substitution,
    PredicatePropagationStage stage,
    bool index_analysis_enabled,
    AddedFilters & added_filters,
    QueryPlan::Nodes & nodes)
{
    auto * target_root = join_node->children[target_idx];
    if (!source_filter_node)
        return 0;
    const auto & source_filter = typeid_cast<const FilterStep &>(*source_filter_node->step);
    auto * target_read = findTargetMergeTreeRead(target_root);
    if (!target_read)
        return 0;
    /// With index analysis off a key column prunes nothing, so an expensive atom has nothing to earn
    NameSet prunable_columns;
    if (index_analysis_enabled)
    {
        const auto & primary_key = target_read->getStorageMetadata()->getPrimaryKey();
        prunable_columns.insert(primary_key.column_names.begin(), primary_key.column_names.end());
    }

    SubstitutionMap filter_level_sub;
    for (const auto & [join_name, target_col] : substitution)
    {
        if (auto filter_name = resolveDown(source_root, join_name, source_filter_node))
            filter_level_sub[*filter_name] = target_col;
    }

    const auto & src_dag = source_filter.getExpression();
    const auto * filter_root = src_dag.tryFindInOutputs(source_filter.getFilterColumnName());
    if (!filter_root)
        return 0;

    ActionsDAG::NodeRawConstPtrs propagatable;
    for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_root))
    {
        if (atomSafelySubstitutable(atom, filter_level_sub)
            && atomWorthCopying(target_root, prunable_columns, atom, filter_level_sub, stage))
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
    auto * added_filter = join_node->children[target_idx];
    added_filter->step->setStepDescription("Propagated equi-join filter");
    added_filters.insert(added_filter);
    /// index analysis has built the cache key of the read already, so the key lacks this copy. The flag stays on the read:
    /// a plan can be optimized again (the plan of a view under `merge`), and that walk would tag the merged filter
    if (stage == PredicatePropagationStage::Shrinking)
        target_read->disableQueryConditionCacheFilterTagging();
    if (target_idx == 1)
        rekeyBuildSideStatistics(typeid_cast<JoinStepLogical &>(*join_node->step), *added_filter);
    return 1;
}

}

size_t tryPropagatePredicateAcrossEquiJoin(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    PredicatePropagationStage stage,
    bool index_analysis_enabled,
    AddedFilters & added_filters,
    const Optimization::ExtraSettings &)
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
    const auto * left_filter  = findSourceFilter(left_root, added_filters);
    const auto * right_filter = findSourceFilter(right_root, added_filters);

    size_t propagated = 0;
    if (can_l_to_r)
        propagated += tryPropagateToSide(
            parent_node, 1, left_root, left_filter, l_to_r, stage, index_analysis_enabled, added_filters, nodes);
    if (can_r_to_l)
        propagated += tryPropagateToSide(
            parent_node, 0, right_root, right_filter, r_to_l, stage, index_analysis_enabled, added_filters, nodes);
    return propagated;
}

}
