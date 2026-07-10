#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/joinFilterPushDownPruningGate.h>

#include <deque>
#include <set>
#include <tuple>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Comparison chains longer than this are left alone to bound optimization time
constexpr size_t max_atoms_to_analyze = 64;

/// A constant bound on a column: `column > constant` (lower) or `column < constant` (upper).
/// Equality contributes both a non-strict lower and upper bound.
struct Bound
{
    const ActionsDAG::Node * constant = nullptr;
    bool is_lower = false;
    bool is_strict = false;
};

/// `smaller <= larger`, or strict `smaller < larger`. Equality contributes both directions.
struct Relation
{
    const ActionsDAG::Node * smaller = nullptr;
    const ActionsDAG::Node * larger = nullptr;
    bool is_strict = false;
};

/// Transitivity is proven only inside one total order with uniform comparison semantics.
/// Native numbers compare accurately across widths and signedness; plain Strings compare
/// lexicographically. Everything else (Enum compares by id against strings, FixedString pads
/// before comparing, Date/Decimal/UUID have their own conversion rules) is excluded.
enum class ComparisonDomain : uint8_t
{
    None,
    Number,
    String,
};

ComparisonDomain classifyType(const DataTypePtr & type)
{
    const auto unwrapped = removeLowCardinality(removeNullable(type));
    if (isNativeNumber(unwrapped))
        return ComparisonDomain::Number;
    if (isString(unwrapped))
        return ComparisonDomain::String;
    return ComparisonDomain::None;
}

ComparisonDomain classifyAtomDomain(const ActionsDAG::Node * lhs, const ActionsDAG::Node * rhs)
{
    const auto lhs_domain = classifyType(lhs->result_type);
    if (lhs_domain == ComparisonDomain::None || lhs_domain != classifyType(rhs->result_type))
        return ComparisonDomain::None;
    return lhs_domain;
}

const ActionsDAG::Node * resolveToInput(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();
    if (node && node->type == ActionsDAG::ActionType::INPUT)
        return node;
    return nullptr;
}

const ActionsDAG::Node * resolveToConstant(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();
    if (!node || node->type != ActionsDAG::ActionType::COLUMN || !node->column)
        return nullptr;
    if (node->column->isNullAt(0))
        return nullptr;
    return node;
}

struct ExtractedComparisons
{
    std::unordered_map<const ActionsDAG::Node *, std::vector<Bound>> bounds;
    std::vector<Relation> relations;
};

/// Candidate atom for removal, normalized to a constant bound on an input column
struct RemovalCandidate
{
    const ActionsDAG::Node * atom = nullptr;
    const ActionsDAG::Node * column = nullptr;
    Bound bound;
};

ExtractedComparisons extractComparisons(
    const ActionsDAG::NodeRawConstPtrs & atoms, const ActionsDAG::Node * excluded_atom, ComparisonDomain domain)
{
    ExtractedComparisons result;
    for (const auto * atom : atoms)
    {
        if (atom == excluded_atom)
            continue;
        if (atom->type != ActionsDAG::ActionType::FUNCTION || atom->children.size() != 2 || !atom->function_base)
            continue;
        if (classifyAtomDomain(atom->children[0], atom->children[1]) != domain)
            continue;
        const auto & name = atom->function_base->getName();
        const bool is_less = name == "less";
        const bool is_less_or_equals = name == "lessOrEquals";
        const bool is_greater = name == "greater";
        const bool is_greater_or_equals = name == "greaterOrEquals";
        const bool is_equals = name == "equals";
        if (!is_less && !is_less_or_equals && !is_greater && !is_greater_or_equals && !is_equals)
            continue;

        const auto * lhs_input = resolveToInput(atom->children[0]);
        const auto * rhs_input = resolveToInput(atom->children[1]);

        if (lhs_input && rhs_input)
        {
            if (is_equals)
            {
                result.relations.push_back({lhs_input, rhs_input, false});
                result.relations.push_back({rhs_input, lhs_input, false});
            }
            else if (is_less || is_less_or_equals)
                result.relations.push_back({lhs_input, rhs_input, is_less});
            else
                result.relations.push_back({rhs_input, lhs_input, is_greater});
            continue;
        }

        const auto * lhs_const = resolveToConstant(atom->children[0]);
        const auto * rhs_const = resolveToConstant(atom->children[1]);
        if ((lhs_input && rhs_const) || (lhs_const && rhs_input))
        {
            const auto * column = lhs_input ? lhs_input : rhs_input;
            const auto * constant = lhs_input ? rhs_const : lhs_const;
            /// Normalize to bounds on `column`; a constant on the left flips the comparison
            const bool constant_on_left = !lhs_input;
            if (is_equals)
            {
                result.bounds[column].push_back({constant, /*is_lower=*/true, /*is_strict=*/false});
                result.bounds[column].push_back({constant, /*is_lower=*/false, /*is_strict=*/false});
            }
            else
            {
                bool upper = is_less || is_less_or_equals;
                if (constant_on_left)
                    upper = !upper;
                result.bounds[column].push_back({constant, /*is_lower=*/!upper, /*is_strict=*/is_less || is_greater});
            }
        }
    }
    return result;
}

/// Whether transitive derivation over the remaining atoms reproduces the candidate exactly:
/// propagate constant bounds through the column relations (a lower bound travels from the
/// smaller column to the larger one, an upper bound the other way; strict if either part is
/// strict) and look for the candidate's own bound among the derived ones. This mirrors what
/// `optimize_and_compare_chain` derives, so a match identifies a condition that optimization
/// (or a human writing the equivalent) created out of the others by transitivity.
bool derivableFromRemaining(const RemovalCandidate & candidate, const ExtractedComparisons & comparisons)
{
    auto matches = [&](const Bound & derived)
    {
        if (derived.is_lower != candidate.bound.is_lower || derived.is_strict != candidate.bound.is_strict)
            return false;
        if (derived.constant == candidate.bound.constant)
            return true;
        /// Result names are not a reliable identity for constants, compare type and value
        return derived.constant->result_type->equals(*candidate.bound.constant->result_type)
            && derived.constant->column->getField() == candidate.bound.constant->column->getField();
    };

    struct QueueEntry
    {
        const ActionsDAG::Node * column = nullptr;
        Bound bound;
    };
    std::set<std::tuple<const ActionsDAG::Node *, const ActionsDAG::Node *, bool, bool>> visited;
    std::deque<QueueEntry> queue;

    for (const auto & [column, bounds] : comparisons.bounds)
    {
        for (const auto & bound : bounds)
        {
            if (column == candidate.column && matches(bound))
                return true;
            visited.insert({column, bound.constant, bound.is_lower, bound.is_strict});
            queue.push_back({column, bound});
        }
    }

    while (!queue.empty())
    {
        auto [column, bound] = queue.front();
        queue.pop_front();

        for (const auto & relation : comparisons.relations)
        {
            const ActionsDAG::Node * derived_column = nullptr;
            if (bound.is_lower && relation.smaller == column)
                derived_column = relation.larger;
            else if (!bound.is_lower && relation.larger == column)
                derived_column = relation.smaller;
            if (!derived_column)
                continue;

            Bound derived{bound.constant, bound.is_lower, bound.is_strict || relation.is_strict};
            if (!visited.insert({derived_column, derived.constant, derived.is_lower, derived.is_strict}).second)
                continue;
            if (derived_column == candidate.column && matches(derived))
                return true;
            queue.push_back({derived_column, derived});
        }
    }
    return false;
}

}

size_t tryRemoveRedundantFilterConditions(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
    auto * filter = typeid_cast<FilterStep *>(parent_node->step.get());
    if (!filter || parent_node->children.size() != 1)
        return 0;

    auto & dag = filter->getExpression();
    const auto * filter_root = dag.tryFindInOutputs(filter->getFilterColumnName());
    if (!filter_root || filter_root->type == ActionsDAG::ActionType::COLUMN)
        return 0;

    auto kept_atoms = ActionsDAG::extractConjunctionAtoms(filter_root);
    if (kept_atoms.size() < 2 || kept_atoms.size() > max_atoms_to_analyze)
        return 0;

    /// Removal candidates: inequality comparisons of an input column against a constant.
    /// Equality atoms are never removed - they are the most selective and prunable form.
    std::vector<RemovalCandidate> candidates;
    for (const auto * atom : kept_atoms)
    {
        if (atom->type != ActionsDAG::ActionType::FUNCTION || atom->children.size() != 2 || !atom->function_base)
            continue;
        const auto & name = atom->function_base->getName();
        const bool is_less = name == "less";
        const bool is_less_or_equals = name == "lessOrEquals";
        const bool is_greater = name == "greater";
        const bool is_greater_or_equals = name == "greaterOrEquals";
        if (!is_less && !is_less_or_equals && !is_greater && !is_greater_or_equals)
            continue;

        const auto * lhs_input = resolveToInput(atom->children[0]);
        const auto * rhs_input = resolveToInput(atom->children[1]);
        const auto * lhs_const = resolveToConstant(atom->children[0]);
        const auto * rhs_const = resolveToConstant(atom->children[1]);
        if (!((lhs_input && rhs_const) || (lhs_const && rhs_input)))
            continue;

        const auto * column = lhs_input ? lhs_input : rhs_input;
        const auto * constant = lhs_input ? rhs_const : lhs_const;
        if (classifyAtomDomain(atom->children[0], atom->children[1]) == ComparisonDomain::None)
            continue;
        bool upper = is_less || is_less_or_equals;
        if (!lhs_input)
            upper = !upper;
        candidates.push_back({atom, column, {constant, /*is_lower=*/!upper, /*is_strict=*/is_less || is_greater}});
    }
    if (candidates.empty())
        return 0;

    size_t removed = 0;
    for (const auto & candidate : candidates)
    {
        /// Provenance by re-derivation comes first because it is cheap: remove only what
        /// transitivity over the remaining atoms reproduces exactly, i.e. what
        /// `optimize_and_compare_chain` would have added. Only proven-derivable candidates
        /// pay for the storage index-analysis probe below.
        auto remaining = extractComparisons(
            kept_atoms, candidate.atom, classifyAtomDomain(candidate.atom->children[0], candidate.atom->children[1]));
        if (remaining.bounds.empty() || remaining.relations.empty())
            continue;
        if (!derivableFromRemaining(candidate, remaining))
            continue;

        /// The gate: a condition that can prune on its receiving table stays no matter what
        auto target = findPruningTargetForColumn(parent_node->children.front(), candidate.column->result_name);
        if (!target)
            continue;
        auto pruning_probe = ActionsDAG::buildFilterActionsDAG({candidate.atom}, {}, /*single_output_condition_node=*/true);
        if (!pruning_probe || pruning_probe->getOutputs().size() != 1)
            continue;
        if (pushedPredicateHelpsPruning(std::move(*pruning_probe), *target))
            continue;

        std::erase(kept_atoms, candidate.atom);
        ++removed;
    }

    if (removed == 0)
        return 0;

    chassert(!kept_atoms.empty());
    dag.keepFilterConjuncts(filter->getFilterColumnName(), kept_atoms, filter->removesFilterColumn());

    parent_node->step = std::make_unique<FilterStep>(
        parent_node->children.front()->step->getOutputHeader(),
        std::move(dag),
        filter->getFilterColumnName(),
        filter->removesFilterColumn());

    return 1;
}

}
