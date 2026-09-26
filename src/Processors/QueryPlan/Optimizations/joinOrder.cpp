#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/joinOrderAlgorithms.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Common/CurrentThread.h>

#include <algorithm>
#include <expected>
#include <functional>
#include <limits>
#include <map>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Core/Joins.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <base/defines.h>
#include <Common/safe_cast.h>


namespace ProfileEvents
{
    extern const Event JoinReorderMicroseconds;
    extern const Event JoinOrderDPhypFallbacks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int EXPERIMENTAL_FEATURE_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NO_COMMON_TYPE;
}

UInt32 checkedRelationBit32(size_t relation)
{
    if (relation >= std::numeric_limits<UInt32>::digits)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Relation {} does not fit in a DPsub UInt32 mask", relation);
    return UInt32{1} << relation;
}

LoggerPtr getJoinOrderOptimizerLogger()
{
    static LoggerPtr log = getLogger("JoinOrderOptimizer");
    return log;
}

DPJoinEntry::DPJoinEntry(size_t id, std::optional<UInt64> rows, std::unordered_map<String, ColumnStats> column_stats_)
    : relations()
    , cost(0.0)
    , estimated_rows(rows)
    , column_stats(std::move(column_stats_))
    , relation_id(static_cast<int>(id))
{
    relations.set(id);
}

DPJoinEntry::DPJoinEntry(DPJoinEntryPtr lhs,
        DPJoinEntryPtr rhs,
        double cost_,
        double selectivity_,
        std::optional<UInt64> cardinality_,
        JoinOperator join_operator_,
        JoinMethod join_method_)
    : relations(lhs->relations | rhs->relations)
    , left(std::move(lhs))
    , right(std::move(rhs))
    , cost(cost_)
    , selectivity(selectivity_)
    , estimated_rows(cardinality_)
    , join_operator(std::move(join_operator_))
    , join_method(join_method_)
{
    /// Merge column stats from both children, then update NDVs for equi-join key columns.
    column_stats = left->column_stats;
    column_stats.insert(right->column_stats.begin(), right->column_stats.end());

    for (const auto & predicate : join_operator.expression)
    {
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;

        if (left_node.fromRight() && right_node.fromLeft())
            std::swap(left_node, right_node);
        if (!left_node.fromLeft() || !right_node.fromRight())
            continue;

        const auto & left_col = left_node.getColumnName();
        const auto & right_col = right_node.getColumnName();
        auto left_it = column_stats.find(left_col);
        auto right_it = column_stats.find(right_col);

        if (left_it != column_stats.end() && right_it != column_stats.end())
        {
            UInt64 min_ndv = std::min(left_it->second.num_distinct_values, right_it->second.num_distinct_values);
            left_it->second.num_distinct_values = min_ndv;
            right_it->second.num_distinct_values = min_ndv;
        }
    }

    /// Cap all NDVs at the estimated output rows.
    if (cardinality_)
    {
        for (auto & [_, stats] : column_stats)
            stats.num_distinct_values = std::min(stats.num_distinct_values, *cardinality_);
    }
}

bool DPJoinEntry::isLeaf() const { return !left && !right; }

/// Resolve a JoinActionRef to an INPUT node suitable for equivalence tracking.
/// Returns nullopt if the ref is not a simple single-relation INPUT column.
static std::optional<JoinActionRef> resolveInput(const JoinActionRef & ref)
{
    auto resolved = ref.resolveAliases();
    if (resolved.getNode()->type != ActionsDAG::ActionType::INPUT)
        return std::nullopt;
    if (!resolved.getSourceRelations().getSingleBit())
        return std::nullopt;
    return resolved;
}

/// A comparison function may resolve even when composing it transitively is unsafe
/// (for example, when it converts one side into a counterpart-dependent domain).
/// Only a valid comparison order domain is an optimizer contract that equality can
/// participate in a transitive equivalence class.
static bool hasTransitiveComparisonDomain(const JoinActionRef & predicate)
{
    const auto * node = predicate.getNode();
    return node && node->function_base && node->function_base->getComparisonOrderDomain().isValid();
}

void QueryGraph::buildColumnEquivalences()
{
    column_equivalences = {};
    this->equivalence_class_relations.clear();

    for (const auto & edge : edges)
    {
        if (!edge)
            continue;

        auto [op, lhs, rhs] = edge.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals || !hasTransitiveComparisonDomain(edge))
            continue;

        auto lhs_resolved = resolveInput(lhs);
        auto rhs_resolved = resolveInput(rhs);
        if (!lhs_resolved || !rhs_resolved)
            continue;

        auto lhs_rel = lhs_resolved->getSourceRelations().getSingleBit();
        auto rhs_rel = rhs_resolved->getSourceRelations().getSingleBit();

        /// Skip predicates involving outer-joined relations: when a LEFT/RIGHT/FULL JOIN
        /// doesn't match, the outer side produces NULLs, so the equality doesn't hold
        /// for all rows and the transitive equivalence would be invalid.
        auto lhs_it = join_kinds.find(*lhs_rel);
        auto rhs_it = join_kinds.find(*rhs_rel);
        if ((lhs_it != join_kinds.end() && !isInner(lhs_it->second.second))
            || (rhs_it != join_kinds.end() && !isInner(rhs_it->second.second)))
            continue;

        if (outer_join_conditions.contains(edge))
            continue;

        column_equivalences.add(*lhs_resolved, *rhs_resolved);

        LOG_TRACE(&Poco::Logger::get("JoinOrderOptimizer"),
            "Column equivalence: relation {} `{}` = relation {} `{}`",
            *lhs_rel, lhs_resolved->getColumnName(), *rhs_rel, rhs_resolved->getColumnName());
    }

    /// Precompute one relation mask per class: `areTransitivelyConnected` runs for every
    /// enumerated candidate pair (~3^n pairs under DPsize), so it must not rescan every
    /// class member each time.
    std::unordered_set<const void *> visited_classes;
    for (const auto & [member, _] : column_equivalences.getMemberToClassMap())
    {
        const auto equiv_class = column_equivalences.getClass(member);
        if (!equiv_class || !visited_classes.insert(equiv_class.get()).second)
            continue;

        BitSet class_relations;
        for (const auto & class_member : *equiv_class)
            if (const auto relation = class_member.getSourceRelations().getSingleBit())
                class_relations.set(*relation);
        if (class_relations.any())
            this->equivalence_class_relations.push_back(std::move(class_relations));
    }
}

bool QueryGraph::areTransitivelyConnected(const BitSet & left, const BitSet & right) const
{
    for (const auto & class_relations : this->equivalence_class_relations)
        if (areIntersecting(class_relations, left) && areIntersecting(class_relations, right))
            return true;
    return false;
}

/// Post-process the join tree to remove redundant predicates and synthesize missing ones.
///
/// Walks bottom-up building equivalence classes from each join step's predicates.
/// At each step:
///   1. Remove predicates whose endpoints are already equivalent from child joins.
///      Non-redundant predicates are added to the equivalence classes immediately,
///      so later predicates at the same step can also be detected as redundant.
///   2. Synthesize one predicate for every region-wide equality class spanning
///      the left and right subtrees that is not already enforced at or below this
///      join. Canonical costing may use every such class, so the selected physical
///      join must enforce the same cut even when it also has residual predicates.
///      In a region whose original joins are all `INNER ALL` (`region_all_inner`),
///      a `Cross` entry can only be the greedy solver's disconnected-pair fallback,
///      so it is synthesized into as well and becomes `Inner` when a class spans it.
static void
cleanupJoinPredicates(const DPJoinEntryPtr & root, const EquivalenceClasses<JoinActionRef> & column_equivalences, bool region_all_inner)
{
    using EquivClasses = EquivalenceClasses<JoinActionRef>;

    std::function<EquivClasses(const DPJoinEntryPtr &)> process =
        [&](const DPJoinEntryPtr & entry) -> EquivClasses
    {
        if (entry->isLeaf())
            return {};

        /// Merge equivalence classes from both children.
        auto equiv = process(entry->left);
        equiv.merge(process(entry->right));

        /// Phase 1: Remove redundant predicates.
        auto & expressions = entry->join_operator.expression;
        bool is_inner = isInner(entry->join_operator.kind);

        std::erase_if(expressions, [&](const JoinActionRef & predicate)
        {
            auto [op, lhs, rhs] = predicate.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals || !hasTransitiveComparisonDomain(predicate))
                return false;

            auto lhs_resolved = resolveInput(lhs);
            auto rhs_resolved = resolveInput(rhs);
            if (!lhs_resolved || !rhs_resolved)
                return false;

            auto lhs_class = equiv.getClass(*lhs_resolved);
            auto rhs_class = equiv.getClass(*rhs_resolved);
            if (lhs_class && rhs_class && lhs_class == rhs_class)
            {
                auto lhs_rel = lhs_resolved->getSourceRelations().getSingleBit();
                auto rhs_rel = rhs_resolved->getSourceRelations().getSingleBit();
                LOG_TRACE(&Poco::Logger::get("JoinOrderOptimizer"),
                    "Removed redundant join predicate: relation {} `{}` = relation {} `{}`",
                    lhs_rel ? *lhs_rel : 0, lhs_resolved->getColumnName(),
                    rhs_rel ? *rhs_rel : 0, rhs_resolved->getColumnName());
                return true;
            }

            /// Only propagate equivalences from inner joins to the parent;
            /// outer join equality holds only for matching rows
            /// and would be invalid for NULL-padded non-matching rows.
            if (is_inner)
                equiv.add(*lhs_resolved, *rhs_resolved);
            return false;
        });

        /// Phase 2: Materialize the full canonical equality cut. A residual
        /// predicate or an equality from another class must not prevent this.
        ///
        /// Every projected member of a region-wide class must be connected, not
        /// merely one representative from each side. Otherwise a cut such as
        /// {A.x, A.y} = {C.z}, where A.y is a key but A.x is not, could be costed
        /// as unique on (A.x, A.y) while the physical join enforces only A.x=C.z.
        ///
        /// A `Cross` entry in an all-inner region is the greedy solver's
        /// disconnected-pair fallback, not query syntax. A canonical cap may have
        /// assumed a spanning class equality is enforced at this join, so synthesize
        /// there too; the predicate is implied by the region's predicates, and the
        /// cross product becomes an equijoin.
        const bool convertible_cross = region_all_inner && isCrossOrComma(entry->join_operator.kind);
        if (isInner(entry->join_operator.kind) || convertible_cross)
        {
            const size_t expressions_before_synthesis = expressions.size();
            const auto & left_rels = entry->left->relations;
            const auto & right_rels = entry->right->relations;

            using ConstClassPtr = EquivClasses::ConstClassPtr;
            std::unordered_set<ConstClassPtr> visited;

            auto connect_members = [&](const JoinActionRef & lhs, const JoinActionRef & rhs)
            {
                const auto lhs_class = equiv.getClass(lhs);
                const auto rhs_class = equiv.getClass(rhs);
                if (lhs_class && rhs_class && lhs_class == rhs_class)
                    return;

                try
                {
                    expressions.push_back(JoinActionRef::transform({lhs, rhs}, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
                }
                catch (const Exception & e)
                {
                    /// Class members equated only through a common third column need not be
                    /// directly comparable (e.g. a `UUID` and an `Enum` each compared against
                    /// one `FixedString` column), so `equals` may not resolve for the pair.
                    /// Skip the synthesized predicate instead of failing a query that ran
                    /// before join reordering: the equality stays implied by the original
                    /// predicates enforced elsewhere in the tree, so the result is unchanged,
                    /// though a canonical cap that assumed this cut may overstate how
                    /// selective the executed join is.
                    if (e.code() != ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT && e.code() != ErrorCodes::NO_COMMON_TYPE)
                        throw;
                    LOG_TRACE(
                        &Poco::Logger::get("JoinOrderOptimizer"),
                        "Skipped synthesizing transitive predicate `{}` = `{}`: {}",
                        lhs.getColumnName(),
                        rhs.getColumnName(),
                        e.message());
                    return;
                }
                equiv.add(lhs, rhs);

                const auto lhs_relation = lhs.getSourceRelations().getSingleBit();
                const auto rhs_relation = rhs.getSourceRelations().getSingleBit();
                LOG_TRACE(
                    &Poco::Logger::get("JoinOrderOptimizer"),
                    "Synthesized transitive predicate: relation {} `{}` = relation {} `{}`",
                    lhs_relation.value_or(0),
                    lhs.getColumnName(),
                    rhs_relation.value_or(0),
                    rhs.getColumnName());
            };

            /// `getMemberToClassMap` is an `unordered_map` hashed on node pointers, so its
            /// iteration order is address-dependent. Collect the candidate classes first and
            /// order them by their minimal (relation, column) member, so the synthesized
            /// predicate order — and therefore `EXPLAIN` output and the join fingerprints
            /// hashed by `calculateJoinFingerprint` — is stable across runs.
            std::vector<std::pair<std::pair<UInt64, std::string_view>, ConstClassPtr>> candidate_classes;
            for (const auto & [member, _] : column_equivalences.getMemberToClassMap())
            {
                const auto member_relation = member.getSourceRelations().getSingleBit();
                if (!member_relation || !left_rels.test(*member_relation))
                    continue;

                const auto equiv_class = column_equivalences.getClass(member);
                if (!equiv_class || !visited.insert(equiv_class).second)
                    continue;

                std::pair<UInt64, std::string_view> key{std::numeric_limits<UInt64>::max(), {}};
                for (const auto & class_member : *equiv_class)
                {
                    const auto relation = class_member.getSourceRelations().getSingleBit();
                    key = std::min(
                        key,
                        std::pair<UInt64, std::string_view>{
                            relation.value_or(std::numeric_limits<UInt64>::max()), class_member.getColumnName()});
                }
                candidate_classes.emplace_back(key, equiv_class);
            }
            std::ranges::sort(candidate_classes, {}, [](const auto & candidate) { return candidate.first; });

            for (const auto & [_, equiv_class] : candidate_classes)
            {
                std::vector<JoinActionRef> left_members;
                std::vector<JoinActionRef> right_members;
                for (const auto & class_member : *equiv_class)
                {
                    const auto relation = class_member.getSourceRelations().getSingleBit();
                    if (!relation)
                        continue;
                    if (left_rels.test(*relation))
                        left_members.push_back(class_member);
                    else if (right_rels.test(*relation))
                        right_members.push_back(class_member);
                }
                if (left_members.empty() || right_members.empty())
                    continue;

                const auto & left_anchor = left_members.front();
                const auto & right_anchor = right_members.front();
                connect_members(left_anchor, right_anchor);
                for (const auto & left_member : left_members)
                    connect_members(left_member, right_anchor);
                for (const auto & right_member : right_members)
                    connect_members(left_anchor, right_member);
            }

            if (convertible_cross && expressions.size() != expressions_before_synthesis)
                entry->join_operator.kind = JoinKind::Inner;
        }

        return equiv;
    };

    process(root);
}

String DPJoinEntry::dump() const
{
    if (isLeaf())
        return fmt::format("Leaf({})", relation_id);
    return fmt::format("Join({})", fmt::join(relations, ","));
}

namespace
{

/// Why one side of an ordinary equality could not be bound to a catalog column.
enum class JoinKeyBindingFailure : UInt8
{
    /// The action has no singleton source relation, so the predicate cannot be an equality
    /// between two leaf columns; the pair stays a residual predicate.
    NoSingletonSource,
    /// A name-matching catalog column has a different type, or the relation/type/alias shape
    /// is outside what the catalog can represent; the region must fail closed.
    UnsupportedType,
    /// No unambiguous identity-preserving catalog column matches; the binding is ambiguous.
    Unresolved,
};

using JoinKeyColumnBinding = std::expected<JoinOrderColumnId, JoinKeyBindingFailure>;

/// Resolve one equality side to exactly one identity-preserving catalog column in a single
/// pass, tracking on the way whether any name-matching column has a mismatched type.
/// Successful resolution wins over a stray type mismatch on another column; a failed
/// resolution reports the strongest failure observed.
JoinKeyColumnBinding resolveJoinKeyColumn(const JoinActionRef & action, const JoinOrderDataPropertyCatalog & catalog)
{
    const auto relation = action.getSourceRelations().getSingleBit();
    if (!relation)
        return std::unexpected(JoinKeyBindingFailure::NoSingletonSource);
    if (*relation >= catalog.relationCount() || !action.getType())
        return std::unexpected(JoinKeyBindingFailure::UnsupportedType);

    const auto resolved = action.resolveAliases();
    if (resolved.getNode()->type != ActionsDAG::ActionType::INPUT)
        return std::unexpected(JoinKeyBindingFailure::UnsupportedType);

    const String type_name = action.getType()->getName();
    std::optional<JoinOrderColumnId> result;
    bool ambiguous = false;
    bool type_mismatch = false;
    for (const auto column_id : catalog.columnsForRelation(safe_cast<UInt32>(*relation)))
    {
        const auto & column = catalog.column(column_id);
        const auto & catalog_name = catalog.name(column.display_name);
        const bool name_matches = catalog_name == action.getColumnName();
        if ((name_matches || catalog_name == resolved.getColumnName()) && catalog.typeName(column_id) != type_name)
        {
            type_mismatch = true;
            continue;
        }
        if (!name_matches)
            continue;
        if (result && *result != column_id)
            ambiguous = true;
        result = column_id;
    }

    auto failure = [&] { return type_mismatch ? JoinKeyBindingFailure::UnsupportedType : JoinKeyBindingFailure::Unresolved; };
    if (!result || ambiguous)
        return std::unexpected(failure());

    const auto & result_column = catalog.column(*result);
    if (resolved.getColumnName() == catalog.name(result_column.display_name))
        return *result;

    const bool has_identity_lineage = std::ranges::any_of(
        catalog.lineageForRelation(safe_cast<UInt32>(*relation)),
        [&](JoinOrderLineageId lineage_id)
        {
            const auto & fact = catalog.lineage(lineage_id);
            const bool preserves_identity = fact.kind == QueryPlanOptimizations::ColumnLineageKind::Identity
                || fact.kind == QueryPlanOptimizations::ColumnLineageKind::ValuePreserving;
            return preserves_identity && fact.output == *result && fact.relation == *relation
                && catalog.name(fact.input_name) == resolved.getColumnName();
        });
    if (!has_identity_lineage)
        return std::unexpected(failure());
    return *result;
}

bool isDeterministicExpression(const ActionsDAG::Node * root)
{
    if (!root)
        return false;
    std::vector<const ActionsDAG::Node *> stack{root};
    std::unordered_set<const ActionsDAG::Node *> visited;
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;
        if (!node->isDeterministic())
            return false;
        stack.append_range(node->children);
    }
    return true;
}

}

JoinOrderPredicatePropertyBinding bindJoinOrderPredicate(const JoinActionRef & predicate, const JoinOrderDataPropertyCatalog & catalog)
{
    auto [op, lhs, rhs] = predicate.asBinaryPredicate();
    if (op == JoinConditionOperator::NullSafeEquals)
        return JoinOrderPropertyUnsupportedReason::NullSafeEquality;
    if (op != JoinConditionOperator::Equals || !lhs || !rhs || !hasTransitiveComparisonDomain(predicate))
        return JoinOrderResidualPredicateBinding{};

    const auto lhs_column = resolveJoinKeyColumn(lhs, catalog);
    const auto rhs_column = resolveJoinKeyColumn(rhs, catalog);
    if (lhs_column && rhs_column)
        return JoinOrderOrdinaryEqualityBinding{*lhs_column, *rhs_column};

    auto failed = [](const JoinKeyColumnBinding & binding, JoinKeyBindingFailure kind) { return !binding && binding.error() == kind; };
    if (failed(lhs_column, JoinKeyBindingFailure::NoSingletonSource) || failed(rhs_column, JoinKeyBindingFailure::NoSingletonSource))
        return JoinOrderResidualPredicateBinding{};
    if (failed(lhs_column, JoinKeyBindingFailure::UnsupportedType) || failed(rhs_column, JoinKeyBindingFailure::UnsupportedType))
        return JoinOrderPropertyUnsupportedReason::UnsupportedEqualityType;
    return JoinOrderPropertyUnsupportedReason::AmbiguousEqualityBinding;
}

/// Whether `equals` resolves for the two column types, using the same resolution that
/// `cleanupJoinPredicates` performs when it synthesizes transitive predicates.
static bool comparableForEquality(const JoinActionRef & lhs, const JoinActionRef & rhs)
{
    if (!lhs.getType() || !rhs.getType())
        return false;

    ActionsDAG probe;
    const auto & lhs_input = probe.addInput("lhs", lhs.getType());
    const auto & rhs_input = probe.addInput("rhs", rhs.getType());
    try
    {
        JoinActionRef::AddFunction add_equals(JoinConditionOperator::Equals);
        add_equals(probe, {&lhs_input, &rhs_input});
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT && e.code() != ErrorCodes::NO_COMMON_TYPE)
            throw;
        return false;
    }
    return true;
}

/// Classes with a member pair that cannot be physically equated. A canonical proof may rely
/// on any two members of an equality class being equal within a candidate: the members need
/// not have been directly compared in the query (e.g. a `UUID` and an `Enum` each compared
/// against one `FixedString` column), and then no `equals` predicate can enforce the pair.
static std::unordered_set<EquivalenceClasses<JoinActionRef>::ConstClassPtr>
findClassesWithIncomparableMembers(const EquivalenceClasses<JoinActionRef> & column_equivalences)
{
    std::unordered_set<EquivalenceClasses<JoinActionRef>::ConstClassPtr> result;
    std::unordered_set<EquivalenceClasses<JoinActionRef>::ConstClassPtr> visited;

    /// `equals` resolution depends only on the two types, so memoize probes by canonical
    /// type name: classes with repeated types do not rebuild the probe DAG per member pair.
    std::map<std::pair<String, String>, bool> comparability_by_type_names;
    auto comparable = [&](const JoinActionRef & lhs, const JoinActionRef & rhs)
    {
        if (!lhs.getType() || !rhs.getType())
            return false;
        std::pair<String, String> key{lhs.getType()->getName(), rhs.getType()->getName()};
        if (key.second < key.first)
            std::swap(key.first, key.second);
        const auto [it, inserted] = comparability_by_type_names.try_emplace(key, false);
        if (inserted)
            it->second = comparableForEquality(lhs, rhs);
        return it->second;
    };

    for (const auto & [member, _] : column_equivalences.getMemberToClassMap())
    {
        const auto equiv_class = column_equivalences.getClass(member);
        if (!equiv_class || !visited.insert(equiv_class).second)
            continue;

        for (auto lhs = equiv_class->begin(); lhs != equiv_class->end() && !result.contains(equiv_class); ++lhs)
        {
            auto rhs = lhs;
            for (++rhs; rhs != equiv_class->end(); ++rhs)
            {
                if (!comparable(*lhs, *rhs))
                {
                    result.insert(equiv_class);
                    break;
                }
            }
        }
    }
    return result;
}

JoinOrderPropertyContext::JoinOrderPropertyContext(
    const QueryGraph & query_graph_,
    JoinOrderPropertyOptions options,
    std::unique_ptr<JoinOrderCanonicalProperties> canonical_properties_,
    JoinOrderOptimizationDebugInfo * debug_info_)
    : proven_uniqueness_enabled(options.proven_uniqueness_enabled)
    , dphyp_proven_edges_enabled(
          options.dphyp_proven_edges_enabled && options.proven_uniqueness_enabled && !options.transitive_predicates_enabled)
    , transitive_predicates_enabled(options.transitive_predicates_enabled)
    , data_property_diagnostics_enabled(options.diagnostics_enabled)
    , query_graph(query_graph_)
    , canonical_properties(std::move(canonical_properties_))
    , debug_info(debug_info_)
    , log(DB::getJoinOrderOptimizerLogger())
{
}

/// `Disabled` is intentionally silent so feature-off performs no canonical diagnostic work.
/// Every other outcome is retained only when a query-local debug sink was requested.
void JoinOrderPropertyContext::recordCanonicalCapAssessment(const JoinOrderCardinalityCap & cap) const
{
    if (const auto * no_cap = std::get_if<JoinOrderNoCardinalityCapReason>(&cap))
    {
        switch (*no_cap)
        {
            case JoinOrderNoCardinalityCapReason::Disabled: return;
            case JoinOrderNoCardinalityCapReason::MissingInputRows:
                if (debug_info)
                    ++debug_info->cap_assessments.missing_input_rows;
                if (data_property_diagnostics_enabled)
                    LOG_TRACE(log, "Canonical join-order cap not applied: missing input row estimate");
                return;
            case JoinOrderNoCardinalityCapReason::NoEqualityCut:
                if (debug_info)
                    ++debug_info->cap_assessments.not_proven;
                if (data_property_diagnostics_enabled)
                    LOG_TRACE(log, "Canonical join-order cap not applied: no equality cut");
                return;
            case JoinOrderNoCardinalityCapReason::NotProven:
                if (debug_info)
                    ++debug_info->cap_assessments.not_proven;
                if (data_property_diagnostics_enabled)
                    LOG_TRACE(log, "Canonical join-order cap not applied: uniqueness not proven");
                return;
        }
    }
    if (const auto * unsupported = std::get_if<JoinOrderPropertyUnsupportedReason>(&cap))
    {
        if (debug_info)
            ++debug_info->cap_assessments.unsupported;
        if (data_property_diagnostics_enabled)
            LOG_TRACE(
                log, "Canonical join-order cap not applied: unsupported ({})", joinOrderPropertyUnsupportedReasonToString(*unsupported));
        return;
    }
    const auto & proof = std::get<JoinOrderCardinalityCapProof>(cap);
    if (debug_info)
        ++debug_info->cap_assessments.proven;
    if (data_property_diagnostics_enabled)
        LOG_TRACE(log, "Canonical join-order cap proven: upper bound {}", proof.upper_bound);
}

JoinOrderCardinalityEstimate JoinOrderPropertyContext::estimateCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind,
    JoinStrictness strictness,
    const JoinOrderCardinalityCap & canonical_cap) const
{
    JoinOrderCardinalityEstimate result{estimateJoinCardinality(left_rows, right_rows, selectivity, join_kind, strictness), {}};
    /// Proven caps exist only for `INNER ALL` regions (the query-graph builder rejects every
    /// other region), so the kind and strictness checks gate cap application.
    const auto * cap = getProvenCap(canonical_cap);
    if (join_kind != JoinKind::Inner || strictness != JoinStrictness::All || !cap)
        return result;

    result.upper_bound = cap->upper_bound;
    if (result.rows)
        result.rows = std::min(*result.rows, cap->upper_bound);
    return result;
}

JoinOrderPropertyContext::JoinCandidateAssessment JoinOrderPropertyContext::assessCandidate(
    const BitSet & left_relations,
    const BitSet & right_relations,
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    JoinKind join_kind,
    JoinCandidateConnectivity connectivity) const
{
    const auto [legacy_connected, has_cross_split_predicate] = connectivity;
    JoinCandidateAssessment result{
        .legacy_connected = legacy_connected,
        .has_cross_split_predicate = has_cross_split_predicate,
        .canonical_cap = {},
    };

    const bool transitively_connected = query_graph.areTransitivelyConnected(left_relations, right_relations);
    result.independently_transitive_connected = transitive_predicates_enabled && transitively_connected;

    /// A disconnected `Inner` candidate can become an equijoin through equivalences, but only a
    /// canonical `Proven` result may authorize that when the independent transitive setting is off.
    /// Legacy-connected candidates are assessed too because their ordinary estimate may still be capped.
    if (join_kind == JoinKind::Inner && (legacy_connected || transitively_connected))
    {
        result.canonical_cap = getCanonicalCap(left_relations, right_relations, left_rows, right_rows);
        recordCanonicalCapAssessment(result.canonical_cap);
    }

    const bool canonical_transitive_cut_proven = transitively_connected && getProvenCap(result.canonical_cap);
    result.proof_gated_transitive_connected
        = !legacy_connected && !has_cross_split_predicate && !transitive_predicates_enabled && canonical_transitive_cut_proven;
    result.equivalence_selectivity_allowed = result.independently_transitive_connected || canonical_transitive_cut_proven;
    return result;
}

class JoinOrderOptimizer
{
public:
    JoinOrderOptimizer(
        QueryGraph query_graph_,
        const std::vector<JoinOrderAlgorithm> & enabled_algorithms_,
        UInt64 max_searched_plans_,
        JoinOrderPropertyOptions property_options,
        JoinOrderOptimizationDebugInfo * debug_info_)
        : query_graph(std::move(query_graph_))
        , max_searched_plans(max_searched_plans_)
        , enabled_algorithms(enabled_algorithms_)
    {
        if (query_graph.data_property_catalog && query_graph.data_property_catalog->relationCount() != query_graph.relation_stats.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Join-order data property catalog has {} relations, expected {}",
                query_graph.data_property_catalog->relationCount(),
                query_graph.relation_stats.size());

        auto context = CurrentThread::tryGetQueryContext();
        if (context)
        {
            query_status = context->getProcessListElementSafe();
            interactive_cancel_callback = context->getInteractiveCancelCallback();
        }

        std::unique_ptr<JoinOrderCanonicalProperties> canonical_properties;
        if ((property_options.proven_uniqueness_enabled || property_options.diagnostics_enabled) && query_graph.data_property_catalog)
        {
            /// Mark the predicates of equality classes containing an incomparable member
            /// pair, so the provider refuses exactly the cuts and proofs that would rely on
            /// synthesizing such a link, while unrelated caps stay available. The check uses
            /// the materialized `column_equivalences`; in diagnostics-only mode they may be
            /// absent, but then no costing consumes the proofs.
            const auto incomparable_classes = findClassesWithIncomparableMembers(query_graph.column_equivalences);
            std::vector<JoinOrderCanonicalPredicate> predicates;
            predicates.reserve(query_graph.edges.size());
            for (size_t index = 0; index < query_graph.edges.size(); ++index)
            {
                const auto & edge = query_graph.edges[index];
                if (!edge)
                    continue;
                auto binding = bindJoinOrderPredicate(edge, *query_graph.data_property_catalog);
                if (auto * equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&binding); equality && !incomparable_classes.empty())
                {
                    const auto [op, lhs, rhs] = edge.asBinaryPredicate();
                    if (op == JoinConditionOperator::Equals && lhs)
                        if (const auto resolved = resolveInput(lhs))
                            equality->members_incomparable
                                = incomparable_classes.contains(query_graph.column_equivalences.getClass(*resolved));
                }
                predicates.push_back(
                    {.stable_id = safe_cast<UInt32>(index + 1),
                     .applicability = edge.getSourceRelations(),
                     .deterministic = isDeterministicExpression(edge.getNode()),
                     .binding = std::move(binding)});
            }
            canonical_properties = std::make_unique<JoinOrderCanonicalProperties>(
                query_graph.data_property_catalog,
                query_graph.relation_stats.size(),
                std::move(predicates),
                query_graph.canonical_property_region_rejection);
        }

        properties.emplace(query_graph, property_options, std::move(canonical_properties), debug_info_);
    }

    std::shared_ptr<DPJoinEntry> solve();

    /// Post-processing of the plan returned by `solve`: materialize the canonical equality
    /// cuts the costing assumed (`cleanupJoinPredicates`), audit the cap postconditions, and
    /// emit/collect diagnostics. Encapsulates the whole protocol so callers cannot reorder or
    /// skip a step.
    void finalizeSelectedPlan(const DPJoinEntryPtr & selected_plan);

private:
    void finalizeSelectedPlanProperties(const DPJoinEntryPtr & selected_plan);
    bool selectedPlanUsedCanonicalCardinalityCap(const DPJoinEntryPtr & selected_plan) const;
    void verifySelectedPlanCapRequirements(const DPJoinEntryPtr & selected_plan) const;

    QueryGraph query_graph;
    /// Refers to `query_graph`, so it is constructed after it and never moved.
    std::optional<JoinOrderPropertyContext> properties;
    const UInt64 max_searched_plans;
    const std::vector<JoinOrderAlgorithm> enabled_algorithms;
    LoggerPtr log = DB::getJoinOrderOptimizerLogger();
    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

bool JoinOrderOptimizer::selectedPlanUsedCanonicalCardinalityCap(const DPJoinEntryPtr & selected_plan) const
{
    if (!selected_plan)
        return false;
    return selected_plan->used_canonical_cap || selectedPlanUsedCanonicalCardinalityCap(selected_plan->left)
        || selectedPlanUsedCanonicalCardinalityCap(selected_plan->right);
}

/// Audit the costing-to-physical postcondition after `cleanupJoinPredicates`:
/// intra-group obligations must hold strictly below the capped join, and every
/// equality class in the cap's cut must be enforced at or below that join.
/// A violation cannot make the selected plan incorrect - every original predicate is still
/// enforced somewhere in the tree - it only means a canonical cap overstated how selective
/// the executed join is. Abort debug builds; log an error and keep the plan in release.
void JoinOrderOptimizer::verifySelectedPlanCapRequirements(const DPJoinEntryPtr & selected_plan) const
{
    const auto * canonical_properties = properties->canonicalProperties();
    if (!canonical_properties || !query_graph.data_property_catalog || !selected_plan)
        return;
    const auto & catalog = *query_graph.data_property_catalog;

    auto report_violation = [&](String message)
    {
        LOG_ERROR(log, "Canonical join-order cap postcondition violated: {}", message);
        chassert(false);
    };

    /// Union-find over catalog columns, built bottom-up from the bound equality predicates of
    /// the selected tree. Predicates never connect columns across a join's two subtrees, so
    /// components are scoped to subtrees and membership checks below stay sound.
    std::vector<UInt32> parent(catalog.columnCount());
    for (UInt32 column = 0; column < parent.size(); ++column)
        parent[column] = column;
    auto find = [&](UInt32 column)
    {
        while (parent[column] != column)
            column = parent[column] = parent[parent[column]];
        return column;
    };

    auto class_enforced_within = [&](size_t class_index, const DPJoinEntryPtr & subtree)
    {
        std::optional<UInt32> root;
        for (const auto member : canonical_properties->equalityClassMembers(class_index))
        {
            const auto relation = catalog.column(member).relation;
            if (!subtree->relations.test(relation))
                continue;
            const UInt32 member_root = find(member.value);
            if (root && *root != member_root)
                return false;
            root = member_root;
        }
        return true;
    };

    auto class_crosses_join = [&](size_t class_index, const DPJoinEntryPtr & entry)
    {
        bool touches_left = false;
        bool touches_right = false;
        for (const auto member : canonical_properties->equalityClassMembers(class_index))
        {
            const auto relation = catalog.column(member).relation;
            touches_left |= entry->left->relations.test(relation);
            touches_right |= entry->right->relations.test(relation);
        }
        return touches_left && touches_right;
    };

    std::function<void(const DPJoinEntryPtr &)> process = [&](const DPJoinEntryPtr & entry)
    {
        if (!entry || entry->isLeaf())
            return;
        process(entry->left);
        process(entry->right);

        if (entry->used_canonical_cap && entry->canonical_cap_obligations)
        {
            /// The obligation ledger is exact: the provider fails closed instead of minting a
            /// proof whose obligation class index would not fit into 64 bits.
            const size_t checked_classes = std::min<size_t>(canonical_properties->equalityClassCount(), 64);
            for (size_t class_index = 0; class_index < checked_classes; ++class_index)
            {
                if (!(entry->canonical_cap_obligations & (UInt64{1} << class_index)))
                    continue;
                for (const auto & child : {entry->left, entry->right})
                {
                    if (class_enforced_within(class_index, child))
                        continue;
                    report_violation(
                        fmt::format(
                            "equality class {} is not enforced below join {} (child {})", class_index, entry->dump(), child->dump()));
                }
            }
        }

        for (const auto & predicate : entry->join_operator.expression)
        {
            const auto [op, lhs, rhs] = predicate.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals)
                continue;
            const auto binding = bindJoinOrderPredicate(predicate, catalog);
            const auto * equality = std::get_if<JoinOrderOrdinaryEqualityBinding>(&binding);
            if (!equality)
                continue;
            parent[find(equality->lhs.value)] = find(equality->rhs.value);
        }

        if (!entry->used_canonical_cap)
            return;
        for (size_t class_index = 0; class_index < canonical_properties->equalityClassCount(); ++class_index)
        {
            if (!class_crosses_join(class_index, entry) || class_enforced_within(class_index, entry))
                continue;
            report_violation(fmt::format("equality class {} of the cut is not enforced at join {}", class_index, entry->dump()));
        }
    };
    process(selected_plan);
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solve()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinReorderMicroseconds);

    std::shared_ptr<DPJoinEntry> best_plan;

    for (size_t algorithm_index = 0; algorithm_index < enabled_algorithms.size(); ++algorithm_index)
    {
        const auto algorithm = enabled_algorithms[algorithm_index];
        LOG_TRACE(log, "Solving join order using {} algorithm", toString(algorithm));
        switch (algorithm)
        {
            case JoinOrderAlgorithm::DPSUB:
                best_plan = solveDPSubJoinOrder(query_graph, *properties);
                break;
            case JoinOrderAlgorithm::DPSIZE:
                best_plan = solveDPSizeJoinOrder(query_graph, *properties, max_searched_plans, query_status, interactive_cancel_callback);
                break;
            case JoinOrderAlgorithm::DPHYP:
                best_plan = solveDPHypJoinOrder(query_graph, *properties, max_searched_plans, query_status, interactive_cancel_callback);
                break;
            case JoinOrderAlgorithm::GREEDY:
                best_plan = solveGreedyJoinOrder(query_graph, *properties);
                if (!best_plan)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order with greedy algorithm");
                break;
        }

        if (best_plan)
            break;
        if (algorithm == JoinOrderAlgorithm::DPHYP && algorithm_index + 1 < enabled_algorithms.size())
            ProfileEvents::increment(ProfileEvents::JoinOrderDPhypFallbacks);
    }

    if (!best_plan)
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
            "Failed to find a valid join order, try adding 'greedy' algorithm as fallback to query_plan_optimize_join_order_algorithm setting.");

    LOG_TRACE(log, "Optimized join order in {:.2f} ms, best plan cost: {}, estimated cardinality: {}",
        static_cast<double>(watch.elapsed()) / 1000.0, best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown");

    return best_plan;
}

void JoinOrderOptimizer::finalizeSelectedPlanProperties(const DPJoinEntryPtr & selected_plan)
{
    if (!selected_plan)
        return;

    const auto * canonical_properties = properties->canonicalProperties();
    auto * debug_info = properties->debugInfo();
    const bool data_property_diagnostics_enabled = properties->data_property_diagnostics_enabled;

    if (data_property_diagnostics_enabled && canonical_properties)
    {
        if (const auto reason = canonical_properties->regionUnsupportedReason())
        {
            LOG_TRACE(log, "Canonical join-order data properties: unsupported={}", joinOrderPropertyUnsupportedReasonToString(*reason));
        }
        else if (const auto group = canonical_properties->getGroup(selected_plan->relations); group)
        {
            const auto group_dump = canonical_properties->dumpGroup(*group);
            const auto metrics_dump = canonical_properties->dumpMetrics();
            LOG_TRACE(log, "Canonical join-order data properties: {}; {}", group_dump, metrics_dump);
        }
    }

    if (debug_info && canonical_properties)
        debug_info->canonical_metrics = canonical_properties->getMetrics();

    if (data_property_diagnostics_enabled && debug_info)
        LOG_TRACE(
            log,
            "Canonical join-order cap assessments: proven={}, missing_input_rows={}, not_proven={}, unsupported={}",
            debug_info->cap_assessments.proven,
            debug_info->cap_assessments.missing_input_rows,
            debug_info->cap_assessments.not_proven,
            debug_info->cap_assessments.unsupported);
}

void JoinOrderOptimizer::finalizeSelectedPlan(const DPJoinEntryPtr & selected_plan)
{
    /// `join_kinds` records only non-`INNER ALL` restrictions, so an empty map means an
    /// all-inner region where any `Cross` entry in the selected tree is optimizer-created.
    const bool region_all_inner = query_graph.join_kinds.empty();

    /// Canonical cardinality caps may use every ordinary equality consequence crossing a
    /// candidate cut. Materialize the same consequences in the selected physical tree even
    /// when the independent selectivity setting is disabled; otherwise a hard cap could
    /// describe a stricter join than the one that is executed.
    if (properties->transitive_predicates_enabled || selectedPlanUsedCanonicalCardinalityCap(selected_plan))
        cleanupJoinPredicates(selected_plan, query_graph.column_equivalences, region_all_inner);
    verifySelectedPlanCapRequirements(selected_plan);
    finalizeSelectedPlanProperties(selected_plan);
}

DPJoinEntryPtr optimizeJoinOrder(
    QueryGraph query_graph, const QueryPlanOptimizationSettings & optimization_settings, JoinOrderOptimizationDebugInfo * debug_info)
{
    if (debug_info)
        *debug_info = {};

    if (query_graph.relation_stats.size() <= 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinOrderOptimizer: number of relations must be greater than 1");

    /// Equivalence classes feed transitive connectivity, canonical proof lookup, and the
    /// equality-cut materialization in `finalizeSelectedPlan`, which reuses the
    /// optimizer-owned copy inside the moved graph.
    if (optimization_settings.enable_join_transitive_predicates
        || optimization_settings.query_plan_optimize_join_order_use_proven_uniqueness)
        query_graph.buildColumnEquivalences();

    /// Carry the conflict-detector setting on the graph so DPsub (which only receives the
    /// `QueryGraph`) can decide whether to build its reordering constraints from CD-A/CD-C.
    query_graph.conflict_detector = optimization_settings.query_plan_optimize_join_order_conflict_detector;

    JoinOrderOptimizer reorderer(
        std::move(query_graph),
        optimization_settings.query_plan_optimize_join_order_algorithm,
        optimization_settings.query_plan_optimize_join_order_max_searched_plans,
        {.proven_uniqueness_enabled = optimization_settings.query_plan_optimize_join_order_use_proven_uniqueness,
         .dphyp_proven_edges_enabled = optimization_settings.query_plan_optimize_join_order_dphyp_proven_edges,
         .transitive_predicates_enabled = optimization_settings.enable_join_transitive_predicates,
         .diagnostics_enabled = optimization_settings.query_plan_optimize_join_order_data_property_diagnostics},
        debug_info);
    auto best_plan = reorderer.solve();
    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");

    reorderer.finalizeSelectedPlan(best_plan);
    return best_plan;
}

}
