#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/joinEnum.h>
#include <Common/CurrentThread.h>

#include <algorithm>
#include <bit>
#include <deque>
#include <expected>
#include <functional>
#include <limits>
#include <map>
#include <Common/typeid_cast.h>
#include <Core/Joins.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/safe_cast.h>
#include <base/defines.h>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Processors/QueryPlan/Optimizations/dpTable.h>
#include <Processors/QueryPlan/Optimizations/enumeratorChecker.h>


namespace ProfileEvents
{
    extern const Event JoinReorderMicroseconds;
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

static UInt32 checkedRelationBit32(size_t relation)
{
    if (relation >= std::numeric_limits<UInt32>::digits)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Relation {} does not fit in a DPsub UInt32 mask", relation);
    return UInt32{1} << relation;
}

/// Pack a `BitSet` (boost::dynamic_bitset) of relation ids into a native 32-bit mask.
/// Used by the DPsub hot path, where the relation count is guaranteed below 32 so every
/// set bit fits, allowing the per-CCP work to run on native integers instead of allocating
/// a `dynamic_bitset` for every plan it considers.
static UInt32 toMask(const BitSet & bits)
{
    UInt32 mask = 0;
    for (auto bit : bits)
    {
        mask |= checkedRelationBit32(bit);
    }
    return mask;
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
        std::optional<UInt64> cardinality_,
        JoinOperator join_operator_,
        JoinMethod join_method_)
    : relations(lhs->relations | rhs->relations)
    , left(std::move(lhs))
    , right(std::move(rhs))
    , cost(cost_)
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

class JoinOrderOptimizer
{
public:
    JoinOrderOptimizer(
        QueryGraph query_graph_,
        const std::vector<JoinOrderAlgorithm> & enabled_algorithms_,
        UInt64 max_searched_plans_,
        bool proven_uniqueness_enabled_,
        bool transitive_predicates_enabled_,
        bool data_property_diagnostics_enabled_,
        JoinOrderOptimizationDebugInfo * debug_info_)
        : query_graph(std::move(query_graph_))
        , max_searched_plans(max_searched_plans_)
        , enabled_algorithms(enabled_algorithms_)
        , proven_uniqueness_enabled(proven_uniqueness_enabled_)
        , transitive_predicates_enabled(transitive_predicates_enabled_)
        , data_property_diagnostics_enabled(data_property_diagnostics_enabled_)
        , debug_info(debug_info_)
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
        if ((proven_uniqueness_enabled || data_property_diagnostics_enabled) && query_graph.data_property_catalog)
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

    template <typename DPTable, std::unsigned_integral Tuint>
    std::shared_ptr<DPJoinEntry> buildPhysicalPlan(const DPTable & dptable, const Tuint & S) const;
    std::shared_ptr<DPJoinEntry> solveDPsub();
    std::shared_ptr<DPJoinEntry> solveDPsize();
    std::shared_ptr<DPJoinEntry> solveGreedy();
    std::shared_ptr<DPJoinEntry> solveDPhyp();

    template <class Tuint, class TDPTable>
    friend class EnumeratorCheckerWithCosts;

    std::optional<JoinKind> isValidJoinOrder(const BitSet & left_mask, const BitSet & right_mask) const;
    std::vector<JoinActionRef *> getApplicableExpressions(const BitSet & left, const BitSet & right);

    double computeSelectivity(const JoinActionRef & edge);
    double computeSelectivity(const std::vector<JoinActionRef *> & edges);
    double computeSelectivity(const std::vector<JoinActionRef *> & edges, const BitSet & left, const BitSet & right);
    JoinOrderCardinalityEstimate estimateCardinality(
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows,
        double selectivity,
        JoinKind join_kind,
        const JoinOrderCardinalityCap & canonical_cap) const;

    /// Assessment of a predicate-free transitively-connected pair for the DPsub acceptor.
    struct TransitivePairAssessment
    {
        bool admitted = false;
        JoinOrderCardinalityCap canonical_cap;
    };
    template <typename Subset>
    TransitivePairAssessment assessTransitivePair(
        const Subset & left_relations,
        const Subset & right_relations,
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows) const;

    struct JoinCandidateConnectivity
    {
        bool legacy_connected = false;
        bool has_cross_split_predicate = false;
    };

    struct JoinCandidateAssessment
    {
        bool legacy_connected = false;
        bool has_cross_split_predicate = false;
        bool independently_transitive_connected = false;
        bool proof_gated_transitive_connected = false;
        bool equivalence_selectivity_allowed = false;
        JoinOrderCardinalityCap canonical_cap;

        bool connected() const { return legacy_connected || independently_transitive_connected || proof_gated_transitive_connected; }
    };

    JoinCandidateAssessment assessCandidate(
        const BitSet & left_relations,
        const BitSet & right_relations,
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows,
        JoinKind join_kind,
        JoinCandidateConnectivity connectivity) const;
    bool costingPropertiesEnabled() const;
    void resetCandidateState();
    void recordCanonicalCapAssessment(const JoinOrderCardinalityCap & cap) const;
    template <typename Subset>
    JoinOrderCardinalityCap getCanonicalCap(
        const Subset & left_relations,
        const Subset & right_relations,
        std::optional<UInt64> left_rows,
        std::optional<UInt64> right_rows) const;
    size_t getColumnStats(const BitSet & rels, const String & column_name);

    /// Native-mask counterparts of the helpers above, used exclusively by the DPsub acceptor.
    /// They operate on `UInt32` relation masks and the data precomputed by `initDPsubScratch`,
    /// so the hot enumeration loop never constructs a `BitSet` (which heap-allocates). They are
    /// equivalent to the `BitSet` overloads but bypass per-CCP allocations entirely.
    void initDPsubScratch();
    std::optional<JoinKind> isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const;
    /// Returns the connecting (and, for two-relation joins, the early non-connecting) predicates,
    /// reusing an internal scratch buffer that is overwritten on every call.
    const std::vector<JoinActionRef *> & collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask);
    double computeSelectivityMask(const std::vector<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask);

    /// Periodically called from potentially long running optimization to check time limits and send progress
    void checkLimits();

    /// Polled inside the DPhyp enumeration loops. Returns false to stop enumeration when a partial plan
    /// cannot be handled or the search budget is exhausted, so `solveDPhyp` returns nullptr and the next
    /// algorithm in the chain runs. Throws via `checkLimits` on query timeout or cancellation.
    bool continueEnumeration();

    /// Try to build the best join plan between left_rels and right_rels.
    /// Updates dp_table if a better plan is found.
    void tryJoin(const BitSet & left_rels, const BitSet & right_rels);

    /// Core plan-building logic shared by DPsize and DPhyp.
    /// Computes selectivity and cost for the given predicates, and updates dp_table if this plan is better.
    /// Returns the new entry if dp_table was updated, nullptr otherwise.
    DPJoinEntryPtr evaluateJoin(
        const DPJoinEntryPtr & left,
        const DPJoinEntryPtr & right,
        JoinKind join_kind,
        std::vector<JoinActionRef *> & predicates,
        const JoinCandidateAssessment & assessment);

    /// DPhyp helpers
    void buildHyperedges();
    BitSet getNeighborhood(const BitSet & node_set) const;

    /// DPhyp enumeration functions from "Dynamic Programming Strikes Back"
    /// (Moerkotte & Neumann, SIGMOD 2008), Section 3.
    void emitCsg(const BitSet & csg);                       /// Generate complement seeds for a connected subgraph
    void enumerateCsgRec(const BitSet & csg, const BitSet & exclusion); /// Grow the primary connected subgraph
    void emitCsgCmp(const BitSet & left_csg, const BitSet & right_csg); /// Evaluate a csg-cmp pair
    void enumerateCmpRec(const BitSet & csg, const BitSet & complement, const BitSet & exclusion); /// Grow the complement

    constexpr static auto APPLY_DP_THRESHOLD = 10;

    QueryGraph query_graph;
    std::unordered_map<JoinActionRef, bool> applied;
    std::unordered_map<JoinActionRef, double> expression_selectivity;
    std::unordered_map<BitSet, DPJoinEntryPtr> dp_table;
    std::unique_ptr<JoinOrderCanonicalProperties> canonical_properties;

    /** Precomputed native-mask view of the query graph for the DPsub hot path. Populated once per
    * `solveDPsub` run by `initDPsubScratch`. Each `BitSet` is packed into a `UInt` type, the outer-join
    * restrictions are indexed by relation, and column-equivalence classes are indexed by relation so
    * `computeSelectivityMask` only visits the classes incident to the left side instead of rescanning
    * the whole equivalence map. The trailing fields are reusable scratch (no per-CCP allocation)
    */
    template <std::unsigned_integral TUInt>
    struct DPsubMaskData
    {
        using EquivClassPtr = EquivalenceClasses<JoinActionRef>::ConstClassPtr;

        std::vector<TUInt> edge_source_mask;   /// per edge: source relations
        std::vector<TUInt> edge_pin_mask;      /// per edge: relations that must all be present (if pinned)
        std::vector<char> edge_pinned;         /// per edge: whether a pin applies

        struct Restriction
        {
            TUInt required = 0;
            JoinKind kind = JoinKind::Inner;
            bool present = false;
        };
        std::vector<Restriction> restriction_by_rel; /// indexed by relation id

        std::vector<EquivClassPtr> equiv_classes;        /// distinct equivalence classes
        std::vector<std::vector<TUInt>> rel_to_classes; /// relation id -> indices into equiv_classes

        std::vector<UInt64> class_visited;        /// generation stamp per equivalence class
        UInt64 equiv_generation = 0;              /// bumped on each computeSelectivityMask call
        std::vector<JoinActionRef *> applicable_scratch; /// reused output of collectJoinEdgesMask
    };
    DPsubMaskData<UInt32> dpsub_data;

    /** A hyperedge in the join graph connecting a set of left relations to a set of right relations.
    * For simple binary predicates (A.x = B.y), |left| = |right| = 1.
    * For complex predicates (A.x = B.y + C.z), left and/or right may span multiple relations.
    */
    struct Hyperedge
    {
        BitSet left;
        BitSet right;
    };

    /// DPhyp hyperedge representation (built lazily by buildHyperedges)
    std::vector<Hyperedge> hyperedges;
    std::vector<std::vector<size_t>> node_to_edge_ids; /// node index -> hyperedge indices

    /// Set by `tryJoin` when it encounters a single-table or constant predicate inside the join edges
    /// that `dphyp` does not yet know how to attach. `solveDPhyp` returns `nullptr` so the fallback
    /// algorithm chain (e.g. `dphyp,greedy`) can produce a valid plan.
    bool dphyp_unsupported_predicate = false;

    /// Number of partial plans enumerated so far and the deterministic budget that bounds it.
    /// When the budget is exceeded the current solver gives up and returns `nullptr` so the next
    /// algorithm in the chain runs. Both are reset at the start of each solver.
    size_t searched_plans = 0;
    bool search_budget_exceeded = false;
    const UInt64 max_searched_plans;

    const std::vector<JoinOrderAlgorithm> enabled_algorithms;
    const bool proven_uniqueness_enabled;
    const bool transitive_predicates_enabled;
    const bool data_property_diagnostics_enabled;
    JoinOrderOptimizationDebugInfo * debug_info;
    LoggerPtr log = getLogger("JoinOrderOptimizer");

    QueryStatusPtr query_status;
    std::function<bool()> interactive_cancel_callback;
};

void JoinOrderOptimizer::checkLimits()
{
    if (query_status)
        query_status->checkTimeLimit();
    if (interactive_cancel_callback)
        interactive_cancel_callback();
}

bool JoinOrderOptimizer::continueEnumeration()
{
    if (dphyp_unsupported_predicate || search_budget_exceeded)
        return false;
    ++searched_plans;
    if (max_searched_plans && searched_plans > max_searched_plans)
    {
        search_budget_exceeded = true;
        LOG_TRACE(log, "Exceeded the limit of {} searched plans, falling back", max_searched_plans);
        return false;
    }
    /// `checkLimits` invokes the interactive cancel callback, which can send progress over the
    /// network and snapshot profile events. Poll it once every few thousand enumerated subsets
    /// instead of on every one, which would otherwise dominate the optimization time.
    if ((searched_plans & 0xFFF) == 0)
        checkLimits();
    return true;
}

size_t JoinOrderOptimizer::getColumnStats(const BitSet & rels, const String & column_name)
{
    const auto & relation_stats = query_graph.relation_stats;
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        /// Look up NDV from the dp_table entry's column_stats (propagated through joins).
        if (auto it = dp_table.find(rels); it != dp_table.end())
        {
            auto col_it = it->second->column_stats.find(column_name);
            if (col_it != it->second->column_stats.end())
                return col_it->second.num_distinct_values;
            return it->second->estimated_rows.value_or(0);
        }
        return 0;
    }

    const auto & relation_stat = relation_stats.at(rel_id.value());
    const auto & col_stats = relation_stat.column_stats;
    if (auto it = col_stats.find(column_name); it != col_stats.end())
        return it->second.num_distinct_values;
    return relation_stat.estimated_rows.value_or(0);
}

double JoinOrderOptimizer::computeSelectivity(const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge, 1.0);
    auto & selectivity = it->second;
    if (!inserted)
        return selectivity;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();

    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return 1.0;

    UInt64 lhs_ndv = getColumnStats(lhs.getSourceRelations(), lhs.getColumnName());
    UInt64 rhs_ndv = getColumnStats(rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv, rhs_ndv);
    if (max_ndv > 0)
        selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
    return selectivity;
}

double JoinOrderOptimizer::computeSelectivity(const std::vector<JoinActionRef *> & edges)
{
    double selectivity = 1.0;
    for (const auto & edge : edges)
        selectivity = std::min(selectivity, computeSelectivity(*edge));
    return selectivity;
}

/// Compute selectivity combining direct edges and transitive equivalence classes.
/// Direct edges and transitive equivalences may cover different columns between
/// the two relation sets, so both contribute to the overall selectivity.
double JoinOrderOptimizer::computeSelectivity(
    const std::vector<JoinActionRef *> & edges, const BitSet & left, const BitSet & right)
{
    double selectivity = computeSelectivity(edges);

    /// Also account for transitively-equivalent columns spanning both sides.
    using ConstClassPtr = EquivalenceClasses<JoinActionRef>::ConstClassPtr;
    std::unordered_set<ConstClassPtr> visited;

    for (const auto & [member, _] : query_graph.column_equivalences.getMemberToClassMap())
    {
        auto member_rel = member.getSourceRelations().getSingleBit();
        if (!member_rel || !left.test(*member_rel))
            continue;

        auto equiv_class = query_graph.column_equivalences.getClass(member);
        if (!equiv_class || !visited.insert(equiv_class).second)
            continue;

        /// Find the maximum NDV across all members of this class that belong
        /// to either side of the join. This is equivalent to evaluating all
        /// (left_member, right_member) pairs and taking the minimum selectivity,
        /// since min(1/max(l,r)) = 1/max(all l's and r's).
        size_t max_ndv = 0;
        bool has_left = false;
        bool has_right = false;
        for (const auto & equiv_member : *equiv_class)
        {
            auto relation = equiv_member.getSourceRelations().getSingleBit();
            if (!relation)
                continue;
            if (left.test(*relation))
            {
                has_left = true;
                max_ndv = std::max(max_ndv, getColumnStats(equiv_member.getSourceRelations(), equiv_member.getColumnName()));
            }
            else if (right.test(*relation))
            {
                has_right = true;
                max_ndv = std::max(max_ndv, getColumnStats(equiv_member.getSourceRelations(), equiv_member.getColumnName()));
            }
        }
        if (has_left && has_right && max_ndv > 0)
            selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
    }

    return selectivity;
}


/// Single source of truth for join cardinality estimation. For outer joins the result is
/// floored by the number of rows from the preserved side(s), since those are always emitted
/// (NULL-padded when there is no match): LEFT keeps all left rows, RIGHT all right rows, FULL both.
static std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind)
{
    if (!left_rows || !right_rows)
        return {};

    double lhs = static_cast<double>(*left_rows);
    double rhs = static_cast<double>(*right_rows);

    double joined_rows = std::max(selectivity * lhs * rhs, 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    if (join_kind == JoinKind::Full)
        joined_rows = std::max(joined_rows, lhs + rhs);

    /// Use >= to avoid undefined behavior when joined_rows is very close to max UInt64
    /// Due to floating point precision, a value slightly less than max when compared
    /// as double could still overflow when cast to UInt64
    if (joined_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    if (joined_rows < 1)
        return 1;
    return static_cast<UInt64>(joined_rows);
}

bool JoinOrderOptimizer::costingPropertiesEnabled() const
{
    return proven_uniqueness_enabled && canonical_properties;
}

void JoinOrderOptimizer::resetCandidateState()
{
    /// Reset both the DP table and the per-edge selectivity cache together, so an earlier
    /// algorithm in the fallback chain can neither leak partial `dp_table` entries nor
    /// cached `1.0` selectivity defaults into this run.
    dp_table.clear();
    expression_selectivity.clear();
}

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

/// `Disabled` is intentionally silent so feature-off performs no canonical diagnostic work.
/// Every other outcome is retained only when a query-local debug sink was requested.
void JoinOrderOptimizer::recordCanonicalCapAssessment(const JoinOrderCardinalityCap & cap) const
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

static BitSet toRelationBitSet(UInt32 subset)
{
    return BitSet::fromUInt(subset);
}

template <typename Subset>
JoinOrderCardinalityCap JoinOrderOptimizer::getCanonicalCap(
    const Subset & left_relations, const Subset & right_relations, std::optional<UInt64> left_rows, std::optional<UInt64> right_rows) const
{
    if (!costingPropertiesEnabled())
        return JoinOrderNoCardinalityCapReason::Disabled;
    return canonical_properties->inferInnerAllCardinalityCap(left_relations, right_relations, left_rows, right_rows);
}

JoinOrderCardinalityEstimate JoinOrderOptimizer::estimateCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind,
    const JoinOrderCardinalityCap & canonical_cap) const
{
    JoinOrderCardinalityEstimate result{estimateJoinCardinality(left_rows, right_rows, selectivity, join_kind), {}};
    const auto * cap = getProvenCap(canonical_cap);
    if (join_kind != JoinKind::Inner || !cap)
        return result;
    result.upper_bound = cap->upper_bound;
    if (result.rows)
        result.rows = std::min(*result.rows, cap->upper_bound);
    return result;
}

template <typename Subset>
JoinOrderOptimizer::TransitivePairAssessment JoinOrderOptimizer::assessTransitivePair(
    const Subset & left_relations, const Subset & right_relations, std::optional<UInt64> left_rows, std::optional<UInt64> right_rows) const
{
    if (!query_graph.areTransitivelyConnected(toRelationBitSet(left_relations), toRelationBitSet(right_relations)))
        return {};

    if (transitive_predicates_enabled)
        return {.admitted = true, .canonical_cap = {}};

    const auto cap = getCanonicalCap(left_relations, right_relations, left_rows, right_rows);
    recordCanonicalCapAssessment(cap);
    return {.admitted = getProvenCap(cap) != nullptr, .canonical_cap = cap};
}

JoinOrderOptimizer::JoinCandidateAssessment JoinOrderOptimizer::assessCandidate(
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

static double computeJoinCost(
    const std::shared_ptr<DPJoinEntry> & left,
    const std::shared_ptr<DPJoinEntry> & right,
    double selectivity,
    std::optional<UInt64> upper_bound)
{
    double local_cost
        = selectivity * static_cast<double>(left->estimated_rows.value_or(1)) * static_cast<double>(right->estimated_rows.value_or(1));
    if (upper_bound)
        local_cost = std::min(local_cost, static_cast<double>(*upper_bound));
    return left->cost + right->cost + local_cost;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solve()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::JoinReorderMicroseconds);

    std::shared_ptr<DPJoinEntry> best_plan;

    for (const auto & algorithm : enabled_algorithms)
    {
        LOG_TRACE(log, "Solving join order using {} algorithm", toString(algorithm));
        switch (algorithm)
        {
            case JoinOrderAlgorithm::DPSUB:
                best_plan = solveDPsub();
                break;
            case JoinOrderAlgorithm::DPSIZE:
                best_plan = solveDPsize();
                break;
            case JoinOrderAlgorithm::DPHYP:
                best_plan = solveDPhyp();
                break;
            case JoinOrderAlgorithm::GREEDY:
                best_plan = solveGreedy();
                if (!best_plan)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order with greedy algorithm");
                break;
        }

        if (best_plan)
            break;
    }

    if (!best_plan)
    {
        dp_table.clear();
        throw Exception(ErrorCodes::EXPERIMENTAL_FEATURE_ERROR,
            "Failed to find a valid join order, try adding 'greedy' algorithm as fallback to query_plan_optimize_join_order_algorithm setting.");
    }

    LOG_TRACE(log, "Optimized join order in {:.2f} ms, best plan cost: {}, estimated cardinality: {}",
        static_cast<double>(watch.elapsed()) / 1000.0, best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown");

    return best_plan;
}

std::vector<JoinActionRef *> JoinOrderOptimizer::getApplicableExpressions(const BitSet & left, const BitSet & right)
{
    std::vector<JoinActionRef *> applicable;

    BitSet joined_rels = left | right;
    for (auto & edge : query_graph.edges)
    {
        if (!edge)
            continue;
        const auto & edge_sources = edge.getSourceRelations();
        if (!isSubsetOf(edge_sources, joined_rels))
            continue;

        auto pin_it = query_graph.outer_join_conditions.find(edge);
        if (pin_it != query_graph.outer_join_conditions.end())
        {
            /// ON-clause predicates of an outer join can be applied only when the
            /// null-supplying relation is joined. That relation appears as a singleton
            /// on one side of the join step (enforced by isValidJoinOrder), so the
            /// predicate becomes applicable exactly at that step.
            if (!joined_rels.test(pin_it->second))
                continue;
        }

        applicable.push_back(&edge);
    }
    return applicable;
}

void JoinOrderOptimizer::initDPsubScratch()
{
    using EquivClass = EquivalenceClasses<JoinActionRef>::Class;

    const size_t num_relations = query_graph.relation_stats.size();
    const auto & edges = query_graph.edges;

    /// Edges: precompute the source-relation mask and any pin mask once per edge
    auto n = edges.size();
    dpsub_data.edge_source_mask.assign(n, 0);
    dpsub_data.edge_pin_mask.assign(n, 0);
    dpsub_data.edge_pinned.assign(n, 0);
    for (size_t i = 0; i < n; ++i)
    {
        const auto & edge = edges[i];
        if (!edge)
            continue;
        dpsub_data.edge_source_mask[i] = toMask(edge.getSourceRelations());
        /// ON-clause predicates of an outer join are pinned to the single null-supplying
        /// relation; the pin becomes applicable exactly when that relation is joined.
        if (auto pin_it = query_graph.outer_join_conditions.find(edge); pin_it != query_graph.outer_join_conditions.end())
        {
            dpsub_data.edge_pinned[i] = 1;
            dpsub_data.edge_pin_mask[i] = checkedRelationBit32(pin_it->second);
        }
    }

    /// Outer-join restrictions: index by the null-supplying relation
    dpsub_data.restriction_by_rel.assign(num_relations, {});
    for (const auto & [rel, restriction] : query_graph.join_kinds)
    {
        if (rel >= num_relations)
            continue;
        auto & native = dpsub_data.restriction_by_rel[rel];
        native.required = toMask(restriction.first);
        native.kind = restriction.second;
        native.present = true;
    }

    /// Column-equivalence classes: build a per-relation incidence list so selectivity only
    /// visits the classes touching the left side instead of rescanning the whole member map
    dpsub_data.equiv_classes.clear();
    dpsub_data.rel_to_classes.assign(num_relations, {});
    std::unordered_map<const EquivClass *, UInt32> class_index;
    for (const auto & [member, class_ptr] : query_graph.column_equivalences.getMemberToClassMap())
    {
        if (!class_ptr)
            continue;
        auto rel = member.getSourceRelations().getSingleBit();
        if (!rel || *rel >= num_relations)
            continue;

        auto [it, inserted] = class_index.try_emplace(class_ptr.get(), static_cast<UInt32>(dpsub_data.equiv_classes.size()));
        if (inserted)
            dpsub_data.equiv_classes.push_back(class_ptr);
        dpsub_data.rel_to_classes[*rel].push_back(it->second);
    }
    dpsub_data.class_visited.assign(dpsub_data.equiv_classes.size(), 0);
    dpsub_data.equiv_generation = 0;
}

std::optional<JoinKind> JoinOrderOptimizer::isValidJoinOrderMask(UInt32 left_mask, UInt32 right_mask) const
{
    auto check = [&](UInt32 lhs, UInt32 rhs) -> std::optional<JoinKind>
    {
        if (std::popcount(lhs) == 1)
        {
            const auto & restriction = dpsub_data.restriction_by_rel[std::countr_zero(lhs)];
            if (restriction.present)
            {
                /// If there are any bits set in `restriction.required`
                /// that are not set in `rhs`, the bitwise AND (&) results in a non-zero value
                if (restriction.required & ~rhs)
                    return {};
                return restriction.kind;
            }
        }
        return JoinKind::Inner;
    };

    JoinKind left_join_type = JoinKind::Inner;
    JoinKind right_join_type = JoinKind::Inner;

    if (auto res = check(left_mask, right_mask))
        left_join_type = isLeftOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    else
        return {};

    if (auto res = check(right_mask, left_mask))
        right_join_type = isRightOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    else
        return {};

    if (left_join_type == JoinKind::Inner)
        return right_join_type;
    if (right_join_type == JoinKind::Inner)
        return left_join_type;
    if (left_join_type == JoinKind::Full && right_join_type == JoinKind::Full)
        return JoinKind::Full;

    /// Conflicting outer-join constraints, the order is not possible
    return {};
}

const std::vector<JoinActionRef *> & JoinOrderOptimizer::collectJoinEdgesMask(UInt32 left_mask, UInt32 right_mask)
{
    auto & out = dpsub_data.applicable_scratch;
    out.clear();

    const UInt32 joined = left_mask | right_mask;
    const bool two_relations = std::popcount(joined) == 2;
    auto & edges = query_graph.edges;

    for (size_t i = 0; i < edges.size(); ++i)
    {
        auto & edge = edges[i];
        if (!edge)
            continue;

        const UInt32 sources = dpsub_data.edge_source_mask[i];
        if (sources & ~joined) /// edge sources not proper subset of joined relations
            continue;
        if (dpsub_data.edge_pinned[i] && (dpsub_data.edge_pin_mask[i] & ~joined))
            continue;

        /// Works much like Extended Eligibility List (EEL) in case of outerjoins:
        /// encoding relations that must be present for the predicate to be applicable (in `pin` mask)
        /// For innerjoins its just the sources of the predicate, i.e., NEL, here pin is empty.
        /// For a single-table conjunct of an outer join's ON
        /// clause (e.g. `t2.value = 'x'` in `... LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'x'`),
        /// `sources` is only `{t2}` but the pin is `{t3}`: the predicate belongs to the ON condition of
        /// the join that brings in `t3`, not to `t2` as a base-table filter. Placing it by `sources`
        /// alone would push it below (or drop it from) that join, silently changing outer-join
        /// semantics, since `t2` may already have been NULL-extended (or default-filled) by an
        /// earlier join.
        const UInt32 pin = dpsub_data.edge_pinned[i] ? dpsub_data.edge_pin_mask[i] : 0;
        const UInt32 applicable = sources | pin;

        if (std::popcount(applicable) <= 1)
        {
            /// Base-relation filter or constant predicate (the edge references at most one relation).
            const bool relation_introduced = applicable != 0 && (left_mask == applicable || right_mask == applicable);
            const bool constant_at_earliest_join = applicable == 0 && two_relations;
            if (relation_introduced || constant_at_earliest_join)
                out.push_back(&edge);
        }
        else if ((applicable & ~left_mask) && (applicable & ~right_mask))
        {
            /// The predicate spans the split (a connecting equi-predicate, or a single-table ON-clause
            /// conjunct pinned to the opposite side): neither side alone contains all the relations it
            /// needs. This join is the lowest one that makes it applicable, so attach it here: into the
            /// correct join's ON condition.
            out.push_back(&edge);
        }
    }
    return out;
}

/// Checks if predicate has sources from both left and right sets.
static bool connects(const JoinActionRef * predicate, const BitSet & left, const BitSet & right)
{
    const auto & participating = predicate->getSourceRelations();
    return areIntersecting(participating, left) && areIntersecting(participating, right);
}

double JoinOrderOptimizer::computeSelectivityMask(
    const std::vector<JoinActionRef *> & edges, UInt32 left_mask, UInt32 right_mask)
{
    double selectivity = computeSelectivity(edges);

    /// Account for transitively-equivalent columns spanning both sides, visiting only the classes
    /// incident to the left relations. A generation stamp deduplicates classes without allocating
    const UInt64 generation = ++dpsub_data.equiv_generation;

    for (UInt32 remaining = left_mask; remaining;)
    {
        const UInt32 rel = std::countr_zero(remaining);
        remaining &= remaining - 1;

        for (UInt32 class_idx : dpsub_data.rel_to_classes[rel])
        {
            if (dpsub_data.class_visited[class_idx] == generation)
                continue;
            dpsub_data.class_visited[class_idx] = generation;

            size_t max_ndv = 0;
            bool has_left = false;
            bool has_right = false;
            for (const auto & equiv_member : *dpsub_data.equiv_classes[class_idx])
            {
                auto relation = equiv_member.getSourceRelations().getSingleBit();
                if (!relation)
                    continue;
                const UInt32 relation_bit = checkedRelationBit32(*relation);
                if (left_mask & relation_bit)
                {
                    has_left = true;
                    max_ndv = std::max(max_ndv, getColumnStats(equiv_member.getSourceRelations(), equiv_member.getColumnName()));
                }
                else if (right_mask & relation_bit)
                {
                    has_right = true;
                    max_ndv = std::max(max_ndv, getColumnStats(equiv_member.getSourceRelations(), equiv_member.getColumnName()));
                }
            }
            if (has_left && has_right && max_ndv > 0)
                selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
        }
    }

    return selectivity;
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveGreedy()
{
    /// Discard any partial state left by an earlier algorithm in the fallback chain
    /// (e.g. `dphyp,greedy`) so cost-model lookups via `getColumnStats` only see
    /// entries built by this run.
    resetCandidateState();

    std::deque<std::shared_ptr<DPJoinEntry>> components;
    for (size_t i = 0; i < query_graph.relation_stats.size(); ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        components.push_back(std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats));
    }

    std::vector<JoinActionRef *> applied_edges;
    /// Iteratively join components until we have a single plan
    while (components.size() > 1)
    {
        std::shared_ptr<DPJoinEntry> best_plan = nullptr;
        size_t best_i = 0;
        size_t best_j = 0;

        /// Try all pairs of components
        for (size_t i = 0; i < components.size(); i++)
        {
            for (size_t j = i + 1; j < components.size(); j++)
            {
                auto left = components[i];
                auto right = components[j];

                auto join_kind = isValidJoinOrder(left->relations, right->relations);
                if (!join_kind)
                    continue;

                auto edges = getApplicableExpressions(left->relations, right->relations);
                const bool legacy_connected = !edges.empty();
                const bool has_cross_split_predicate
                    = std::ranges::any_of(edges, [&](const auto * edge) { return connects(edge, left->relations, right->relations); });
                const auto assessment = assessCandidate(
                    left->relations,
                    right->relations,
                    left->estimated_rows,
                    right->estimated_rows,
                    *join_kind,
                    {.legacy_connected = legacy_connected, .has_cross_split_predicate = has_cross_split_predicate});
                const bool connected = assessment.connected();
                if (!connected && best_plan)
                    continue;

                auto selectivity = assessment.equivalence_selectivity_allowed ? computeSelectivity(edges, left->relations, right->relations)
                                                                              : computeSelectivity(edges);
                const auto effective_kind = (*join_kind == JoinKind::Inner && !connected) ? JoinKind::Cross : *join_kind;
                auto estimate = estimateCardinality(
                    left->estimated_rows, right->estimated_rows, selectivity, effective_kind, assessment.canonical_cap);
                auto current_cost = computeJoinCost(left, right, selectivity, estimate.upper_bound);
                if (!best_plan || current_cost < best_plan->cost)
                {
                    JoinOperator join_operator(effective_kind, JoinStrictness::All, JoinLocality::Unspecified);
                    bool is_inner_step = isInner(join_kind.value()) || isCrossOrComma(join_kind.value());
                    for (const auto * e : edges)
                    {
                        /// A filter predicate applied at an outer join step must not go to the
                        /// ON clause, where it would affect matching instead of filtering and
                        /// let non-matching rows of the preserved side survive NULL-extended.
                        /// Apply it after the join instead.
                        if (is_inner_step || query_graph.outer_join_conditions.contains(*e))
                            join_operator.expression.push_back(*e);
                        else
                            join_operator.residual_filter.push_back(*e);
                    }
                    applied_edges.swap(edges);
                    best_plan = std::make_shared<DPJoinEntry>(left, right, current_cost, estimate.rows, std::move(join_operator));
                    best_plan->used_canonical_cap = estimate.upper_bound.has_value();
                    const auto * proven_cap = getProvenCap(assessment.canonical_cap);
                    best_plan->canonical_cap_obligations = proven_cap ? proven_cap->obligation_classes : 0;
                    best_i = i;
                    best_j = j;
                }
            }
        }

        /// The loop above accepts any pair passing isValidJoinOrder, even an unconnected
        /// one (which becomes a cross product), as long as no best plan exists yet. So
        /// reaching this point means no pair of components can be joined at all: the
        /// outer join restrictions are stuck. This cannot happen for a query graph built
        /// from a well-formed join tree: required partner sets follow the original tree's
        /// scoping, so the original join order always remains valid.
        if (!best_plan)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "No valid join pair found among components [{}], the outer join restrictions cannot be satisfied",
                fmt::join(components | std::views::transform([](const auto & c) { return c->dump(); }), ", "));

        LOG_TEST(log, "Best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, join operator: {}",
            best_plan->dump(), best_plan->left->dump(), best_plan->right->dump(),
            best_plan->cost, best_plan->estimated_rows ? toString(*best_plan->estimated_rows) : "unknown",
            best_plan->join_operator.dump());

        /// replace the two components with the best plan
        components.erase(components.begin() + std::max(best_i, best_j));
        components.erase(components.begin() + std::min(best_i, best_j));
        components.push_front(best_plan);
        dp_table[best_plan->relations] = best_plan;

        for (auto * edge : applied_edges)
            *edge = nullptr;
    }

    for (auto * edge : applied_edges)
        *edge = nullptr;

    auto non_applied_edges = std::views::filter(query_graph.edges, [](auto & edge) { return bool(edge); });
    if (!non_applied_edges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Some expressions was not applied: [{}]",
            fmt::join(non_applied_edges | std::views::take(5) | std::views::transform(&JoinActionRef::dump), ", "));

    return components.at(0);
}


template <typename DPTable, std::unsigned_integral TUInt>
std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::buildPhysicalPlan(const DPTable & dptable, const TUInt & S) const
{
    auto& entry = dptable[S];
    if (!entry.left && !entry.right)
        return std::make_shared<DPJoinEntry>(std::countr_zero(S), entry.estimated_rows, entry.column_stats);

    JoinOperator join_operator(entry.kind, JoinStrictness::All, JoinLocality::Unspecified);
    /// A filter predicate applied at an outer join step must not go to the ON clause, where it
    /// would affect matching instead of filtering and let non-matching rows of the preserved side
    /// survive NULL-extended. Apply it after the join instead (see `solveGreedy`).
    bool is_inner_step = isInner(entry.kind) || isCrossOrComma(entry.kind);
    for (const auto * e : entry.edges)
    {
        if (is_inner_step || query_graph.outer_join_conditions.contains(*e))
            join_operator.expression.push_back(*e);
        else
            join_operator.residual_filter.push_back(*e);
    }

    auto left = buildPhysicalPlan(dptable, entry.left);
    auto right = buildPhysicalPlan(dptable, entry.right);
    auto result = std::make_shared<DPJoinEntry>(left, right, entry.cost, entry.estimated_rows, std::move(join_operator));
    result->used_canonical_cap = entry.used_canonical_cap;
    result->canonical_cap_obligations = entry.canonical_cap_obligations;
    return result;
}

/** Implements the `Dpsub` bottom-up dynamic programming algorithm for optimal bushy join tree generation.
* This algorithm constructs optimal join trees by iterating over subsets of the relations in an ascending order
* based on an integer bitmask (from 1 to 2^n - 2). This ordering ensures that for any relation subset S,
* the best plans for all its proper sub-plans (subsets S1 ⊂ S) have already been computed, thereby adhering to
* Bellman's optimality principle.
* This methodical evaluation of all connected subsets results in the creation of the best plan for each.
* The final answer is the optimal plan for the complete set of relations.
* For more detailed information, see "Building Query Compilers":
* (https://pi3.informatik.uni-mannheim.de/~moer/querycompiler.pdf)
*/
std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDPsub()
{
    resetCandidateState();

    const size_t n = query_graph.relation_stats.size();
    using Bitvector = UInt32; // choose UInt64 or even UInt128 for larger sets
    // A budget cap on nr. of connected components considered by DPsub to avoid excessive optimization time on large join graphs.
    // This budget cap is obtained from empirical testing using different queries and join graphs.
    static constexpr UInt32 max_nr_ccps = 50'000;

    if (n >= std::numeric_limits<Bitvector>::digits)
    {
        LOG_TRACE(log,
            "Number of relations {} exceeds the DP threshold {}, skipping DP optimization invoking greedy algorithm",
            n, std::numeric_limits<Bitvector>::digits);
        return nullptr;
    }

    struct DPEntry
    {
        Bitvector neighbor{0};
        Bitvector left{0};
        Bitvector right{0};
        std::optional<UInt64> estimated_rows = {};
        std::unordered_map<String, ColumnStats> column_stats = {};
        double cost{.0};
        double sel{.0};
        JoinKind kind{JoinKind::Inner};
        bool used_canonical_cap{false};
        UInt64 canonical_cap_obligations{0};
        std::vector<JoinActionRef*> edges; // needed for physical plan generation
    };
    using DPTable = DPTable<DPEntry, Bitvector>;
    using Checker = EnumeratorCheckerWithCosts<DPTable, JoinOrderOptimizer>;
    using Enumerator = EnumCcpSub<Checker, DPTable, QueryGraph>;

    /// Precompute the native-mask view of the query graph so the acceptor's per-CCP work
    /// (validity, edge collection, selectivity) runs without allocating a `BitSet` each time
    /// That is, we don't have to convert Bitvector -> BitSet -> Bitvector for every subset S
    /// and its subcomponents S1, S2
    initDPsubScratch();

    Checker checker(n, *this);
    Enumerator enumerator(n, max_nr_ccps, log);
    enumerator.enumerate(checker, query_graph);

    const Bitvector full_set = (static_cast<Bitvector>(1) << n) - 1;
    const auto & dptable = checker.getDPTable();

    /// a. If the join graph is too complex we break early
    /// b. The full set is assembled only if the join graph is connected. When it is not e.g.
    /// cross products, or predicates that reference a single relation or a constant and thus
    /// create no binary edge (`... LEFT JOIN t ON t.x = 5`): DPsub cannot stitch the
    /// disconnected components, so the full-set entry is missing (or was never given a join)
    const bool full_built = dptable.isConnected(full_set)
                            && (dptable[full_set].left != 0 || dptable[full_set].right != 0);
    if (!full_built)
    {
        LOG_TRACE(log, "DPsub: join graph is either too complex or disconnected!");
        return nullptr;
    }

    return buildPhysicalPlan(dptable, full_set);
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDPsize()
{
    const size_t total_relations_count = query_graph.relation_stats.size();

    /// Components by size (index 0 is not used that why the size is N+1)
    std::vector<std::unordered_map<BitSet, DPJoinEntryPtr>> components(total_relations_count + 1);

    /// Populate DP table for components of size=1.
    resetCandidateState();
    searched_plans = 0;
    for (size_t i = 0; i < total_relations_count; ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        auto entry = std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats);
        components[1][entry->relations] = entry;
        dp_table[entry->relations] = entry;
    }

    /// Iteratively build components of size from 2 to N
    for (size_t component_size = 2; component_size <= total_relations_count; ++component_size)
    {
        for (size_t smaller_component_size = 1; smaller_component_size <= component_size / 2; ++smaller_component_size)
        {
            const size_t bigger_component_size = component_size - smaller_component_size;

            for (const auto & [_, right] : components[smaller_component_size])
            {
                for (const auto & [_, left] : components[bigger_component_size])
                {
                    /// Do components overlap?
                    if (left->relations & right->relations)
                        continue;

                    /// If both components are of the same size then check each pair just once, not twice
                    if (smaller_component_size == bigger_component_size && *left->relations.begin() > *right->relations.begin())
                        continue;

                    ++searched_plans;
                    if (max_searched_plans && searched_plans > max_searched_plans)
                    {
                        LOG_TRACE(log, "Exceeded the limit of {} searched plans, falling back", max_searched_plans);
                        return nullptr;
                    }
                    /// `checkLimits` invokes the interactive cancel callback, which can send progress over
                    /// the network. Poll it once every few thousand pairs instead of on every one.
                    if ((searched_plans & 0xFFF) == 0)
                        checkLimits();

                    auto join_kind = isValidJoinOrder(left->relations, right->relations);
                    if (!join_kind)
                        continue;

                    /// FIXME: Restrict to Inner joins for now because isValidJoinOrder seems to not handle non-Inner joins with swapped inputs correctly
                    if (*join_kind != JoinKind::Inner)
                        continue;

                    auto applicable_edge = getApplicableExpressions(left->relations, right->relations);
                    /// Only leave the edges that connect left and right.
                    /// DPsize also includes non-connecting predicates (single-table filters) at the earliest
                    /// stage (component_size == 2), unlike DPhyp which handles them separately via the hyperedge graph.
                    std::vector<JoinActionRef *> edge;
                    bool has_cross_split_predicate = false;
                    for (auto & edge_it : applicable_edge)
                    {
                        if (connects(edge_it, left->relations, right->relations))
                        {
                            has_cross_split_predicate = true;
                            LOG_TEST(log, "Adding predicate connecting {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            edge.push_back(edge_it);
                        }
                        else if ((edge_it->fromLeft() || edge_it->fromRight() || edge_it->fromNone()) && component_size == 2)
                        {
                            LOG_TEST(log, "Adding early non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                            edge.push_back(edge_it);
                        }
                        else
                        {
                            LOG_TEST(log, "Skipping non-connecting predicate for {} and {} : {}", left->dump(), right->dump(), edge_it->dump());
                        }
                    }

                    const auto assessment = assessCandidate(
                        left->relations,
                        right->relations,
                        left->estimated_rows,
                        right->estimated_rows,
                        *join_kind,
                        {.legacy_connected = !edge.empty(), .has_cross_split_predicate = has_cross_split_predicate});

                    LOG_TEST(
                        log,
                        "Considering join between {} and {}, predicates count: {}, legacy connected: {}, cross-split predicate: {}, "
                        "independently transitive: {}, proof-gated transitive: {}",
                        left->dump(),
                        right->dump(),
                        edge.size(),
                        assessment.legacy_connected,
                        assessment.has_cross_split_predicate,
                        assessment.independently_transitive_connected,
                        assessment.proof_gated_transitive_connected);

                    if (!assessment.connected())
                        continue;

                    auto new_entry = evaluateJoin(left, right, *join_kind, edge, assessment);
                    if (new_entry)
                        components[component_size][new_entry->relations] = new_entry;
                }
            }
        }
    }

    auto best_full_plan = dp_table.find(BitSet::allSet(total_relations_count));
    if (best_full_plan != dp_table.end())
        return best_full_plan->second;

    LOG_TRACE(log, "Failed to find best plan using DPsize algorithm");
    return nullptr;
}

void JoinOrderOptimizer::tryJoin(const BitSet & left_rels, const BitSet & right_rels)
{
    auto left_entry = dp_table.find(left_rels);
    if (left_entry == dp_table.end())
        return;

    auto right_entry = dp_table.find(right_rels);
    if (right_entry == dp_table.end())
        return;

    auto join_kind = isValidJoinOrder(left_rels, right_rels);
    if (!join_kind)
        return;

    /// Restrict to inner joins for now (same as DPsize FIXME)
    if (*join_kind != JoinKind::Inner)
        return;

    auto applicable_predicates = getApplicableExpressions(left_rels, right_rels);
    std::vector<JoinActionRef *> connecting_predicates;
    for (auto * predicate : applicable_predicates)
    {
        if (connects(predicate, left_rels, right_rels))
        {
            connecting_predicates.push_back(predicate);
            continue;
        }

        /// Predicates spanning 2+ relations were already applied in a sub-join.
        /// Single-table or constant predicates (e.g. moved into `ON` by
        /// `query_plan_merge_filter_into_join_condition`) are not handled by `dphyp` here;
        /// `dpsize` attaches them at the smallest containing join, but `dphyp` would need
        /// extra bookkeeping to avoid double-application. For now, mark the query as
        /// unsupported and let `solveDPhyp` return `nullptr` so the fallback chain runs.
        if (predicate->getSourceRelations().count() < 2)
        {
            LOG_TRACE(log, "DPhyp cannot attach non-connecting predicate {} (sources: {{ {} }}), falling back",
                predicate->dump(), fmt::join(predicate->getSourceRelations(), ","));
            dphyp_unsupported_predicate = true;
            return;
        }
    }

    /// A predicate-free pair can be emitted only through a synthetic hyperedge, whose
    /// installation is controlled by the independent transitive setting (see `buildHyperedges`);
    /// `cleanupJoinPredicates` synthesizes the missing predicate for a selected transitive join
    /// after optimization. The shared assessment keeps admission, selectivity, and canonical-cap
    /// consumption consistent with the other enumerators, so a proofless candidate is costed
    /// exactly like feature-off.
    /// Every connecting DPhyp predicate crosses the split by construction, so `legacy_connected`
    /// and `has_cross_split_predicate` coincide here.
    const bool legacy_connected = !connecting_predicates.empty();
    const auto assessment = assessCandidate(
        left_rels,
        right_rels,
        left_entry->second->estimated_rows,
        right_entry->second->estimated_rows,
        *join_kind,
        {.legacy_connected = legacy_connected, .has_cross_split_predicate = legacy_connected});
    if (!assessment.connected())
        return;

    evaluateJoin(left_entry->second, right_entry->second, *join_kind, connecting_predicates, assessment);
}

DPJoinEntryPtr JoinOrderOptimizer::evaluateJoin(
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    JoinKind join_kind,
    std::vector<JoinActionRef *> & predicates,
    const JoinCandidateAssessment & assessment)
{
    auto selectivity = assessment.equivalence_selectivity_allowed ? computeSelectivity(predicates, left->relations, right->relations)
                                                                  : computeSelectivity(predicates);

    /// Transitively connected pairs are inner joins; their predicate is synthesized later.
    auto effective_kind = (assessment.connected() && join_kind == JoinKind::Cross) ? JoinKind::Inner : join_kind;
    auto estimate = estimateCardinality(left->estimated_rows, right->estimated_rows, selectivity, effective_kind, assessment.canonical_cap);
    auto new_cost = computeJoinCost(left, right, selectivity, estimate.upper_bound);

    const BitSet combined_rels = left->relations | right->relations;
    auto current_best = dp_table.find(combined_rels);
    if (current_best != dp_table.end() && new_cost >= current_best->second->cost)
        return nullptr;

    JoinOperator join_operator(
        effective_kind, JoinStrictness::All, JoinLocality::Unspecified,
        std::ranges::to<std::vector>(predicates | std::views::transform([](const auto * p) { return *p; })));
    auto new_entry = std::make_shared<DPJoinEntry>(left, right, new_cost, estimate.rows, std::move(join_operator));
    new_entry->used_canonical_cap = estimate.upper_bound.has_value();
    const auto * proven_cap = getProvenCap(assessment.canonical_cap);
    new_entry->canonical_cap_obligations = proven_cap ? proven_cap->obligation_classes : 0;

    LOG_TEST(log, "New best plan for '{}' as '{} JOIN {}', cost: {}, cardinality: {}, operator: {}",
        new_entry->dump(), left->dump(), right->dump(),
        new_entry->cost, new_entry->estimated_rows ? toString(*new_entry->estimated_rows) : "unknown",
        new_entry->join_operator.dump());

    dp_table[combined_rels] = new_entry;
    return new_entry;
}

/// Build the hyperedge representation of the join graph used by DPhyp.
/// Each join predicate becomes a hyperedge (left_rels, right_rels).
/// When the independent transitive-predicate setting is enabled, column equivalence classes
/// add synthetic edges for transitively-connected pairs.
/// The adjacency index `node_to_edge_ids` maps each relation to the hyperedges that touch it.
void JoinOrderOptimizer::buildHyperedges()
{
    const size_t num_relations = query_graph.relation_stats.size();
    node_to_edge_ids.assign(num_relations, {});
    hyperedges.clear();

    auto add_hyperedge = [&](const BitSet & left_rels, const BitSet & right_rels)
    {
        size_t hyperedge_id = hyperedges.size();
        hyperedges.push_back({left_rels, right_rels});

        for (auto rel : left_rels)
            if (rel < num_relations)
                node_to_edge_ids[rel].push_back(hyperedge_id);
        for (auto rel : right_rels)
            if (rel < num_relations && !left_rels.test(rel))
                node_to_edge_ids[rel].push_back(hyperedge_id);
    };

    /// Phase 1: create hyperedges from explicit join predicates.
    /// Duplicate edges for the same relation pair (e.g. A.x=B.x AND A.y=B.y) are harmless:
    /// `getNeighborhood` ORs results into a BitSet, and `tryJoin` collects predicates
    /// from `query_graph.edges`, not from hyperedges.
    for (const auto & predicate : query_graph.edges)
    {
        if (!predicate)
            continue;

        BitSet left_rels;
        BitSet right_rels;

        auto [op, lhs, rhs] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Unknown && lhs && rhs)
        {
            left_rels  = lhs.getSourceRelations();
            right_rels = rhs.getSourceRelations();
        }
        else
        {
            /// Non-binary predicate: treat the full source set as both endpoints.
            left_rels  = predicate.getSourceRelations();
            right_rels = predicate.getSourceRelations();
        }

        if (!left_rels.any() || !right_rels.any())
            continue;

        add_hyperedge(left_rels, right_rels);
    }

    /// Phase 2 adds synthetic hyperedges for transitively-connected relation pairs and is
    /// authorized only by the independent transitive-predicate setting. Column equivalence
    /// classes may be materialized solely for canonical proof lookup
    /// (`query_plan_optimize_join_order_use_proven_uniqueness`); their existence alone must not
    /// enlarge the search topology, its budget consumption, or the fallback behavior.
    /// Unique-key proofs cannot authorize static topology either: they are specific to an
    /// exact subset pair and its row estimates, neither of which exists before enumeration,
    /// and a proven composite-key cut does not imply any proven singleton cut that a
    /// relation-pair edge could represent. Canonical cardinality caps therefore apply only
    /// to candidates reachable through explicit predicates when the transitive setting is
    /// off.
    if (!transitive_predicates_enabled)
        return;

    /// Column equivalence classes (e.g. A.key=B.key AND B.key=C.key implies A.key=C.key)
    /// connect relations that have no direct predicate. Without these edges DPhyp's
    /// neighborhood traversal would never discover the pair.

    /// Build a connectivity matrix from explicit edges to avoid duplicating them.
    std::vector<BitSet> connected_rels(num_relations);
    for (const auto & hyperedge : hyperedges)
    {
        auto left_rel = hyperedge.left.getSingleBit();
        auto right_rel = hyperedge.right.getSingleBit();
        if (left_rel && right_rel)
        {
            connected_rels[*left_rel].set(*right_rel);
            connected_rels[*right_rel].set(*left_rel);
        }
    }

    using ConstClassPtr = EquivalenceClasses<JoinActionRef>::ConstClassPtr;
    std::unordered_set<ConstClassPtr> processed_classes;

    for (const auto & [member, equiv_class] : query_graph.column_equivalences.getMemberToClassMap())
    {
        if (!equiv_class || !processed_classes.insert(equiv_class).second)
            continue;

        /// Collect all distinct relations in this equivalence class.
        BitSet seen_rels;
        std::vector<size_t> class_rels;
        for (const auto & column : *equiv_class)
        {
            auto relation = column.getSourceRelations().getSingleBit();
            if (relation && *relation < num_relations && !seen_rels.test(*relation))
            {
                seen_rels.set(*relation);
                class_rels.push_back(*relation);
            }
        }

        for (size_t i = 0; i < class_rels.size(); ++i)
        {
            for (size_t j = i + 1; j < class_rels.size(); ++j)
            {
                if (connected_rels[class_rels[i]].test(class_rels[j]))
                    continue;

                connected_rels[class_rels[i]].set(class_rels[j]);
                connected_rels[class_rels[j]].set(class_rels[i]);

                BitSet left_singleton;
                BitSet right_singleton;
                left_singleton.set(class_rels[i]);
                right_singleton.set(class_rels[j]);
                add_hyperedge(left_singleton, right_singleton);
            }
        }
    }
}

/// Returns the set of all relations adjacent to `node_set` via any hyperedge,
/// excluding `node_set` itself.
///
/// A hyperedge (L, R) represents a join predicate with left sources L and right sources R.
/// For example, `A.x = B.y` gives L={A}, R={B}; `A.x + B.y = C.z` gives L={A,B}, R={C}.
/// R is reachable from node_set when L is fully contained in node_set (and vice versa).
///
/// Non-binary predicates like `f(A,B,C) = const` are represented as L={A,B,C}, R={A,B,C}
BitSet JoinOrderOptimizer::getNeighborhood(const BitSet & node_set) const
{
    BitSet neighbors;
    BitSet visited_edges;
    for (auto node : node_set)
    {
        if (node >= node_to_edge_ids.size())
            continue;
        for (auto hyperedge_id : node_to_edge_ids[node])
        {
            if (visited_edges.test(hyperedge_id))
                continue;
            visited_edges.set(hyperedge_id);
            const auto & edge = hyperedges[hyperedge_id];
            if (edge.left == edge.right)
            {
                /// In case of non-binary predicate (`f(A,B,C) = const`) the hyperedge is
                /// represented as L={A,B,C}, R={A,B,C}
                neighbors |= edge.left;
            }
            else
            {
                if (isSubsetOf(edge.left, node_set))
                    neighbors |= edge.right;
                if (isSubsetOf(edge.right, node_set))
                    neighbors |= edge.left;
            }
        }
    }
    auto result = neighbors.andNot(node_set);
    LOG_TEST(log, "DPhyp: getNeighborhood({}) = {}",
        fmt::join(node_set, ","), fmt::join(result, ","));
    return result;
}

/// Enumerate all non-empty subsets of `mask`, calling `func` for each.
/// Uses an integer bitmask over the positions of set bits in `mask`.
template <typename F>
static void forEachNonEmptySubset(const BitSet & mask, F && func)
{
    std::vector<size_t> bit_positions;
    for (auto bit : mask)
        bit_positions.push_back(bit);

    const size_t num_bits = bit_positions.size();
    if (num_bits == 0)
        return;
    chassert(num_bits < 64);

    const UInt64 num_subsets = 1ULL << num_bits;
    for (UInt64 subset_mask = 1; subset_mask < num_subsets; ++subset_mask)
    {
        BitSet subset;
        for (size_t i = 0; i < num_bits; ++i)
            if (subset_mask & (1ULL << i))
                subset.set(bit_positions[i]);
        /// The callback returns false to stop enumeration early.
        if (!func(subset))
            return;
    }
}

/// The four functions below implement the core DPhyp enumeration from
/// "Dynamic Programming Strikes Back" (Moerkotte & Neumann, SIGMOD 2008), Section 3.
///
/// `emitCsg` (paper: EmitCsg, Sec 3.3) -- given a connected subgraph S1, generates all
///     complement seeds S2 = {v} from the neighborhood and extends them via `enumerateCmpRec`.
/// `enumerateCmpRec` (paper: EnumerateCmpRec, Sec 3.4) -- recursively extends complement S2
///     by adding neighboring nodes, emitting each valid csg-cmp pair.
/// `enumerateCsgRec` (paper: EnumerateCsgRec, Sec 3.2) -- recursively extends the primary
///     connected subgraph S1 by adding neighboring nodes.
/// `emitCsgCmp` (paper: EmitCsgCmp, Sec 3.5) -- evaluates a (S1, S2) pair for plan construction.
///
/// Deviation from the paper: EmitCsg checks connectivity (existence of a hyperedge
/// connecting S1 and S2) before calling EmitCsgCmp. We skip this check here and let
/// `tryJoin` handle it, which avoids duplicating the connectivity logic.

/// Evaluate a csg-cmp pair for plan construction.
void JoinOrderOptimizer::emitCsgCmp(const BitSet & left_csg, const BitSet & right_csg)
{
    if (dphyp_unsupported_predicate)
        return;
    LOG_TEST(log, "DPhyp: emitCsgCmp({{ {} }}, {{ {} }})",
        fmt::join(left_csg, ","), fmt::join(right_csg, ","));
    tryJoin(left_csg, right_csg);
}

/// Recursively extend complement S2 by adding subsets of its neighborhood.
/// `exclusion` (paper: X) prevents revisiting already-processed nodes.
void JoinOrderOptimizer::enumerateCmpRec(const BitSet & csg, const BitSet & complement, const BitSet & exclusion)
{
    if (dphyp_unsupported_predicate)
        return;

    LOG_TEST(log, "DPhyp: enumerateCmpRec(csg={{ {} }}, cmp={{ {} }}, excl={{ {} }})",
        fmt::join(csg, ","), fmt::join(complement, ","), fmt::join(exclusion, ","));

    BitSet complement_neighborhood = getNeighborhood(complement).andNot(exclusion);
    if (!complement_neighborhood)
        return;

    LOG_TEST(log, "DPhyp: enumerateCmpRec neighborhood={{ {} }}",
        fmt::join(complement_neighborhood, ","));

    /// First pass: emit pairs for every connected extension of the complement.
    forEachNonEmptySubset(complement_neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        BitSet extended_complement = complement | extension;
        if (dp_table.contains(extended_complement))
            emitCsgCmp(csg, extended_complement);
        return true;
    });

    /// Second pass: recurse with extended exclusion (paper: X = X | N(S2, X)).
    BitSet incremental_exclusion = exclusion | complement_neighborhood;
    forEachNonEmptySubset(complement_neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        enumerateCmpRec(csg, complement | extension, incremental_exclusion);
        return true;
    });
}

/// Generate all complement seeds for a given connected subgraph S1.
/// Seeds are single neighbor nodes, processed in descending index order. Each processed seed is
/// added to the exclusion passed to later seeds, so a complement spanning several neighbors is grown
/// from only one of them and each (S1, S2) pair is enumerated exactly once.
///
/// `exclusion` (paper: X) = S1 | B_min(S1), where B_min(S1) = {v : v < min(S1)}.
/// B_min excludes all relations ordered before the smallest relation in S1.
/// This is the key mechanism that prevents generating symmetric pairs:
/// the complement can only contain relations ordered after the CSG's minimum.
void JoinOrderOptimizer::emitCsg(const BitSet & csg)
{
    if (dphyp_unsupported_predicate)
        return;
    LOG_TEST(log, "DPhyp: emitCsg({{ {} }})", fmt::join(csg, ","));

    BitSet exclusion = csg | BitSet::allSet(*csg.begin());

    BitSet csg_neighborhood = getNeighborhood(csg).andNot(exclusion);
    if (!csg_neighborhood)
        return;

    LOG_TEST(log, "DPhyp: emitCsg neighborhood={{ {} }}, exclusion={{ {} }}",
        fmt::join(csg_neighborhood, ","), fmt::join(exclusion, ","));

    std::vector<size_t> neighbor_nodes;
    for (size_t n : csg_neighborhood)
        neighbor_nodes.push_back(n);

    /// Process seeds in descending index order, excluding each already-processed seed from the
    /// complements grown by later seeds. Without this, the same complement (e.g. {1,2}) would be
    /// reached from both the {2} seed and the {1} seed, enumerating the (S1, S2) pair twice.
    BitSet seed_exclusion = exclusion;
    for (auto it = neighbor_nodes.rbegin(); it != neighbor_nodes.rend(); ++it)
    {
        if (!continueEnumeration())
            return;
        BitSet single_node;
        single_node.set(*it);
        emitCsgCmp(csg, single_node);
        enumerateCmpRec(csg, single_node, seed_exclusion);
        seed_exclusion.set(*it);
    }
}

/// Recursively extend connected subgraph S1 by adding subsets of its neighborhood.
/// `exclusion` (paper: X) prevents revisiting already-processed nodes.
/// For each connected extension found in dp_table, calls `emitCsg` to generate complements.
void JoinOrderOptimizer::enumerateCsgRec(const BitSet & csg, const BitSet & exclusion)
{
    if (dphyp_unsupported_predicate)
        return;

    LOG_TEST(log, "DPhyp: enumerateCsgRec(csg={{ {} }}, excl={{ {} }})",
        fmt::join(csg, ","), fmt::join(exclusion, ","));

    BitSet neighborhood = getNeighborhood(csg).andNot(exclusion);
    if (!neighborhood)
        return;

    LOG_TEST(log, "DPhyp: enumerateCsgRec neighborhood={{ {} }}",
        fmt::join(neighborhood, ","));

    /// First pass: emit complements for every connected extension of S1.
    forEachNonEmptySubset(neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        BitSet extended_csg = csg | extension;
        if (dp_table.contains(extended_csg))
            emitCsg(extended_csg);
        return true;
    });

    /// Second pass: recurse with extended exclusion (paper: X = X | N(S1, X)).
    BitSet extended_exclusion = exclusion | neighborhood;
    forEachNonEmptySubset(neighborhood, [&](const BitSet & extension)
    {
        if (!continueEnumeration())
            return false;
        enumerateCsgRec(csg | extension, extended_exclusion);
        return true;
    });
}

std::shared_ptr<DPJoinEntry> JoinOrderOptimizer::solveDPhyp()
{
    const size_t num_relations = query_graph.relation_stats.size();

    /// DPhyp's subset enumeration uses a 64-bit bitmask, so it cannot handle neighborhoods
    /// larger than 63 relations. Bail out gracefully so the fallback algorithm chain can continue.
    if (num_relations >= 64)
    {
        LOG_TRACE(log, "Too many relations ({}) for DPhyp, falling back", num_relations);
        return nullptr;
    }

    dphyp_unsupported_predicate = false;
    search_budget_exceeded = false;
    searched_plans = 0;

    /// Initialize dp_table with a leaf entry for each base relation.
    /// Also reset the per-edge selectivity cache so this run is independent of any
    /// earlier algorithm in the fallback chain.
    dp_table.clear();
    expression_selectivity.clear();
    for (size_t i = 0; i < num_relations; ++i)
    {
        const auto & rel = query_graph.relation_stats[i];
        auto entry = std::make_shared<DPJoinEntry>(i, rel.estimated_rows, rel.column_stats);
        dp_table[entry->relations] = entry;
    }

    buildHyperedges();

    LOG_TEST(log, "DPhyp: {} relations, {} hyperedges", num_relations, hyperedges.size());
    for (size_t e = 0; e < hyperedges.size(); ++e)
        LOG_TEST(log, "DPhyp: hyperedge {}: ({{ {} }}, {{ {} }})", e,
            fmt::join(hyperedges[e].left, ","), fmt::join(hyperedges[e].right, ","));

    /// Main DPhyp loop (paper: Solve, Sec 3.1).
    /// Seed with each single-relation CSG in descending index order.
    /// For each seed {v}, `emitCsg` finds complements (the other side of the join),
    /// and `enumerateCsgRec` grows {v} into larger connected subgraphs.
    /// The exclusion set B_v = {w : w < v} | {v} ensures each unordered (S1, S2) pair
    /// is considered exactly once (the side with the smaller min-index is always S1).
    BitSet exclusion = BitSet::allSet(num_relations);
    for (int i = static_cast<int>(num_relations) - 1; i >= 0; --i)
    {
        /// Once enumeration is aborted, the result is discarded below, so stop seeding.
        if (dphyp_unsupported_predicate || search_budget_exceeded)
            break;

        BitSet seed;
        seed.set(static_cast<size_t>(i));

        LOG_TEST(log, "DPhyp: === seed {} ===", i);
        emitCsg(seed);
        exclusion.set(i, false);
        enumerateCsgRec(seed, exclusion);
    }

    if (dphyp_unsupported_predicate || search_budget_exceeded)
    {
        LOG_TRACE(log, "DPhyp could not produce a plan ({}), falling back",
            dphyp_unsupported_predicate ? "unsupported predicate" : "search budget exceeded");
        return nullptr;
    }

    auto best = dp_table.find(BitSet::allSet(num_relations));
    if (best != dp_table.end())
        return best->second;

    /// DPhyp cannot produce a plan for disconnected graphs (no cross products).
    /// The caller's fallback chain (e.g. dphyp,greedy) handles this.
    LOG_TRACE(log, "Failed to find best plan using DPhyp algorithm");
    return nullptr;
}

std::optional<JoinKind> JoinOrderOptimizer::isValidJoinOrder(const BitSet & left_mask, const BitSet & right_mask) const
{
    auto check = [&](const auto & lhs, const auto & rhs) -> std::optional<JoinKind>
    {
        auto rel_id = lhs.getSingleBit();
        if (rel_id.has_value())
        {
            auto it = query_graph.join_kinds.find(rel_id.value());
            if (it != query_graph.join_kinds.end())
            {
                if (isSubsetOf(it->second.first, rhs))
                    return it->second.second;
                return {};
            }
        }
        return JoinKind::Inner;
    };

    JoinKind left_join_type = JoinKind::Inner;
    JoinKind right_join_type = JoinKind::Inner;

    if (auto res = check(left_mask, right_mask))
    {
        /// When original join stored a Left/Full kind for the left relation,
        /// and it now appears on the left side of reordered join, reverse the kind
        left_join_type = isLeftOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    }
    else
        return {};

    if (auto res = check(right_mask, left_mask))
        right_join_type = isRightOrFull(res.value()) ? reverseJoinKind(res.value()) : res.value();
    else
        return {};

    if (left_join_type == JoinKind::Inner)
        return right_join_type;
    if (right_join_type == JoinKind::Inner)
        return left_join_type;
    /// Allow FULL join as it's restricted to table swapping and no reordering
    if (left_join_type == JoinKind::Full && right_join_type == JoinKind::Full)
        return JoinKind::Full;

    /// Conflict, join is not possible:
    /// FROM t1 LEFT JOIN t2 LEFT JOIN t3
    /// t1 -> Inner, t2 -> Left, t3 -> Left
    /// Cannot do (t2 x t3)
    return {};
}

void JoinOrderOptimizer::finalizeSelectedPlanProperties(const DPJoinEntryPtr & selected_plan)
{
    if (!selected_plan)
        return;

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

    dp_table.clear();
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
    if (transitive_predicates_enabled || selectedPlanUsedCanonicalCardinalityCap(selected_plan))
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
    if (optimization_settings.enable_join_transitive_predicates
        || optimization_settings.query_plan_optimize_join_order_use_proven_uniqueness)
        query_graph.buildColumnEquivalences();
    JoinOrderOptimizer reorderer(
        std::move(query_graph),
        optimization_settings.query_plan_optimize_join_order_algorithm,
        optimization_settings.query_plan_optimize_join_order_max_searched_plans,
        optimization_settings.query_plan_optimize_join_order_use_proven_uniqueness,
        optimization_settings.enable_join_transitive_predicates,
        optimization_settings.query_plan_optimize_join_order_data_property_diagnostics,
        debug_info);
    auto best_plan = reorderer.solve();
    if (!best_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find a valid join order");
    reorderer.finalizeSelectedPlan(best_plan);
    return best_plan;
}
}
