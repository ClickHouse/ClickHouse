#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCanonicalProperties.h>

#include <array>
#include <bit>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

struct Leaf
{
    Block header;
    DataPropertySet properties;
};

Leaf makeLeaf(std::initializer_list<String> names, std::vector<size_t> key = {}, bool nullable = false)
{
    DataTypePtr type = std::make_shared<DataTypeUInt64>();
    if (nullable)
        type = std::make_shared<DataTypeNullable>(type);

    Leaf result;
    for (const auto & name : names)
        result.header.insert(ColumnWithTypeAndName(type->createColumn(), type, name));
    for (size_t position = 0; position < result.header.columns(); ++position)
        if (!nullable)
            result.properties.addNonNullColumn({position, result.header.getByPosition(position).name});
    if (!key.empty())
    {
        QueryPlanOptimizations::ColumnSet columns;
        for (const auto position : key)
            columns.push_back({position, result.header.getByPosition(position).name});
        result.properties.addUniqueKey(UniqueKeyFact::fromAggregationGrouping(std::move(columns)));
    }
    return result;
}

Leaf makeWideLeaf(UInt32 relation, size_t column_count)
{
    const auto type = std::make_shared<DataTypeUInt64>();
    Leaf result;
    for (size_t position = 0; position < column_count; ++position)
    {
        const String name = "r" + std::to_string(relation) + "_c" + std::to_string(position);
        result.header.insert(ColumnWithTypeAndName(type->createColumn(), type, name));
        result.properties.addNonNullColumn({position, name});
    }
    return result;
}

std::shared_ptr<const JoinOrderDataPropertyCatalog> makeCatalog(std::vector<Leaf> leaves, bool diagnostics = false)
{
    JoinOrderDataPropertyCatalogBuilder builder;
    for (const auto & leaf : leaves)
        builder.appendLeaf(leaf.properties, leaf.header);
    return std::move(builder).finalize(
        diagnostics ? JoinOrderDataPropertyCatalogMode::Diagnostics : JoinOrderDataPropertyCatalogMode::Costing);
}

std::unique_ptr<JoinOrderCanonicalProperties> makeProvider(
    std::shared_ptr<const JoinOrderDataPropertyCatalog> catalog,
    std::initializer_list<std::pair<UInt32, UInt32>> equalities,
    std::optional<JoinOrderPropertyUnsupportedReason> rejection = {},
    std::initializer_list<size_t> incomparable_predicates = {})
{
    std::vector<JoinOrderCanonicalPredicate> predicates;
    UInt32 predicate_id = 1;
    for (const auto [lhs, rhs] : equalities)
    {
        const JoinOrderColumnId lhs_id{lhs};
        const JoinOrderColumnId rhs_id{rhs};
        BitSet applicability;
        applicability.set(catalog->column(lhs_id).relation);
        applicability.set(catalog->column(rhs_id).relation);
        const size_t predicate_index = predicates.size();
        predicates.push_back(
            {.stable_id = predicate_id++,
             .applicability = std::move(applicability),
             .binding = JoinOrderOrdinaryEqualityBinding{
                 lhs_id, rhs_id, std::ranges::find(incomparable_predicates, predicate_index) != incomparable_predicates.end()}});
    }
    return std::make_unique<JoinOrderCanonicalProperties>(catalog, catalog->relationCount(), std::move(predicates), rejection);
}

JoinOrderLogicalGroupId requireGroup(JoinOrderCanonicalProperties & provider, const BitSet & subset)
{
    const auto lookup = provider.getGroup(subset);
    EXPECT_TRUE(lookup.has_value());
    return lookup.value_or(JoinOrderLogicalGroupId{});
}

JoinOrderColumnSetId requireDemand(JoinOrderCanonicalProperties & provider, std::initializer_list<UInt32> columns)
{
    std::vector<JoinOrderColumnId> ids;
    for (const UInt32 column : columns)
        ids.push_back({column});
    const auto lookup = provider.internColumnSet(ids);
    EXPECT_TRUE(lookup.has_value());
    return lookup.value_or(JoinOrderColumnSetId{});
}

struct CanonicalTree
{
    JoinOrderLogicalGroupId group;
    std::optional<JoinOrderEqualityCutId> cut;
    std::shared_ptr<const CanonicalTree> left;
    std::shared_ptr<const CanonicalTree> right;
};

using CanonicalTrees = std::vector<std::shared_ptr<const CanonicalTree>>;

CanonicalTrees enumerateConnectedTrees(JoinOrderCanonicalProperties & provider, UInt32 mask)
{
    const auto group = provider.getGroup(mask);
    EXPECT_TRUE(group.has_value());
    if (!group)
        return {};
    if (std::popcount(mask) == 1)
        return {std::make_shared<CanonicalTree>(CanonicalTree{.group = *group, .cut = {}, .left = {}, .right = {}})};

    CanonicalTrees result;
    const UInt32 first_relation = UInt32{1} << std::countr_zero(mask);
    for (UInt32 left_mask = (mask - 1) & mask; left_mask; left_mask = (left_mask - 1) & mask)
    {
        if (!(left_mask & first_relation))
            continue;
        const UInt32 right_mask = mask ^ left_mask;
        if (!right_mask)
            continue;

        auto left_trees = enumerateConnectedTrees(provider, left_mask);
        auto right_trees = enumerateConnectedTrees(provider, right_mask);
        if (left_trees.empty() || right_trees.empty())
            continue;

        const auto cut = getEqualityCutId(provider.getEqualityCut(left_trees.front()->group, right_trees.front()->group));
        if (!cut)
            continue;

        for (const auto & left : left_trees)
            for (const auto & right : right_trees)
                result.push_back(
                    std::make_shared<CanonicalTree>(CanonicalTree{.group = *group, .cut = cut, .left = left, .right = right}));
    }
    return result;
}

void validateCanonicalTree(JoinOrderCanonicalProperties & provider, const CanonicalTree & tree)
{
    const auto description = provider.describeGroup(tree.group);
    const auto group = provider.getGroup(description.subset);
    ASSERT_TRUE(group.has_value());
    EXPECT_EQ(*group, tree.group);

    if (!tree.left || !tree.right)
    {
        EXPECT_FALSE(tree.cut.has_value());
        return;
    }

    ASSERT_TRUE(tree.cut.has_value());
    const auto cut = getEqualityCutId(provider.getEqualityCut(tree.left->group, tree.right->group));
    ASSERT_TRUE(cut.has_value());
    EXPECT_EQ(*cut, *tree.cut);
    validateCanonicalTree(provider, *tree.left);
    validateCanonicalTree(provider, *tree.right);
}
}

TEST(JoinOrderCanonicalProperties, TriangleIsUniqueFromEveryLogicalTree)
{
    /// A: [a_id key, x, y], B: [b_id key, x], C: [c_id key, z].
    auto catalog = makeCatalog({makeLeaf({"a_id", "a_x", "a_y"}, {0}), makeLeaf({"b_id", "b_x"}, {0}), makeLeaf({"c_id", "c_z"}, {0})});
    auto provider = makeProvider(catalog, {{1, 4}, {2, 5}, {3, 6}});

    const BitSet full = BitSet::allSet(3);
    const auto first = requireGroup(*provider, full);
    const auto second = requireGroup(*provider, BitSet().set(2).set(0).set(1));
    const auto native = provider->getGroup(UInt32{0b111});
    ASSERT_TRUE(native);
    EXPECT_EQ(first, second);
    EXPECT_EQ(first, *native);

    const auto demand = requireDemand(*provider, {0});
    const auto result = provider->isUniqueOn(first, demand);
    EXPECT_NE(getUniquenessProof(result), nullptr);

    const auto costing_metrics = provider->getMetrics();
    EXPECT_EQ(costing_metrics.retained_expanded_predicate_closure_members, 0u);
    EXPECT_EQ(costing_metrics.retained_expanded_output_contract_members, 0u);

    const auto description = provider->describeGroup(first);
    EXPECT_EQ(description.subset, full);
    EXPECT_NE(description.region.value, 0u);
    EXPECT_NE(description.predicate_closure.value, 0u);
    EXPECT_NE(description.output_contract.value, 0u);
    EXPECT_NE(description.output_contract, provider->describeGroup(requireGroup(*provider, BitSet().set(0).set(1))).output_contract);
    EXPECT_GT(provider->getMetrics().retained_expanded_predicate_closure_members, 0u);
    EXPECT_GT(provider->getMetrics().retained_expanded_output_contract_members, 0u);
}

TEST(JoinOrderCanonicalProperties, WideSchemaRetainsOnlyNativeSubsetMasksDuringCosting)
{
    constexpr UInt32 relation_count = 10;
    constexpr size_t columns_per_relation = 100;
    std::vector<Leaf> leaves;
    leaves.reserve(relation_count);
    for (UInt32 relation = 0; relation < relation_count; ++relation)
        leaves.push_back(makeWideLeaf(relation, columns_per_relation));

    auto catalog = makeCatalog(std::exchange(leaves, {}));
    auto provider = makeProvider(catalog, {});
    for (UInt32 mask = 1; mask < (UInt32{1} << relation_count); ++mask)
    {
        BitSet subset;
        for (UInt32 relation = 0; relation < relation_count; ++relation)
            if (mask & (UInt32{1} << relation))
                subset.set(relation);
        const auto native = provider->getGroup(mask);
        const auto generic_caller = provider->getGroup(subset);
        ASSERT_TRUE(native) << mask;
        ASSERT_TRUE(generic_caller) << mask;
        EXPECT_EQ(native, generic_caller) << mask;
    }

    const auto metrics = provider->getMetrics();
    EXPECT_EQ(metrics.groups, 1023u);
    EXPECT_EQ(metrics.retained_subset_payload_members, 1023u);
    EXPECT_EQ(metrics.retained_subset_payload_bytes, 1023u * sizeof(UInt32));
    EXPECT_EQ(metrics.retained_expanded_predicate_closure_members, 0u);
    EXPECT_EQ(metrics.retained_expanded_output_contract_members, 0u);
}

TEST(JoinOrderCanonicalProperties, GenericGroupsPreserveFarRelationIdentityAndProperties)
{
    constexpr UInt32 relation_count = 33;
    std::vector<Leaf> leaves;
    leaves.reserve(relation_count);
    for (UInt32 relation = 0; relation < relation_count; ++relation)
    {
        if (relation == 0)
            leaves.push_back(makeLeaf({"r0_c0"}, {0}));
        else if (relation == 32)
            leaves.push_back(makeLeaf({"r32_c0"}, {0}));
        else
            leaves.push_back(makeWideLeaf(relation, 1));
    }

    auto catalog = makeCatalog(std::exchange(leaves, {}));
    auto provider = makeProvider(catalog, {{0, 32}});
    const BitSet wide_subset = BitSet().set(32).set(0);
    const auto wide = requireGroup(*provider, wide_subset);
    const auto after_first_lookup = provider->getMetrics();
    EXPECT_EQ(after_first_lookup.generic_subset_scratch_capacity_changes, 1u);
    EXPECT_EQ(after_first_lookup.generic_subset_scratch_uses, 1u);
    EXPECT_EQ(wide, requireGroup(*provider, BitSet().set(0).set(32)));

    const auto native_low = provider->getGroup(UInt32{1});
    ASSERT_TRUE(native_low);
    const auto low = requireGroup(*provider, BitSet().set(0));
    EXPECT_EQ(*native_low, low);
    const auto high = requireGroup(*provider, BitSet().set(32));

    auto metrics = provider->getMetrics();
    EXPECT_EQ(metrics.groups, 3u);
    EXPECT_EQ(metrics.retained_subset_payload_members, 4u);
    EXPECT_EQ(metrics.retained_subset_payload_bytes, 4u * sizeof(UInt32));
    EXPECT_EQ(metrics.generic_subset_scratch_capacity_changes, 1u);
    EXPECT_EQ(metrics.generic_subset_scratch_uses, 5u);
    EXPECT_EQ(metrics.retained_expanded_predicate_closure_members, 0u);
    EXPECT_EQ(metrics.retained_expanded_output_contract_members, 0u);

    const auto overlap = provider->getEqualityCut(wide, high);
    const auto * overlap_reason = getUnsupportedReason(overlap);
    ASSERT_NE(overlap_reason, nullptr);
    EXPECT_EQ(*overlap_reason, JoinOrderPropertyUnsupportedReason::InvalidGroup);

    const auto forward = getEqualityCutId(provider->getEqualityCut(low, high));
    const auto reverse = getEqualityCutId(provider->getEqualityCut(high, low));
    ASSERT_TRUE(forward);
    ASSERT_TRUE(reverse);
    EXPECT_NE(*forward, *reverse);

    const auto uniqueness = provider->isUniqueOn(wide, requireDemand(*provider, {0}));
    EXPECT_NE(getUniquenessProof(uniqueness), nullptr);

    const auto forward_cap = provider->inferCardinalityCapForCut(low, high, *forward, 100, 20);
    const auto reverse_cap = provider->inferCardinalityCapForCut(high, low, *reverse, 20, 100);
    ASSERT_NE(getProvenCap(forward_cap), nullptr);
    ASSERT_NE(getProvenCap(reverse_cap), nullptr);
    EXPECT_EQ(getProvenCap(forward_cap)->upper_bound, 20u);
    EXPECT_EQ(getProvenCap(reverse_cap)->upper_bound, 20u);

    const auto description = provider->describeGroup(wide);
    EXPECT_EQ(description.subset, wide_subset);
    EXPECT_NE(description.predicate_closure.value, 0u);
    EXPECT_NE(description.output_contract.value, 0u);

    metrics = provider->getMetrics();
    EXPECT_EQ(metrics.generic_subset_scratch_capacity_changes, 1u);
    EXPECT_EQ(metrics.generic_subset_scratch_uses, 5u);
}

TEST(JoinOrderCanonicalProperties, TriangleContinuationCapsAreIndependentOfCachedWinnerState)
{
    auto catalog = makeCatalog({makeLeaf({"a_id", "a_x", "a_y"}, {0}), makeLeaf({"b_id", "b_x"}, {0}), makeLeaf({"c_id", "c_z"}, {0})});
    auto provider = makeProvider(catalog, {{1, 4}, {2, 5}, {3, 6}});

    const auto ab = requireGroup(*provider, BitSet().set(0).set(1));
    const auto c = requireGroup(*provider, BitSet().set(2));
    const auto cut = getEqualityCutId(provider->getEqualityCut(ab, c));
    ASSERT_TRUE(cut);
    const auto cap = provider->inferCardinalityCapForCut(ab, c, *cut, 100, 20);
    ASSERT_NE(getProvenCap(cap), nullptr);
    EXPECT_EQ(getProvenCap(cap)->upper_bound, 100u);

    /// Repeating the same subset through a different construction order reuses
    /// the logical answer, not a selected child's tree state.
    const auto ab_again = requireGroup(*provider, BitSet().set(1).set(0));
    EXPECT_EQ(ab, ab_again);
    const auto cut_again = getEqualityCutId(provider->getEqualityCut(ab_again, c));
    ASSERT_TRUE(cut_again);
    EXPECT_EQ(*cut, *cut_again);
}

TEST(JoinOrderCanonicalProperties, ExternalBridgeLinksAreRecordedAsObligations)
{
    /// Relations: a{a_k, a_x} key {a_k}, b{b_x} key {b_x}, c{c_k}, e{e_x}.
    /// Column ids: a_k=0, a_x=1, b_x=2, c_k=3, e_x=4.
    /// Classes: {a_k, c_k} (index 0) and {a_x, b_x, e_x} (index 1, bridged through `e`).
    auto catalog = makeCatalog({makeLeaf({"a_k", "a_x"}, {0}), makeLeaf({"b_x"}, {0}), makeLeaf({"c_k"}), makeLeaf({"e_x"})});
    auto provider = makeProvider(catalog, {{0, 3}, {1, 4}, {4, 2}});

    /// The cut of {a,b} vs {c} demands only `a_k`; proving uniqueness of the group needs `b`
    /// determined, reachable only through the class bridged by relation `e` outside the
    /// group. That link is not justified by any intra-group predicate, so the proof must
    /// carry it as an obligation.
    const auto left = requireGroup(*provider, BitSet().set(0).set(1));
    const auto right = requireGroup(*provider, BitSet().set(2));
    const auto cut = getEqualityCutId(provider->getEqualityCut(left, right));
    ASSERT_TRUE(cut);
    const auto cap = provider->inferCardinalityCapForCut(left, right, *cut, 4, 1000000);
    ASSERT_NE(getProvenCap(cap), nullptr);
    EXPECT_EQ(getProvenCap(cap)->obligation_classes, UInt64{1} << 1);
}

TEST(JoinOrderCanonicalProperties, IntraGroupLinksCarryNoObligations)
{
    /// The same shape without the bridge: `a_x = b_x` is a direct intra-group predicate,
    /// so the proof of {a,b} uniqueness needs no synthesized enforcement.
    auto catalog = makeCatalog({makeLeaf({"a_k", "a_x"}, {0}), makeLeaf({"b_x"}, {0}), makeLeaf({"c_k"})});
    auto provider = makeProvider(catalog, {{0, 3}, {1, 2}});

    const auto left = requireGroup(*provider, BitSet().set(0).set(1));
    const auto right = requireGroup(*provider, BitSet().set(2));
    const auto cut = getEqualityCutId(provider->getEqualityCut(left, right));
    ASSERT_TRUE(cut);
    const auto cap = provider->inferCardinalityCapForCut(left, right, *cut, 4, 1000000);
    ASSERT_NE(getProvenCap(cap), nullptr);
    EXPECT_EQ(getProvenCap(cap)->obligation_classes, 0u);
}

TEST(JoinOrderCanonicalProperties, IncomparableClassesFailClosedOnlyWhereRelied)
{
    /// Relations: a{a_k} key, b{b_k} key, c{c_x}, d{d_x}. Columns: a_k=0, b_k=1, c_x=2, d_x=3.
    /// Class {a_k, b_k} is comparable; class {c_x, d_x} is marked incomparable.
    auto catalog = makeCatalog({makeLeaf({"a_k"}, {0}), makeLeaf({"b_k"}, {0}), makeLeaf({"c_x"}), makeLeaf({"d_x"})});
    auto provider = makeProvider(catalog, {{0, 1}, {2, 3}}, {}, {1});

    /// A cut untouched by the incomparable class keeps its cap.
    const auto a = requireGroup(*provider, BitSet().set(0));
    const auto b = requireGroup(*provider, BitSet().set(1));
    const auto ab_cut = getEqualityCutId(provider->getEqualityCut(a, b));
    ASSERT_TRUE(ab_cut);
    const auto ab_cap = provider->inferCardinalityCapForCut(a, b, *ab_cut, 100, 20);
    EXPECT_NE(getProvenCap(ab_cap), nullptr);

    /// A cut relying on the incomparable class fails closed with a typed reason.
    const auto c = requireGroup(*provider, BitSet().set(2));
    const auto d = requireGroup(*provider, BitSet().set(3));
    const auto cd_cut = provider->getEqualityCut(c, d);
    const auto * cd_reason = getUnsupportedReason(cd_cut);
    ASSERT_NE(cd_reason, nullptr);
    EXPECT_EQ(*cd_reason, JoinOrderPropertyUnsupportedReason::UnsupportedEqualityType);
}

TEST(JoinOrderCanonicalProperties, IncomparableObligationFailsProofClosed)
{
    /// The external-bridge shape with the bridged class marked incomparable: the cut itself
    /// uses the comparable class {a_k, c_k}, but the proof of `{a,b}` uniqueness would need
    /// the incomparable bridged link `a_x = b_x` synthesized, so it must fail closed.
    auto catalog = makeCatalog({makeLeaf({"a_k", "a_x"}, {0}), makeLeaf({"b_x"}, {0}), makeLeaf({"c_k"}), makeLeaf({"e_x"})});
    auto provider = makeProvider(catalog, {{0, 3}, {1, 4}, {4, 2}}, {}, {1, 2});

    const auto left = requireGroup(*provider, BitSet().set(0).set(1));
    const auto right = requireGroup(*provider, BitSet().set(2));
    const auto cut = getEqualityCutId(provider->getEqualityCut(left, right));
    ASSERT_TRUE(cut);
    const auto cap = provider->inferCardinalityCapForCut(left, right, *cut, 4, 1000000);
    const auto * unsupported = std::get_if<JoinOrderPropertyUnsupportedReason>(&cap);
    ASSERT_NE(unsupported, nullptr);
    EXPECT_EQ(*unsupported, JoinOrderPropertyUnsupportedReason::UnsupportedEqualityType);
}

TEST(JoinOrderCanonicalProperties, OneToManyAndManyToManyCapsFollowBagUniqueness)
{
    {
        auto catalog = makeCatalog({makeLeaf({"left_id"}), makeLeaf({"right_id"}, {0})});
        auto provider = makeProvider(catalog, {{0, 1}});
        const auto left = requireGroup(*provider, BitSet().set(0));
        const auto right = requireGroup(*provider, BitSet().set(1));
        const auto cut = getEqualityCutId(provider->getEqualityCut(left, right));
        ASSERT_TRUE(cut);
        const auto cap = provider->inferCardinalityCapForCut(left, right, *cut, 100, 20);
        ASSERT_NE(getProvenCap(cap), nullptr);
        EXPECT_EQ(getProvenCap(cap)->upper_bound, 100u);
    }
    {
        auto catalog = makeCatalog({makeLeaf({"left_id"}), makeLeaf({"right_id"})});
        auto provider = makeProvider(catalog, {{0, 1}});
        const auto left = requireGroup(*provider, BitSet().set(0));
        const auto right = requireGroup(*provider, BitSet().set(1));
        const auto cut = getEqualityCutId(provider->getEqualityCut(left, right));
        ASSERT_TRUE(cut);
        const auto cap = provider->inferCardinalityCapForCut(left, right, *cut, 100, 20);
        const auto * no_cap = std::get_if<JoinOrderNoCardinalityCapReason>(&cap);
        ASSERT_NE(no_cap, nullptr);
        EXPECT_EQ(*no_cap, JoinOrderNoCardinalityCapReason::NotProven);
    }
}

TEST(JoinOrderCanonicalProperties, CardinalityCapOutcomesAreExplicitAndFailClosed)
{
    const JoinOrderCardinalityCap disabled;
    ASSERT_NE(std::get_if<JoinOrderNoCardinalityCapReason>(&disabled), nullptr);
    EXPECT_EQ(std::get<JoinOrderNoCardinalityCapReason>(disabled), JoinOrderNoCardinalityCapReason::Disabled);

    auto keyed_catalog = makeCatalog({makeLeaf({"left_id"}), makeLeaf({"right_id"}, {0})});
    auto keyed_provider = makeProvider(keyed_catalog, {{0, 1}});
    const auto keyed_left = requireGroup(*keyed_provider, BitSet().set(0));
    const auto keyed_right = requireGroup(*keyed_provider, BitSet().set(1));
    const auto keyed_cut = getEqualityCutId(keyed_provider->getEqualityCut(keyed_left, keyed_right));
    ASSERT_TRUE(keyed_cut);

    const auto missing = keyed_provider->inferInnerAllCardinalityCap(UInt32{1}, UInt32{2}, std::nullopt, 20);
    ASSERT_NE(std::get_if<JoinOrderNoCardinalityCapReason>(&missing), nullptr);
    EXPECT_EQ(std::get<JoinOrderNoCardinalityCapReason>(missing), JoinOrderNoCardinalityCapReason::MissingInputRows);

    const auto proven = keyed_provider->inferCardinalityCapForCut(keyed_left, keyed_right, *keyed_cut, 100, 20);
    ASSERT_NE(getProvenCap(proven), nullptr);
    EXPECT_EQ(getProvenCap(proven)->upper_bound, 100u);

    auto unkeyed_catalog = makeCatalog({makeLeaf({"left_id"}), makeLeaf({"right_id"})});
    auto unkeyed_provider = makeProvider(unkeyed_catalog, {{0, 1}});
    const auto unkeyed_left = requireGroup(*unkeyed_provider, BitSet().set(0));
    const auto unkeyed_right = requireGroup(*unkeyed_provider, BitSet().set(1));
    const auto unkeyed_cut = getEqualityCutId(unkeyed_provider->getEqualityCut(unkeyed_left, unkeyed_right));
    ASSERT_TRUE(unkeyed_cut);
    const auto not_proven = unkeyed_provider->inferCardinalityCapForCut(unkeyed_left, unkeyed_right, *unkeyed_cut, 100, 20);
    ASSERT_NE(std::get_if<JoinOrderNoCardinalityCapReason>(&not_proven), nullptr);
    EXPECT_EQ(std::get<JoinOrderNoCardinalityCapReason>(not_proven), JoinOrderNoCardinalityCapReason::NotProven);

    const auto unsupported
        = keyed_provider->inferCardinalityCapForCut(keyed_left, keyed_right, JoinOrderEqualityCutId{999, keyed_left.provider}, 100, 20);
    ASSERT_NE(std::get_if<JoinOrderPropertyUnsupportedReason>(&unsupported), nullptr);
    EXPECT_EQ(std::get<JoinOrderPropertyUnsupportedReason>(unsupported), JoinOrderPropertyUnsupportedReason::InvalidCut);
}

TEST(JoinOrderCanonicalProperties, CompositeAndCrossSourceDemandsAreExactSets)
{
    auto catalog = makeCatalog({makeLeaf({"tenant", "id", "payload"}, {0, 1}), makeLeaf({"foreign_tenant", "foreign_id"}, {0, 1})});
    auto provider = makeProvider(catalog, {{0, 3}, {1, 4}});

    const auto left = requireGroup(*provider, BitSet().set(0));
    const auto partial_demand = provider->isUniqueOn(left, requireDemand(*provider, {0}));
    EXPECT_TRUE(std::holds_alternative<JoinOrderUniquenessNotProven>(partial_demand));
    EXPECT_NE(getUniquenessProof(provider->isUniqueOn(left, requireDemand(*provider, {0, 1}))), nullptr);

    const auto full = requireGroup(*provider, BitSet::allSet(2));
    EXPECT_NE(getUniquenessProof(provider->isUniqueOn(full, requireDemand(*provider, {0, 4}))), nullptr);
}

TEST(JoinOrderCanonicalProperties, ExplicitFunctionalDependencyCannotSeedBagUniqueness)
{
    auto leaf = makeLeaf({"determinant", "dependent"});
    leaf.properties.addFunctionalDependency(
        {{{0, "determinant"}}, {{1, "dependent"}}, DataPropertyDependencyKind::Exact, DataPropertyProvenance::aggregationGrouping()});
    auto catalog = makeCatalog({std::move(leaf)}, true);
    auto provider = makeProvider(catalog, {});

    const auto group = requireGroup(*provider, BitSet().set(0));
    const auto result = provider->isUniqueOn(group, requireDemand(*provider, {0}));
    EXPECT_TRUE(std::holds_alternative<JoinOrderUniquenessNotProven>(result));
}

TEST(JoinOrderCanonicalProperties, StorageDeclarationCannotProduceProof)
{
    auto leaf = makeLeaf({"id"});
    leaf.properties.addUniqueKey(UniqueKeyFact::fromStorageDeclaration({{0, "id"}}));
    auto catalog = makeCatalog({std::move(leaf)}, true);
    auto provider = makeProvider(catalog, {});

    const auto group = requireGroup(*provider, BitSet().set(0));
    const auto result = provider->isUniqueOn(group, requireDemand(*provider, {0}));
    EXPECT_TRUE(std::holds_alternative<JoinOrderUniquenessNotProven>(result));
}

TEST(JoinOrderCanonicalProperties, NullableDemandAndKeyFailClosed)
{
    auto catalog = makeCatalog({makeLeaf({"nullable_id"}, {0}, true)});
    auto provider = makeProvider(catalog, {});
    const auto group = requireGroup(*provider, BitSet().set(0));
    const auto demand = requireDemand(*provider, {0});
    const auto first = provider->isUniqueOn(group, demand);
    const auto second = provider->isUniqueOn(group, demand);
    EXPECT_EQ(first, second);
    const auto * first_reason = getUnsupportedReason(first);
    ASSERT_NE(first_reason, nullptr);
    EXPECT_EQ(*first_reason, JoinOrderPropertyUnsupportedReason::NullableDemandColumn);
    const auto metrics = provider->getMetrics();
    EXPECT_EQ(metrics.cache_misses, 1u);
    EXPECT_EQ(metrics.cache_hits, 1u);
}

TEST(JoinOrderCanonicalProperties, NullSafeEqualityRejectsWholeRegion)
{
    auto catalog = makeCatalog({makeLeaf({"left_id"}, {0}), makeLeaf({"right_id"}, {0})});
    JoinOrderCanonicalPredicate predicate{
        .stable_id = 1, .applicability = BitSet::allSet(2), .binding = JoinOrderPropertyUnsupportedReason::NullSafeEquality};
    JoinOrderCanonicalProperties provider(catalog, 2, {std::move(predicate)});

    const auto group = provider.getGroup(BitSet::allSet(2));
    ASSERT_FALSE(group);
    EXPECT_EQ(group.error(), JoinOrderPropertyUnsupportedReason::NullSafeEquality);
}

TEST(JoinOrderCanonicalProperties, InvalidIdsAndCrossProviderHandlesAreRejected)
{
    auto catalog = makeCatalog({makeLeaf({"id"}, {0})});
    auto first = makeProvider(catalog, {});
    auto second = makeProvider(catalog, {});
    const auto first_group = requireGroup(*first, BitSet().set(0));
    const auto first_demand = requireDemand(*first, {0});
    const auto second_demand = requireDemand(*second, {0});

    const auto mixed = first->isUniqueOn(first_group, second_demand);
    const auto * mixed_reason = getUnsupportedReason(mixed);
    ASSERT_NE(mixed_reason, nullptr);
    EXPECT_EQ(*mixed_reason, JoinOrderPropertyUnsupportedReason::ProviderMismatch);

    const std::array invalid{JoinOrderColumnId{999}};
    const auto invalid_demand = first->internColumnSet(invalid);
    ASSERT_FALSE(invalid_demand);
    EXPECT_EQ(invalid_demand.error(), JoinOrderPropertyUnsupportedReason::InvalidColumnId);

    const auto forged_demand = first->isUniqueOn(first_group, JoinOrderColumnSetId{999, first_group.provider});
    const auto * forged_demand_reason = getUnsupportedReason(forged_demand);
    ASSERT_NE(forged_demand_reason, nullptr);
    EXPECT_EQ(*forged_demand_reason, JoinOrderPropertyUnsupportedReason::InvalidColumnId);

    const auto forged_group = first->isUniqueOn(JoinOrderLogicalGroupId{999, first_group.provider}, first_demand);
    const auto * forged_group_reason = getUnsupportedReason(forged_group);
    ASSERT_NE(forged_group_reason, nullptr);
    EXPECT_EQ(*forged_group_reason, JoinOrderPropertyUnsupportedReason::InvalidGroup);

    const auto forged_cut
        = first->inferCardinalityCapForCut(first_group, first_group, JoinOrderEqualityCutId{999, first_group.provider}, 1, 1);
    ASSERT_NE(std::get_if<JoinOrderPropertyUnsupportedReason>(&forged_cut), nullptr);
    EXPECT_EQ(std::get<JoinOrderPropertyUnsupportedReason>(forged_cut), JoinOrderPropertyUnsupportedReason::InvalidCut);
}

TEST(JoinOrderCanonicalProperties, DuplicateOutputNamesRejectWholeRegion)
{
    auto catalog = makeCatalog({makeLeaf({"id"}, {0}), makeLeaf({"id"}, {0})});
    auto provider = makeProvider(catalog, {{0, 1}});

    const auto group = provider->getGroup(BitSet::allSet(2));
    ASSERT_FALSE(group);
    EXPECT_EQ(group.error(), JoinOrderPropertyUnsupportedReason::AmbiguousOutputContract);
}

TEST(JoinOrderCanonicalProperties, SixRelationChainEnumeratesEveryCanonicalTree)
{
    auto catalog = makeCatalog(
        {makeLeaf({"a"}, {0}),
         makeLeaf({"b"}, {0}),
         makeLeaf({"c"}, {0}),
         makeLeaf({"d"}, {0}),
         makeLeaf({"e"}, {0}),
         makeLeaf({"f"}, {0})});
    auto provider = makeProvider(catalog, {{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}});

    const BitSet full = BitSet::allSet(6);
    const auto group = requireGroup(*provider, full);
    const auto demand = requireDemand(*provider, {0});
    const auto expected = provider->isUniqueOn(group, demand);
    ASSERT_NE(getUniquenessProof(expected), nullptr);

    const auto trees = enumerateConnectedTrees(*provider, 0b111111);
    ASSERT_EQ(trees.size(), 945u);
    for (const auto & tree : trees)
    {
        ASSERT_TRUE(tree);
        EXPECT_EQ(tree->group, group);
        validateCanonicalTree(*provider, *tree);
        EXPECT_EQ(provider->isUniqueOn(tree->group, demand), expected);
    }
}

TEST(JoinOrderCanonicalProperties, ExhaustiveFiniteBagsRespectEveryClaimedCap)
{
    const std::vector<std::vector<Int8>> bags{{}, {0}, {1}, {0, 0}, {0, 1}, {1, 0}, {1, 1}};
    auto is_key_valid = [](const std::vector<Int8> & bag) { return std::unordered_set<Int8>(bag.begin(), bag.end()).size() == bag.size(); };
    auto join_rows = [](const std::vector<Int8> & left, const std::vector<Int8> & right)
    {
        UInt64 rows = 0;
        for (const auto lhs : left)
            for (const auto rhs : right)
                rows += lhs == rhs;
        return rows;
    };

    for (const bool right_is_key : {false, true})
    {
        auto catalog = makeCatalog(
            {makeLeaf({"left_id"}, {0}), makeLeaf({"right_id"}, right_is_key ? std::vector<size_t>{0} : std::vector<size_t>{})});
        auto provider = makeProvider(catalog, {{0, 1}});
        const auto left_group = requireGroup(*provider, BitSet().set(0));
        const auto right_group = requireGroup(*provider, BitSet().set(1));
        const auto cut = getEqualityCutId(provider->getEqualityCut(left_group, right_group));
        ASSERT_TRUE(cut);

        for (const auto & left : bags)
        {
            if (!is_key_valid(left))
                continue;
            for (const auto & right : bags)
            {
                if (right_is_key && !is_key_valid(right))
                    continue;
                const auto cap = provider->inferCardinalityCapForCut(left_group, right_group, *cut, left.size(), right.size());
                ASSERT_NE(getProvenCap(cap), nullptr);
                EXPECT_LE(join_rows(left, right), getProvenCap(cap)->upper_bound)
                    << "right_is_key=" << right_is_key << ", left_size=" << left.size() << ", right_size=" << right.size();
            }
        }
    }

    /// Exhaust both legal three-way continuations with duplicate-bearing B.
    /// A and C are strong keys; the physical row counts below are computed
    /// independently from the provider.
    auto catalog = makeCatalog({makeLeaf({"a"}, {0}), makeLeaf({"b"}), makeLeaf({"c"}, {0})});
    auto provider = makeProvider(catalog, {{0, 1}, {1, 2}});
    const auto a_group = requireGroup(*provider, BitSet().set(0));
    const auto ab_group = requireGroup(*provider, BitSet().set(0).set(1));
    const auto bc_group = requireGroup(*provider, BitSet().set(1).set(2));
    const auto c_group = requireGroup(*provider, BitSet().set(2));
    const auto ab_c_cut = getEqualityCutId(provider->getEqualityCut(ab_group, c_group));
    const auto a_bc_cut = getEqualityCutId(provider->getEqualityCut(a_group, bc_group));
    ASSERT_TRUE(ab_c_cut);
    ASSERT_TRUE(a_bc_cut);

    for (const auto & a : bags)
    {
        if (!is_key_valid(a))
            continue;
        for (const auto & b : bags)
        {
            for (const auto & c : bags)
            {
                if (!is_key_valid(c))
                    continue;
                const UInt64 ab_rows = join_rows(a, b);
                const UInt64 bc_rows = join_rows(b, c);
                UInt64 abc_rows = 0;
                for (const auto av : a)
                    for (const auto bv : b)
                        for (const auto cv : c)
                            abc_rows += av == bv && bv == cv;

                const auto ab_c_cap = provider->inferCardinalityCapForCut(ab_group, c_group, *ab_c_cut, ab_rows, c.size());
                const auto a_bc_cap = provider->inferCardinalityCapForCut(a_group, bc_group, *a_bc_cut, a.size(), bc_rows);
                ASSERT_NE(getProvenCap(ab_c_cap), nullptr);
                ASSERT_NE(getProvenCap(a_bc_cap), nullptr);
                EXPECT_LE(abc_rows, getProvenCap(ab_c_cap)->upper_bound);
                EXPECT_LE(abc_rows, getProvenCap(a_bc_cap)->upper_bound);
            }
        }
    }
}

TEST(JoinOrderCanonicalProperties, EqualityCutDoesNotInsertCombinedGroupAndCachesSymmetricNegative)
{
    auto catalog = makeCatalog({makeLeaf({"left"}), makeLeaf({"middle"}), makeLeaf({"right"})});
    auto provider = makeProvider(catalog, {{0, 1}});
    const auto left = requireGroup(*provider, BitSet().set(0));
    const auto middle = requireGroup(*provider, BitSet().set(1));
    const auto right = requireGroup(*provider, BitSet().set(2));
    const auto groups_before_cuts = provider->getMetrics().groups;

    const auto negative = provider->getEqualityCut(left, right);
    EXPECT_TRUE(std::holds_alternative<JoinOrderNoEqualityCut>(negative));
    const auto reverse_negative = provider->getEqualityCut(right, left);
    EXPECT_EQ(reverse_negative, negative);
    auto metrics = provider->getMetrics();
    EXPECT_EQ(metrics.groups, groups_before_cuts);
    EXPECT_EQ(metrics.cuts, 0u);
    EXPECT_EQ(metrics.cut_cache_misses, 1u);
    EXPECT_EQ(metrics.cut_cache_hits, 1u);
    EXPECT_EQ(metrics.negative_cut_cache_hits, 1u);
    EXPECT_EQ(metrics.cut_scratch_initializations, 1u);
    EXPECT_GT(metrics.cut_scratch_capacity_changes, 0u);
    EXPECT_EQ(metrics.cut_scratch_uses, 1u);
    const auto initial_capacity_changes = metrics.cut_scratch_capacity_changes;

    const auto forward = getEqualityCutId(provider->getEqualityCut(left, middle));
    const auto reverse = getEqualityCutId(provider->getEqualityCut(middle, left));
    ASSERT_TRUE(forward);
    ASSERT_TRUE(reverse);
    EXPECT_NE(*forward, *reverse);
    metrics = provider->getMetrics();
    EXPECT_EQ(metrics.groups, groups_before_cuts);
    EXPECT_EQ(metrics.cuts, 2u);
    EXPECT_EQ(metrics.cut_scratch_initializations, 1u);
    EXPECT_EQ(metrics.cut_scratch_capacity_changes, initial_capacity_changes);
    EXPECT_EQ(metrics.cut_scratch_uses, 3u);
}

TEST(JoinOrderCanonicalProperties, UniquenessScratchResetsAndReusesAcrossMisses)
{
    auto catalog = makeCatalog({makeLeaf({"key"}, {0}), makeLeaf({"non_key"})});
    auto provider = makeProvider(catalog, {{0, 1}});
    const auto left = requireGroup(*provider, BitSet().set(0));
    const auto right = requireGroup(*provider, BitSet().set(1));
    const auto left_demand = requireDemand(*provider, {0});
    const auto right_demand = requireDemand(*provider, {1});

    const auto proven = provider->isUniqueOn(left, left_demand);
    ASSERT_NE(getUniquenessProof(proven), nullptr);
    const auto after_proven = provider->getMetrics();
    EXPECT_EQ(after_proven.uniqueness_scratch_initializations, 1u);
    EXPECT_EQ(after_proven.uniqueness_scratch_uses, 1u);
    EXPECT_GT(after_proven.uniqueness_scratch_capacity_changes, 0u);

    const auto not_proven = provider->isUniqueOn(right, right_demand);
    EXPECT_TRUE(std::holds_alternative<JoinOrderUniquenessNotProven>(not_proven));
    const auto after_not_proven = provider->getMetrics();
    EXPECT_EQ(after_not_proven.uniqueness_scratch_initializations, 1u);
    EXPECT_EQ(after_not_proven.uniqueness_scratch_capacity_changes, after_proven.uniqueness_scratch_capacity_changes);
    EXPECT_EQ(after_not_proven.uniqueness_scratch_uses, 2u);

    EXPECT_EQ(provider->isUniqueOn(left, left_demand), proven);
    EXPECT_EQ(provider->isUniqueOn(right, right_demand), not_proven);
    const auto after_hits = provider->getMetrics();
    EXPECT_EQ(after_hits.uniqueness_scratch_capacity_changes, after_proven.uniqueness_scratch_capacity_changes);
    EXPECT_EQ(after_hits.uniqueness_scratch_uses, 2u);
}

TEST(JoinOrderCanonicalProperties, MissingRowsBypassGroupAndUniquenessInference)
{
    auto catalog = makeCatalog({makeLeaf({"left"}, {0}), makeLeaf({"right"}, {0})});
    auto provider = makeProvider(catalog, {{0, 1}});
    const auto left = requireGroup(*provider, BitSet().set(0));
    const auto right = requireGroup(*provider, BitSet().set(1));
    const auto cut = getEqualityCutId(provider->getEqualityCut(left, right));
    ASSERT_TRUE(cut);
    const auto before = provider->getMetrics();

    const auto missing_left = provider->inferInnerAllCardinalityCap(UInt32{1}, UInt32{2}, std::nullopt, 0);
    const auto missing_right = provider->inferInnerAllCardinalityCap(UInt32{1}, UInt32{2}, 0, std::nullopt);
    ASSERT_NE(std::get_if<JoinOrderNoCardinalityCapReason>(&missing_left), nullptr);
    ASSERT_NE(std::get_if<JoinOrderNoCardinalityCapReason>(&missing_right), nullptr);
    EXPECT_EQ(std::get<JoinOrderNoCardinalityCapReason>(missing_left), JoinOrderNoCardinalityCapReason::MissingInputRows);
    EXPECT_EQ(std::get<JoinOrderNoCardinalityCapReason>(missing_right), JoinOrderNoCardinalityCapReason::MissingInputRows);
    auto after_missing = provider->getMetrics();
    EXPECT_EQ(after_missing.cache_misses, before.cache_misses);
    EXPECT_EQ(after_missing.uniqueness_scratch_uses, before.uniqueness_scratch_uses);
    EXPECT_EQ(after_missing.proofs, before.proofs);

    const auto zero_cap = provider->inferCardinalityCapForCut(left, right, *cut, 0, 0);
    ASSERT_NE(getProvenCap(zero_cap), nullptr);
    EXPECT_EQ(getProvenCap(zero_cap)->upper_bound, 0u);

    const auto invalid = provider->inferCardinalityCapForCut(JoinOrderLogicalGroupId{999, left.provider}, right, *cut, 0, 0);
    ASSERT_NE(std::get_if<JoinOrderPropertyUnsupportedReason>(&invalid), nullptr);
    EXPECT_EQ(std::get<JoinOrderPropertyUnsupportedReason>(invalid), JoinOrderPropertyUnsupportedReason::InvalidCut);
}

TEST(JoinOrderCanonicalProperties, PositiveNegativeAndUnsupportedAnswersAreCachedExactly)
{
    auto catalog = makeCatalog({makeLeaf({"id"}, {0}), makeLeaf({"other"})});
    auto provider = makeProvider(catalog, {{0, 1}});
    const auto left = requireGroup(*provider, BitSet().set(0));
    const auto demand = requireDemand(*provider, {0});

    const auto first = provider->isUniqueOn(left, demand);
    const auto second = provider->isUniqueOn(left, demand);
    EXPECT_EQ(first, second);
    ASSERT_NE(getUniquenessProof(first), nullptr);
    EXPECT_NE(getUniquenessProof(first)->proof.value, 0u);
    const auto metrics = provider->getMetrics();
    EXPECT_EQ(metrics.cache_misses, 1u);
    EXPECT_EQ(metrics.cache_hits, 1u);
    EXPECT_GE(metrics.key_firings, 1u);

    const auto right = requireGroup(*provider, BitSet().set(1));
    const auto cut = getEqualityCutId(provider->getEqualityCut(left, right));
    ASSERT_TRUE(cut);
    const auto proofs_before_caps = provider->getMetrics().proofs;
    const auto first_cap = provider->inferCardinalityCapForCut(left, right, *cut, 10, 20);
    const auto second_cap = provider->inferCardinalityCapForCut(left, right, *cut, 30, 40);
    ASSERT_NE(getProvenCap(first_cap), nullptr);
    ASSERT_NE(getProvenCap(second_cap), nullptr);
    EXPECT_EQ(getProvenCap(first_cap)->proof, getProvenCap(second_cap)->proof);
    EXPECT_EQ(provider->getMetrics().proofs, proofs_before_caps + 1);
}
