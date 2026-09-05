#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Core/SortDescription.h>

using namespace DB;

namespace
{

SortColumnDescription sortColumn(const String & name, int direction = 1, int nulls_direction = 1)
{
    SortColumnDescription column;
    column.column_name = name;
    column.direction = direction;
    column.nulls_direction = nulls_direction;
    return column;
}

SortDescription sortBy(std::initializer_list<SortColumnDescription> columns)
{
    SortDescription description;
    for (const auto & column : columns)
        description.push_back(column);
    return description;
}

ExpressionProperties sorted(SortDescription sorting)
{
    ExpressionProperties properties;
    properties.sorting = std::move(sorting);
    return properties;
}

ExpressionProperties singleStream()
{
    ExpressionProperties properties;
    properties.stream_layout = StreamLayout::Single;
    return properties;
}

ExpressionProperties disjointStreams(DistributionColumns columns)
{
    ExpressionProperties properties;
    properties.setDisjointStreams(std::move(columns));
    return properties;
}

ExpressionProperties distributed(size_t node_count, bool is_replicated, DistributionColumns columns = {}, Names hash_type_names = {})
{
    ExpressionProperties properties;
    properties.distribution.node_count = node_count;
    properties.distribution.is_replicated = is_replicated;
    properties.distribution.columns = std::move(columns);
    properties.distribution.hash_type_names = std::move(hash_type_names);
    return properties;
}

size_t hashOf(const ExpressionProperties & properties)
{
    return ExpressionPropertiesHash{}(properties);
}

}

/// The hash must be consistent with equality: equal properties hash equally, and properties
/// that compare unequal because of a sorting field used by operator== must hash differently.
/// This is what lets `ORDER BY k ASC` and `ORDER BY k DESC` coexist as distinct alternatives.
TEST(CascadesProperties, SortingHashMatchesEquality)
{
    const auto asc = sorted(sortBy({sortColumn("k", 1)}));
    const auto desc = sorted(sortBy({sortColumn("k", -1)}));
    const auto asc_again = sorted(sortBy({sortColumn("k", 1)}));

    EXPECT_EQ(asc, asc_again);
    EXPECT_EQ(hashOf(asc), hashOf(asc_again));

    EXPECT_NE(asc, desc);
    EXPECT_NE(hashOf(asc), hashOf(desc));

    /// NULLS FIRST vs NULLS LAST is also part of identity.
    const auto nulls_first = sorted(sortBy({sortColumn("k", 1, 1)}));
    const auto nulls_last = sorted(sortBy({sortColumn("k", 1, -1)}));
    EXPECT_NE(nulls_first, nulls_last);
    EXPECT_NE(hashOf(nulls_first), hashOf(nulls_last));
}

/// Fields that SortColumnDescription::operator== ignores (alias, with_fill) must not affect
/// either equality or the hash.
TEST(CascadesProperties, SortingIgnoresNonComparedFields)
{
    auto base = sortColumn("k");
    auto with_alias = sortColumn("k");
    with_alias.alias = "renamed";
    auto with_fill = sortColumn("k");
    with_fill.with_fill = true;

    const auto a = sorted(sortBy({base}));
    const auto b = sorted(sortBy({with_alias}));
    const auto c = sorted(sortBy({with_fill}));

    EXPECT_EQ(a, b);
    EXPECT_EQ(hashOf(a), hashOf(b));
    EXPECT_EQ(a, c);
    EXPECT_EQ(hashOf(a), hashOf(c));
}

TEST(CascadesProperties, DistributionShapeHash)
{
    EXPECT_NE(hashOf(distributed(4, false)), hashOf(distributed(2, false)));
    EXPECT_NE(hashOf(distributed(4, false)), hashOf(distributed(4, true)));
    EXPECT_EQ(hashOf(distributed(4, false)), hashOf(distributed(4, false)));
}

/// Required sorting is satisfied only by an existing sorting that has it as a prefix, with the
/// exact same direction/nulls handling per column.
TEST(CascadesProperties, SortingSatisfaction)
{
    const auto by_k = sortBy({sortColumn("k")});
    const auto by_k_ts = sortBy({sortColumn("k"), sortColumn("ts")});

    /// A prefix requirement is satisfied by a longer existing sort.
    EXPECT_TRUE(ExpressionProperties::isSortingSatisfiedBy(by_k, by_k_ts));
    /// A longer requirement is not satisfied by a shorter sort.
    EXPECT_FALSE(ExpressionProperties::isSortingSatisfiedBy(by_k_ts, by_k));
    /// Opposite direction does not satisfy.
    EXPECT_FALSE(ExpressionProperties::isSortingSatisfiedBy(
        sortBy({sortColumn("k", 1)}), sortBy({sortColumn("k", -1)})));
    /// An empty requirement is always satisfied.
    EXPECT_TRUE(ExpressionProperties::isSortingSatisfiedBy(SortDescription{}, by_k));
}

TEST(CascadesProperties, DistributionSatisfaction)
{
    /// node_count and replication must match exactly.
    EXPECT_FALSE(ExpressionProperties::isDistributionSatisfiedBy(
        distributed(4, false).distribution, distributed(2, false).distribution));
    EXPECT_FALSE(ExpressionProperties::isDistributionSatisfiedBy(
        distributed(4, false).distribution, distributed(4, true).distribution));

    /// An empty required column list accepts any same-shape distribution.
    EXPECT_TRUE(ExpressionProperties::isDistributionSatisfiedBy(
        distributed(4, false).distribution,
        distributed(4, false, {NameSet{"a"}}).distribution));

    /// Column matching is positional: (a, b) is not satisfied by (b, a).
    const auto required_ab = distributed(4, false, {NameSet{"a"}, NameSet{"b"}}).distribution;
    const auto existing_ba = distributed(4, false, {NameSet{"b"}, NameSet{"a"}}).distribution;
    EXPECT_FALSE(ExpressionProperties::isDistributionSatisfiedBy(required_ab, existing_ba));
    EXPECT_TRUE(ExpressionProperties::isDistributionSatisfiedBy(
        required_ab, distributed(4, false, {NameSet{"a"}, NameSet{"b"}}).distribution));

    /// Hash cast types must agree, so equal key columns still land in the same bucket.
    auto required_typed = distributed(4, false, {NameSet{"a"}}, Names{"UInt64"}).distribution;
    auto existing_other_type = distributed(4, false, {NameSet{"a"}}, Names{"UInt32"}).distribution;
    EXPECT_FALSE(ExpressionProperties::isDistributionSatisfiedBy(required_typed, existing_other_type));

    /// A required name is satisfied by an equivalent name present in the existing set.
    const auto required_a = distributed(4, false, {NameSet{"a"}}).distribution;
    const auto existing_equiv = distributed(4, false, {NameSet{"a", "b"}}).distribution;
    EXPECT_TRUE(ExpressionProperties::isDistributionSatisfiedBy(required_a, existing_equiv));
}

TEST(CascadesProperties, StreamLayoutSatisfaction)
{
    const auto unknown = ExpressionProperties{};
    const auto single = singleStream();
    const auto by_a = disjointStreams({NameSet{"a"}});
    const auto by_ab = disjointStreams({NameSet{"a"}, NameSet{"b"}});

    /// An unknown requirement accepts every layout.
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(unknown, unknown));
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(unknown, single));
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(unknown, by_a));

    /// One stream keeps every group whole, so Single satisfies everything.
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(single, single));
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_a, single));
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_ab, single));

    /// Unknown and Disjoint do not satisfy a Single requirement.
    EXPECT_FALSE(ExpressionProperties::isStreamLayoutSatisfiedBy(single, unknown));
    EXPECT_FALSE(ExpressionProperties::isStreamLayoutSatisfiedBy(single, by_a));

    /// Streams disjoint on a subset of the required columns keep the required groups whole;
    /// a superset splits them.
    EXPECT_FALSE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_a, unknown));
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_a, by_a));
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_ab, by_a));
    EXPECT_FALSE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_a, by_ab));

    /// Different columns do not satisfy each other.
    const auto by_c = disjointStreams({NameSet{"c"}});
    EXPECT_FALSE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_a, by_c));

    /// An equivalent name in the existing set matches the required set.
    const auto by_a_equiv = disjointStreams({NameSet{"x", "a"}});
    EXPECT_TRUE(ExpressionProperties::isStreamLayoutSatisfiedBy(by_a, by_a_equiv));
}

/// `setDisjointStreams` orders the column sets canonically, so the same disjointness
/// written in a different key order stays one goal for the memo bookkeeping.
TEST(CascadesProperties, StreamLayoutHashAndEquality)
{
    const auto ab = disjointStreams({NameSet{"a"}, NameSet{"b"}});
    const auto ba = disjointStreams({NameSet{"b"}, NameSet{"a"}});
    EXPECT_EQ(ab, ba);
    EXPECT_EQ(hashOf(ab), hashOf(ba));

    /// Sets that share their smallest name still get one canonical order.
    const auto shared_min = disjointStreams({NameSet{"a", "b"}, NameSet{"a", "c"}});
    const auto shared_min_swapped = disjointStreams({NameSet{"a", "c"}, NameSet{"a", "b"}});
    EXPECT_EQ(shared_min, shared_min_swapped);
    EXPECT_EQ(hashOf(shared_min), hashOf(shared_min_swapped));

    EXPECT_NE(disjointStreams({NameSet{"a"}}), disjointStreams({NameSet{"b"}}));
    EXPECT_NE(hashOf(disjointStreams({NameSet{"a"}})), hashOf(disjointStreams({NameSet{"b"}})));

    EXPECT_NE(ExpressionProperties{}, singleStream());
    EXPECT_NE(hashOf(ExpressionProperties{}), hashOf(singleStream()));
    EXPECT_NE(singleStream(), disjointStreams({NameSet{"a"}}));
}

/// The layout takes part in full-properties satisfaction alongside sorting and distribution.
TEST(CascadesProperties, StreamLayoutInIsSatisfiedBy)
{
    auto required = sorted(sortBy({sortColumn("k")}));
    required.setDisjointStreams({NameSet{"k"}});

    auto existing_plain = sorted(sortBy({sortColumn("k")}));
    EXPECT_FALSE(required.isSatisfiedBy(existing_plain));

    auto existing_single = existing_plain;
    existing_single.stream_layout = StreamLayout::Single;
    EXPECT_TRUE(required.isSatisfiedBy(existing_single));

    auto existing_disjoint = sorted(sortBy({sortColumn("k")}));
    existing_disjoint.setDisjointStreams({NameSet{"k"}});
    EXPECT_TRUE(required.isSatisfiedBy(existing_disjoint));
}
