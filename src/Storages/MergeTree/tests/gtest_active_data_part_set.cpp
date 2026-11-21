#include <gtest/gtest.h>

#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/OverlappingPartCovering.h>
#include <Common/Exception.h>

using namespace DB;

namespace
{
constexpr auto FORMAT_VERSION = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
}

/// Two parts can both be marked "uncovered" (no other part contains either of them) and
/// still intersect, because containment and intersection are independent relations:
///
///   `all_0_5_2`  covers blocks 0..5  level 2
///   `all_2_11_3` covers blocks 2..11 level 3
///
/// Neither contains the other (the min/max block ranges are not nested), but they share
/// blocks 2..5. This is the exact failure mode reproduced by the BuzzHouse fuzzer in
/// `StorageReplicatedMergeTree::checkPartsImpl` when both parts pass the
/// `uncovered == true` filter and are then passed to `ActiveDataPartSet::add` (STID
/// `2352-49be`). Use `tryAdd` to return `HasIntersectingPart` without throwing.
TEST(ActiveDataPartSet, IntersectingPartsWithoutContainmentTryAddReturnsOutcome)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_0_5_2"));

    String reason;
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::HasIntersectingPart, set.tryAdd("all_2_11_3", &reason));
    EXPECT_FALSE(reason.empty());

    /// The set still contains the originally-added part and nothing else.
    EXPECT_EQ(1u, set.size());
    EXPECT_EQ("all_0_5_2", set.getContainingPart("all_0_5_1"));
}

/// Symmetric direction: insert the larger range first, then the smaller intersecting one.
/// Confirms the intersection check fires when scanning to the left of the lower bound
/// in `addImpl`.
TEST(ActiveDataPartSet, IntersectingPartsWithoutContainmentTryAddReverseOrder)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_2_11_3"));

    String reason;
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::HasIntersectingPart, set.tryAdd("all_0_5_2", &reason));
    EXPECT_FALSE(reason.empty());
    EXPECT_EQ(1u, set.size());
}

TEST(ActiveDataPartSet, ContainedPartReturnsHasCovering)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_0_11_3"));
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::HasCovering, set.tryAdd("all_2_5_1"));
}

TEST(ActiveDataPartSet, DisjointPartsBothAdd)
{
    ActiveDataPartSet set(FORMAT_VERSION);

    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_0_5_2"));
    EXPECT_EQ(ActiveDataPartSet::AddPartOutcome::Added, set.tryAdd("all_6_11_3"));
    EXPECT_EQ(2u, set.size());
}


/// `OverlappingPartCovering` tests ---------------------------------------------------------------
///
/// Bot review on PR #103537 surfaced a downstream concern with the previous "tryAdd + skip" fix:
/// silently dropping the second of two intersecting empty markers can still trip
/// `TOO_MANY_UNEXPECTED_DATA_PARTS` later, because the dropped marker may have been the only one
/// covering some unrelated unexpected part. `OverlappingPartCovering` keeps every marker as a
/// covering candidate via an auxiliary list, so coverage detection is correct regardless of which
/// of two intersecting markers is the one that lands in the underlying `ActiveDataPartSet`.

/// Bot's exact scenario, in the order that triggered the original abort:
/// markers `all_0_5_2` (blocks 0..5) and `all_2_11_3` (blocks 2..11) plus an unexpected part
/// `all_9_10_1` whose range is contained only by `all_2_11_3`. The previous fix (`tryAdd` + skip)
/// would drop `all_2_11_3` and therefore fail to cover `all_9_10_1`. With
/// `OverlappingPartCovering`, the auxiliary list keeps `all_2_11_3` as a covering candidate.
TEST(OverlappingPartCovering, IntersectingMarkersStillCoverDownstreamParts)
{
    OverlappingPartCovering markers(FORMAT_VERSION);
    markers.add("all_0_5_2");
    markers.add("all_2_11_3");

    EXPECT_EQ(2u, markers.size());
    /// Exactly one marker is in the auxiliary "overlapping" list — whichever was added second.
    EXPECT_EQ(1u, markers.getOverlappingParts().size());

    /// The unexpected part `all_9_10_1` is contained ONLY by `all_2_11_3`. With the simple
    /// "skip on intersection" fix this could resolve to "" depending on iteration order; with the
    /// auxiliary-list fix it always resolves to `all_2_11_3`.
    EXPECT_EQ("all_2_11_3", markers.getContainingPart("all_9_10_1"));

    /// Coverage by the other marker still works.
    EXPECT_EQ("all_0_5_2", markers.getContainingPart("all_0_5_1"));

    /// A part outside both ranges is uncovered.
    EXPECT_TRUE(markers.getContainingPart("all_12_15_1").empty());
}

/// Same scenario but the markers are inserted in the opposite order. Confirms the fix is order
/// independent — the second marker can land in the primary set or in the auxiliary list and
/// either way the unexpected part is correctly classified as covered.
TEST(OverlappingPartCovering, IntersectingMarkersOrderIndependent)
{
    OverlappingPartCovering markers(FORMAT_VERSION);
    markers.add("all_2_11_3");
    markers.add("all_0_5_2");

    EXPECT_EQ(2u, markers.size());
    EXPECT_EQ(1u, markers.getOverlappingParts().size());
    EXPECT_EQ("all_2_11_3", markers.getContainingPart("all_9_10_1"));
    EXPECT_EQ("all_0_5_2", markers.getContainingPart("all_0_5_1"));
}

/// A part contained by either intersecting marker should resolve to one of them — never to "".
/// This is the most common downstream check: an unexpected non-empty part on disk falls inside
/// the union range of the empty markers.
TEST(OverlappingPartCovering, PartCoveredByBothMarkers)
{
    OverlappingPartCovering markers(FORMAT_VERSION);
    markers.add("all_0_5_2");
    markers.add("all_2_11_3");

    /// Blocks 3..4 fall inside both `all_0_5_2` and `all_2_11_3`. Either is a valid answer; the
    /// implementation prefers the primary set's match over the auxiliary list.
    String covering = markers.getContainingPart("all_3_4_1");
    EXPECT_TRUE(covering == "all_0_5_2" || covering == "all_2_11_3") << "got " << covering;
}

/// `add` must accept a part that is fully contained by an existing one without growing the set
/// (the existing entry already implies coverage). This mirrors `ActiveDataPartSet::tryAddPart`
/// returning `HasCovering`.
TEST(OverlappingPartCovering, ContainedPartIsNotStored)
{
    OverlappingPartCovering markers(FORMAT_VERSION);
    markers.add("all_0_11_3");
    markers.add("all_2_5_1");

    EXPECT_EQ(1u, markers.size());
    EXPECT_TRUE(markers.getOverlappingParts().empty());
    EXPECT_EQ("all_0_11_3", markers.getContainingPart("all_2_5_1"));
}

/// Disjoint markers go into the primary set and produce an empty overlapping list.
TEST(OverlappingPartCovering, DisjointMarkersGoIntoPrimarySet)
{
    OverlappingPartCovering markers(FORMAT_VERSION);
    markers.add("all_0_5_2");
    markers.add("all_6_11_3");

    EXPECT_EQ(2u, markers.size());
    EXPECT_TRUE(markers.getOverlappingParts().empty());
    EXPECT_EQ("all_0_5_2", markers.getContainingPart("all_0_3_1"));
    EXPECT_EQ("all_6_11_3", markers.getContainingPart("all_8_10_1"));
}

/// Three markers, two of which intersect. The third disjoint marker still works; the two
/// intersecting markers both contribute to coverage via the primary set + auxiliary list.
TEST(OverlappingPartCovering, MixedMarkersAllParticipateInCoverage)
{
    OverlappingPartCovering markers(FORMAT_VERSION);
    markers.add("all_0_5_2");      /// primary set
    markers.add("all_2_11_3");     /// auxiliary list (intersects all_0_5_2 without containment)
    markers.add("all_20_30_2");    /// primary set (disjoint from both)

    EXPECT_EQ(3u, markers.size());
    EXPECT_EQ(1u, markers.getOverlappingParts().size());

    EXPECT_EQ("all_0_5_2", markers.getContainingPart("all_1_2_1"));
    EXPECT_EQ("all_2_11_3", markers.getContainingPart("all_9_10_1"));
    EXPECT_EQ("all_20_30_2", markers.getContainingPart("all_25_28_1"));
    EXPECT_TRUE(markers.getContainingPart("all_15_18_1").empty());
}

/// `getParts` returns every stored part, primary + auxiliary, suitable for diagnostic logging.
TEST(OverlappingPartCovering, GetPartsListsEverything)
{
    OverlappingPartCovering markers(FORMAT_VERSION);
    markers.add("all_0_5_2");
    markers.add("all_2_11_3");
    markers.add("all_20_30_2");

    Strings parts = markers.getParts();
    EXPECT_EQ(3u, parts.size());

    /// All three names must be present (order is not part of the contract).
    auto contains_part = [&](const String & name)
    {
        return std::find(parts.begin(), parts.end(), name) != parts.end();
    };
    EXPECT_TRUE(contains_part("all_0_5_2"));
    EXPECT_TRUE(contains_part("all_2_11_3"));
    EXPECT_TRUE(contains_part("all_20_30_2"));
}

/// Empty set behaves correctly.
TEST(OverlappingPartCovering, EmptySet)
{
    OverlappingPartCovering markers(FORMAT_VERSION);

    EXPECT_TRUE(markers.empty());
    EXPECT_EQ(0u, markers.size());
    EXPECT_TRUE(markers.getContainingPart("all_0_5_1").empty());
}
