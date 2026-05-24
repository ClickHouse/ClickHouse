#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <Storages/MergeTree/MarkRange.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <utility>

using namespace DB;

namespace
{

/// Builds a `RangesInDataPartDescription` whose analyzed view (`ranges` / `rows`) AND underlying
/// total mark count both equal `marks`. This is the simplest shape: the part has `marks` marks
/// on disk and the announcing replica analyzed all of them.
RangesInDataPartDescription makePart(const String & partition_id, Int64 min_block, Int64 max_block, UInt32 level, size_t marks)
{
    RangesInDataPartDescription desc;
    desc.info = MergeTreePartInfo(partition_id, min_block, max_block, level);
    desc.ranges = MarkRanges{MarkRange{0, marks}};
    desc.rows = marks * 8192;
    desc.total_marks_in_part = marks;
    return desc;
}

/// Builds a `RangesInDataPartDescription` that decouples the analyzed view from the underlying
/// part. `analyzed_marks` is the per-replica PK / skip-index-selected mark count (what the
/// coordinator sees in `description.ranges`); `total_marks` is the on-disk part's actual mark
/// count (what every replica should agree on for the same underlying part). Used to simulate
/// either legitimate divergent analysis of the same part (different `analyzed_marks`, same
/// `total_marks`) or divergent underlying data (different `total_marks`).
RangesInDataPartDescription makePartWithAnalyzedAndTotal(
    const String & partition_id,
    Int64 min_block,
    Int64 max_block,
    UInt32 level,
    size_t analyzed_marks,
    size_t total_marks,
    size_t rows)
{
    RangesInDataPartDescription desc;
    desc.info = MergeTreePartInfo(partition_id, min_block, max_block, level);
    desc.ranges = MarkRanges{MarkRange{0, analyzed_marks}};
    desc.rows = rows;
    desc.total_marks_in_part = total_marks;
    return desc;
}

InitialAllRangesAnnouncement makeAnnouncement(size_t replica_num, RangesInDataPartsDescription parts)
{
    return InitialAllRangesAnnouncement(
        CoordinationMode::WithOrder,
        std::move(parts),
        replica_num,
        /*mark_segment_size=*/0,
        /*min_marks_per_request=*/24,
        /*stream_id=*/"default.t2");
}

InitialAllRangesAnnouncement makeDefaultAnnouncement(size_t replica_num, RangesInDataPartsDescription parts)
{
    return InitialAllRangesAnnouncement(
        CoordinationMode::Default,
        std::move(parts),
        replica_num,
        /*mark_segment_size=*/128,
        /*min_marks_per_request=*/24,
        /*stream_id=*/"default.t2");
}

}

/// Reproducer for the AST-fuzzer LOGICAL_ERROR
///   `Trying to get non existing mark 120, while size is 62`
/// (STID 4920-51f2, observed on PR #105706 on 2026-05-23).
///
/// The crash is triggered when `parallel_replicas_for_non_replicated_merge_tree = 1` is used on
/// a cluster whose members each have INDEPENDENT non-replicated `MergeTree` data: each member's
/// local "first part" is named `all_1_1_0` but the underlying parts on disk hold a different
/// number of marks. The coordinator previously deduplicated parts purely by part info (name +
/// version), so the second replica's announcement of a 61-mark underlying part was silently
/// merged into the first replica's earlier announcement of a 128-mark underlying part. The
/// coordinator then dispatched mark range `[120, 128)` to the second replica, whose local copy
/// only had 61 marks, triggering the LOGICAL_ERROR inside `MergeTreeIndexGranularityConstant::getMarkRows`.
///
/// With the fix, the second `handleInitialAllRangesAnnouncement` raises `BAD_ARGUMENTS` because
/// `description.total_marks_in_part` differs between the two announcements.
TEST(ParallelReplicasCoordinator, InOrderRejectsDivergentTotalMarksInPart)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Replica 1 announces a part whose underlying disk version has 128 marks.
    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(parts)));
    }

    /// Replica 0 then announces a same-named part but with only 61 underlying marks. Before the
    /// fix this is silently merged. After the fix it raises a BAD_ARGUMENTS Exception.
    RangesInDataPartsDescription divergent;
    divergent.push_back(makePart("all", 1, 1, 0, /*marks=*/61));
    EXPECT_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(divergent))),
        DB::Exception);
}

/// The same divergence must be rejected when announced in the opposite order.
TEST(ParallelReplicasCoordinator, InOrderRejectsDivergentTotalMarksInPartReverseOrder)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePart("all", 1, 1, 0, /*marks=*/61));
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    RangesInDataPartsDescription divergent;
    divergent.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
    EXPECT_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(divergent))),
        DB::Exception);
}

/// Identical announcements from two replicas (the normal `ReplicatedMergeTree` case) must continue
/// to work and just record both replicas as owners of the part.
TEST(ParallelReplicasCoordinator, InOrderAcceptsIdenticalAnnouncementsFromMultipleReplicas)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    RangesInDataPartsDescription parts1;
    parts1.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
    coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(parts1)));

    RangesInDataPartsDescription parts2;
    parts2.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(parts2))));
}

/// Same check for `Default` coordination mode (consistent-hash dispatch). Without this check the
/// coordinator would later assign hash-mapped segments of replica-0's larger-version part to
/// replica-1, whose local copy is smaller, producing the same crash inside the mark-range reader.
TEST(ParallelReplicasCoordinator, DefaultRejectsDivergentTotalMarksInPart)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    RangesInDataPartsDescription parts;
    parts.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
    coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts)));

    RangesInDataPartsDescription divergent;
    divergent.push_back(makePart("all", 1, 1, 0, /*marks=*/61));
    EXPECT_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(divergent))),
        DB::Exception);
}

/// Default coordinator accepts identical announcements from multiple replicas (normal case).
TEST(ParallelReplicasCoordinator, DefaultAcceptsIdenticalAnnouncementsFromMultipleReplicas)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    RangesInDataPartsDescription parts1;
    parts1.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
    coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts1)));

    RangesInDataPartsDescription parts2;
    parts2.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(parts2))));
}

/// Regression test: after the first replica announces and the coordinator dispatches some marks
/// in response to a read request, `all_parts_to_read[i].description.ranges` is consumed in place
/// (popped or shrunk). A subsequent replica announcing the same part with the SAME underlying
/// total mark count must still be accepted; the divergence check must compare against the
/// snapshot taken at first announcement (`Part::initial_total_marks_in_part`), not against the
/// live (consumed) `description.ranges`.
TEST(ParallelReplicasCoordinator, InOrderAcceptsIdenticalAnnouncementAfterDispatch)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Replica 0 announces a part with 8 marks and then drains the coordinator's range queue.
    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    RangesInDataPartsDescription request_parts;
    request_parts.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
    request_parts.front().ranges.clear();  // request carries only part identity, no mark ranges
    ParallelReadRequest request(
        CoordinationMode::WithOrder,
        /*replica_num=*/0,
        /*min_marks_per_request=*/1000,  // ask for more than the part has, drains everything
        std::move(request_parts),
        /*stream_id=*/"default.t2");
    auto response = coordinator.handleRequest(std::move(request));
    EXPECT_FALSE(response.description.empty());

    /// Replica 1 now announces the same part with the same underlying layout. Without the
    /// snapshot fix this would raise BAD_ARGUMENTS because the live `description.ranges` has
    /// been popped to empty.
    RangesInDataPartsDescription same_announcement;
    same_announcement.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(same_announcement))));
}

/// Same regression in `Default` coordination mode.
TEST(ParallelReplicasCoordinator, DefaultAcceptsIdenticalAnnouncementAfterDispatch)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    RangesInDataPartsDescription request_parts;
    request_parts.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
    request_parts.front().ranges.clear();
    ParallelReadRequest request(
        CoordinationMode::Default,
        /*replica_num=*/0,
        /*min_marks_per_request=*/1000,
        std::move(request_parts),
        /*stream_id=*/"default.t2");
    coordinator.handleRequest(std::move(request));

    RangesInDataPartsDescription same_announcement;
    same_announcement.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(same_announcement))));
}

/// Regression test for the iteration-3 false-positive on PR #105710 (CIDB tests
/// `03928_materialized_cte_index_2` and `04028_pr_move_global_in_to_prewhere`): two replicas
/// observe the SAME underlying part on disk but produce different analyzed views from local PK
/// or skip-index analysis. For example, the first replica's local statistics might prune all
/// but 4 marks while another replica selects all 10000 marks of the same 10000-mark part. The
/// previous check rejected this as divergent ("Replica 0 announced part all_1_1_0 with 10000
/// rows / 10000 marks, but an earlier replica announced ... with 4 rows / 4 marks ..."), but
/// the parts are identical on disk and dispatch is safe.
///
/// The fix compares `total_marks_in_part` (the underlying part's mark count, invariant across
/// replicas) instead of analyzed-view fields, so this case is now accepted.
TEST(ParallelReplicasCoordinator, InOrderAcceptsDivergentAnalyzedViewForSamePart)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Replica 1 reports that its local index analysis pruned the 10000-mark part down to just
    /// 4 marks (4 rows in the analyzed view).
    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePartWithAnalyzedAndTotal(
            "all", 1, 1, 0, /*analyzed_marks=*/4, /*total_marks=*/10000, /*rows=*/4));
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(parts)));
    }

    /// Replica 0 reports the same underlying 10000-mark part but selected all 10000 marks.
    /// Underlying-part totals match, so the announcement must be accepted.
    RangesInDataPartsDescription divergent_view;
    divergent_view.push_back(makePartWithAnalyzedAndTotal(
        "all", 1, 1, 0, /*analyzed_marks=*/10000, /*total_marks=*/10000, /*rows=*/10000));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(divergent_view))));
}

/// Same regression in `Default` coordination mode.
TEST(ParallelReplicasCoordinator, DefaultAcceptsDivergentAnalyzedViewForSamePart)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePartWithAnalyzedAndTotal(
            "all", 1, 1, 0, /*analyzed_marks=*/1, /*total_marks=*/122, /*rows=*/8192));
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    RangesInDataPartsDescription divergent_view;
    divergent_view.push_back(makePartWithAnalyzedAndTotal(
        "all", 1, 1, 0, /*analyzed_marks=*/122, /*total_marks=*/122, /*rows=*/1'000'000));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(divergent_view))));
}

/// Regression test for the iteration-5 perf concern raised on PR #105710: with a large parts
/// snapshot and many post-snapshot announcements, `DefaultCoordinator::initializeReadingState`
/// previously did `std::find_if` over the full `all_parts_to_read` per announced part, producing
/// O(parts^2) work in startup. The fix replaces the scan with an `unordered_map` index built
/// once after `all_parts_to_read` is sorted. This test exercises the post-snapshot path with
/// many parts to guard the correctness of that index: every same-named-but-divergent part must
/// still be rejected, every same-named identical part must still be accepted, and parts with
/// names that are NOT in the snapshot must be ignored.
TEST(ParallelReplicasCoordinator, DefaultDivergenceCheckUsesIndexForManyParts)
{
    constexpr size_t num_parts = 32;
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Replica 0 announces `num_parts` distinct same-block-level parts with 8 underlying marks
    /// each. After the initial announcement the coordinator's snapshot is final and sorted; the
    /// index maps every part name to its position in `all_parts_to_read`.
    {
        RangesInDataPartsDescription parts;
        for (size_t i = 0; i < num_parts; ++i)
            parts.push_back(makePart("all", static_cast<Int64>(i + 1), static_cast<Int64>(i + 1), 0, /*marks=*/8));
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    /// Replica 1 re-announces all `num_parts` parts with identical underlying layout: the index
    /// must locate each one and the divergence check must accept all of them.
    RangesInDataPartsDescription parts_identical;
    for (size_t i = 0; i < num_parts; ++i)
        parts_identical.push_back(makePart("all", static_cast<Int64>(i + 1), static_cast<Int64>(i + 1), 0, /*marks=*/8));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(parts_identical))));
}

TEST(ParallelReplicasCoordinator, DefaultDivergenceCheckRejectsViaIndex)
{
    constexpr size_t num_parts = 32;
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Snapshot from replica 0: each part has 8 underlying marks.
    {
        RangesInDataPartsDescription parts;
        for (size_t i = 0; i < num_parts; ++i)
            parts.push_back(makePart("all", static_cast<Int64>(i + 1), static_cast<Int64>(i + 1), 0, /*marks=*/8));
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    /// Replica 1 re-announces but the part in the middle of the set has a DIFFERENT underlying
    /// mark count. The index must correctly locate that one same-named entry (it is not the
    /// first or last in either the original announcement order or the sorted snapshot order)
    /// and `sameLocalLayout` must reject it.
    RangesInDataPartsDescription parts_divergent;
    for (size_t i = 0; i < num_parts; ++i)
    {
        size_t marks = (i == num_parts / 2) ? 5 : 8;
        parts_divergent.push_back(makePart("all", static_cast<Int64>(i + 1), static_cast<Int64>(i + 1), 0, /*marks=*/marks));
    }
    EXPECT_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(parts_divergent))),
        DB::Exception);
}

TEST(ParallelReplicasCoordinator, DefaultDivergenceCheckIgnoresUnknownNames)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Snapshot from replica 0 contains only one part named `all_1_1_0`.
    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    /// Replica 1 announces a DIFFERENT part name (`all_2_2_0`). The coordinator's working set
    /// is frozen to the first replica's snapshot, so this part is just discarded. The index
    /// lookup must miss cleanly without throwing.
    RangesInDataPartsDescription parts2;
    parts2.push_back(makePart("all", 2, 2, 0, /*marks=*/8));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(parts2))));
}

/// Backward-compatibility: announcements that do not carry `total_marks_in_part` (older replica
/// protocol versions that predate `DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_TOTAL_MARKS_IN_PART`)
/// arrive with `total_marks_in_part == 0`. The coordinator must skip divergence validation in
/// that case rather than reject every announcement; otherwise mixed-version clusters would fail
/// every parallel-replicas query.
TEST(ParallelReplicasCoordinator, InOrderSkipsValidationWhenTotalMarksUnset)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// First announcement: older replica that doesn't populate `total_marks_in_part`.
    {
        RangesInDataPartDescription part;
        part.info = MergeTreePartInfo("all", 1, 1, 0);
        part.ranges = MarkRanges{MarkRange{0, 8}};
        part.rows = 65536;
        /// `total_marks_in_part` left at default 0.
        RangesInDataPartsDescription parts;
        parts.push_back(part);
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(parts)));
    }

    /// Second announcement: newer replica that DOES populate `total_marks_in_part`. Even though
    /// the two `total_marks_in_part` values disagree (0 vs 8), the divergence check must be
    /// skipped because the snapshot side is unset.
    RangesInDataPartsDescription parts2;
    parts2.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(parts2))));
}
