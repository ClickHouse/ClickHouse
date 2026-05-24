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

RangesInDataPartDescription makePart(const String & partition_id, Int64 min_block, Int64 max_block, UInt32 level, size_t marks)
{
    RangesInDataPartDescription desc;
    desc.info = MergeTreePartInfo(partition_id, min_block, max_block, level);
    desc.ranges = MarkRanges{MarkRange{0, marks}};
    desc.rows = marks * 8192;
    return desc;
}

/// Same as `makePart` but allows decoupling row count from mark count, used to simulate two
/// replicas whose local parts hold the same number of rows but with different mark layouts (for
/// example under adaptive granularity).
RangesInDataPartDescription makePartWithRows(
    const String & partition_id, Int64 min_block, Int64 max_block, UInt32 level, size_t marks, size_t rows)
{
    RangesInDataPartDescription desc;
    desc.info = MergeTreePartInfo(partition_id, min_block, max_block, level);
    desc.ranges = MarkRanges{MarkRange{0, marks}};
    desc.rows = rows;
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

}

/// Reproducer for the AST-fuzzer LOGICAL_ERROR
///   `Trying to get non existing mark 120, while size is 62`
/// (STID 4920-51f2, observed on PR #105706 on 2026-05-23).
///
/// The crash is triggered when `parallel_replicas_for_non_replicated_merge_tree = 1` is used on
/// a cluster whose members each have INDEPENDENT non-replicated `MergeTree` data: each member's
/// local "first part" is named `all_1_1_0` but stores a different number of marks. The coordinator
/// previously deduplicated parts purely by part info (name + version), so the second replica's
/// announcement of `all_1_1_0[(0, 61)]` was silently merged into the first replica's earlier
/// announcement of `all_1_1_0[(0, 128)]`. The coordinator then dispatched mark range `[120, 128)`
/// to the second replica, whose local copy only had 61 marks, triggering the LOGICAL_ERROR inside
/// `MergeTreeIndexGranularityConstant::getMarkRows`.
///
/// With the fix, the second `handleInitialAllRangesAnnouncement` raises `BAD_ARGUMENTS` with a
/// message that names the diverging part and points the user at `ReplicatedMergeTree`.
TEST(ParallelReplicasCoordinator, InOrderRejectsDivergentRangesForSamePart)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Replica 1 announces a 128-mark version of `all_1_1_0`.
    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePart("all", 1, 1, 0, /*marks=*/128));
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(parts)));
    }

    /// Replica 0 then announces a same-named part but with only 61 marks. Before the fix this is
    /// silently merged. After the fix it raises a BAD_ARGUMENTS Exception.
    RangesInDataPartsDescription divergent;
    divergent.push_back(makePart("all", 1, 1, 0, /*marks=*/61));
    EXPECT_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(divergent))),
        DB::Exception);
}

/// The same divergence must be rejected when announced in the opposite order.
TEST(ParallelReplicasCoordinator, InOrderRejectsDivergentRangesForSamePartReverseOrder)
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

namespace
{

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

/// Same check for `Default` coordination mode (consistent-hash dispatch). Without this check the
/// coordinator would later assign hash-mapped segments of replica-0's larger-version part to
/// replica-1, whose local copy is smaller, producing the same crash inside the mark-range reader.
TEST(ParallelReplicasCoordinator, DefaultRejectsDivergentRangesForSamePart)
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

/// Two replicas announce the same part with the same row count but different mark layouts (the
/// adaptive-granularity case raised by `clickhouse-gh[bot]` review on PR #105710). Without the
/// mark-space check the coordinator would accept the merge and later hand out marks beyond the
/// smaller replica's local mark space.
TEST(ParallelReplicasCoordinator, InOrderRejectsDivergentMarkLayoutForSamePart)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    /// Replica 1 announces the part with 100K rows split across 12 marks.
    {
        RangesInDataPartsDescription parts;
        parts.push_back(makePartWithRows("all", 1, 1, 0, /*marks=*/12, /*rows=*/100'000));
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(parts)));
    }

    /// Replica 0 announces the same row count but its local part has 20 marks.
    RangesInDataPartsDescription divergent;
    divergent.push_back(makePartWithRows("all", 1, 1, 0, /*marks=*/20, /*rows=*/100'000));
    EXPECT_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/0, std::move(divergent))),
        DB::Exception);
}

/// The same mark-layout divergence must also be rejected in `Default` coordination mode.
TEST(ParallelReplicasCoordinator, DefaultRejectsDivergentMarkLayoutForSamePart)
{
    ParallelReplicasReadingCoordinator coordinator(/*replicas_count_=*/2);

    RangesInDataPartsDescription parts;
    parts.push_back(makePartWithRows("all", 1, 1, 0, /*marks=*/12, /*rows=*/100'000));
    coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/0, std::move(parts)));

    RangesInDataPartsDescription divergent;
    divergent.push_back(makePartWithRows("all", 1, 1, 0, /*marks=*/20, /*rows=*/100'000));
    EXPECT_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeDefaultAnnouncement(/*replica_num=*/1, std::move(divergent))),
        DB::Exception);
}

/// Regression test: after the first replica announces and the coordinator dispatches some marks
/// in response to a read request, `all_parts_to_read[i].description.ranges` is consumed in place
/// (popped or shrunk). A subsequent replica announcing the same part with the SAME original
/// layout must still be accepted; the divergence check must compare against the immutable
/// snapshot taken at first announcement, not against the live (consumed) `description.ranges`.
/// Before the snapshot fix, this case incorrectly raised `BAD_ARGUMENTS` because the live
/// `description` showed `rows=N marks=0 max_end=0` after dispatch while the second announcement
/// showed `rows=N marks=M max_end=M`, breaking ~11k parallel-replicas Fast tests on PR #105710.
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

    /// Replica 1 now announces the same part with the same layout. Without the snapshot fix this
    /// would raise BAD_ARGUMENTS because the live `description.ranges` has been popped to empty.
    RangesInDataPartsDescription same_announcement;
    same_announcement.push_back(makePart("all", 1, 1, 0, /*marks=*/8));
    EXPECT_NO_THROW(
        coordinator.handleInitialAllRangesAnnouncement(makeAnnouncement(/*replica_num=*/1, std::move(same_announcement))));
}

/// Same regression in `Default` coordination mode: range dispatch consumes
/// `all_parts_to_read[i].description.ranges` via `pop_front` / `range.begin += taken`, and a
/// subsequent identical announcement must still be accepted.
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
