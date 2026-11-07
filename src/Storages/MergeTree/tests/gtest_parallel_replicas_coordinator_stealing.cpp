#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>
#include <Storages/MergeTree/RequestResponse.h>

#include <gtest/gtest.h>

#include <map>
#include <vector>

using namespace DB;

namespace
{

constexpr size_t mark_segment_size = 128;
/// Half a segment, so an owner's request takes only a part of a segment.
constexpr size_t min_marks_per_request = 64;
const String stream_id = "t";

/// Ranges do not start or end at segment boundaries, so the first and last segments of each range are partial.
RangesInDataPartsDescription makeParts()
{
    RangesInDataPartsDescription parts;
    for (Int64 block = 1; block <= 4; ++block)
    {
        RangesInDataPartDescription part;
        part.info = MergeTreePartInfo("all", block, block, 0);
        part.ranges = MarkRanges{{37, 900}, {950, 1203}};
        part.rows = 1203 * 8192;
        part.min_marks_per_task = 8;
        parts.push_back(std::move(part));
    }
    return parts;
}

void announce(ParallelReplicasReadingCoordinator & coordinator, size_t replica_num)
{
    coordinator.handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement(
        CoordinationMode::Default,
        makeParts(),
        replica_num,
        mark_segment_size,
        min_marks_per_request,
        stream_id,
        /*wants_analyzed_ranges=*/false));
}

ParallelReadResponse request(ParallelReplicasReadingCoordinator & coordinator, size_t replica_num)
{
    return coordinator.handleRequest(
        ParallelReadRequest(CoordinationMode::Default, replica_num, min_marks_per_request, RangesInDataPartsDescription{}, stream_id));
}

/// How many times each mark of each part was handed out.
using Coverage = std::map<String, std::vector<size_t>>;

void addToCoverage(Coverage & coverage, const RangesInDataPartsDescription & description)
{
    for (const auto & part : description)
    {
        auto & marks = coverage[part.getPartOrProjectionName()];
        for (const auto & range : part.ranges)
        {
            if (marks.size() < range.end)
                marks.resize(range.end);
            for (size_t mark = range.begin; mark < range.end; ++mark)
                ++marks[mark];
        }
    }
}

/// Every announced mark must be handed out exactly once, and nothing else.
void expectExactCoverage(const Coverage & coverage)
{
    Coverage expected;
    addToCoverage(expected, makeParts());

    ASSERT_EQ(coverage.size(), expected.size());
    for (const auto & [part_name, expected_marks] : expected)
    {
        auto it = coverage.find(part_name);
        ASSERT_NE(it, coverage.end()) << part_name;
        auto marks = it->second;
        marks.resize(std::max(marks.size(), expected_marks.size()));
        for (size_t mark = 0; mark < marks.size(); ++mark)
        {
            const size_t want = mark < expected_marks.size() ? expected_marks[mark] : 0;
            ASSERT_EQ(marks[mark], want) << "part " << part_name << ", mark " << mark;
        }
    }
}

/// Reads with `replica_num` until the coordinator says it's finished.
void drain(ParallelReplicasReadingCoordinator & coordinator, size_t replica_num, Coverage & coverage)
{
    for (size_t i = 0; i < 100'000; ++i)
    {
        auto response = request(coordinator, replica_num);
        addToCoverage(coverage, response.description);
        if (response.finish)
            return;
    }
    FAIL() << "Replica " << replica_num << " did not finish";
}

}

/// Both replicas are announced and available. Replica 0 takes its first task and then never asks again,
/// as a replica that is too slow to come back would. Replica 1 must be able to read everything else: no
/// segment may be reserved for stealing by its own owner.
TEST(ParallelReplicasCoordinator, IdleReplicaDrainsStalledOwner)
{
    ParallelReplicasReadingCoordinator coordinator(2);
    announce(coordinator, 0);
    announce(coordinator, 1);

    Coverage coverage;
    const auto first = request(coordinator, 0);
    ASSERT_FALSE(first.finish);
    addToCoverage(coverage, first.description);

    /// The stalled replica holds less than a segment: the rest of that segment must still reach replica 1.
    size_t first_marks = 0;
    for (const auto & part : first.description)
        first_marks += part.ranges.getNumberOfMarks();
    ASSERT_EQ(first_marks, min_marks_per_request);
    ASSERT_LT(first_marks, mark_segment_size);

    drain(coordinator, 1, coverage);
    expectExactCoverage(coverage);
}

/// The same with the roles swapped: the stalled replica is not the one whose announcement became the snapshot.
TEST(ParallelReplicasCoordinator, SnapshotReplicaDrainsStalledFollower)
{
    ParallelReplicasReadingCoordinator coordinator(2);
    announce(coordinator, 0);
    announce(coordinator, 1);

    Coverage coverage;
    const auto first = request(coordinator, 1);
    ASSERT_FALSE(first.finish);
    addToCoverage(coverage, first.description);

    drain(coordinator, 0, coverage);
    expectExactCoverage(coverage);
}

/// With one replica there is nobody to steal for: it reads every mark itself, exactly once.
TEST(ParallelReplicasCoordinator, SingleReplicaReadsEverything)
{
    ParallelReplicasReadingCoordinator coordinator(1);
    announce(coordinator, 0);

    Coverage coverage;
    drain(coordinator, 0, coverage);
    expectExactCoverage(coverage);
}
