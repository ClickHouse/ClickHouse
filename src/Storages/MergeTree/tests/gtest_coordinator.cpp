#include <gtest/gtest.h>

#include <utility>
#include <limits>
#include <set>

#include <Storages/MergeTree/IntersectionsIndexes.h>

#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

using namespace DB;


TEST(HalfIntervals, Simple)
{
    ASSERT_TRUE((
        HalfIntervals{{{1, 2}, {3, 4}}}.negate() ==
        HalfIntervals{{{0, 1}, {2, 3}, {4, 18446744073709551615UL}}}
    ));

    {
        auto left = HalfIntervals{{{0, 2}, {4, 6}}}.negate();
        ASSERT_TRUE((
            left ==
            HalfIntervals{{{2, 4}, {6, 18446744073709551615UL}}}
        ));
    }

    {
        auto left = HalfIntervals{{{0, 2}, {4, 6}}};
        auto right = HalfIntervals{{{1, 5}}}.negate();
        auto intersection = left.intersect(right);

        ASSERT_TRUE((
            intersection ==
            HalfIntervals{{{0, 1}, {5, 6}}}
        ));
    }

    {
        auto left = HalfIntervals{{{1, 2}, {2, 3}}};
        auto right = HalfIntervals::initializeWithEntireSpace();
        auto intersection = right.intersect(left.negate());

        ASSERT_TRUE((
            intersection ==
            HalfIntervals{{{0, 1}, {3, 18446744073709551615UL}}}
        ));
    }

    {
        auto left = HalfIntervals{{{1, 2}, {2, 3}, {3, 4}, {4, 5}}};

        ASSERT_EQ(getIntersection(left, HalfIntervals{{{1, 4}}}).convertToMarkRangesFinal().size(), 3);
        ASSERT_EQ(getIntersection(left, HalfIntervals{{{1, 5}}}).convertToMarkRangesFinal().size(), 4);
    }

    {
        auto left = HalfIntervals{{{1, 3}, {3, 5}, {5, 7}}};

        ASSERT_EQ(getIntersection(left, HalfIntervals{{{3, 5}}}).convertToMarkRangesFinal().size(), 1);
        ASSERT_EQ(getIntersection(left, HalfIntervals{{{3, 7}}}).convertToMarkRangesFinal().size(), 2);
        ASSERT_EQ(getIntersection(left, HalfIntervals{{{4, 6}}}).convertToMarkRangesFinal().size(), 2);
        ASSERT_EQ(getIntersection(left, HalfIntervals{{{1, 7}}}).convertToMarkRangesFinal().size(), 3);
    }

    {
        auto left = HalfIntervals{{{1, 3}}};

        ASSERT_EQ(getIntersection(left, HalfIntervals{{{3, 4}}}).convertToMarkRangesFinal().size(), 0);
    }

    {
        auto left = HalfIntervals{{{1, 2}, {3, 4}, {5, 6}}};

        ASSERT_EQ(getIntersection(left, HalfIntervals{{{2, 3}}}).convertToMarkRangesFinal().size(), 0);
        ASSERT_EQ(getIntersection(left, HalfIntervals{{{4, 5}}}).convertToMarkRangesFinal().size(), 0);
        ASSERT_EQ(getIntersection(left, HalfIntervals{{{1, 6}}}).convertToMarkRangesFinal().size(), 3);
    }
}

TEST(HalfIntervals, TwoRequests)
{
    auto left = HalfIntervals{{{1, 2}, {2, 3}}};
    auto right = HalfIntervals{{{2, 3}, {3, 4}}};
    auto intersection = left.intersect(right);

    ASSERT_TRUE((
        intersection ==
        HalfIntervals{{{2, 3}}}
    ));

    /// With negation
    left = HalfIntervals{{{1, 2}, {2, 3}}}.negate();
    right = HalfIntervals{{{2, 3}, {3, 4}}};
    intersection = left.intersect(right);


    ASSERT_TRUE((
        intersection ==
        HalfIntervals{{{3, 4}}}
    ));
}

TEST(HalfIntervals, SelfIntersection)
{
    auto left = HalfIntervals{{{1, 2}, {2, 3}, {4, 5}}};
    auto right = left;
    auto intersection = left.intersect(right);

    ASSERT_TRUE((
        intersection == right
    ));

    left = HalfIntervals{{{1, 2}, {2, 3}, {4, 5}}};
    right = left;
    right.negate();
    intersection = left.intersect(right);

    ASSERT_TRUE((
        intersection == HalfIntervals{}
    ));
}


TEST(Coordinator, Simple)
{
    PartitionReadRequest request;
    request.partition_id = "a";
    request.part_name = "b";
    request.projection_name = "c";
    request.block_range = PartBlockRange{1, 2};
    request.mark_ranges = MarkRanges{{1, 2}, {3, 4}};

    ParallelReplicasReadingCoordinator coordinator;
    auto response = coordinator.handleRequest(request);

    ASSERT_FALSE(response.denied) << "Process request at first has to be accepted";

    ASSERT_EQ(response.mark_ranges.size(), request.mark_ranges.size());

    for (int i = 0; i < response.mark_ranges.size(); ++i)
        EXPECT_EQ(response.mark_ranges[i], request.mark_ranges[i]);

    response = coordinator.handleRequest(request);
    ASSERT_TRUE(response.denied) << "Process the same request second time";
}


TEST(Coordinator, TwoRequests)
{
    PartitionReadRequest first;
    first.partition_id = "a";
    first.part_name = "b";
    first.projection_name = "c";
    first.block_range = PartBlockRange{0, 0};
    first.mark_ranges = MarkRanges{{1, 2}, {2, 3}};

    auto second = first;
    second.mark_ranges = MarkRanges{{2, 3}, {3, 4}};

    ParallelReplicasReadingCoordinator coordinator;
    auto response = coordinator.handleRequest(first);

    ASSERT_FALSE(response.denied) << "First request must me accepted";

    ASSERT_EQ(response.mark_ranges.size(), first.mark_ranges.size());
    for (int i = 0; i < response.mark_ranges.size(); ++i)
        EXPECT_EQ(response.mark_ranges[i], first.mark_ranges[i]);

    response = coordinator.handleRequest(second);
    ASSERT_FALSE(response.denied);
    ASSERT_EQ(response.mark_ranges.size(), 1);
    ASSERT_EQ(response.mark_ranges.front(), (MarkRange{3, 4}));
}


TEST(Coordinator, PartIntersections)
{
    {
        PartSegments boundaries;

        boundaries.addPart(PartToRead{{1, 1}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{2, 2}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{3, 3}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{4, 4}, {"TestPart", "TestProjection"}});

        ASSERT_EQ(boundaries.getIntersectionResult({{1, 4}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{0, 5}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 1}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::EXACTLY_ONE_INTERSECTION);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 1}, {"ClickHouse", "AnotherProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 2}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);

        boundaries.addPart(PartToRead{{5, 5}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{0, 0}, {"TestPart", "TestProjection"}});

        ASSERT_EQ(boundaries.getIntersectionResult({{0, 5}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 1}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::EXACTLY_ONE_INTERSECTION);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 1}, {"ClickHouse", "AnotherProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 2}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{0, 3}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
    }

    {
        PartSegments boundaries;
        boundaries.addPart(PartToRead{{1, 3}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{4, 5}, {"TestPart", "TestProjection"}});

        ASSERT_EQ(boundaries.getIntersectionResult({{2, 4}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{0, 6}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
    }

    {
        PartSegments boundaries;
        boundaries.addPart(PartToRead{{1, 3}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{4, 6}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{7, 9}, {"TestPart", "TestProjection"}});

        ASSERT_EQ(boundaries.getIntersectionResult({{2, 8}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{4, 6}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::EXACTLY_ONE_INTERSECTION);
        ASSERT_EQ(boundaries.getIntersectionResult({{3, 7}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{5, 7}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
    }

    {
        PartSegments boundaries;

        ASSERT_EQ(boundaries.getIntersectionResult({{1, 1}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::NO_INTERSECTION);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 3}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::NO_INTERSECTION);
        ASSERT_EQ(boundaries.getIntersectionResult({{0, 100500}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::NO_INTERSECTION);

        boundaries.addPart(PartToRead{{1, 1}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{2, 2}, {"TestPart", "TestProjection"}});
        boundaries.addPart(PartToRead{{3, 3}, {"TestPart", "TestProjection"}});

        ASSERT_EQ(boundaries.getIntersectionResult({{1, 1}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::EXACTLY_ONE_INTERSECTION);
        ASSERT_EQ(boundaries.getIntersectionResult({{1, 3}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::REJECT);
        ASSERT_EQ(boundaries.getIntersectionResult({{100, 100500}, {"TestPart", "TestProjection"}}), PartSegments::IntersectionResult::NO_INTERSECTION);
    }
}
