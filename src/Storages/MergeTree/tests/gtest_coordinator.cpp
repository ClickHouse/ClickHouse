#include <gtest/gtest.h>

#include <utility>
#include <limits>
#include <set>

#include <Storages/MergeTree/IntersectionsIndexes.h>

#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

using namespace DB;

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

    for (int i = 0; i < response.mark_ranges.size(); ++i) {
        EXPECT_EQ(response.mark_ranges[i], request.mark_ranges[i]);
    }

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
    for (int i = 0; i < response.mark_ranges.size(); ++i) {
        EXPECT_EQ(response.mark_ranges[i], first.mark_ranges[i]);
    }

    response = coordinator.handleRequest(second);
    ASSERT_FALSE(response.denied);
    ASSERT_EQ(response.mark_ranges.size(), 1);
    ASSERT_EQ(response.mark_ranges.front(), (MarkRange{3, 4}));
}



TEST(Coordinator, Boundaries)
{
    {
        PartRangesIntersectionsIndex boundaries;

        boundaries.addPart(PartToRead{{1, 1}, "Test"});
        boundaries.addPart(PartToRead{{2, 2}, "Test"});
        boundaries.addPart(PartToRead{{3, 3}, "Test"});
        boundaries.addPart(PartToRead{{4, 4}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 4}), 4);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 5}), 4);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 5}), 4);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 1}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 2}), 2);

        boundaries.addPart(PartToRead{{5, 5}, "Test"});
        boundaries.addPart(PartToRead{{0, 0}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 5}), 6);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 1}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 2}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 3}), 4);
    }

    {
        PartRangesIntersectionsIndex boundaries;
        boundaries.addPart(PartToRead{{1, 3}, "Test"});
        boundaries.addPart(PartToRead{{3, 5}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({2, 4}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 6}), 2);
    }

    {
        PartRangesIntersectionsIndex boundaries;
        boundaries.addPart(PartToRead{{1, 3}, "Test"});
        boundaries.addPart(PartToRead{{4, 6}, "Test"});
        boundaries.addPart(PartToRead{{7, 9}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({2, 8}), 3);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({4, 6}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 7}), 3);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({5, 7}), 2);
    }

    {
        PartRangesIntersectionsIndex boundaries;

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 1}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 3}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 100500}), 0);
    }
}


TEST(Coordinator, MarkBoundaries)
{
    {
        MarkRangesIntersectionsIndex boundaries;

        boundaries.addRange({1, 2});
        boundaries.addRange({2, 3});
        boundaries.addRange({3, 4});
        boundaries.addRange({4, 5});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 4}), 3);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 5}), 4);
    }

    {
        MarkRangesIntersectionsIndex boundaries;
        boundaries.addRange({1, 3});
        boundaries.addRange({3, 5});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 5}), 1);

        boundaries.addRange({5, 7});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 5}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 7}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({4, 6}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 7}), 3);
    }

    {
        MarkRangesIntersectionsIndex boundaries;
        boundaries.addRange({1, 3});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 4}), 0);
    }

    {
        MarkRangesIntersectionsIndex boundaries;
        boundaries.addRange({1, 2});
        boundaries.addRange({3, 4});
        boundaries.addRange({5, 6});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({2, 3}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({4, 5}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 6}), 3);
    }
}
