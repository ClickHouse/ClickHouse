#include <IO/OffsetMap.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(OffsetMap, SingleObject)
{
    StoredObjects objects;
    objects.emplace_back("obj_a", "", 1000);

    OffsetMap map;
    map.build(objects);
    EXPECT_EQ(map.totalSize(), 1000);

    auto ranges = map.map(Range{100, 200});
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].object.remote_path, "obj_a");
    EXPECT_EQ(ranges[0].object_offset, 100);
    EXPECT_EQ(ranges[0].size, 200);
}

TEST(OffsetMap, MultipleObjects)
{
    StoredObjects objects;
    objects.emplace_back("blob_0", "", 300);
    objects.emplace_back("blob_1", "", 500);
    objects.emplace_back("blob_2", "", 200);

    OffsetMap map;
    map.build(objects);
    EXPECT_EQ(map.totalSize(), 1000);

    /// Range fully within first object
    auto r1 = map.map(Range{0, 100});
    ASSERT_EQ(r1.size(), 1);
    EXPECT_EQ(r1[0].object.remote_path, "blob_0");
    EXPECT_EQ(r1[0].object_offset, 0);
    EXPECT_EQ(r1[0].size, 100);

    /// Range spanning first and second objects
    auto r2 = map.map(Range{200, 200});
    ASSERT_EQ(r2.size(), 2);
    EXPECT_EQ(r2[0].object.remote_path, "blob_0");
    EXPECT_EQ(r2[0].object_offset, 200);
    EXPECT_EQ(r2[0].size, 100);
    EXPECT_EQ(r2[1].object.remote_path, "blob_1");
    EXPECT_EQ(r2[1].object_offset, 0);
    EXPECT_EQ(r2[1].size, 100);

    /// Range spanning all three objects
    auto r3 = map.map(Range{250, 700});
    ASSERT_EQ(r3.size(), 3);
    EXPECT_EQ(r3[0].size, 50);
    EXPECT_EQ(r3[1].size, 500);
    EXPECT_EQ(r3[2].size, 150);
}

TEST(OffsetMap, RangeAtObjectBoundary)
{
    StoredObjects objects;
    objects.emplace_back("a", "", 100);
    objects.emplace_back("b", "", 100);

    OffsetMap map;
    map.build(objects);

    auto r = map.map(Range{100, 50});
    ASSERT_EQ(r.size(), 1);
    EXPECT_EQ(r[0].object.remote_path, "b");
    EXPECT_EQ(r[0].object_offset, 0);
    EXPECT_EQ(r[0].size, 50);
}
