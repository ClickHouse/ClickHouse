#include <IO/OffsetMap.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(OffsetMap, SingleObject)
{
    StoredObjects objects;
    objects.emplace_back("obj_a", "", 1000);

    OffsetMap map;
    map.build(objects);
    EXPECT_EQ(map.totalSize(), 1000u);
    EXPECT_FALSE(map.hasUnknownSize());

    size_t file_offset = 12345;
    const auto * o = map.findObjectAt(100, &file_offset);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->remote_path, "obj_a");
    EXPECT_EQ(file_offset, 0u);

    EXPECT_NE(map.findObjectAt(999), nullptr);
    EXPECT_EQ(map.findObjectAt(1000), nullptr);  // at end
    EXPECT_EQ(map.findObjectAt(5000), nullptr);  // past end
}

TEST(OffsetMap, MultipleObjects)
{
    StoredObjects objects;
    objects.emplace_back("blob_0", "", 300);
    objects.emplace_back("blob_1", "", 500);
    objects.emplace_back("blob_2", "", 200);

    OffsetMap map;
    map.build(objects);
    EXPECT_EQ(map.totalSize(), 1000u);

    size_t file_offset = 0;
    const auto * a = map.findObjectAt(0, &file_offset);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->remote_path, "blob_0");
    EXPECT_EQ(file_offset, 0u);

    const auto * b = map.findObjectAt(300, &file_offset);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(b->remote_path, "blob_1");
    EXPECT_EQ(file_offset, 300u);

    const auto * c = map.findObjectAt(800, &file_offset);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->remote_path, "blob_2");
    EXPECT_EQ(file_offset, 800u);

    EXPECT_NE(map.findObjectAt(999), nullptr);
    EXPECT_EQ(map.findObjectAt(1000), nullptr);
}

TEST(OffsetMap, ObjectBoundary)
{
    StoredObjects objects;
    objects.emplace_back("a", "", 100);
    objects.emplace_back("b", "", 100);

    OffsetMap map;
    map.build(objects);

    size_t file_offset = 0;
    const auto * o = map.findObjectAt(100, &file_offset);  // first byte of the second object
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->remote_path, "b");
    EXPECT_EQ(file_offset, 100u);

    EXPECT_EQ(map.findObjectAt(200), nullptr);
}

TEST(OffsetMap, UnknownSize)
{
    StoredObjects objects;
    objects.emplace_back("obj", "", StoredObject::UnknownSize);

    OffsetMap map;
    map.build(objects);
    EXPECT_TRUE(map.hasUnknownSize());
    EXPECT_EQ(map.totalSize(), StoredObject::UnknownSize);

    /// Any offset below the sentinel resolves to the single object.
    const auto * o = map.findObjectAt(1'000'000);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->remote_path, "obj");
}
