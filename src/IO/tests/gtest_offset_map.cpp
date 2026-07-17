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

    const auto * o = map.findObjectAt(100);
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->object.remote_path, "obj_a");
    EXPECT_EQ(o->file_offset, 0u);

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

    const auto * a = map.findObjectAt(0);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->object.remote_path, "blob_0");
    EXPECT_EQ(a->file_offset, 0u);

    const auto * b = map.findObjectAt(300);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(b->object.remote_path, "blob_1");
    EXPECT_EQ(b->file_offset, 300u);

    const auto * c = map.findObjectAt(800);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(c->object.remote_path, "blob_2");
    EXPECT_EQ(c->file_offset, 800u);

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

    const auto * o = map.findObjectAt(100);  // first byte of the second object
    ASSERT_NE(o, nullptr);
    EXPECT_EQ(o->object.remote_path, "b");
    EXPECT_EQ(o->file_offset, 100u);

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
    EXPECT_EQ(o->object.remote_path, "obj");
}

TEST(OffsetMap, MapWithinOneObject)
{
    StoredObjects objects;
    objects.emplace_back("a", "", 300);
    objects.emplace_back("b", "", 300);

    OffsetMap map;
    map.build(objects);

    /// A range wholly inside the first object maps to one piece, object-local.
    auto pieces = map.map(ByteRange{50, 100});
    ASSERT_EQ(pieces.size(), 1u);
    EXPECT_EQ(pieces[0].object.remote_path, "a");
    EXPECT_EQ(pieces[0].object_offset, 50u);
    EXPECT_EQ(pieces[0].size, 100u);
}

TEST(OffsetMap, MapSpansObjectBoundary)
{
    StoredObjects objects;
    objects.emplace_back("a", "", 300);
    objects.emplace_back("b", "", 300);
    objects.emplace_back("c", "", 300);

    OffsetMap map;
    map.build(objects);

    /// [250, 650) spans the tail of `a`, all of `b`, and the head of `c`.
    auto pieces = map.map(ByteRange{250, 400});
    ASSERT_EQ(pieces.size(), 3u);

    EXPECT_EQ(pieces[0].object.remote_path, "a");
    EXPECT_EQ(pieces[0].object_offset, 250u);
    EXPECT_EQ(pieces[0].size, 50u);       // [250, 300)

    EXPECT_EQ(pieces[1].object.remote_path, "b");
    EXPECT_EQ(pieces[1].object_offset, 0u);
    EXPECT_EQ(pieces[1].size, 300u);      // [300, 600)

    EXPECT_EQ(pieces[2].object.remote_path, "c");
    EXPECT_EQ(pieces[2].object_offset, 0u);
    EXPECT_EQ(pieces[2].size, 50u);       // [600, 650)

    /// The sizes reconstruct the requested length exactly.
    EXPECT_EQ(pieces[0].size + pieces[1].size + pieces[2].size, 400u);
}

TEST(OffsetMap, MapClampsToTotalSize)
{
    StoredObjects objects;
    objects.emplace_back("a", "", 100);
    objects.emplace_back("b", "", 100);

    OffsetMap map;
    map.build(objects);

    /// A range extending past the file end is clipped to the last object's end.
    auto pieces = map.map(ByteRange{150, 500});
    ASSERT_EQ(pieces.size(), 1u);
    EXPECT_EQ(pieces[0].object.remote_path, "b");
    EXPECT_EQ(pieces[0].object_offset, 50u);
    EXPECT_EQ(pieces[0].size, 50u);       // [150, 200), not past 200

    /// A range at/after the end maps to nothing.
    EXPECT_TRUE(map.map(ByteRange{200, 100}).empty());
}
