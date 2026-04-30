#include <IO/Rope.h>
#include <gtest/gtest.h>

#include <cstring>

using namespace DB;

TEST(Range, Basic)
{
    Range r{100, 50};
    EXPECT_EQ(r.offset, 100);
    EXPECT_EQ(r.size, 50);
    EXPECT_EQ(r.end(), 150);
}

TEST(Range, ZeroSize)
{
    Range r{0, 0};
    EXPECT_EQ(r.end(), 0);
}

TEST(OwnedRopeBuffer, AllocateAndAccess)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(1024);
    EXPECT_EQ(buf->size(), 1024);
    EXPECT_NE(buf->data(), nullptr);
    std::memset(buf->data(), 'A', 1024);
    EXPECT_EQ(buf->data()[0], 'A');
    EXPECT_EQ(buf->data()[1023], 'A');
}

TEST(RopeNode, SliceOfBuffer)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(1024);
    std::memset(buf->data(), 0, 1024);
    buf->data()[100] = 'X';
    buf->data()[199] = 'Y';
    RopeNode node{buf, 100, 100, 5000};
    EXPECT_EQ(node.data()[0], 'X');
    EXPECT_EQ(node.data()[99], 'Y');
    EXPECT_EQ(node.size, 100);
    EXPECT_EQ(node.range().offset, 5000);
    EXPECT_EQ(node.range().size, 100);
}

TEST(RopeNode, SharedOwnership)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(512);
    RopeNode node1{buf, 0, 256, 0};
    RopeNode node2{buf, 256, 256, 256};
    EXPECT_EQ(buf.use_count(), 3);
    EXPECT_EQ(node1.data(), buf->data());
    EXPECT_EQ(node2.data(), buf->data() + 256);
}

TEST(Rope, AppendAndIterate)
{
    auto buf1 = std::make_shared<OwnedRopeBuffer>(100);
    std::memset(buf1->data(), 'A', 100);
    auto buf2 = std::make_shared<OwnedRopeBuffer>(200);
    std::memset(buf2->data(), 'B', 200);

    Rope rope;
    rope.append(RopeNode{buf1, 0, 100, 0});
    rope.append(RopeNode{buf2, 0, 200, 100});

    EXPECT_EQ(rope.range().offset, 0);
    EXPECT_EQ(rope.range().size, 300);
    EXPECT_EQ(rope.getNodes().size(), 2);
    EXPECT_FALSE(rope.empty());
}

TEST(Rope, AppendRope)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(100);

    Rope rope1;
    rope1.append(RopeNode{buf, 0, 50, 0});

    Rope rope2;
    rope2.append(RopeNode{buf, 50, 50, 50});

    rope1.append(std::move(rope2));
    EXPECT_EQ(rope1.getNodes().size(), 2);
    EXPECT_EQ(rope1.range().size, 100);
}

TEST(Rope, SliceFullRange)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(300);
    std::memset(buf->data(), 'X', 300);

    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 100, 100, 100});
    rope.append(RopeNode{buf, 200, 100, 200});

    auto slice = rope.slice(Range{0, 300});
    EXPECT_EQ(slice.range().offset, 0);
    EXPECT_EQ(slice.range().size, 300);
    EXPECT_EQ(slice.totalBytes(), 300);
    EXPECT_EQ(slice.getNodes().size(), 3);
}

TEST(Rope, SliceMiddle)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(300);

    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 100, 100, 100});
    rope.append(RopeNode{buf, 200, 100, 200});

    auto slice = rope.slice(Range{50, 200});
    EXPECT_EQ(slice.range().offset, 50);
    EXPECT_EQ(slice.range().size, 200);
    EXPECT_EQ(slice.totalBytes(), 200);

    const auto & nodes = slice.getNodes();
    EXPECT_EQ(nodes[0].logical_offset, 50);
    EXPECT_EQ(nodes[0].size, 50);
    EXPECT_EQ(nodes[0].buffer_offset, 50);
    EXPECT_EQ(nodes[1].logical_offset, 100);
    EXPECT_EQ(nodes[1].size, 100);
    EXPECT_EQ(nodes[2].logical_offset, 200);
    EXPECT_EQ(nodes[2].size, 50);
}

TEST(Rope, SliceSingleNodeMiddle)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(1000);

    Rope rope;
    rope.append(RopeNode{buf, 0, 1000, 0});

    auto slice = rope.slice(Range{100, 200});
    EXPECT_EQ(slice.getNodes().size(), 1);
    EXPECT_EQ(slice.getNodes()[0].buffer_offset, 100);
    EXPECT_EQ(slice.getNodes()[0].size, 200);
    EXPECT_EQ(slice.getNodes()[0].logical_offset, 100);
}

TEST(Rope, EmptyRope)
{
    Rope rope;
    EXPECT_TRUE(rope.empty());
    EXPECT_EQ(rope.range().size, 0);
}

TEST(Rope, SliceKeepsBufferAlive)
{
    std::weak_ptr<RopeBuffer> weak;
    Rope slice;
    {
        auto buf = std::make_shared<OwnedRopeBuffer>(64);
        weak = buf;

        Rope rope;
        rope.append(RopeNode{buf, 0, 64, 0});
        slice = rope.slice(Range{0, 64});
    }
    EXPECT_FALSE(weak.expired());
}

TEST(Rope, TotalBytes)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(300);

    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 100, 150, 100});

    EXPECT_EQ(rope.totalBytes(), 250);
}

TEST(Rope, PopFront)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(300);
    std::memset(buf->data(), 'A', 100);
    std::memset(buf->data() + 100, 'B', 100);
    std::memset(buf->data() + 200, 'C', 100);

    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 100, 100, 100});
    rope.append(RopeNode{buf, 200, 100, 200});

    auto node1 = rope.popFront();
    EXPECT_EQ(node1.logical_offset, 0);
    EXPECT_EQ(node1.size, 100);
    EXPECT_EQ(node1.data()[0], 'A');
    EXPECT_EQ(rope.getNodes().size(), 2);

    auto node2 = rope.popFront();
    EXPECT_EQ(node2.logical_offset, 100);
    EXPECT_EQ(node2.data()[0], 'B');
    EXPECT_EQ(rope.getNodes().size(), 1);

    auto node3 = rope.popFront();
    EXPECT_EQ(node3.logical_offset, 200);
    EXPECT_EQ(node3.data()[0], 'C');
    EXPECT_TRUE(rope.empty());
}

TEST(Rope, PopFrontReleasesBuffer)
{
    std::weak_ptr<RopeBuffer> weak;
    {
        auto buf = std::make_shared<OwnedRopeBuffer>(64);
        weak = buf;

        Rope rope;
        rope.append(RopeNode{buf, 0, 64, 0});
        buf.reset(); /// only rope holds the ref now

        auto node = rope.popFront();
        /// node holds the last ref
        EXPECT_FALSE(weak.expired());
    }
    /// node destroyed, buffer freed
    EXPECT_TRUE(weak.expired());
}
