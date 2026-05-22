#include <IO/Rope.h>
#include <gtest/gtest.h>

#include <cstring>

using namespace DB;

TEST(ByteRange, Basic)
{
    ByteRange r{100, 50};
    EXPECT_EQ(r.offset, 100);
    EXPECT_EQ(r.size, 50);
    EXPECT_EQ(r.end(), 150);
}

TEST(ByteRange, ZeroSize)
{
    ByteRange r{0, 0};
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

    auto slice = rope.slice(ByteRange{0, 300});
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

    auto slice = rope.slice(ByteRange{50, 200});
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

    auto slice = rope.slice(ByteRange{100, 200});
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
        slice = rope.slice(ByteRange{0, 64});
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

namespace
{
    /// Build a Rope of `count` adjacent nodes, each `node_size` bytes, starting
    /// at `start_offset`. Node i's bytes are `0x10 + i` repeated.
    Rope makeAdjacentRope(size_t count, size_t node_size, size_t start_offset = 0)
    {
        Rope rope;
        for (size_t i = 0; i < count; ++i)
        {
            auto buf = std::make_shared<OwnedRopeBuffer>(node_size);
            std::memset(buf->data(), static_cast<int>(0x10 + i), node_size);
            rope.append(RopeNode{buf, 0, node_size, start_offset + i * node_size});
        }
        return rope;
    }
}

TEST(Rope, CoveredBytesMatchesActualCoverage)
{
    /// Three 10-byte nodes covering [0, 30). Query coverage of various sub-ranges.
    auto rope = makeAdjacentRope(/*count=*/3, /*node_size=*/10);
    EXPECT_EQ(rope.coveredBytes({0, 30}), 30u);
    EXPECT_EQ(rope.coveredBytes({5, 20}), 20u);
    EXPECT_EQ(rope.coveredBytes({0, 100}), 30u);  // over-asks; covered <= req
    EXPECT_EQ(rope.coveredBytes({30, 10}), 0u);   // entirely outside
    EXPECT_EQ(rope.coveredBytes({100, 50}), 0u);
}

TEST(Rope, GapsReturnsMissingRanges)
{
    /// Two nodes [0, 10) and [20, 30). Gap at [10, 20).
    Rope rope;
    auto b1 = std::make_shared<OwnedRopeBuffer>(10);
    auto b2 = std::make_shared<OwnedRopeBuffer>(10);
    rope.append(RopeNode{b1, 0, 10, 0});
    rope.append(RopeNode{b2, 0, 10, 20});

    auto g = rope.gaps({0, 30});
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].offset, 10u);
    EXPECT_EQ(g[0].size, 10u);

    /// Outside the rope's range entirely.
    auto g2 = rope.gaps({40, 10});
    ASSERT_EQ(g2.size(), 1u);
    EXPECT_EQ(g2[0].offset, 40u);
    EXPECT_EQ(g2[0].size, 10u);

    /// No gap inside [0, 5).
    EXPECT_TRUE(rope.gaps({0, 5}).empty());
}

TEST(Rope, GapsHandlesUnsortedAndOverlappingNodes)
{
    /// Nodes inserted out of order with an overlap.
    Rope rope;
    auto b1 = std::make_shared<OwnedRopeBuffer>(10);
    auto b2 = std::make_shared<OwnedRopeBuffer>(10);
    auto b3 = std::make_shared<OwnedRopeBuffer>(10);
    rope.append(RopeNode{b2, 0, 10, 20}); // [20, 30)
    rope.append(RopeNode{b1, 0, 10, 0});  // [0, 10)
    rope.append(RopeNode{b3, 0, 10, 25}); // [25, 35) — overlaps [20, 30)

    auto g = rope.gaps({0, 35});
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].offset, 10u);
    EXPECT_EQ(g[0].size, 10u);
}

TEST(Rope, CoversIsCorrectInverseOfGaps)
{
    Rope rope;
    auto b = std::make_shared<OwnedRopeBuffer>(20);
    rope.append(RopeNode{b, 0, 20, 0});  // [0, 20)
    EXPECT_TRUE(rope.covers({0, 20}));
    EXPECT_TRUE(rope.covers({5, 10}));
    EXPECT_FALSE(rope.covers({15, 10})); // 5 bytes past the end
    EXPECT_FALSE(rope.covers({20, 1}));  // entirely past
    EXPECT_TRUE(rope.covers({0, 0}));    // empty range trivially covered
}

TEST(Rope, ExtractMatchesSliceWhenCovered)
{
    auto rope = makeAdjacentRope(3, 10);
    Rope ex = rope.extract({5, 20});
    Rope sl = rope.slice({5, 20});
    ASSERT_EQ(ex.getNodes().size(), sl.getNodes().size());
    EXPECT_EQ(ex.totalBytes(), 20u);
    /// `extract` and `slice` produce equivalent node ranges on full coverage.
    for (size_t i = 0; i < ex.getNodes().size(); ++i)
    {
        EXPECT_EQ(ex.getNodes()[i].logical_offset, sl.getNodes()[i].logical_offset);
        EXPECT_EQ(ex.getNodes()[i].size, sl.getNodes()[i].size);
    }
}

TEST(Rope, ShiftAdjustsLogicalOffsets)
{
    auto rope = makeAdjacentRope(3, 10);
    rope.shift(100);
    EXPECT_EQ(rope.range().offset, 100u);
    EXPECT_EQ(rope.range().size, 30u);
    /// Negative shift back.
    rope.shift(-50);
    EXPECT_EQ(rope.range().offset, 50u);
}

TEST(Rope, CopyToFlattensCoveredRange)
{
    auto rope = makeAdjacentRope(3, 10);
    std::vector<char> out(20, '\0');
    EXPECT_EQ(rope.copyTo(out.data(), {5, 20}), 20u);
    /// First 5 bytes come from node 0 (filled with 0x10), next 10 from
    /// node 1 (0x11), last 5 from node 2 (0x12).
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ(static_cast<unsigned char>(out[i]), 0x10u) << "byte " << i;
    for (size_t i = 5; i < 15; ++i)
        EXPECT_EQ(static_cast<unsigned char>(out[i]), 0x11u) << "byte " << i;
    for (size_t i = 15; i < 20; ++i)
        EXPECT_EQ(static_cast<unsigned char>(out[i]), 0x12u) << "byte " << i;
}

TEST(Rope, CopyToWorksWithUnsortedNodes)
{
    /// Nodes appended out of order should still flatten correctly via copyTo
    /// (it sorts internally).
    Rope rope;
    auto b1 = std::make_shared<OwnedRopeBuffer>(5);
    auto b2 = std::make_shared<OwnedRopeBuffer>(5);
    std::memset(b1->data(), 'A', 5);
    std::memset(b2->data(), 'B', 5);
    rope.append(RopeNode{b2, 0, 5, 5});
    rope.append(RopeNode{b1, 0, 5, 0});

    std::vector<char> out(10, '\0');
    EXPECT_EQ(rope.copyTo(out.data(), {0, 10}), 10u);
    EXPECT_EQ(std::string(out.begin(), out.begin() + 5), "AAAAA");
    EXPECT_EQ(std::string(out.begin() + 5, out.end()), "BBBBB");
}
