#include <IO/Rope.h>
#include <Common/CurrentMetrics.h>
#include <gtest/gtest.h>

#include <cstring>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorRopeBytes;
}

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

/// The `ReaderExecutorRopeBytes` gauge tracks live rope-buffer memory: it goes
/// up by `size` while an `OwnedRopeBuffer` is alive and returns to baseline once
/// it is freed. This is the async/current-value replacement for the old
/// cumulative `allocated_bytes` counter.
TEST(OwnedRopeBuffer, RopeBytesGaugeTracksLiveAllocation)
{
    const auto baseline = CurrentMetrics::get(CurrentMetrics::ReaderExecutorRopeBytes);
    {
        auto outer = std::make_shared<OwnedRopeBuffer>(4096);
        EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorRopeBytes) - baseline, 4096);
        {
            auto inner = std::make_shared<OwnedRopeBuffer>(2048);
            EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorRopeBytes) - baseline, 4096 + 2048);
        }
        EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorRopeBytes) - baseline, 4096);
    }
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorRopeBytes), baseline);
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

TEST(Rope, AppendPartiallyConsumedRope)
{
    /// Appending a partially-consumed rope must not resurrect its consumed
    /// prefix: `append(Rope&&)` normalizes `other` by its `front_offset`, so the
    /// combined rope's coverage and `peek` agree. Regression for splicing the raw
    /// front node (which still started at the original, consumed `logical_offset`).
    auto buf = std::make_shared<OwnedRopeBuffer>(10);
    for (size_t i = 0; i < 10; ++i)
        buf->data()[i] = static_cast<char>('0' + i);   // "0123456789"

    Rope other;
    other.append(RopeNode{buf, 0, 10, 0});   // covers [0, 10)
    other.advance(5);                        // consume "01234"; live coverage is [5, 10)
    ASSERT_EQ(other.range().offset, 5u);
    ASSERT_EQ(other.range().size, 5u);

    Rope dst;
    dst.append(std::move(other));

    /// Coverage is the live range only - the consumed prefix [0, 5) is gone.
    EXPECT_EQ(dst.range().offset, 5u) << "consumed prefix must not reappear after append";
    EXPECT_EQ(dst.range().size, 5u);
    EXPECT_FALSE(dst.covers(ByteRange{0, 5}));
    EXPECT_TRUE(dst.covers(ByteRange{5, 5}));

    /// peek agrees with coverage: it serves the live bytes at offset 5, not 0.
    auto span = dst.peek();
    EXPECT_EQ(span.logical_offset, 5u) << "peek must start at the live offset, not the consumed prefix";
    EXPECT_EQ(span.size, 5u);
    EXPECT_EQ(String(span.data, span.size), "56789");
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

TEST(Rope, PeekAndAdvanceWalkNodes)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(300);
    std::memset(buf->data(), 'A', 100);
    std::memset(buf->data() + 100, 'B', 100);
    std::memset(buf->data() + 200, 'C', 100);

    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 100, 100, 100});
    rope.append(RopeNode{buf, 200, 100, 200});

    auto s1 = rope.peek();
    EXPECT_EQ(s1.logical_offset, 0u);
    EXPECT_EQ(s1.size, 100u);
    EXPECT_EQ(s1.data[0], 'A');
    rope.advance(100);
    EXPECT_EQ(rope.getNodes().size(), 2u);

    auto s2 = rope.peek();
    EXPECT_EQ(s2.logical_offset, 100u);
    EXPECT_EQ(s2.data[0], 'B');
    rope.advance(100);
    EXPECT_EQ(rope.getNodes().size(), 1u);

    auto s3 = rope.peek();
    EXPECT_EQ(s3.logical_offset, 200u);
    EXPECT_EQ(s3.data[0], 'C');
    rope.advance(100);
    EXPECT_TRUE(rope.atEnd());
}

TEST(Rope, AdvanceReleasesBuffer)
{
    std::weak_ptr<RopeBuffer> weak;
    Rope rope;
    {
        auto buf = std::make_shared<OwnedRopeBuffer>(64);
        weak = buf;
        rope.append(RopeNode{buf, 0, 64, 0});
        buf.reset(); /// only rope holds the ref
        EXPECT_FALSE(weak.expired());
    }
    /// Consume the whole node — the underlying buffer should be released.
    rope.advance(64);
    EXPECT_TRUE(rope.atEnd());
    EXPECT_TRUE(weak.expired());
}

TEST(Rope, AdvanceUpdatesCoverageAndRange)
{
    /// `range` / `covers` / `coveredBytes` must reflect what is still
    /// reachable after the cursor advances — NOT the appended coverage.
    /// Regression for the stale-intervals bug that caused
    /// `UNKNOWN_CODEC: codec family 0` in fast-test on
    /// `03785_rebuild_projection_with_part_offset`.
    auto buf = std::make_shared<OwnedRopeBuffer>(300);
    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 100, 100, 100});
    rope.append(RopeNode{buf, 200, 100, 200});

    EXPECT_EQ(rope.range().offset, 0u);
    EXPECT_EQ(rope.range().size, 300u);
    EXPECT_TRUE(rope.covers({50, 50}));

    /// Advance past the first node — coverage shrinks to [100, 300).
    rope.advance(100);
    EXPECT_EQ(rope.range().offset, 100u);
    EXPECT_EQ(rope.range().size, 200u);
    EXPECT_FALSE(rope.covers({50, 50}));
    EXPECT_TRUE(rope.covers({150, 50}));
    EXPECT_EQ(rope.coveredBytes({0, 300}), 200u);

    /// Advance past the second node — coverage shrinks to [200, 300).
    rope.advance(100);
    EXPECT_EQ(rope.range().offset, 200u);
    EXPECT_EQ(rope.range().size, 100u);
    EXPECT_FALSE(rope.covers({150, 50}));
    EXPECT_TRUE(rope.covers({250, 50}));

    /// Advance past the last node — empty rope.
    rope.advance(100);
    EXPECT_TRUE(rope.atEnd());
    EXPECT_EQ(rope.range().offset, 0u);
    EXPECT_EQ(rope.range().size, 0u);
}

TEST(Rope, AdvancePartialKeepsBufferAndAdjustsCursor)
{
    /// Partial consumption inside the front node: `peek` should return
    /// the remaining tail; `range` should shrink from the front by the
    /// consumed amount; the buffer stays alive.
    auto buf = std::make_shared<OwnedRopeBuffer>(100);
    for (size_t i = 0; i < 100; ++i)
        buf->data()[i] = static_cast<char>('a' + i);
    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});

    rope.advance(30);
    EXPECT_EQ(rope.range().offset, 30u);
    EXPECT_EQ(rope.range().size, 70u);

    auto s = rope.peek();
    EXPECT_EQ(s.logical_offset, 30u);
    EXPECT_EQ(s.size, 70u);
    EXPECT_EQ(s.data[0], static_cast<char>('a' + 30));
}

TEST(Rope, TryRewindBackwardInsideFrontNode)
{
    /// After consuming 50 bytes, a backward rewind to position 20 must
    /// restore coverage and `peek` from the rewound position.
    auto buf = std::make_shared<OwnedRopeBuffer>(100);
    for (size_t i = 0; i < 100; ++i)
        buf->data()[i] = static_cast<char>('a' + i);
    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});

    rope.advance(50);
    EXPECT_EQ(rope.range().offset, 50u);

    ASSERT_TRUE(rope.tryRewind(20));
    EXPECT_EQ(rope.range().offset, 20u);
    EXPECT_EQ(rope.range().size, 80u);

    auto s = rope.peek();
    EXPECT_EQ(s.logical_offset, 20u);
    EXPECT_EQ(s.data[0], static_cast<char>('a' + 20));
}

TEST(Rope, TryRewindForwardSkipsToLaterNode)
{
    /// Forward rewind past the current front node releases it and
    /// advances into a later node.
    auto buf = std::make_shared<OwnedRopeBuffer>(200);
    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 100, 100, 100});

    ASSERT_TRUE(rope.tryRewind(150));
    EXPECT_EQ(rope.getNodes().size(), 1u);
    EXPECT_EQ(rope.range().offset, 150u);
    EXPECT_EQ(rope.range().size, 50u);
}

TEST(Rope, TryRewindOutOfRangeFails)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(100);
    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 50});  /// covers [50, 150)

    EXPECT_FALSE(rope.tryRewind(10));   /// before front node
    EXPECT_FALSE(rope.tryRewind(200));  /// past back node
    EXPECT_EQ(rope.range().offset, 50u);
    EXPECT_EQ(rope.range().size, 100u);
}

TEST(Rope, TryRewindIntoGapFails)
{
    /// Disjoint nodes [0, 10) and [20, 30) with a gap [10, 20). A position in
    /// the gap is covered by no held node, so tryRewind must fail (the caller
    /// re-reads) rather than skip the gap and land the cursor at a later byte.
    /// A position inside the second node must land `peek` exactly there.
    auto buf = std::make_shared<OwnedRopeBuffer>(30);
    for (size_t i = 0; i < 30; ++i)
        buf->data()[i] = static_cast<char>('A' + i);
    Rope rope;
    rope.append(RopeNode{buf, 0, 10, 0});    /// covers [0, 10)
    rope.append(RopeNode{buf, 20, 10, 20});  /// covers [20, 30)

    EXPECT_FALSE(rope.tryRewind(15));
    EXPECT_EQ(rope.peek().logical_offset, 0u);

    ASSERT_TRUE(rope.tryRewind(25));
    auto s = rope.peek();
    EXPECT_EQ(s.logical_offset, 25u);
    EXPECT_EQ(s.size, 5u);
    EXPECT_EQ(s.data[0], static_cast<char>('A' + 25));
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
    /// (Rope::append keeps `nodes` sorted by logical_offset on the way in).
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

TEST(Rope, AppendKeepsNodesSortedByLogicalOffset)
{
    /// `append` inserts into `nodes` sorted by `logical_offset`, so consumers
    /// (PipelineReadBuffer::popFront, copyTo) can rely on monotonic iteration
    /// regardless of insertion order.
    auto buf = std::make_shared<OwnedRopeBuffer>(100);
    Rope rope;
    rope.append(RopeNode{buf, 0, 10, 50});
    rope.append(RopeNode{buf, 0, 10, 0});
    rope.append(RopeNode{buf, 0, 10, 30});
    rope.append(RopeNode{buf, 0, 10, 70});

    const auto & ns = rope.getNodes();
    ASSERT_EQ(ns.size(), 4u);
    EXPECT_EQ(ns[0].logical_offset, 0u);
    EXPECT_EQ(ns[1].logical_offset, 30u);
    EXPECT_EQ(ns[2].logical_offset, 50u);
    EXPECT_EQ(ns[3].logical_offset, 70u);
}

TEST(Rope, AppendEqualOffsetIsStable)
{
    /// Equal-offset nodes keep insertion order (matters when a duplicate
    /// node is appended for the same logical bytes — e.g. cache hit + later
    /// source read for the same offset).
    auto b1 = std::make_shared<OwnedRopeBuffer>(1);
    auto b2 = std::make_shared<OwnedRopeBuffer>(1);
    *b1->data() = 'F';
    *b2->data() = 'S';
    Rope rope;
    rope.append(RopeNode{b1, 0, 1, 42});
    rope.append(RopeNode{b2, 0, 1, 42});

    const auto & ns = rope.getNodes();
    ASSERT_EQ(ns.size(), 2u);
    EXPECT_EQ(*ns[0].data(), 'F');
    EXPECT_EQ(*ns[1].data(), 'S');
}

TEST(Rope, AppendOverlappingNodeKeepsCoverageIntact)
{
    /// The scenario that motivated the coverage-tracking redesign:
    /// after `{0,100}` is in, a later `{50,10}` is redundant — coverage
    /// stays exactly `[0,100)` and a single interval represents it.
    auto buf = std::make_shared<OwnedRopeBuffer>(200);
    Rope rope;
    rope.append(RopeNode{buf, 0, 100, 0});
    rope.append(RopeNode{buf, 50, 10, 50});

    EXPECT_TRUE(rope.covers({0, 100}));
    EXPECT_EQ(rope.coveredBytes({0, 100}), 100u);
    EXPECT_EQ(rope.range().offset, 0u);
    EXPECT_EQ(rope.range().size, 100u);

    const auto & ivs = rope.getIntervals();
    ASSERT_EQ(ivs.size(), 1u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 100u);
}

TEST(Rope, AppendDisjointNodesProducesMultipleIntervals)
{
    /// Two non-touching ranges produce two intervals, and `gaps` reports the
    /// hole between them.
    auto buf = std::make_shared<OwnedRopeBuffer>(200);
    Rope rope;
    rope.append(RopeNode{buf, 0, 30, 0});
    rope.append(RopeNode{buf, 50, 30, 50});

    const auto & ivs = rope.getIntervals();
    ASSERT_EQ(ivs.size(), 2u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 30u);
    EXPECT_EQ(ivs[1].offset, 50u);
    EXPECT_EQ(ivs[1].size, 30u);

    EXPECT_FALSE(rope.covers({0, 80}));
    auto g = rope.gaps({0, 80});
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].offset, 30u);
    EXPECT_EQ(g[0].size, 20u);
}

TEST(Rope, AppendTouchingNodesCoalesceIntoOneInterval)
{
    /// `[0,50)` then `[50,50)` — strictly adjacent (no overlap) but touching
    /// — should collapse into a single interval `[0,100)`.
    auto buf = std::make_shared<OwnedRopeBuffer>(200);
    Rope rope;
    rope.append(RopeNode{buf, 0, 50, 0});
    rope.append(RopeNode{buf, 50, 50, 50});

    const auto & ivs = rope.getIntervals();
    ASSERT_EQ(ivs.size(), 1u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 100u);
}

TEST(Rope, AppendBridgingNodeMergesMultipleIntervals)
{
    /// `[0,20)` and `[50,20)` start as two intervals; then `[15,40)` bridges
    /// them into one. Exercises the "merge multiple existing intervals into
    /// one" branch of `mergeInterval`.
    auto buf = std::make_shared<OwnedRopeBuffer>(100);
    Rope rope;
    rope.append(RopeNode{buf, 0, 20, 0});
    rope.append(RopeNode{buf, 0, 20, 50});
    ASSERT_EQ(rope.getIntervals().size(), 2u);

    rope.append(RopeNode{buf, 0, 40, 15});
    const auto & ivs = rope.getIntervals();
    ASSERT_EQ(ivs.size(), 1u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 70u);
}

TEST(Rope, AppendRopeMergesNodesAndIntervals)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(100);
    Rope a;
    a.append(RopeNode{buf, 0, 10, 0});
    a.append(RopeNode{buf, 0, 10, 40});

    Rope b;
    b.append(RopeNode{buf, 0, 10, 20});
    b.append(RopeNode{buf, 0, 10, 60});

    a.append(std::move(b));

    const auto & ns = a.getNodes();
    ASSERT_EQ(ns.size(), 4u);
    EXPECT_EQ(ns[0].logical_offset, 0u);
    EXPECT_EQ(ns[1].logical_offset, 20u);
    EXPECT_EQ(ns[2].logical_offset, 40u);
    EXPECT_EQ(ns[3].logical_offset, 60u);

    const auto & ivs = a.getIntervals();
    ASSERT_EQ(ivs.size(), 4u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[1].offset, 20u);
    EXPECT_EQ(ivs[2].offset, 40u);
    EXPECT_EQ(ivs[3].offset, 60u);
}

TEST(Rope, ShiftMovesNodesAndIntervals)
{
    auto buf = std::make_shared<OwnedRopeBuffer>(100);
    Rope rope;
    rope.append(RopeNode{buf, 0, 10, 100});
    rope.append(RopeNode{buf, 0, 10, 200});

    rope.shift(-50);
    EXPECT_EQ(rope.getNodes()[0].logical_offset, 50u);
    EXPECT_EQ(rope.getNodes()[1].logical_offset, 150u);
    EXPECT_EQ(rope.getIntervals()[0].offset, 50u);
    EXPECT_EQ(rope.getIntervals()[1].offset, 150u);
    EXPECT_EQ(rope.range().offset, 50u);
    EXPECT_EQ(rope.range().size, 110u);
}
