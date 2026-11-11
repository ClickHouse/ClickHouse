#include <IO/ChainedBuffers.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

#include <cstring>

using namespace DB;

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorChainedBufferBytes;
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

TEST(OwnedChainedBuffer, AllocateAndAccess)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(1024);
    EXPECT_EQ(buf->size(), 1024);
    EXPECT_NE(buf->data(), nullptr);
    std::memset(buf->data(), 'A', 1024);
    EXPECT_EQ(buf->data()[0], 'A');
    EXPECT_EQ(buf->data()[1023], 'A');
}

/// The `ReaderExecutorChainedBufferBytes` gauge tracks live chain-buffer memory: it goes
/// up by `size` while an `OwnedChainedBuffer` is alive and returns to baseline once
/// it is freed. This is the async/current-value replacement for the old
/// cumulative `allocated_bytes` counter.
TEST(OwnedChainedBuffer, ChainedBufferBytesGaugeTracksLiveAllocation)
{
    const auto baseline = CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes);
    {
        auto outer = std::make_shared<OwnedChainedBuffer>(4096);
        EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes) - baseline, 4096);
        {
            auto inner = std::make_shared<OwnedChainedBuffer>(2048);
            EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes) - baseline, 4096 + 2048);
        }
        EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes) - baseline, 4096);
    }
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes), baseline);
}

TEST(OwnedChainedBuffer, HugeSizeThrows)
{
    /// SIMD padding uses checked addition: a size that would wrap on `+ PADDING_FOR_SIMD`
    /// throws instead of allocating a tiny block, and the live-bytes gauge stays untouched.
    const auto baseline = CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes);
    EXPECT_THROW(OwnedChainedBuffer(static_cast<size_t>(-1)), Exception);
    EXPECT_EQ(CurrentMetrics::get(CurrentMetrics::ReaderExecutorChainedBufferBytes), baseline);
}

TEST(ChainedBufferNode, SliceOfBuffer)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(1024);
    std::memset(buf->data(), 0, 1024);
    buf->data()[100] = 'X';
    buf->data()[199] = 'Y';
    ChainedBufferNode node{buf, 100, 100, 5000};
    EXPECT_EQ(node.data()[0], 'X');
    EXPECT_EQ(node.data()[99], 'Y');
    EXPECT_EQ(node.size, 100);
    EXPECT_EQ(node.range().offset, 5000);
    EXPECT_EQ(node.range().size, 100);
}

TEST(ChainedBufferNode, SharedOwnership)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(512);
    ChainedBufferNode node1{buf, 0, 256, 0};
    ChainedBufferNode node2{buf, 256, 256, 256};
    EXPECT_EQ(buf.use_count(), 3);
    EXPECT_EQ(node1.data(), buf->data());
    EXPECT_EQ(node2.data(), buf->data() + 256);
}

TEST(ChainedBuffers, AppendAndIterate)
{
    auto buf1 = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(buf1->data(), 'A', 100);
    auto buf2 = std::make_shared<OwnedChainedBuffer>(200);
    std::memset(buf2->data(), 'B', 200);

    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf1, 0, 100, 0});
    chain.append(ChainedBufferNode{buf2, 0, 200, 100});

    EXPECT_EQ(chain.range().offset, 0);
    EXPECT_EQ(chain.range().size, 300);
    EXPECT_EQ(chain.getNodes().size(), 2);
    EXPECT_FALSE(chain.empty());
}

TEST(ChainedBuffers, AppendChainedBuffers)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(100);

    ChainedBuffers chain1;
    chain1.append(ChainedBufferNode{buf, 0, 50, 0});

    ChainedBuffers chain2;
    chain2.append(ChainedBufferNode{buf, 50, 50, 50});

    chain1.append(std::move(chain2));
    EXPECT_EQ(chain1.getNodes().size(), 2);
    EXPECT_EQ(chain1.range().size, 100);
}

TEST(ChainedBuffers, AppendPartiallyConsumedChainedBuffers)
{
    /// Appending a partially-consumed chain must not resurrect its consumed
    /// prefix: `append(ChainedBuffers&&)` normalizes `other` by its `front_offset`, so the
    /// combined chain's coverage and `peek` agree. Regression for splicing the raw
    /// front node (which still started at the original, consumed `logical_offset`).
    auto buf = std::make_shared<OwnedChainedBuffer>(10);
    for (size_t i = 0; i < 10; ++i)
        buf->data()[i] = static_cast<char>('0' + i);   // "0123456789"

    ChainedBuffers other;
    other.append(ChainedBufferNode{buf, 0, 10, 0});   // covers [0, 10)
    other.advance(5);                        // consume "01234"; live coverage is [5, 10)
    ASSERT_EQ(other.range().offset, 5u);
    ASSERT_EQ(other.range().size, 5u);

    ChainedBuffers dst;
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

TEST(ChainedBuffers, SliceFullRange)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(300);
    std::memset(buf->data(), 'X', 300);

    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.append(ChainedBufferNode{buf, 100, 100, 100});
    chain.append(ChainedBufferNode{buf, 200, 100, 200});

    auto slice = chain.slice(ByteRange{0, 300});
    EXPECT_EQ(slice.range().offset, 0);
    EXPECT_EQ(slice.range().size, 300);
    EXPECT_EQ(slice.totalBytes(), 300);
    EXPECT_EQ(slice.getNodes().size(), 3);
}

TEST(ChainedBuffers, SliceMiddle)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(300);

    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.append(ChainedBufferNode{buf, 100, 100, 100});
    chain.append(ChainedBufferNode{buf, 200, 100, 200});

    auto slice = chain.slice(ByteRange{50, 200});
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

TEST(ChainedBuffers, SliceSingleNodeMiddle)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(1000);

    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 1000, 0});

    auto slice = chain.slice(ByteRange{100, 200});
    EXPECT_EQ(slice.getNodes().size(), 1);
    EXPECT_EQ(slice.getNodes()[0].buffer_offset, 100);
    EXPECT_EQ(slice.getNodes()[0].size, 200);
    EXPECT_EQ(slice.getNodes()[0].logical_offset, 100);
}

TEST(ChainedBuffers, EmptyChainedBuffers)
{
    ChainedBuffers chain;
    EXPECT_TRUE(chain.empty());
    EXPECT_EQ(chain.range().size, 0);
}

TEST(ChainedBuffers, SliceKeepsBufferAlive)
{
    std::weak_ptr<ChainedBuffer> weak;
    ChainedBuffers slice;
    {
        auto buf = std::make_shared<OwnedChainedBuffer>(64);
        weak = buf;

        ChainedBuffers chain;
        chain.append(ChainedBufferNode{buf, 0, 64, 0});
        slice = chain.slice(ByteRange{0, 64});
    }
    EXPECT_FALSE(weak.expired());
}

TEST(ChainedBuffers, SliceSkipsNodeEntirelyBehindCursor)
{
    /// Overlapping nodes: A=[0,10) keeps the front from being dropped, so after advancing
    /// past B=[5,8) entirely, B survives in `nodes` but sits fully behind the cursor.
    /// `slice` must skip it (the "node entirely consumed" branch).
    auto buf = std::make_shared<OwnedChainedBuffer>(20);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 10, 0});   /// A = [0, 10)
    chain.append(ChainedBufferNode{buf, 5, 3, 5});    /// B = [5, 8), overlaps and sits behind A
    chain.advance(9);                                  /// cursor 9: A survives (ends at 10), B fully behind
    ASSERT_FALSE(chain.atEnd());

    ChainedBuffers s = chain.slice(ByteRange{0, 20});
    EXPECT_EQ(s.getNodes().size(), 1u);   /// only A's live tail [9, 10); B skipped
    EXPECT_EQ(s.range().offset, 9u);
    EXPECT_EQ(s.range().size, 1u);
}

TEST(ChainedBuffers, TotalBytes)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(300);

    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.append(ChainedBufferNode{buf, 100, 150, 100});

    EXPECT_EQ(chain.totalBytes(), 250);
}

TEST(ChainedBuffers, PeekAndAdvanceWalkNodes)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(300);
    std::memset(buf->data(), 'A', 100);
    std::memset(buf->data() + 100, 'B', 100);
    std::memset(buf->data() + 200, 'C', 100);

    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.append(ChainedBufferNode{buf, 100, 100, 100});
    chain.append(ChainedBufferNode{buf, 200, 100, 200});

    auto s1 = chain.peek();
    EXPECT_EQ(s1.logical_offset, 0u);
    EXPECT_EQ(s1.size, 100u);
    EXPECT_EQ(s1.data[0], 'A');
    chain.advance(100);
    EXPECT_EQ(chain.getNodes().size(), 2u);

    auto s2 = chain.peek();
    EXPECT_EQ(s2.logical_offset, 100u);
    EXPECT_EQ(s2.data[0], 'B');
    chain.advance(100);
    EXPECT_EQ(chain.getNodes().size(), 1u);

    auto s3 = chain.peek();
    EXPECT_EQ(s3.logical_offset, 200u);
    EXPECT_EQ(s3.data[0], 'C');
    chain.advance(100);
    EXPECT_TRUE(chain.atEnd());
}

TEST(ChainedBuffers, AdvanceReleasesBuffer)
{
    std::weak_ptr<ChainedBuffer> weak;
    ChainedBuffers chain;
    {
        auto buf = std::make_shared<OwnedChainedBuffer>(64);
        weak = buf;
        chain.append(ChainedBufferNode{buf, 0, 64, 0});
        buf.reset(); /// only chain holds the ref
        EXPECT_FALSE(weak.expired());
    }
    /// Consume the whole node — the underlying buffer should be released.
    chain.advance(64);
    EXPECT_TRUE(chain.atEnd());
    EXPECT_TRUE(weak.expired());
}

TEST(ChainedBuffers, AdvanceUpdatesCoverageAndRange)
{
    /// `range` / `covers` / `coveredBytes` must reflect what is still
    /// reachable after the cursor advances — NOT the appended coverage.
    /// Regression for the stale-intervals bug that caused
    /// `UNKNOWN_CODEC: codec family 0` in fast-test on
    /// `03785_rebuild_projection_with_part_offset`.
    auto buf = std::make_shared<OwnedChainedBuffer>(300);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.append(ChainedBufferNode{buf, 100, 100, 100});
    chain.append(ChainedBufferNode{buf, 200, 100, 200});

    EXPECT_EQ(chain.range().offset, 0u);
    EXPECT_EQ(chain.range().size, 300u);
    EXPECT_TRUE(chain.covers({50, 50}));

    /// Advance past the first node — coverage shrinks to [100, 300).
    chain.advance(100);
    EXPECT_EQ(chain.range().offset, 100u);
    EXPECT_EQ(chain.range().size, 200u);
    EXPECT_FALSE(chain.covers({50, 50}));
    EXPECT_TRUE(chain.covers({150, 50}));
    EXPECT_EQ(chain.coveredBytes({0, 300}), 200u);

    /// Advance past the second node — coverage shrinks to [200, 300).
    chain.advance(100);
    EXPECT_EQ(chain.range().offset, 200u);
    EXPECT_EQ(chain.range().size, 100u);
    EXPECT_FALSE(chain.covers({150, 50}));
    EXPECT_TRUE(chain.covers({250, 50}));

    /// Advance past the last node — empty chain.
    chain.advance(100);
    EXPECT_TRUE(chain.atEnd());
    EXPECT_EQ(chain.range().offset, 0u);
    EXPECT_EQ(chain.range().size, 0u);
}

TEST(ChainedBuffers, AdvancePartialKeepsBufferAndAdjustsCursor)
{
    /// Partial consumption inside the front node: `peek` should return
    /// the remaining tail; `range` should shrink from the front by the
    /// consumed amount; the buffer stays alive.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    for (size_t i = 0; i < 100; ++i)
        buf->data()[i] = static_cast<char>('a' + i);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});

    chain.advance(30);
    EXPECT_EQ(chain.range().offset, 30u);
    EXPECT_EQ(chain.range().size, 70u);

    auto s = chain.peek();
    EXPECT_EQ(s.logical_offset, 30u);
    EXPECT_EQ(s.size, 70u);
    EXPECT_EQ(s.data[0], static_cast<char>('a' + 30));
}

TEST(ChainedBuffers, TryRewindBackwardInsideFrontNode)
{
    /// After consuming 50 bytes, a backward rewind to position 20 must
    /// restore coverage and `peek` from the rewound position.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    for (size_t i = 0; i < 100; ++i)
        buf->data()[i] = static_cast<char>('a' + i);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});

    chain.advance(50);
    EXPECT_EQ(chain.range().offset, 50u);

    ASSERT_TRUE(chain.tryRewind(20));
    EXPECT_EQ(chain.range().offset, 20u);
    EXPECT_EQ(chain.range().size, 80u);

    auto s = chain.peek();
    EXPECT_EQ(s.logical_offset, 20u);
    EXPECT_EQ(s.data[0], static_cast<char>('a' + 20));
}

TEST(ChainedBuffers, TryRewindForwardSkipsToLaterNode)
{
    /// Forward rewind past the current front node releases it and
    /// advances into a later node.
    auto buf = std::make_shared<OwnedChainedBuffer>(200);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.append(ChainedBufferNode{buf, 100, 100, 100});

    ASSERT_TRUE(chain.tryRewind(150));
    EXPECT_EQ(chain.getNodes().size(), 1u);
    EXPECT_EQ(chain.range().offset, 150u);
    EXPECT_EQ(chain.range().size, 50u);
}

TEST(ChainedBuffers, TryRewindOutOfRangeFails)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 50});  /// covers [50, 150)

    EXPECT_FALSE(chain.tryRewind(10));   /// before front node
    EXPECT_FALSE(chain.tryRewind(200));  /// past back node
    EXPECT_EQ(chain.range().offset, 50u);
    EXPECT_EQ(chain.range().size, 100u);
}

TEST(ChainedBuffers, TryRewindIntoGapFails)
{
    /// Disjoint nodes [0, 10) and [20, 30) with a gap [10, 20). A position in
    /// the gap is covered by no held node, so tryRewind must fail (the caller
    /// re-reads) rather than skip the gap and land the cursor at a later byte.
    /// A position inside the second node must land `peek` exactly there.
    auto buf = std::make_shared<OwnedChainedBuffer>(30);
    for (size_t i = 0; i < 30; ++i)
        buf->data()[i] = static_cast<char>('A' + i);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 10, 0});    /// covers [0, 10)
    chain.append(ChainedBufferNode{buf, 20, 10, 20});  /// covers [20, 30)

    EXPECT_FALSE(chain.tryRewind(15));
    EXPECT_EQ(chain.peek().logical_offset, 0u);

    ASSERT_TRUE(chain.tryRewind(25));
    auto s = chain.peek();
    EXPECT_EQ(s.logical_offset, 25u);
    EXPECT_EQ(s.size, 5u);
    EXPECT_EQ(s.data[0], static_cast<char>('A' + 25));
}

namespace
{
    /// Build a ChainedBuffers of `count` adjacent nodes, each `node_size` bytes, starting
    /// at `start_offset`. Node i's bytes are `0x10 + i` repeated.
    ChainedBuffers makeAdjacentChainedBuffers(size_t count, size_t node_size, size_t start_offset = 0)
    {
        ChainedBuffers chain;
        for (size_t i = 0; i < count; ++i)
        {
            auto buf = std::make_shared<OwnedChainedBuffer>(node_size);
            std::memset(buf->data(), static_cast<int>(0x10 + i), node_size);
            chain.append(ChainedBufferNode{buf, 0, node_size, start_offset + i * node_size});
        }
        return chain;
    }
}

TEST(ChainedBuffers, CoveredBytesMatchesActualCoverage)
{
    /// Three 10-byte nodes covering [0, 30). Query coverage of various sub-ranges.
    auto chain = makeAdjacentChainedBuffers(/*count=*/3, /*node_size=*/10);
    EXPECT_EQ(chain.coveredBytes({0, 30}), 30u);
    EXPECT_EQ(chain.coveredBytes({5, 20}), 20u);
    EXPECT_EQ(chain.coveredBytes({0, 100}), 30u);  // over-asks; covered <= req
    EXPECT_EQ(chain.coveredBytes({30, 10}), 0u);   // entirely outside
    EXPECT_EQ(chain.coveredBytes({100, 50}), 0u);
}

TEST(ChainedBuffers, GapsReturnsMissingRanges)
{
    /// Two nodes [0, 10) and [20, 30). Gap at [10, 20).
    ChainedBuffers chain;
    auto b1 = std::make_shared<OwnedChainedBuffer>(10);
    auto b2 = std::make_shared<OwnedChainedBuffer>(10);
    chain.append(ChainedBufferNode{b1, 0, 10, 0});
    chain.append(ChainedBufferNode{b2, 0, 10, 20});

    auto g = chain.gaps({0, 30});
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].offset, 10u);
    EXPECT_EQ(g[0].size, 10u);

    /// Outside the chain's range entirely.
    auto g2 = chain.gaps({40, 10});
    ASSERT_EQ(g2.size(), 1u);
    EXPECT_EQ(g2[0].offset, 40u);
    EXPECT_EQ(g2[0].size, 10u);

    /// No gap inside [0, 5).
    EXPECT_TRUE(chain.gaps({0, 5}).empty());
}

TEST(ChainedBuffers, GapsHandlesUnsortedAndOverlappingNodes)
{
    /// Nodes inserted out of order with an overlap.
    ChainedBuffers chain;
    auto b1 = std::make_shared<OwnedChainedBuffer>(10);
    auto b2 = std::make_shared<OwnedChainedBuffer>(10);
    auto b3 = std::make_shared<OwnedChainedBuffer>(10);
    chain.append(ChainedBufferNode{b2, 0, 10, 20}); // [20, 30)
    chain.append(ChainedBufferNode{b1, 0, 10, 0});  // [0, 10)
    chain.append(ChainedBufferNode{b3, 0, 10, 25}); // [25, 35) — overlaps [20, 30)

    auto g = chain.gaps({0, 35});
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].offset, 10u);
    EXPECT_EQ(g[0].size, 10u);
}

TEST(ChainedBuffers, CoversIsCorrectInverseOfGaps)
{
    ChainedBuffers chain;
    auto b = std::make_shared<OwnedChainedBuffer>(20);
    chain.append(ChainedBufferNode{b, 0, 20, 0});  // [0, 20)
    EXPECT_TRUE(chain.covers({0, 20}));
    EXPECT_TRUE(chain.covers({5, 10}));
    EXPECT_FALSE(chain.covers({15, 10})); // 5 bytes past the end
    EXPECT_FALSE(chain.covers({20, 1}));  // entirely past
    EXPECT_TRUE(chain.covers({0, 0}));    // empty range trivially covered
}

TEST(ChainedBuffers, ExtractMatchesSliceWhenCovered)
{
    auto chain = makeAdjacentChainedBuffers(3, 10);
    ChainedBuffers ex = chain.extract({5, 20});
    ChainedBuffers sl = chain.slice({5, 20});
    ASSERT_EQ(ex.getNodes().size(), sl.getNodes().size());
    EXPECT_EQ(ex.totalBytes(), 20u);
    /// `extract` and `slice` produce equivalent node ranges on full coverage.
    for (size_t i = 0; i < ex.getNodes().size(); ++i)
    {
        EXPECT_EQ(ex.getNodes()[i].logical_offset, sl.getNodes()[i].logical_offset);
        EXPECT_EQ(ex.getNodes()[i].size, sl.getNodes()[i].size);
    }
}

TEST(ChainedBuffers, ShiftAdjustsLogicalOffsets)
{
    auto chain = makeAdjacentChainedBuffers(3, 10);
    chain.shift(100);
    EXPECT_EQ(chain.range().offset, 100u);
    EXPECT_EQ(chain.range().size, 30u);
    /// Negative shift back.
    chain.shift(-50);
    EXPECT_EQ(chain.range().offset, 50u);
}

TEST(ChainedBuffers, CopyToFlattensCoveredRange)
{
    auto chain = makeAdjacentChainedBuffers(3, 10);
    std::vector<char> out(20, '\0');
    EXPECT_EQ(chain.copyTo(out.data(), {5, 20}), 20u);
    /// First 5 bytes come from node 0 (filled with 0x10), next 10 from
    /// node 1 (0x11), last 5 from node 2 (0x12).
    for (size_t i = 0; i < 5; ++i)
        EXPECT_EQ(static_cast<unsigned char>(out[i]), 0x10u) << "byte " << i;
    for (size_t i = 5; i < 15; ++i)
        EXPECT_EQ(static_cast<unsigned char>(out[i]), 0x11u) << "byte " << i;
    for (size_t i = 15; i < 20; ++i)
        EXPECT_EQ(static_cast<unsigned char>(out[i]), 0x12u) << "byte " << i;
}

TEST(ChainedBuffers, CopyToWorksWithUnsortedNodes)
{
    /// Nodes appended out of order should still flatten correctly via copyTo
    /// (ChainedBuffers::append keeps `nodes` sorted by logical_offset on the way in).
    ChainedBuffers chain;
    auto b1 = std::make_shared<OwnedChainedBuffer>(5);
    auto b2 = std::make_shared<OwnedChainedBuffer>(5);
    std::memset(b1->data(), 'A', 5);
    std::memset(b2->data(), 'B', 5);
    chain.append(ChainedBufferNode{b2, 0, 5, 5});
    chain.append(ChainedBufferNode{b1, 0, 5, 0});

    std::vector<char> out(10, '\0');
    EXPECT_EQ(chain.copyTo(out.data(), {0, 10}), 10u);
    EXPECT_EQ(std::string(out.begin(), out.begin() + 5), "AAAAA");
    EXPECT_EQ(std::string(out.begin() + 5, out.end()), "BBBBB");
}

TEST(ChainedBuffers, AppendKeepsNodesSortedByLogicalOffset)
{
    /// `append` inserts into `nodes` sorted by `logical_offset`, so consumers
    /// (PipelineReadBuffer::popFront, copyTo) can rely on monotonic iteration
    /// regardless of insertion order.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 10, 50});
    chain.append(ChainedBufferNode{buf, 0, 10, 0});
    chain.append(ChainedBufferNode{buf, 0, 10, 30});
    chain.append(ChainedBufferNode{buf, 0, 10, 70});

    const auto & ns = chain.getNodes();
    ASSERT_EQ(ns.size(), 4u);
    EXPECT_EQ(ns[0].logical_offset, 0u);
    EXPECT_EQ(ns[1].logical_offset, 30u);
    EXPECT_EQ(ns[2].logical_offset, 50u);
    EXPECT_EQ(ns[3].logical_offset, 70u);
}

TEST(ChainedBuffers, AppendEqualOffsetIsStable)
{
    /// Equal-offset nodes keep insertion order (matters when a duplicate
    /// node is appended for the same logical bytes — e.g. cache hit + later
    /// source read for the same offset).
    auto b1 = std::make_shared<OwnedChainedBuffer>(1);
    auto b2 = std::make_shared<OwnedChainedBuffer>(1);
    *b1->data() = 'F';
    *b2->data() = 'S';
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{b1, 0, 1, 42});
    chain.append(ChainedBufferNode{b2, 0, 1, 42});

    const auto & ns = chain.getNodes();
    ASSERT_EQ(ns.size(), 2u);
    EXPECT_EQ(*ns[0].data(), 'F');
    EXPECT_EQ(*ns[1].data(), 'S');
}

TEST(ChainedBuffers, AppendOverlappingNodeKeepsCoverageIntact)
{
    /// The scenario that motivated the coverage-tracking redesign:
    /// after `{0,100}` is in, a later `{50,10}` is redundant — coverage
    /// stays exactly `[0,100)` and a single interval represents it.
    auto buf = std::make_shared<OwnedChainedBuffer>(200);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.append(ChainedBufferNode{buf, 50, 10, 50});

    EXPECT_TRUE(chain.covers({0, 100}));
    EXPECT_EQ(chain.coveredBytes({0, 100}), 100u);
    EXPECT_EQ(chain.range().offset, 0u);
    EXPECT_EQ(chain.range().size, 100u);

    const auto & ivs = chain.getIntervals();
    ASSERT_EQ(ivs.size(), 1u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 100u);
}

TEST(ChainedBuffers, AppendDisjointNodesProducesMultipleIntervals)
{
    /// Two non-touching ranges produce two intervals, and `gaps` reports the
    /// hole between them.
    auto buf = std::make_shared<OwnedChainedBuffer>(200);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 30, 0});
    chain.append(ChainedBufferNode{buf, 50, 30, 50});

    const auto & ivs = chain.getIntervals();
    ASSERT_EQ(ivs.size(), 2u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 30u);
    EXPECT_EQ(ivs[1].offset, 50u);
    EXPECT_EQ(ivs[1].size, 30u);

    EXPECT_FALSE(chain.covers({0, 80}));
    auto g = chain.gaps({0, 80});
    ASSERT_EQ(g.size(), 1u);
    EXPECT_EQ(g[0].offset, 30u);
    EXPECT_EQ(g[0].size, 20u);
}

TEST(ChainedBuffers, AppendTouchingNodesCoalesceIntoOneInterval)
{
    /// `[0,50)` then `[50,50)` — strictly adjacent (no overlap) but touching
    /// — should collapse into a single interval `[0,100)`.
    auto buf = std::make_shared<OwnedChainedBuffer>(200);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 50, 0});
    chain.append(ChainedBufferNode{buf, 50, 50, 50});

    const auto & ivs = chain.getIntervals();
    ASSERT_EQ(ivs.size(), 1u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 100u);
}

TEST(ChainedBuffers, AppendBridgingNodeMergesMultipleIntervals)
{
    /// `[0,20)` and `[50,20)` start as two intervals; then `[15,40)` bridges
    /// them into one. Exercises the "merge multiple existing intervals into
    /// one" branch of `mergeInterval`.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 20, 0});
    chain.append(ChainedBufferNode{buf, 0, 20, 50});
    ASSERT_EQ(chain.getIntervals().size(), 2u);

    chain.append(ChainedBufferNode{buf, 0, 40, 15});
    const auto & ivs = chain.getIntervals();
    ASSERT_EQ(ivs.size(), 1u);
    EXPECT_EQ(ivs[0].offset, 0u);
    EXPECT_EQ(ivs[0].size, 70u);
}

TEST(ChainedBuffers, AppendChainedBuffersMergesNodesAndIntervals)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers a;
    a.append(ChainedBufferNode{buf, 0, 10, 0});
    a.append(ChainedBufferNode{buf, 0, 10, 40});

    ChainedBuffers b;
    b.append(ChainedBufferNode{buf, 0, 10, 20});
    b.append(ChainedBufferNode{buf, 0, 10, 60});

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

TEST(ChainedBuffers, ShiftMovesNodesAndIntervals)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 10, 100});
    chain.append(ChainedBufferNode{buf, 0, 10, 200});

    chain.shift(-50);
    EXPECT_EQ(chain.getNodes()[0].logical_offset, 50u);
    EXPECT_EQ(chain.getNodes()[1].logical_offset, 150u);
    EXPECT_EQ(chain.getIntervals()[0].offset, 50u);
    EXPECT_EQ(chain.getIntervals()[1].offset, 150u);
    EXPECT_EQ(chain.range().offset, 50u);
    EXPECT_EQ(chain.range().size, 110u);
}

TEST(ChainedBuffers, AppendZeroSizeNodeIsIgnored)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(16);
    ChainedBuffers chain;

    /// A zero-size node must not enter `nodes`: otherwise `peek` returns an empty span
    /// while `atEnd` is false, so a drain loop advancing by the span size hangs.
    chain.append(ChainedBufferNode{buf, 0, 0, 0});
    EXPECT_TRUE(chain.atEnd());
    EXPECT_EQ(chain.totalBytes(), 0u);
    EXPECT_TRUE(chain.getNodes().empty());

    /// A subsequent non-empty append still works.
    chain.append(ChainedBufferNode{buf, 0, 8, 0});
    EXPECT_FALSE(chain.atEnd());
    EXPECT_EQ(chain.peek().size, 8u);
}

TEST(ChainedBuffers, DrainSkipsNodeEntirelyBehindCursor)
{
    /// Streaming an overlapping chain serves the coverage union exactly once: a node that
    /// falls entirely behind the cursor after an advance is dropped, not re-served.
    auto a = std::make_shared<OwnedChainedBuffer>(100);
    auto b = std::make_shared<OwnedChainedBuffer>(10);
    std::memset(a->data(), 'A', 100);
    std::memset(b->data(), 'B', 10);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{a, 0, 100, 0});
    chain.append(ChainedBufferNode{b, 0, 10, 50});  /// [50, 60) is entirely inside [0, 100)

    std::string out;
    while (!chain.atEnd())
    {
        auto s = chain.peek();
        out.append(s.data, s.size);
        chain.advance(s.size);
    }
    EXPECT_EQ(out, std::string(100, 'A'));  /// 100 bytes, not 110; the redundant node skipped
}

TEST(ChainedBuffers, DrainServesOnlyNovelTailOfOverlappingNode)
{
    /// Partial overlap [0,100) then [50,150): drain serves [0,100) from the first node and
    /// only the novel [100,150) tail of the second -- the overlapped [50,100) is not re-served.
    auto a = std::make_shared<OwnedChainedBuffer>(100);
    auto b = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(a->data(), 'A', 100);
    std::memset(b->data(), 'B', 100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{a, 0, 100, 0});
    chain.append(ChainedBufferNode{b, 0, 100, 50});  /// [50, 150)

    std::string out;
    while (!chain.atEnd())
    {
        auto s = chain.peek();
        out.append(s.data, s.size);
        chain.advance(s.size);
    }
    EXPECT_EQ(out.size(), 150u);
    EXPECT_EQ(out, std::string(100, 'A') + std::string(50, 'B'));
}

TEST(ChainedBuffers, AdvancePastEndClampsWithoutOverflow)
{
    /// A huge advance must clamp to EOF, not overflow `cur + bytes` and wrap below the
    /// cursor (which would move it backwards / leave nodes intact).
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.advance(50);
    EXPECT_EQ(chain.peek().logical_offset, 50u);

    chain.advance(static_cast<size_t>(-1));  /// SIZE_MAX
    EXPECT_TRUE(chain.atEnd());
}

TEST(ChainedBuffers, TryRewindIntoFrontNodeUnderOverlap)
{
    /// nodes [0,1000) and [100,110): sorted by start, the back node ends earlier, so the
    /// reachable upper bound must come from merged coverage (1000), not nodes.back().end
    /// (110). Byte 500 is still in the front node, so the rewind must succeed.
    auto big = std::make_shared<OwnedChainedBuffer>(1000);
    auto small = std::make_shared<OwnedChainedBuffer>(10);
    for (size_t i = 0; i < 1000; ++i)
        big->data()[i] = static_cast<char>('A' + (i % 26));
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{big, 0, 1000, 0});
    chain.append(ChainedBufferNode{small, 0, 10, 100});  /// [100, 110), entirely inside [0, 1000)

    ASSERT_TRUE(chain.tryRewind(500));
    auto s = chain.peek();
    EXPECT_EQ(s.logical_offset, 500u);
    EXPECT_EQ(static_cast<unsigned char>(s.data[0]), static_cast<unsigned char>('A' + (500 % 26)));
}

TEST(ChainedBuffers, AppendBehindCursorAfterAdvanceIsTrimmed)
{
    /// Appending to a partly-consumed chain must not re-cover consumed bytes nor steal the
    /// front node's `front_offset`: a node behind the cursor is dropped, one straddling it
    /// is trimmed to its reachable tail.
    auto a = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(a->data(), 'A', 100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{a, 0, 100, 100});  /// [100, 200)
    chain.advance(50);                        /// consume [100, 150); cursor at 150
    ASSERT_EQ(chain.peek().logical_offset, 150u);
    ASSERT_EQ(chain.peek().size, 50u);

    /// Entirely behind the cursor -> dropped (no OOB peek, no coverage resurrection).
    auto b = std::make_shared<OwnedChainedBuffer>(60);
    std::memset(b->data(), 'B', 60);
    chain.append(ChainedBufferNode{b, 0, 60, 50});  /// [50, 110), end <= 150
    EXPECT_EQ(chain.range().offset, 150u);
    EXPECT_EQ(chain.peek().logical_offset, 150u);
    EXPECT_EQ(chain.peek().size, 50u);

    /// Straddles the cursor -> trimmed to [150, 210).
    auto c = std::make_shared<OwnedChainedBuffer>(160);
    std::memset(c->data(), 'C', 160);
    chain.append(ChainedBufferNode{c, 0, 160, 50});  /// [50, 210)
    EXPECT_EQ(chain.range().offset, 150u);
    EXPECT_EQ(chain.range().end(), 210u);

    std::string out;
    while (!chain.atEnd())
    {
        auto s = chain.peek();
        out.append(s.data, s.size);
        chain.advance(s.size);
    }
    EXPECT_EQ(out, std::string(50, 'A') + std::string(10, 'C'));  /// [150,200)=A, [200,210)=C
}

TEST(ChainedBuffers, SliceTrimsConsumedBytesAcrossOverlap)
{
    /// Overlapping nodes after an advance: `slice` must not expose already-consumed bytes,
    /// even from a later node that started before the cursor.
    auto buf = std::make_shared<OwnedChainedBuffer>(175);
    std::memset(buf->data(), 'Z', 175);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});    /// [0, 100)
    chain.append(ChainedBufferNode{buf, 50, 100, 50});  /// [50, 150)
    chain.append(ChainedBufferNode{buf, 75, 100, 75});  /// [75, 175)
    chain.advance(100);                         /// cursor at 100; coverage [100, 175)
    ASSERT_EQ(chain.range().offset, 100u);

    /// Request dips below the cursor; the [75, 100) part is consumed and must be excluded.
    ChainedBuffers s = chain.slice({75, 50});  /// [75, 125)
    EXPECT_EQ(s.range().offset, 100u);
    EXPECT_EQ(s.range().end(), 125u);

    std::string out;
    while (!s.atEnd())
    {
        auto sp = s.peek();
        out.append(sp.data, sp.size);
        s.advance(sp.size);
    }
    EXPECT_EQ(out.size(), 25u);  /// [100, 125) served once despite overlapping nodes
}

TEST(ChainedBuffers, AppendChainedBuffersToConsumedDestinationTrims)
{
    /// append(ChainedBuffers&&) into a partly-consumed destination must trim moved nodes against the
    /// cursor too (the splice path used to bypass the single-node trim).
    auto a = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(a->data(), 'A', 100);
    ChainedBuffers dst;
    dst.append(ChainedBufferNode{a, 0, 100, 100});  /// [100, 200)
    dst.advance(50);                        /// cursor at 150
    ASSERT_EQ(dst.peek().logical_offset, 150u);

    auto b = std::make_shared<OwnedChainedBuffer>(60);
    std::memset(b->data(), 'B', 60);
    ChainedBuffers other;
    other.append(ChainedBufferNode{b, 0, 60, 50});  /// [50, 110), entirely behind the cursor
    dst.append(std::move(other));

    EXPECT_EQ(dst.range().offset, 150u);     /// consumed bytes not re-covered
    EXPECT_EQ(dst.peek().logical_offset, 150u);
    EXPECT_EQ(dst.peek().size, 50u);         /// front node still [150, 200), no OOB peek
}

TEST(ChainedBuffers, AppendBeforeFrontAfterBoundaryAdvanceIsDropped)
{
    /// advance can drop the front node and reset front_offset to 0 while the consumed
    /// frontier has moved past it; an append before the new front must still be dropped
    /// (the `consumed_pos` check, not a `front_offset != 0` one).
    auto buf = std::make_shared<OwnedChainedBuffer>(400);
    std::memset(buf->data(), 'A', 400);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 100, 100, 100});  /// [100, 200)
    chain.append(ChainedBufferNode{buf, 300, 100, 300});  /// [300, 400), gap [200, 300)
    chain.advance(100);                            /// consume [100, 200); front node dropped
    ASSERT_EQ(chain.peek().logical_offset, 300u);  /// gap skipped; cursor lands at 300
    ASSERT_EQ(chain.frontOffsetForTest(), 0u);     /// ...with front_offset back to 0

    auto b = std::make_shared<OwnedChainedBuffer>(10);
    std::memset(b->data(), 'B', 10);
    chain.append(ChainedBufferNode{b, 0, 10, 150});  /// [150, 160): behind the consumed frontier (200)
    EXPECT_EQ(chain.peek().logical_offset, 300u);  /// not resurrected to 150
    EXPECT_EQ(chain.range().offset, 300u);
}

TEST(ChainedBuffers, ShiftMovesConsumedFrontier)
{
    /// `shift` re-bases all logical coordinates, including the consumed frontier, so a
    /// partly-consumed chain keeps `slice`/`append` clamped after the shift.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(buf->data(), 'A', 100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 100});  /// [100, 200)
    chain.advance(25);                           /// consume [100, 125); consumed_pos 125
    chain.shift(100);                            /// -> [200, 300); cursor and consumed_pos -> 225
    ASSERT_EQ(chain.peek().logical_offset, 225u);
    ASSERT_EQ(chain.range().offset, 225u);

    /// A slice dipping below the shifted cursor must not expose consumed bytes.
    ChainedBuffers s = chain.slice({200, 50});  /// [200, 250): [200, 225) is consumed
    EXPECT_EQ(s.range().offset, 225u);

    /// An append below the shifted consumed frontier is dropped, not inserted before front.
    auto b = std::make_shared<OwnedChainedBuffer>(10);
    std::memset(b->data(), 'B', 10);
    chain.append(ChainedBufferNode{b, 0, 10, 150});  /// [150, 160), below 225
    EXPECT_EQ(chain.peek().logical_offset, 225u);
    EXPECT_EQ(chain.range().offset, 225u);
}

TEST(ChainedBuffers, AdvanceZeroIsNoOp)
{
    /// advance(0) must not anchor the consumed frontier: a fresh chain still accepts a
    /// legal out-of-order append before the front node afterwards.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 100});  /// [100, 200)
    chain.advance(0);

    auto early = std::make_shared<OwnedChainedBuffer>(50);
    chain.append(ChainedBufferNode{early, 0, 50, 0});  /// [0, 50): must not be dropped
    EXPECT_EQ(chain.range().offset, 0u);
    EXPECT_EQ(chain.peek().logical_offset, 0u);
    EXPECT_EQ(chain.totalBytes(), 150u);
}

TEST(ChainedBuffers, GapFillAppendBetweenFrontierAndFront)
{
    /// Advancing over a gap leaves the frontier (200) behind the cursor (300). A node
    /// appended into the never-consumed gap is kept (trimmed to the frontier), becomes
    /// the new front, and is served before the rest.
    auto buf = std::make_shared<OwnedChainedBuffer>(400);
    std::memset(buf->data(), 'A', 400);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 100, 100, 100});  /// [100, 200)
    chain.append(ChainedBufferNode{buf, 300, 100, 300});  /// [300, 400), gap [200, 300)
    chain.advance(100);                            /// consume [100, 200)
    ASSERT_EQ(chain.peek().logical_offset, 300u);

    auto fill = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(fill->data(), 'B', 100);
    chain.append(ChainedBufferNode{fill, 0, 100, 150});     /// [150, 250): [150, 200) is consumed
    EXPECT_EQ(chain.peek().logical_offset, 200u);  /// trimmed to [200, 250), new front

    std::string out;
    while (!chain.atEnd())
    {
        auto s = chain.peek();
        out.append(s.data, s.size);
        chain.advance(s.size);
    }
    EXPECT_EQ(out, std::string(50, 'B') + std::string(100, 'A'));  /// [200,250) then [300,400)
}

TEST(ChainedBuffers, AppendToDrainedChainedBuffersRespectsFrontier)
{
    /// A fully drained chain is not a fresh one: the frontier persists, so re-appending
    /// consumed bytes is dropped and a straddling node keeps only its reachable tail.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.advance(100);
    ASSERT_TRUE(chain.atEnd());

    chain.append(ChainedBufferNode{buf, 50, 10, 50});  /// [50, 60): consumed, dropped
    EXPECT_TRUE(chain.atEnd());

    auto b = std::make_shared<OwnedChainedBuffer>(40);
    chain.append(ChainedBufferNode{b, 0, 40, 80});  /// [80, 120) -> trimmed to [100, 120)
    EXPECT_EQ(chain.peek().logical_offset, 100u);
    EXPECT_EQ(chain.peek().size, 20u);
    EXPECT_EQ(chain.range().offset, 100u);
}

TEST(ChainedBuffers, DrainEqualOffsetDuplicateServedOnce)
{
    /// Two nodes for the same byte (e.g. cache hit + later source read): streaming serves
    /// the byte once, from the first-appended node.
    auto b1 = std::make_shared<OwnedChainedBuffer>(1);
    auto b2 = std::make_shared<OwnedChainedBuffer>(1);
    *b1->data() = 'F';
    *b2->data() = 'S';
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{b1, 0, 1, 42});
    chain.append(ChainedBufferNode{b2, 0, 1, 42});

    std::string out;
    while (!chain.atEnd())
    {
        auto s = chain.peek();
        out.append(s.data, s.size);
        chain.advance(s.size);
    }
    EXPECT_EQ(out, "F");
}

TEST(ChainedBuffers, TryRewindBackwardReopensFrontier)
{
    /// A backward rewind lowers the consumed frontier: the re-opened bytes are slice-able
    /// and appendable again, and draining still serves the union exactly once.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(buf->data(), 'A', 100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 100});  /// [100, 200)
    chain.advance(50);                           /// frontier 150
    ASSERT_TRUE(chain.tryRewind(120));           /// re-open [120, 150)

    ChainedBuffers s = chain.slice({120, 30});
    EXPECT_EQ(s.range().offset, 120u);
    EXPECT_EQ(s.range().size, 30u);

    auto b = std::make_shared<OwnedChainedBuffer>(40);
    std::memset(b->data(), 'B', 40);
    chain.append(ChainedBufferNode{b, 0, 40, 110});  /// [110, 150): only [110, 120) is still consumed
    EXPECT_EQ(chain.getNodes().size(), 2u);

    std::string out;
    while (!chain.atEnd())
    {
        auto sp = chain.peek();
        out.append(sp.data, sp.size);
        chain.advance(sp.size);
    }
    EXPECT_EQ(out.size(), 80u);  /// [120, 200) once, no duplicate from the appended overlap
}

TEST(ChainedBuffers, TryRewindToExactEndReachesEOF)
{
    /// `new_position == range().end()` (the exclusive end) is a valid rewind target: EOF.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    ASSERT_TRUE(chain.tryRewind(100));
    EXPECT_TRUE(chain.atEnd());
    EXPECT_FALSE(chain.tryRewind(100));  /// nothing reachable once empty
}

TEST(ChainedBuffers, TryRewindForwardAcrossOverlapBoundary)
{
    /// Forward rewind past the front into an overlapping second node lands exactly on the
    /// requested position with the correct remaining span.
    auto a = std::make_shared<OwnedChainedBuffer>(100);
    auto b = std::make_shared<OwnedChainedBuffer>(100);
    std::memset(a->data(), 'A', 100);
    std::memset(b->data(), 'B', 100);
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{a, 0, 100, 0});   /// [0, 100)
    chain.append(ChainedBufferNode{b, 0, 100, 50});  /// [50, 150)

    ASSERT_TRUE(chain.tryRewind(120));
    auto s = chain.peek();
    EXPECT_EQ(s.logical_offset, 120u);
    EXPECT_EQ(s.size, 30u);
    EXPECT_EQ(s.data[0], 'B');
}

TEST(ChainedBuffers, CopyToAfterPartialAdvance)
{
    /// copyTo on a partly-consumed chain: the consumed prefix is no longer covered, and the
    /// first node is read from its unconsumed part.
    auto buf = std::make_shared<OwnedChainedBuffer>(100);
    for (size_t i = 0; i < 100; ++i)
        buf->data()[i] = static_cast<char>('A' + (i % 26));
    ChainedBuffers chain;
    chain.append(ChainedBufferNode{buf, 0, 100, 0});
    chain.advance(30);

    EXPECT_FALSE(chain.covers({0, 100}));
    std::vector<char> out(40, '\0');
    EXPECT_EQ(chain.copyTo(out.data(), {30, 40}), 40u);
    for (size_t i = 0; i < 40; ++i)
        ASSERT_EQ(out[i], static_cast<char>('A' + ((30 + i) % 26))) << "byte " << i;
}

TEST(ChainedBuffers, PeekOnEmptyChainedBuffers)
{
    ChainedBuffers chain;
    auto s = chain.peek();
    EXPECT_EQ(s.data, nullptr);
    EXPECT_EQ(s.size, 0u);
    EXPECT_EQ(s.logical_offset, 0u);
    EXPECT_TRUE(chain.atEnd());
}

TEST(ChainedBuffers, AppendEmptyChainedBuffersIsNoOp)
{
    auto chain = makeAdjacentChainedBuffers(/*count=*/1, /*node_size=*/10);
    chain.append(ChainedBuffers{});
    EXPECT_EQ(chain.getNodes().size(), 1u);
    EXPECT_EQ(chain.range().offset, 0u);
    EXPECT_EQ(chain.range().size, 10u);
}

TEST(ChainedBuffers, ZeroSizeCoverageQueries)
{
    auto chain = makeAdjacentChainedBuffers(/*count=*/2, /*node_size=*/10);  /// [0, 20)
    EXPECT_EQ(chain.coveredBytes({5, 0}), 0u);
    EXPECT_TRUE(chain.gaps({5, 0}).empty());
    EXPECT_TRUE(chain.covers({5, 0}));
}

TEST(ChainedBuffers, SliceAndCopyToSkipNodesOutsideRequest)
{
    /// A sub-range request over a multi-node chain: nodes entirely before the request are
    /// skipped, the first node past it stops the walk -- for both slice and copyTo.
    auto chain = makeAdjacentChainedBuffers(/*count=*/3, /*node_size=*/10);  /// 0x10 | 0x11 | 0x12

    ChainedBuffers s = chain.slice({12, 5});  /// inside the middle node only
    ASSERT_EQ(s.getNodes().size(), 1u);
    EXPECT_EQ(s.range().offset, 12u);
    EXPECT_EQ(s.range().size, 5u);
    EXPECT_EQ(static_cast<unsigned char>(s.peek().data[0]), 0x11u);

    std::vector<char> out(5, '\0');
    EXPECT_EQ(chain.copyTo(out.data(), {12, 5}), 5u);
    for (size_t i = 0; i < out.size(); ++i)
        EXPECT_EQ(static_cast<unsigned char>(out[i]), 0x11u) << "byte " << i;
}

TEST(OwnedChainedBuffer, ConstDataOverload)
{
    auto buf = std::make_shared<OwnedChainedBuffer>(8);
    std::memset(buf->data(), 'Z', 8);
    const OwnedChainedBuffer & cref = *buf;
    EXPECT_EQ(cref.data(), buf->data());
    EXPECT_EQ(cref.data()[7], 'Z');
}
