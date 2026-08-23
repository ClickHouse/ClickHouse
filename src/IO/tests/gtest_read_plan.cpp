#include <IO/ReadPlan.h>

#include <gtest/gtest.h>

using namespace DB;
using CacheResolution = ICacheProvider::CacheResolution;

namespace
{

/// Minimal cache reader: only its `range` matters to the plan (serving is the executor's job).
class MockReader : public CacheReader
{
public:
    explicit MockReader(ByteRange r_) : r(r_) {}
    ByteRange range() const override { return r; }
    ChainedBuffers read(ByteRange) override { return {}; }
private:
    ByteRange r;
};

/// Minimal cache writer with a settable committed prefix, so a test can model a miss segment filling.
class MockWriter : public CacheWriter
{
public:
    explicit MockWriter(ByteRange r_, bool whole_ = false) : r(r_), frontier(r_.offset), whole(whole_) {}
    void commit(ByteRange c) { frontier = c.end(); }   /// a prefix `[r.offset, c.end())`
    ByteRange range() const override { return r; }
    size_t committed() const override { return frontier; }
    bool fillsWholeSegment() const override { return whole; }
    size_t write(ChainedBuffers, const Claim &) override { return 0; }
    ChainedBuffers read(ByteRange) override { return {}; }
private:
    ByteRange r;
    size_t frontier;
    bool whole;
};

CacheResolution hit(ByteRange range)
{
    CacheResolution c;
    c.kind = CacheResolution::Kind::Hit;
    c.range = range;
    c.reader = std::make_unique<MockReader>(range);
    return c;
}

CacheResolution miss(ByteRange range, bool with_writer = true, bool whole_segment = false)
{
    CacheResolution c;
    c.kind = CacheResolution::Kind::Miss;
    c.range = range;
    if (with_writer)
        c.writer = std::make_unique<MockWriter>(range, whole_segment);
    return c;
}

PlanTier tier(CacheTier t, bool populates, std::vector<CacheResolution> cells)
{
    PlanTier pt;
    pt.tier = t;
    pt.populates = populates;
    for (auto & c : cells)
        pt.cells.push_back(std::move(c));
    return pt;
}

/// Variadic (not an initializer_list) because PlanTier is move-only - a braced list would copy.
template <typename... Ts>
VectorWithMemoryTracking<PlanTier> tiers(Ts &&... ts)
{
    VectorWithMemoryTracking<PlanTier> out;
    (out.push_back(std::forward<Ts>(ts)), ...);
    return out;
}

}

TEST(ReadPlan, HitsServeFromReaderPerCell)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 1}));
    c.push_back(hit({1, 1}));
    c.push_back(hit({2, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(3, tiers(tier(CacheTier::PageCache, true, std::move(c))));

    auto r0 = plan.runAt(0);
    EXPECT_NE(r0.reader, nullptr);
    EXPECT_FALSE(r0.isFetch());
    EXPECT_EQ(r0.range.offset, 0u);
    EXPECT_EQ(r0.range.end(), 1u);   /// serves to the hit cell end

    EXPECT_NE(plan.runAt(2).reader, nullptr);
}

TEST(ReadPlan, MissesCoalesceIntoOneFetchRun)
{
    std::vector<CacheResolution> c;
    c.push_back(miss({0, 1}));
    c.push_back(miss({1, 1}));
    c.push_back(hit({2, 1}));
    c.push_back(miss({3, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(4, tiers(tier(CacheTier::PageCache, true, std::move(c))));

    /// [0,2) is an uncommitted miss run; it coalesces and stops at the hit at 2.
    auto r = plan.runAt(0);
    EXPECT_TRUE(r.isFetch());
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.end(), 2u);

    /// writersFor spans both miss cells in the fetch run.
    EXPECT_EQ(plan.writersFor({0, 2}).size(), 2u);

    /// The hit caps the run and is servable.
    EXPECT_NE(plan.runAt(2).reader, nullptr);
}

TEST(ReadPlan, CommittedWriterBecomesServable)
{
    ReadPlan plan;
    plan.reset(0);
    auto missed = miss({0, 2});
    auto * writer = static_cast<MockWriter *>(missed.writer.get());
    std::vector<CacheResolution> c;
    c.push_back(std::move(missed));
    plan.extend(2, tiers(tier(CacheTier::PageCache, true, std::move(c))));

    EXPECT_TRUE(plan.runAt(0).isFetch());   /// nothing committed yet

    writer->commit({0, 2});                 /// the executor filled it
    auto r = plan.runAt(0);
    EXPECT_FALSE(r.isFetch());
    EXPECT_NE(r.writer, nullptr);
    EXPECT_EQ(r.range.end(), 2u);
}

TEST(ReadPlan, FastestTierWinsAndSlowHitCapsFetch)
{
    std::vector<CacheResolution> page;
    page.push_back(miss({0, 1}));
    page.push_back(miss({1, 1}));
    page.push_back(miss({2, 1}));
    page.push_back(miss({3, 1}));
    std::vector<CacheResolution> fs;
    fs.push_back(miss({0, 1}));
    fs.push_back(miss({1, 1}));
    fs.push_back(hit({2, 2}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(4, tiers(
        tier(CacheTier::PageCache, true, std::move(page)),
        tier(CacheTier::FilesystemCache, true, std::move(fs))));

    /// [0,2) miss on both tiers -> fetch, capped at the fs hit at 2.
    auto r = plan.runAt(0);
    EXPECT_TRUE(r.isFetch());
    EXPECT_EQ(r.range.end(), 2u);

    /// At 2 the fs tier serves it (fastest tier missed).
    auto r2 = plan.runAt(2);
    EXPECT_FALSE(r2.isFetch());
    EXPECT_NE(r2.reader, nullptr);
    EXPECT_EQ(r2.range.end(), 4u);

    /// A fetch of [0,2) writes up both populating tiers.
    EXPECT_EQ(plan.writersFor({0, 2}).size(), 4u);
}

TEST(ReadPlan, RetireBeforeReleasesConsumedPrefix)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 1}));
    c.push_back(hit({1, 1}));
    c.push_back(hit({2, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(3, tiers(tier(CacheTier::PageCache, true, std::move(c))));

    plan.retireBefore(2);
    EXPECT_EQ(plan.spanStart(), 2u);
    EXPECT_NE(plan.runAt(2).reader, nullptr);
}

TEST(ReadPlan, ExtendGrowsRightAndDropsOverhang)
{
    ReadPlan plan;
    plan.reset(0);
    /// First span [0,2): a miss segment overhangs to 3.
    std::vector<CacheResolution> first;
    first.push_back(hit({0, 1}));
    first.push_back(miss({1, 2}));   /// [1,3) overhangs the span end 2
    plan.extend(2, tiers(tier(CacheTier::PageCache, true, std::move(first))));
    /// Next span [2,4): resolve re-returns the [1,3) segment; extend must drop the overlap.
    std::vector<CacheResolution> second;
    second.push_back(miss({1, 2}));   /// duplicate of the held overhang -> dropped
    second.push_back(hit({3, 1}));
    plan.extend(4, tiers(tier(CacheTier::PageCache, true, std::move(second))));

    EXPECT_EQ(plan.resolvedEnd(), 4u);
    /// [1,3) is one coalesced miss (not doubled); the hit at 3 caps it.
    auto r = plan.runAt(1);
    EXPECT_TRUE(r.isFetch());
    EXPECT_EQ(r.range.end(), 3u);
    EXPECT_EQ(plan.writersFor({1, 3}).size(), 1u);   /// one writer, not two
    EXPECT_NE(plan.runAt(3).reader, nullptr);
}

TEST(ReadPlan, FetchExtendsLeftToFillFrontier)
{
    /// One incremental miss segment [0,4). A read that opens mid-segment must fetch from the segment's
    /// write frontier so the append-only fill is contiguous - below `offset` when nothing is committed.
    auto missed = miss({0, 4});
    auto * writer = static_cast<MockWriter *>(missed.writer.get());
    std::vector<CacheResolution> c;
    c.push_back(std::move(missed));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(4, tiers(tier(CacheTier::FilesystemCache, true, std::move(c))));

    /// Virgin: the fetch extends left to the segment start (0), not `offset` (2).
    auto r = plan.runAt(2);
    EXPECT_TRUE(r.isFetch());
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.end(), 4u);

    /// The window caps the right end; the left extension is independent of it.
    EXPECT_EQ(plan.runAt(2, 1).range.offset, 0u);
    EXPECT_EQ(plan.runAt(2, 1).range.end(), 3u);

    /// After committing [0,2), a read at 3 fetches from the frontier 2, not the segment start.
    writer->commit({0, 2});
    auto r2 = plan.runAt(3);
    EXPECT_TRUE(r2.isFetch());
    EXPECT_EQ(r2.range.offset, 2u);
    EXPECT_EQ(r2.range.end(), 4u);
}

TEST(ReadPlan, WholeSegmentHeadFetchedEntireEvenPastSpanEnd)
{
    /// A whole-segment cell [0,4) that overhangs the resolved span (span_end = 2). The head fetch must
    /// still cover the ENTIRE cell - it is populated only by an all-or-nothing write - so the extent
    /// reaches the true segment end past span_end, and neither the window caps it below the cell.
    std::vector<CacheResolution> c;
    c.push_back(miss({0, 4}, /*with_writer=*/true, /*whole_segment=*/true));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(2, tiers(tier(CacheTier::PageCache, true, std::move(c))));   /// span_end = 2, cell to 4

    auto r = plan.runAt(0, /*max_fetch_ahead=*/1);
    EXPECT_TRUE(r.isFetch());
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.end(), 4u);
}

TEST(ReadPlan, ResetDiscardsAndReanchors)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 1}));
    c.push_back(hit({1, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(2, tiers(tier(CacheTier::PageCache, true, std::move(c))));

    plan.reset(10);
    EXPECT_TRUE(plan.empty());
    EXPECT_EQ(plan.spanStart(), 10u);
    EXPECT_EQ(plan.resolvedEnd(), 10u);
}
