#include <IO/ReadPlan.h>

#include <gtest/gtest.h>

#include <limits>

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
    size_t write(ChainedBuffers, const FillRole &) override { return 0; }
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

PlanTier tier(CacheTier t, std::vector<CacheResolution> cells)
{
    PlanTier pt;
    pt.tier = t;
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

/// `runAt` with no cap on the fetch extent.
constexpr size_t NO_LIMIT = std::numeric_limits<size_t>::max();

/// Extract a specific `PlanRun` alternative from `run`, or nullptr if it is a different outcome.
template <typename T>
const T * as(const ReadPlan::PlanRun & run) { return std::get_if<T>(&run); }

}

TEST(ReadPlan, HitsServeFromReaderPerCell)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 1}));
    c.push_back(hit({1, 1}));
    c.push_back(hit({2, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(3, tiers(tier(CacheTier::PageCache, std::move(c))));

    auto r0 = plan.runAt(0, NO_LIMIT);
    const auto * hit0 = as<ReadPlan::ServeFromReader>(r0);
    ASSERT_NE(hit0, nullptr);
    EXPECT_EQ(hit0->range.offset, 0u);
    EXPECT_EQ(hit0->range.end(), 1u);   /// serves to the hit cell end

    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(2, NO_LIMIT)), nullptr);
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
    plan.extend(4, tiers(tier(CacheTier::PageCache, std::move(c))));

    /// [0,2) is an uncommitted miss run; it coalesces and stops at the hit at 2.
    auto r = plan.runAt(0, NO_LIMIT);
    const auto * fetch = as<ReadPlan::Fetch>(r);
    ASSERT_NE(fetch, nullptr);
    EXPECT_EQ(fetch->range.offset, 0u);
    EXPECT_EQ(fetch->range.end(), 2u);

    /// writersFor spans both miss cells in the fetch run.
    EXPECT_EQ(plan.writersFor({0, 2}).size(), 2u);

    /// The hit caps the run and is servable.
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(2, NO_LIMIT)), nullptr);
}

TEST(ReadPlan, CommittedWriterBecomesServable)
{
    ReadPlan plan;
    plan.reset(0);
    auto missed = miss({0, 2});
    auto * writer = static_cast<MockWriter *>(missed.writer.get());
    std::vector<CacheResolution> c;
    c.push_back(std::move(missed));
    plan.extend(2, tiers(tier(CacheTier::PageCache, std::move(c))));

    EXPECT_NE(as<ReadPlan::Fetch>(plan.runAt(0, NO_LIMIT)), nullptr);   /// nothing committed yet

    writer->commit({0, 2});                 /// the executor filled it
    auto r = plan.runAt(0, NO_LIMIT);
    const auto * srv = as<ReadPlan::ServeFromWriter>(r);
    ASSERT_NE(srv, nullptr);
    EXPECT_EQ(srv->range.end(), 2u);
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
        tier(CacheTier::PageCache, std::move(page)),
        tier(CacheTier::FilesystemCache, std::move(fs))));

    /// [0,2) miss on both tiers -> fetch, capped at the fs hit at 2.
    auto r = plan.runAt(0, NO_LIMIT);
    const auto * fetch = as<ReadPlan::Fetch>(r);
    ASSERT_NE(fetch, nullptr);
    EXPECT_EQ(fetch->range.end(), 2u);

    /// At 2 the fs tier serves it (fastest tier missed).
    auto r2 = plan.runAt(2, NO_LIMIT);
    const auto * hit2 = as<ReadPlan::ServeFromReader>(r2);
    ASSERT_NE(hit2, nullptr);
    EXPECT_EQ(hit2->range.end(), 4u);

    /// A fetch of [0,2) writes up both populating tiers.
    EXPECT_EQ(plan.writersFor({0, 2}).size(), 4u);
}

TEST(ReadPlan, DropBeforeReleasesConsumedPrefix)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 1}));
    c.push_back(hit({1, 1}));
    c.push_back(hit({2, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(3, tiers(tier(CacheTier::PageCache, std::move(c))));

    plan.dropBefore(2);
    EXPECT_EQ(plan.begin(), 2u);
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(2, NO_LIMIT)), nullptr);
}

TEST(ReadPlan, ExtendKeepsTheSuffixOfAStraddlingCell)
{
    /// A writer-less tier reports a segment from its own start at its live committed end, so a refill
    /// re-returns it longer than the held copy. The grown suffix must stay covered.
    ReadPlan plan;
    plan.reset(0);
    std::vector<CacheResolution> first;
    first.push_back(hit({0, 2}));
    plan.extend(2, tiers(tier(CacheTier::FilesystemCache, std::move(first))));

    std::vector<CacheResolution> warmer;
    warmer.push_back(hit({0, 4}));   /// same segment, now committed through 4
    plan.extend(4, tiers(tier(CacheTier::FilesystemCache, std::move(warmer))));

    auto run = plan.runAt(2, NO_LIMIT);
    const auto * grown = as<ReadPlan::ServeFromReader>(run);
    ASSERT_NE(grown, nullptr);
    EXPECT_EQ(grown->range.end(), 4u);
}

TEST(ReadPlan, ExtendKeepsThePartialSegmentsWriter)
{
    /// A partial segment resolves as two cells from one start: the committed prefix as a hit, then the
    /// whole extent as a miss carrying the writer. The miss must survive its own prefix hit.
    ReadPlan plan;
    plan.reset(0);
    std::vector<CacheResolution> first;
    first.push_back(hit({0, 2}));
    plan.extend(2, tiers(tier(CacheTier::FilesystemCache, std::move(first))));

    auto missed = miss({2, 4});                                /// segment [2,6)
    static_cast<MockWriter *>(missed.writer.get())->commit({2, 2});   /// its frontier is at 4
    std::vector<CacheResolution> partial;
    partial.push_back(hit({2, 2}));                            /// committed prefix [2,4)
    partial.push_back(std::move(missed));
    plan.extend(6, tiers(tier(CacheTier::FilesystemCache, std::move(partial))));

    EXPECT_EQ(plan.writersFor({4, 2}).size(), 1u);
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(2, NO_LIMIT)), nullptr);
    /// The tail is a fetch that starts at the frontier, not below it.
    auto tail = plan.runAt(4, NO_LIMIT);
    const auto * fetch = as<ReadPlan::Fetch>(tail);
    ASSERT_NE(fetch, nullptr);
    EXPECT_EQ(fetch->range.offset, 4u);
}

TEST(ReadPlan, DropAfterReleasesTheUnreachedTail)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 1}));
    c.push_back(hit({1, 1}));
    c.push_back(hit({2, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(3, tiers(tier(CacheTier::PageCache, std::move(c))));

    plan.dropAfter(1);
    EXPECT_EQ(plan.end(), 1u);
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(0, NO_LIMIT)), nullptr);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(plan.runAt(1, NO_LIMIT)));

    std::vector<CacheResolution> again;
    again.push_back(hit({1, 1}));
    again.push_back(hit({2, 1}));
    plan.extend(3, tiers(tier(CacheTier::PageCache, std::move(again))));
    EXPECT_EQ(plan.end(), 3u);
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(2, NO_LIMIT)), nullptr);
}

TEST(ReadPlan, DropAfterKeepsTheCellStraddlingTheCut)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 2}));
    c.push_back(hit({2, 2}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(4, tiers(tier(CacheTier::PageCache, std::move(c))));

    plan.dropAfter(3);
    EXPECT_EQ(plan.end(), 3u);
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(2, NO_LIMIT)), nullptr);
    EXPECT_TRUE(std::holds_alternative<std::monostate>(plan.runAt(3, NO_LIMIT)));
}

TEST(ReadPlan, ExtendGrowsRightAndDropsOverhang)
{
    ReadPlan plan;
    plan.reset(0);
    /// First range [0,2): a miss segment overhangs to 3.
    std::vector<CacheResolution> first;
    first.push_back(hit({0, 1}));
    first.push_back(miss({1, 2}));   /// [1,3) overhangs the range end 2
    plan.extend(2, tiers(tier(CacheTier::PageCache, std::move(first))));
    /// Next range [2,4): resolve re-returns the [1,3) segment; extend must drop the overlap.
    std::vector<CacheResolution> second;
    second.push_back(miss({1, 2}));   /// duplicate of the held overhang -> dropped
    second.push_back(hit({3, 1}));
    plan.extend(4, tiers(tier(CacheTier::PageCache, std::move(second))));

    EXPECT_EQ(plan.end(), 4u);
    /// [1,3) is one coalesced miss (not doubled); the hit at 3 caps it.
    auto r = plan.runAt(1, NO_LIMIT);
    const auto * fetch = as<ReadPlan::Fetch>(r);
    ASSERT_NE(fetch, nullptr);
    EXPECT_EQ(fetch->range.end(), 3u);
    EXPECT_EQ(plan.writersFor({1, 3}).size(), 1u);   /// one writer, not two
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(3, NO_LIMIT)), nullptr);
}

/// `CacheTier` does not identify a tier: a stacked cache-on-cache chain has two `FilesystemCache`
/// layers. Pairing held tiers to resolved tiers by the enum folded both onto the FIRST resolved entry,
/// so the second layer appended moved-from cells (null `reader`) and its own cells were dropped.
TEST(ReadPlan, StackedSameTierLayersKeepTheirOwnCells)
{
    ReadPlan plan;
    plan.reset(0);
    /// Span [0,2): both layers miss.
    std::vector<CacheResolution> fast_first;
    fast_first.push_back(miss({0, 2}));
    std::vector<CacheResolution> slow_first;
    slow_first.push_back(miss({0, 2}));
    plan.extend(2, tiers(tier(CacheTier::FilesystemCache, std::move(fast_first)),
                         tier(CacheTier::FilesystemCache, std::move(slow_first))));

    /// Span [2,4): the fast layer now holds [2,3), the slow layer holds [3,4).
    std::vector<CacheResolution> fast_second;
    fast_second.push_back(hit({2, 1}));
    std::vector<CacheResolution> slow_second;
    slow_second.push_back(hit({3, 1}));
    plan.extend(4, tiers(tier(CacheTier::FilesystemCache, std::move(fast_second)),
                         tier(CacheTier::FilesystemCache, std::move(slow_second))));

    EXPECT_EQ(plan.end(), 4u);
    /// The fast layer's cell serves 2 ...
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(2, NO_LIMIT)), nullptr);
    /// ... and the slow layer keeps its OWN cell, so 3 is served rather than re-fetched.
    EXPECT_NE(as<ReadPlan::ServeFromReader>(plan.runAt(3, NO_LIMIT)), nullptr);
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
    plan.extend(4, tiers(tier(CacheTier::FilesystemCache, std::move(c))));

    /// Virgin: the fetch extends left to the segment start (0), not `offset` (2).
    auto r = plan.runAt(2, NO_LIMIT);
    const auto * fetch = as<ReadPlan::Fetch>(r);
    ASSERT_NE(fetch, nullptr);
    EXPECT_EQ(fetch->range.offset, 0u);
    EXPECT_EQ(fetch->range.end(), 4u);

    /// The window caps the right end; the left extension is independent of it.
    auto rc = plan.runAt(2, 1);
    const auto * capped = as<ReadPlan::Fetch>(rc);
    ASSERT_NE(capped, nullptr);
    EXPECT_EQ(capped->range.offset, 0u);
    EXPECT_EQ(capped->range.end(), 3u);

    /// After committing [0,2), a read at 3 fetches from the frontier 2, not the segment start.
    writer->commit({0, 2});
    auto r2 = plan.runAt(3, NO_LIMIT);
    const auto * fetch2 = as<ReadPlan::Fetch>(r2);
    ASSERT_NE(fetch2, nullptr);
    EXPECT_EQ(fetch2->range.offset, 2u);
    EXPECT_EQ(fetch2->range.end(), 4u);
}

TEST(ReadPlan, WholeSegmentHeadFetchedEntireEvenPastSpanEnd)
{
    /// A whole-segment cell [0,4) that overhangs the resolved range (range_end = 2). The head fetch must
    /// still cover the ENTIRE cell - it is populated only by an all-or-nothing write - so the extent
    /// reaches the true segment end past range_end, and neither the window caps it below the cell.
    std::vector<CacheResolution> c;
    c.push_back(miss({0, 4}, /*with_writer=*/true, /*whole_segment=*/true));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(2, tiers(tier(CacheTier::PageCache, std::move(c))));   /// range_end = 2, cell to 4

    auto r = plan.runAt(0, /*fetch_limit=*/1);
    const auto * fetch = as<ReadPlan::Fetch>(r);
    ASSERT_NE(fetch, nullptr);
    EXPECT_EQ(fetch->range.offset, 0u);
    EXPECT_EQ(fetch->range.end(), 4u);
}

TEST(ReadPlan, ResetDiscardsAndReanchors)
{
    std::vector<CacheResolution> c;
    c.push_back(hit({0, 1}));
    c.push_back(hit({1, 1}));

    ReadPlan plan;
    plan.reset(0);
    plan.extend(2, tiers(tier(CacheTier::PageCache, std::move(c))));

    plan.reset(10);
    EXPECT_TRUE(plan.empty());
    EXPECT_EQ(plan.begin(), 10u);
    EXPECT_EQ(plan.end(), 10u);
}

TEST(ReadPlan, MemoryHoldServedFirstAndFreedOnDrop)
{
    /// Bytes a fetch pulled that no tier accepted are held in the plan, served before any tier, and
    /// freed as the cursor passes.
    std::vector<CacheResolution> c;
    c.push_back(miss({0, 4}));
    ReadPlan plan;
    plan.reset(0);
    plan.extend(4, tiers(tier(CacheTier::FilesystemCache, std::move(c))));

    auto buf = std::make_shared<OwnedChainedBuffer>(2);
    ChainedBuffers held;
    held.append(ChainedBufferNode{buf, 0, 2, 1});   /// holds [1, 3)
    plan.hold(std::move(held));

    /// A miss offset not held → FETCH; a held offset → memory, up to the hold's end.
    EXPECT_NE(as<ReadPlan::Fetch>(plan.runAt(0, NO_LIMIT)), nullptr);
    auto r = plan.runAt(1, NO_LIMIT);
    const auto * mem = as<ReadPlan::ServeFromMemory>(r);
    ASSERT_NE(mem, nullptr);
    EXPECT_EQ(mem->range.offset, 1u);
    EXPECT_EQ(mem->range.end(), 3u);
    EXPECT_EQ(mem->memory->slice(ByteRange{1, 2}).totalBytes(), 2u);

    /// Drop past the hold frees it; the offset is then a plain miss again.
    plan.dropBefore(3);
    EXPECT_EQ(as<ReadPlan::ServeFromMemory>(plan.runAt(3, NO_LIMIT)), nullptr);
}
