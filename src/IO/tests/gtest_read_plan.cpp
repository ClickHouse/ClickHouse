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

/// Minimal cache writer with a settable committed set, so a test can model a miss segment filling.
class MockWriter : public CacheWriter
{
public:
    explicit MockWriter(ByteRange r_) : r(r_) {}
    void commit(ByteRange c) { committed_ranges.add(c); }
    ByteRange range() const override { return r; }
    IntervalSet committed() const override { return committed_ranges; }
    size_t write(ChainedBuffers, const Claim &) override { return 0; }
    ChainedBuffers read(ByteRange) override { return {}; }
private:
    ByteRange r;
    IntervalSet committed_ranges;
};

CacheResolution hit(ByteRange range)
{
    CacheResolution c;
    c.kind = CacheResolution::Kind::Hit;
    c.range = range;
    c.reader = std::make_unique<MockReader>(range);
    return c;
}

CacheResolution miss(ByteRange range, bool with_writer = true)
{
    CacheResolution c;
    c.kind = CacheResolution::Kind::Miss;
    c.range = range;
    if (with_writer)
        c.writer = std::make_unique<MockWriter>(range);
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

VectorWithMemoryTracking<PlanTier> tiers(std::vector<PlanTier> ts)
{
    VectorWithMemoryTracking<PlanTier> out;
    for (auto & t : ts)
        out.push_back(std::move(t));
    return out;
}

}

TEST(ReadPlan, HitsServeFromReaderPerCell)
{
    ReadPlan plan;
    plan.reset(0);
    plan.extend(3, tiers({tier(CacheTier::PageCache, true, [] {
        std::vector<CacheResolution> v;
        v.push_back(hit({0, 1}));
        v.push_back(hit({1, 1}));
        v.push_back(hit({2, 1}));
        return v;
    }())}));

    auto r0 = plan.runAt(0);
    EXPECT_NE(r0.reader, nullptr);
    EXPECT_FALSE(r0.isFetch());
    EXPECT_EQ(r0.range.offset, 0u);
    EXPECT_EQ(r0.range.end(), 1u);   /// serves to the hit cell end

    EXPECT_NE(plan.runAt(2).reader, nullptr);
}

TEST(ReadPlan, MissesCoalesceIntoOneFetchRun)
{
    ReadPlan plan;
    plan.reset(0);
    plan.extend(4, tiers({tier(CacheTier::PageCache, true, [] {
        std::vector<CacheResolution> v;
        v.push_back(miss({0, 1}));
        v.push_back(miss({1, 1}));
        v.push_back(hit({2, 1}));
        v.push_back(miss({3, 1}));
        return v;
    }())}));

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
    std::vector<CacheResolution> cells;
    cells.push_back(std::move(missed));
    plan.extend(2, tiers({tier(CacheTier::PageCache, true, std::move(cells))}));

    EXPECT_TRUE(plan.runAt(0).isFetch());   /// nothing committed yet

    writer->commit({0, 2});                 /// the executor filled it
    auto r = plan.runAt(0);
    EXPECT_FALSE(r.isFetch());
    EXPECT_NE(r.writer, nullptr);
    EXPECT_EQ(r.range.end(), 2u);
}

TEST(ReadPlan, FastestTierWinsAndSlowHitCapsFetch)
{
    ReadPlan plan;
    plan.reset(0);
    /// page (fast): all miss; fs (slow): hit at [2,4).
    plan.extend(4, tiers({
        tier(CacheTier::PageCache, true, [] {
            std::vector<CacheResolution> v;
            v.push_back(miss({0, 1}));
            v.push_back(miss({1, 1}));
            v.push_back(miss({2, 1}));
            v.push_back(miss({3, 1}));
            return v;
        }()),
        tier(CacheTier::FilesystemCache, true, [] {
            std::vector<CacheResolution> v;
            v.push_back(miss({0, 1}));
            v.push_back(miss({1, 1}));
            v.push_back(hit({2, 2}));
            return v;
        }()),
    }));

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
    ReadPlan plan;
    plan.reset(0);
    plan.extend(3, tiers({tier(CacheTier::PageCache, true, [] {
        std::vector<CacheResolution> v;
        v.push_back(hit({0, 1}));
        v.push_back(hit({1, 1}));
        v.push_back(hit({2, 1}));
        return v;
    }())}));

    plan.retireBefore(2);
    EXPECT_EQ(plan.spanStart(), 2u);
    EXPECT_NE(plan.runAt(2).reader, nullptr);
}

TEST(ReadPlan, ExtendGrowsRightAndDropsOverhang)
{
    ReadPlan plan;
    plan.reset(0);
    /// First span [0,2): a miss segment overhangs to 3.
    plan.extend(2, tiers({tier(CacheTier::PageCache, true, [] {
        std::vector<CacheResolution> v;
        v.push_back(hit({0, 1}));
        v.push_back(miss({1, 2}));   /// [1,3) overhangs the span end 2
        return v;
    }())}));
    /// Next span [2,4): resolve re-returns the [1,3) segment; extend must drop the overlap.
    plan.extend(4, tiers({tier(CacheTier::PageCache, true, [] {
        std::vector<CacheResolution> v;
        v.push_back(miss({1, 2}));   /// duplicate of the held overhang -> dropped
        v.push_back(hit({3, 1}));
        return v;
    }())}));

    EXPECT_EQ(plan.resolvedEnd(), 4u);
    /// [1,3) is one coalesced miss (not doubled); the hit at 3 caps it.
    auto r = plan.runAt(1);
    EXPECT_TRUE(r.isFetch());
    EXPECT_EQ(r.range.end(), 3u);
    EXPECT_EQ(plan.writersFor({1, 3}).size(), 1u);   /// one writer, not two
    EXPECT_NE(plan.runAt(3).reader, nullptr);
}

TEST(ReadPlan, ResetDiscardsAndReanchors)
{
    ReadPlan plan;
    plan.reset(0);
    plan.extend(2, tiers({tier(CacheTier::PageCache, true, [] {
        std::vector<CacheResolution> v;
        v.push_back(hit({0, 1}));
        v.push_back(hit({1, 1}));
        return v;
    }())}));

    plan.reset(10);
    EXPECT_TRUE(plan.empty());
    EXPECT_EQ(plan.spanStart(), 10u);
    EXPECT_EQ(plan.resolvedEnd(), 10u);
}
