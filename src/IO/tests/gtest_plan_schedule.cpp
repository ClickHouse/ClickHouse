#include <IO/PlanSchedule.h>
#include <IO/ReadPlanGeometry.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

GeometryEntry tierEntry(CacheTier tier,
                        std::vector<ByteRange> resident,
                        std::vector<ByteRange> aligned_miss,
                        size_t head_align = 1, size_t tail_align = 1)
{
    GeometryEntry e;
    e.tier = tier;
    e.head_align = head_align;
    e.tail_align = tail_align;
    for (auto r : resident) e.resident.push_back(r);
    for (auto m : aligned_miss) e.aligned_miss.push_back(m);
    return e;
}

ReadPlanGeometry geometry(size_t plan_start, size_t plan_end, std::vector<GeometryEntry> entries)
{
    ReadPlanGeometry g;
    g.plan_start = plan_start;
    g.plan_end = plan_end;
    for (auto & e : entries) g.entries.push_back(std::move(e));
    return g;
}

PlanSchedule describe(const ReadPlanGeometry & g, ByteRange request)
{
    return describePlan(g, request, MemoryPressureLevel{}, /*min_bytes_for_seek=*/2);
}

struct Seg { size_t off; size_t size; PlanSchedule::Purpose purpose; bool resident; };

void expectRanges(const PlanSchedule & s, const std::vector<Seg> & want)
{
    ASSERT_EQ(s.ranges.size(), want.size()) << "range count";
    for (size_t i = 0; i < want.size(); ++i)
    {
        EXPECT_EQ(s.ranges[i].range.offset, want[i].off) << "range[" << i << "].off";
        EXPECT_EQ(s.ranges[i].range.size, want[i].size) << "range[" << i << "].size";
        EXPECT_EQ(s.ranges[i].purpose == PlanSchedule::Purpose::User, want[i].purpose == PlanSchedule::Purpose::User)
            << "range[" << i << "].purpose";
        EXPECT_EQ(s.ranges[i].resident, want[i].resident) << "range[" << i << "].resident";
    }
}

void expectSteps(const PlanSchedule & s, const std::vector<ByteRange> & want)
{
    ASSERT_EQ(s.steps.size(), want.size()) << "step count";
    for (size_t i = 0; i < want.size(); ++i)
    {
        EXPECT_EQ(s.steps[i].output.offset, want[i].offset) << "step[" << i << "].off";
        EXPECT_EQ(s.steps[i].output.size, want[i].size) << "step[" << i << "].size";
    }
}

constexpr auto User = PlanSchedule::Purpose::User;
constexpr auto Fill = PlanSchedule::Purpose::FillOnly;

PlanSchedule describeSeek(const ReadPlanGeometry & g, ByteRange request, size_t min_bytes_for_seek)
{
    return describePlan(g, request, MemoryPressureLevel{}, min_bytes_for_seek);
}

bool intoHas(const PlanSchedule::Retrieve & r, size_t entry, ByteRange cell)
{
    for (const auto & t : r.into)
        if (t.entry == entry && t.cell.offset == cell.offset && t.cell.size == cell.size)
            return true;
    return false;
}

bool rangeContains(ByteRange outer, ByteRange inner)
{
    return inner.offset >= outer.offset && inner.end() <= outer.end();
}

}

TEST(PlanScheduleSteps, ColdAllGap)
{
    auto g = geometry(0, 8, {tierEntry(CacheTier::FilesystemCache, {}, {{0, 8}})});
    auto s = describe(g, {0, 8});
    expectRanges(s, {{0, 8, User, false}});
    expectSteps(s, {{0, 8}});
}

TEST(PlanScheduleSteps, AllResident)
{
    auto g = geometry(0, 8, {tierEntry(CacheTier::PageCache, {{0, 8}}, {})});
    auto s = describe(g, {0, 8});
    expectRanges(s, {{0, 8, User, true}});
    expectSteps(s, {{0, 8}});
}

/// The DESIGN.md worked example: request [4,8); page holds [3,5); fs misses
/// the whole [0,6) and [6,8) segments.
TEST(PlanScheduleSteps, DesignWorkedExample)
{
    auto g = geometry(0, 8, {
        tierEntry(CacheTier::PageCache, {{3, 2}}, {{5, 3}}),         // resident [3,5), miss [5,8)
        // fs head_align=6 = the segment size: the gap [5,8) aligns its head down
        // to the segment start 0, demanding the before-slack [0,4).
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 2}}, /*head_align=*/6),
    });
    auto s = describe(g, {4, 4});  // request [4,8)

    /// fill_region = [0,8); slack [0,4).
    expectRanges(s, {
        {0, 3, Fill, false},  // [0,3) before-request gap
        {3, 1, Fill, true},   // [3,4) before-request page hit
        {4, 1, User, true},   // [4,5) user page hit
        {5, 3, User, false},  // [5,8) user gap
    });
    /// readNextWindow returns the page hit [4,5), then the gap [5,8).
    expectSteps(s, {{4, 1}, {5, 3}});
}

/// Request fully resident -> no fill closure beyond the request, no slack.
TEST(PlanScheduleSteps, ResidentRequestNoSlack)
{
    auto g = geometry(0, 16, {
        tierEntry(CacheTier::PageCache, {{4, 8}}, {}),              // resident [4,12)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {12, 4}}), // misses outside request
    });
    auto s = describe(g, {4, 8});  // request [4,12), fully page-resident
    expectRanges(s, {{4, 8, User, true}});
    expectSteps(s, {{4, 8}});
}

/// Request clamps to the plan end.
TEST(PlanScheduleSteps, RequestClampedToPlanEnd)
{
    auto g = geometry(0, 8, {tierEntry(CacheTier::FilesystemCache, {}, {{0, 8}})});
    auto s = describe(g, {4, 100});  // request [4,104) clamps to [4,8)
    expectSteps(s, {{4, 4}});
}

/// A resident island splits the request into hit / gap / hit steps.
TEST(PlanScheduleSteps, ResidentIslandSplitsSteps)
{
    auto g = geometry(0, 12, {
        tierEntry(CacheTier::PageCache, {{4, 2}}, {}),                  // resident [4,6)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 6}}),    // misses [0,6),[6,12)
    });
    auto s = describe(g, {0, 12});
    /// [0,4) gap, [4,6) page hit, [6,12) gap.
    expectSteps(s, {{0, 4}, {4, 2}, {6, 6}});
    expectRanges(s, {
        {0, 4, User, false},
        {4, 2, User, true},
        {6, 6, User, false},
    });
}

/// Stage 2: the worked example's one Remote retrieve and its routing.
TEST(PlanScheduleRetrieves, DesignWorkedExample)
{
    auto g = geometry(0, 8, {
        tierEntry(CacheTier::PageCache, {{3, 2}}, {{5, 3}}, /*head*/1, /*tail*/2),  // whole-block
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 2}}, /*head*/6),     // incremental
    });
    auto s = describe(g, {4, 4});  // request [4,8)

    ASSERT_EQ(s.retrieves.size(), 1u);  // no promote ([4,5) is fastest-tier resident)
    const auto & r = s.retrieves[0];
    EXPECT_EQ(r.source, PlanSchedule::Source::Remote);
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.size, 8u);
    EXPECT_TRUE(r.retain_for_serve);
    EXPECT_TRUE(intoHas(r, 1, {0, 6})) << "fs segment [0,6)";
    EXPECT_TRUE(intoHas(r, 1, {6, 2})) << "fs segment [6,8)";
    EXPECT_TRUE(intoHas(r, 0, {5, 3})) << "page block [5,8) for the user tail";

    /// The gap step [5,8) waits on this retrieve; the hit step does not.
    ASSERT_EQ(s.steps.size(), 2u);
    EXPECT_FALSE(s.steps[0].require_retrieve.has_value());  // [4,5) page hit
    ASSERT_TRUE(s.steps[1].require_retrieve.has_value());   // [5,8) gap
    EXPECT_EQ(*s.steps[1].require_retrieve, 0u);
}

/// Slack is filled only into the owning (coarser) lower tier, never promoted
/// into a faster tier that also misses it.
TEST(PlanScheduleRetrieves, SlackNotPromotedToFasterTier)
{
    auto g = geometry(0, 8, {
        // page misses a slice of the before-slack [0,1) AND the user tail [5,8).
        tierEntry(CacheTier::PageCache, {{3, 2}}, {{0, 1}, {5, 3}}, /*head*/1, /*tail*/2),
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 2}}, /*head*/6),
    });
    auto s = describe(g, {4, 4});  // request [4,8)

    ASSERT_EQ(s.retrieves.size(), 1u);
    const auto & r = s.retrieves[0];
    EXPECT_FALSE(intoHas(r, 0, {0, 1})) << "page slack cell must NOT be filled (not promoted)";
    EXPECT_TRUE(intoHas(r, 1, {0, 6})) << "fs owns the slack";
    EXPECT_TRUE(intoHas(r, 0, {5, 3})) << "page user tail is filled";
}

/// A small resident hole between two gaps is bridged into one connection; a
/// larger hole splits into two.
TEST(PlanScheduleRetrieves, BridgeVersusSplit)
{
    auto g = geometry(0, 12, {
        tierEntry(CacheTier::PageCache, {{4, 4}}, {}),                 // resident [4,8)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 4}, {8, 4}}),   // miss [0,4),[8,12), head=1
    });
    // hole [4,8) is 4 wide
    auto bridged = describeSeek(g, {0, 12}, /*min_bytes_for_seek=*/4);
    ASSERT_EQ(bridged.retrieves.size(), 1u) << "hole <= seek bound -> bridged";
    EXPECT_EQ(bridged.retrieves[0].range.offset, 0u);
    EXPECT_EQ(bridged.retrieves[0].range.size, 12u);

    auto split = describeSeek(g, {0, 12}, /*min_bytes_for_seek=*/2);
    ASSERT_EQ(split.retrieves.size(), 2u) << "hole > seek bound -> split";
    EXPECT_EQ(split.retrieves[0].range.offset, 0u);
    EXPECT_EQ(split.retrieves[0].range.size, 4u);
    EXPECT_EQ(split.retrieves[1].range.offset, 8u);
    EXPECT_EQ(split.retrieves[1].range.size, 4u);
}

/// One fs segment filled by two split connections: the later one appends after
/// the earlier (natural-order dep).
TEST(PlanScheduleRetrieves, SpanningSegmentSplitHasDep)
{
    auto g = geometry(0, 12, {
        tierEntry(CacheTier::PageCache, {{4, 4}}, {}),         // resident [4,8)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 12}}),  // ONE segment [0,12), incremental
    });
    auto s = describeSeek(g, {0, 12}, /*min_bytes_for_seek=*/2);  // split
    ASSERT_EQ(s.retrieves.size(), 2u);
    // both write the spanning fs segment [0,12)
    EXPECT_TRUE(intoHas(s.retrieves[0], 1, {0, 12}));
    EXPECT_TRUE(intoHas(s.retrieves[1], 1, {0, 12}));
    ASSERT_EQ(s.retrieves[1].deps.size(), 1u) << "later connection appends after the earlier";
    EXPECT_EQ(s.retrieves[1].deps[0], 0u);
    EXPECT_TRUE(s.retrieves[0].deps.empty());
}

/// A user range resident in a SLOWER tier is promoted up into the faster tier
/// that misses it (HandedRope, no remote).
TEST(PlanScheduleRetrieves, PromoteFromSlowerTier)
{
    auto g = geometry(0, 8, {
        tierEntry(CacheTier::PageCache, {}, {{0, 8}}, /*head*/1, /*tail*/2),  // page misses all
        tierEntry(CacheTier::FilesystemCache, {{0, 8}}, {}),                  // fs resident [0,8)
    });
    auto s = describe(g, {0, 8});  // request [0,8), served from fs, promoted to page

    // No remote retrieve (fully resident in fs); one HandedRope promote into page.
    size_t promotes = 0;
    for (const auto & r : s.retrieves)
        if (r.source == PlanSchedule::Source::HandedRope)
        {
            ++promotes;
            EXPECT_EQ(r.upper_source_tier, CacheTier::FilesystemCache);
            EXPECT_TRUE(intoHas(r, 0, {0, 8})) << "promote into the page miss cell";
            EXPECT_FALSE(r.retain_for_serve);
        }
    EXPECT_EQ(promotes, 1u);
    // The step is a cache hit (served from fs), so it waits on no retrieve.
    ASSERT_EQ(s.steps.size(), 1u);
    EXPECT_FALSE(s.steps[0].require_retrieve.has_value());
}

/// A cache cell wider than the plan (a slow tier's block, or a seek mid-segment)
/// straddles the plan bounds: the retrieve must carry the WHOLE cell as a fill
/// target (the executor fetches it unclamped), not clamp it to the plan span.
TEST(PlanScheduleRetrieves, StraddlingCellBeyondPlan)
{
    auto g = geometry(64, 100, {  // plan [64,100)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 256}}, /*head*/256, /*tail*/256),
    });
    auto s = describe(g, {64, 36});  // request [64,100)
    ASSERT_EQ(s.retrieves.size(), 1u);
    EXPECT_EQ(s.retrieves[0].range.offset, 0u) << "extends below plan_start to the segment start";
    EXPECT_EQ(s.retrieves[0].range.size, 256u) << "and past plan_end to the segment end";
    EXPECT_TRUE(intoHas(s.retrieves[0], 0, {0, 256})) << "the whole straddling cell is a fill target";
}

/// Several separate gaps -> several Remote retrieves, each gap step wired to its
/// OWN retrieve; hit steps wait on none. Pins the multi-retrieve step wiring.
TEST(PlanScheduleRetrieves, SeveralGapsEachWiredToOwnRetrieve)
{
    auto g = geometry(0, 20, {
        tierEntry(CacheTier::PageCache, {{4, 2}, {12, 2}}, {{0, 4}, {6, 6}, {14, 6}}, /*head*/1, /*tail*/2),
    });
    // resident islands [4,6) and [12,14) -> gaps [0,4), [6,12), [14,20)
    auto s = describe(g, {0, 20});

    expectSteps(s, {{0, 4}, {4, 2}, {6, 6}, {12, 2}, {14, 6}});

    /// Three Remote retrieves, one per gap (holes are resident runs of size 2,
    /// but min_bytes_for_seek=2 would bridge - use a fresh describe with mbs 0).
    auto split = describeSeek(g, {0, 20}, /*min_bytes_for_seek=*/0);
    size_t remotes = 0;
    for (const auto & r : split.retrieves)
        if (r.source == PlanSchedule::Source::Remote)
            ++remotes;
    EXPECT_EQ(remotes, 3u) << "one Remote retrieve per gap";

    /// Each gap step points to a retrieve covering it; the three gap steps point
    /// to three DISTINCT retrieves; hit steps have none.
    ASSERT_EQ(split.steps.size(), 5u);
    EXPECT_FALSE(split.steps[1].require_retrieve.has_value());  // hit [4,6)
    EXPECT_FALSE(split.steps[3].require_retrieve.has_value());  // hit [12,14)
    ASSERT_TRUE(split.steps[0].require_retrieve.has_value());
    ASSERT_TRUE(split.steps[2].require_retrieve.has_value());
    ASSERT_TRUE(split.steps[4].require_retrieve.has_value());
    const size_t r0 = *split.steps[0].require_retrieve;
    const size_t r2 = *split.steps[2].require_retrieve;
    const size_t r4 = *split.steps[4].require_retrieve;
    EXPECT_NE(r0, r2);
    EXPECT_NE(r2, r4);
    EXPECT_NE(r0, r4);
    // and each names a retrieve whose range covers that gap step's output
    EXPECT_TRUE(rangeContains(split.retrieves[r0].range, split.steps[0].output));
    EXPECT_TRUE(rangeContains(split.retrieves[r2].range, split.steps[2].output));
    EXPECT_TRUE(rangeContains(split.retrieves[r4].range, split.steps[4].output));
}
