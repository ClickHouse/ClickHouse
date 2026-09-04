#include <IO/PlanSchedule.h>
#include <IO/CoverageMap.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

GeometryEntry tierEntry(CacheTier tier,
                        std::vector<ByteRange> resident,
                        std::vector<ByteRange> aligned_miss)
{
    GeometryEntry e;
    e.tier = tier;
    for (auto r : resident) e.resident.push_back(r);
    for (auto m : aligned_miss) e.aligned_miss.push_back(m);
    return e;
}

CoverageMap geometry(size_t plan_start, size_t plan_end, std::vector<GeometryEntry> entries)
{
    CoverageMap g;
    g.plan_start = plan_start;
    g.plan_end = plan_end;
    for (auto & e : entries) g.entries.push_back(std::move(e));
    return g;
}

PlanSchedule describe(const CoverageMap & g)
{
    return buildSchedule(g, /*serve_window_bytes=*/1 << 20, /*serve_block_bytes=*/64 * 1024);
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
    ASSERT_EQ(s.serve_runs.size(), want.size()) << "step count";
    for (size_t i = 0; i < want.size(); ++i)
    {
        EXPECT_EQ(s.serve_runs[i].output.offset, want[i].offset) << "step[" << i << "].off";
        EXPECT_EQ(s.serve_runs[i].output.size, want[i].size) << "step[" << i << "].size";
    }
}

constexpr auto User = PlanSchedule::Purpose::User;
constexpr auto Fill = PlanSchedule::Purpose::FillOnly;

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
    auto s = describe(g);
    expectRanges(s, {{0, 8, User, false}});
    expectSteps(s, {{0, 8}});
}

TEST(PlanScheduleSteps, AllResident)
{
    auto g = geometry(0, 8, {tierEntry(CacheTier::PageCache, {{0, 8}}, {})});
    auto s = describe(g);
    expectRanges(s, {{0, 8, User, true}});
    expectSteps(s, {{0, 8}});
}

/// The DESIGN.md worked example: request [4,8); page holds [3,5); fs misses
/// the whole [0,6) and [6,8) segments.
TEST(PlanScheduleSteps, DesignWorkedExample)
{
    auto g = geometry(4, 8, {
        tierEntry(CacheTier::PageCache, {{3, 2}}, {{5, 3}}),         // resident [3,5), miss [5,8)
        // fs cells [0,6) and [6,8): the span's head falls inside the [0,6)
        // cell, so the cell closure demands the before-slack [0,4).
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 2}}),
    });
    auto s = describe(g);  // span [4,8), cursor mid-cell

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
    auto g = geometry(4, 12, {
        tierEntry(CacheTier::PageCache, {{4, 8}}, {}),              // resident [4,12)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {12, 4}}), // misses outside the span
    });
    auto s = describe(g);  // span [4,12), fully page-resident
    expectRanges(s, {{4, 8, User, true}});
    expectSteps(s, {{4, 8}});
}

/// A span starting mid-cell: the cell's head becomes before-slack, the serve
/// starts at the span.
TEST(PlanScheduleSteps, MidCellSpanEmitsBeforeSlack)
{
    auto g = geometry(4, 8, {tierEntry(CacheTier::FilesystemCache, {}, {{0, 8}})});
    auto s = describe(g);  // span [4,8) inside the cold cell [0,8)
    expectRanges(s, {
        {0, 4, Fill, false},  // the cell head below the span: fetched, not served
        {4, 4, User, false},
    });
    expectSteps(s, {{4, 4}});
}

/// A resident island splits the request into hit / gap / hit steps.
TEST(PlanScheduleSteps, ResidentIslandSplitsSteps)
{
    auto g = geometry(0, 12, {
        tierEntry(CacheTier::PageCache, {{4, 2}}, {}),                  // resident [4,6)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 6}}),    // misses [0,6),[6,12)
    });
    auto s = describe(g);
    /// [0,4) gap, [4,6) page hit, [6,12) gap.
    expectSteps(s, {{0, 4}, {4, 2}, {6, 6}});
    expectRanges(s, {
        {0, 4, User, false},
        {4, 2, User, true},
        {6, 6, User, false},
    });
}

/// Adjacent resident runs on different tiers merge into ONE hit step, matching
/// `serveCacheBlock` which streams contiguous resident bytes across tiers in a single
/// window. Split per-tier steps would make one served window overrun its step.
TEST(PlanScheduleSteps, AdjacentCrossTierResidentMergesIntoOneStep)
{
    auto g = geometry(0, 8, {
        tierEntry(CacheTier::PageCache, {{0, 4}}, {}),         // resident [0,4)
        tierEntry(CacheTier::FilesystemCache, {{4, 4}}, {}),   // resident [4,8), adjacent
    });
    auto s = describe(g);
    expectSteps(s, {{0, 8}});  // one merged hit step, not [0,4)+[4,8)
}

/// Stage 2: the worked example's one Remote retrieve and its routing.
TEST(PlanScheduleRetrieves, DesignWorkedExample)
{
    auto g = geometry(4, 8, {
        tierEntry(CacheTier::PageCache, {{3, 2}}, {{5, 3}}),  // whole-block
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 2}}),     // incremental
    });
    // The 2-wide page hit [3,5) splits the fetch runs; whether one connection
    // bridges it on the open GET is a runtime decision, invisible here.
    auto s = describe(g);  // span [4,8)

    ASSERT_EQ(s.retrieves.size(), 1u);  // one fetch fills every missing tier - no serve-front promote
    const auto & r = s.retrieves[0];
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.size, 8u);
    EXPECT_TRUE(intoHas(r, 1, {0, 6})) << "fs segment [0,6)";
    EXPECT_TRUE(intoHas(r, 1, {6, 2})) << "fs segment [6,8)";
    /// The user tail's page block [5,8) is filled by the SAME fetch now - every tier that
    /// misses a consumed cell is populated at the source read.
    EXPECT_TRUE(intoHas(r, 0, {5, 3})) << "page block [5,8) is filled by the fetch";

    /// The gap step [5,8) waits on the retrieve; the hit step does not.
    ASSERT_EQ(s.serve_runs.size(), 2u);
    EXPECT_FALSE(s.serve_runs[0].require_retrieve.has_value());  // [4,5) page hit
    ASSERT_TRUE(s.serve_runs[1].require_retrieve.has_value());   // [5,8) gap
    EXPECT_EQ(*s.serve_runs[1].require_retrieve, 0u);
}

/// `fetch_runs` are the schedule's EXECUTABLE source ranges: the retrieve's (merged,
/// cell-aligned) `range` minus every embedded resident region - resident bytes are
/// served from their tier, never SCHEDULED as a source read (whether the executor
/// reads through one at run time is a display-state decision). The executor fetches
/// the runs verbatim - no geometry query at serve time.
TEST(PlanScheduleRetrieves, FetchRunsSplitAtEmbeddedResident)
{
    /// The worked example: the merged range [0,8) spans the embedded page hit [3,5).
    auto g = geometry(4, 8, {
        tierEntry(CacheTier::PageCache, {{3, 2}}, {{5, 3}}),
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 2}}),
    });

    auto s = describe(g);  // span [4,8)
    ASSERT_FALSE(s.retrieves.empty());
    const auto & r = s.retrieves[0];
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.size, 8u);
    ASSERT_EQ(r.fetch_runs.size(), 2u) << "the merged range splits at the embedded page hit";
    EXPECT_EQ(r.fetch_runs[0].offset, 0u);
    EXPECT_EQ(r.fetch_runs[0].size, 3u);
    EXPECT_EQ(r.fetch_runs[1].offset, 5u);
    EXPECT_EQ(r.fetch_runs[1].size, 3u);
}

/// A cold gap's after-slack can extend past `plan_end` (the cell is object-bounded,
/// not plan-bounded); the geometry has no residency info there, so the run extends
/// to the range end - one maximal run, not split at the plan boundary.
TEST(PlanScheduleRetrieves, FetchRunsColdGapIsOneRunAcrossPlanEnd)
{
    auto g = geometry(0, 8, {
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 12}}),
    });
    auto s = describe(g);

    ASSERT_EQ(s.retrieves.size(), 1u);
    const auto & r = s.retrieves[0];
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.size, 12u) << "tail-aligned to the cell, past plan_end";
    ASSERT_EQ(r.fetch_runs.size(), 1u) << "no resident region: one maximal run";
    EXPECT_EQ(r.fetch_runs[0].offset, 0u);
    EXPECT_EQ(r.fetch_runs[0].size, 12u);
}

/// Slack is filled only into the owning (coarser) lower tier, never into a faster tier that
/// also misses it - even though a USER cell is now filled in every missing tier at the fetch.
TEST(PlanScheduleRetrieves, SlackNotFilledIntoFasterTier)
{
    auto g = geometry(4, 8, {
        // page misses a slice of the before-slack [0,1) AND the user tail [5,8).
        tierEntry(CacheTier::PageCache, {{3, 2}}, {{0, 1}, {5, 3}}),
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 6}, {6, 2}}),
    });
    auto s = describe(g);  // span [4,8)

    ASSERT_EQ(s.retrieves.size(), 1u);  // one fetch, no promote
    const auto & r = s.retrieves[0];
    EXPECT_FALSE(intoHas(r, 0, {0, 1})) << "page slack cell must NOT be filled (slack stays coarsest-tier)";
    EXPECT_TRUE(intoHas(r, 1, {0, 6})) << "fs owns the slack";
    /// The page USER tail IS filled by the fetch now - every tier that misses a consumed cell.
    EXPECT_TRUE(intoHas(r, 0, {5, 3})) << "page user tail is filled by the fetch";
}

/// A tier that schedules NO fill cells (a bypass-mode cache: it can hold resident bytes but
/// `aligned_miss` stays empty) must not shape the fetch grids - nothing could hold the grid
/// extension, so the serve would fetch-and-discard it every window. Only POPULATING tiers'
/// cells count.
TEST(PlanScheduleRetrieves, BypassTierGetsNoWriteTargets)
{
    auto g = geometry(0, 8, {
        tierEntry(CacheTier::PageCache, {}, {{4, 4}}),          // populating: one cell [4,8)
        tierEntry(CacheTier::FilesystemCache, {{0, 4}}, {}),    // bypass-mode: resident, NO cells
    });
    auto s = describe(g);

    ASSERT_FALSE(s.retrieves.empty());
    const auto & r = s.retrieves[0];
    /// Fetch shaping is the `into` cells: only the populating tier contributes one.
    ASSERT_EQ(r.into.size(), 1u) << "only the populating tier's cells shape/receive the fetch";
    EXPECT_EQ(r.into[0].entry, 0u);
    EXPECT_EQ(r.into[0].cell.offset, 4u);
    EXPECT_EQ(r.into[0].cell.size, 4u);
}

/// The schedule does NOT group gaps into connections - it lists each cache-cell-aligned gap as
/// its own Remote retrieve. Whether one source connection spans the resident hole between them
/// (bridge) or reopens at it (split) is a RUNTIME decision
/// (`LongConnection::canContinue` / `scheduleLookaheadReach`), invisible to the schedule.
TEST(PlanScheduleRetrieves, PerGapRetrievesNotGrouped)
{
    auto g = geometry(0, 12, {
        tierEntry(CacheTier::PageCache, {{4, 4}}, {}),                 // resident [4,8)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 4}, {8, 4}}),   // miss [0,4),[8,12), head=1
    });
    auto s = describe(g);
    ASSERT_EQ(s.retrieves.size(), 2u) << "one Remote retrieve per gap, never grouped";
    EXPECT_EQ(s.retrieves[0].range.offset, 0u);
    EXPECT_EQ(s.retrieves[0].range.size, 4u);
    EXPECT_EQ(s.retrieves[1].range.offset, 8u);
    EXPECT_EQ(s.retrieves[1].range.size, 4u);
}

/// One fs segment spanning an embedded resident hit: the cell closure of either gap
/// is the whole cell, so the schedule emits ONE Remote job for the cell with the two
/// gaps as its `fetch_runs` - the runtime decides how many connections span them.
TEST(PlanScheduleRetrieves, SpanningSegmentIsOneJobWithSplitRuns)
{
    auto g = geometry(0, 12, {
        tierEntry(CacheTier::PageCache, {{4, 4}}, {}),         // resident [4,8)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 12}}),  // ONE segment [0,12), incremental
    });
    auto s = describe(g);
    ASSERT_EQ(s.retrieves.size(), 1u);
    const auto & r = s.retrieves[0];
    EXPECT_TRUE(intoHas(r, 1, {0, 12}));
    EXPECT_EQ(r.range.offset, 0u);
    EXPECT_EQ(r.range.size, 12u);
    ASSERT_EQ(r.fetch_runs.size(), 2u) << "runs split at the embedded resident hit";
    EXPECT_EQ(r.fetch_runs[0].offset, 0u);
    EXPECT_EQ(r.fetch_runs[0].size, 4u);
    EXPECT_EQ(r.fetch_runs[1].offset, 8u);
    EXPECT_EQ(r.fetch_runs[1].size, 4u);
}

/// A cache cell wider than the plan (a slow tier's block, or a seek mid-segment)
/// straddles the plan bounds: the retrieve must carry the WHOLE cell as a fill
/// target (the executor fetches it unclamped), not clamp it to the plan span.
TEST(PlanScheduleRetrieves, StraddlingCellBeyondPlan)
{
    auto g = geometry(64, 100, {  // plan [64,100)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 256}}),
    });
    auto s = describe(g);  // request [64,100)
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
        tierEntry(CacheTier::PageCache, {{4, 2}, {12, 2}}, {{0, 4}, {6, 6}, {14, 6}}),
    });
    // resident islands [4,6) and [12,14) -> gaps [0,4), [6,12), [14,20)
    auto s = describe(g);

    expectSteps(s, {{0, 4}, {4, 2}, {6, 6}, {12, 2}, {14, 6}});

    /// Three Remote retrieves, one per gap.
    const auto & split = s;
    EXPECT_EQ(split.retrieves.size(), 3u) << "one retrieve per gap";

    /// Each gap step points to a retrieve covering it; the three gap steps point
    /// to three DISTINCT retrieves; hit steps have none.
    ASSERT_EQ(split.serve_runs.size(), 5u);
    EXPECT_FALSE(split.serve_runs[1].require_retrieve.has_value());  // hit [4,6)
    EXPECT_FALSE(split.serve_runs[3].require_retrieve.has_value());  // hit [12,14)
    ASSERT_TRUE(split.serve_runs[0].require_retrieve.has_value());
    ASSERT_TRUE(split.serve_runs[2].require_retrieve.has_value());
    ASSERT_TRUE(split.serve_runs[4].require_retrieve.has_value());
    const size_t r0 = *split.serve_runs[0].require_retrieve;
    const size_t r2 = *split.serve_runs[2].require_retrieve;
    const size_t r4 = *split.serve_runs[4].require_retrieve;
    EXPECT_NE(r0, r2);
    EXPECT_NE(r2, r4);
    EXPECT_NE(r0, r4);
    // and each names a retrieve whose range covers that gap step's output
    EXPECT_TRUE(rangeContains(split.retrieves[r0].range, split.serve_runs[0].output));
    EXPECT_TRUE(rangeContains(split.retrieves[r2].range, split.serve_runs[2].output));
    EXPECT_TRUE(rangeContains(split.retrieves[r4].range, split.serve_runs[4].output));
}


/// T8: the serve GRANULARITY is schedule data. A job run carries the window bound (the fetch
/// it may pump amortises over it), a hit run the block bound (in-flight memory only) - a
/// swapped argument pair would invert warm/cold serve sizing while every range assertion
/// stays green, so pin the mapping itself.
TEST(PlanScheduleServeRuns, ServeBoundPerRunKind)
{
    auto g = geometry(0, 12, {
        tierEntry(CacheTier::PageCache, {{4, 4}}, {}),         // resident [4,8)
        tierEntry(CacheTier::FilesystemCache, {}, {{0, 12}}),  // one segment [0,12)
    });
    auto s = buildSchedule(g,
        /*serve_window_bytes=*/1 << 20, /*serve_block_bytes=*/64 * 1024);
    bool saw_hit = false;
    bool saw_job = false;
    for (const auto & run : s.serve_runs)
    {
        if (run.require_retrieve.has_value())
        {
            EXPECT_EQ(run.serve_bound, 1u << 20) << "a job run serves at the WINDOW bound";
            saw_job = true;
        }
        else
        {
            EXPECT_EQ(run.serve_bound, 64u * 1024) << "a hit run serves at the BLOCK bound";
            saw_hit = true;
        }
    }
    EXPECT_TRUE(saw_hit);
    EXPECT_TRUE(saw_job);
}
