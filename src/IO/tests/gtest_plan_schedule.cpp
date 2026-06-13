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
