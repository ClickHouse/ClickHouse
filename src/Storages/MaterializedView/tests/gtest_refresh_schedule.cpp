#include <gtest/gtest.h>

#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTTimeInterval.h>

#include <limits>

using namespace DB;

namespace
{

/// RefreshSchedule is only constructible from an AST, and the value under test is the spread.
RefreshSchedule scheduleWithSpreadSeconds(UInt64 spread_seconds)
{
    auto strategy = make_intrusive<ASTRefreshStrategy>();
    strategy->schedule_kind = RefreshScheduleKind::EVERY;
    auto period = make_intrusive<ASTTimeInterval>();
    period->interval.seconds = 3600;
    strategy->set(strategy->period, period);
    auto spread = make_intrusive<ASTTimeInterval>();
    spread->interval.seconds = spread_seconds;
    strategy->set(strategy->spread, spread);
    return RefreshSchedule(*strategy);
}

Int64 spreadMicroseconds(UInt64 spread_seconds, Int64 randomness, Int64 when_microseconds)
{
    auto schedule = scheduleWithSpreadSeconds(spread_seconds);
    auto when = std::chrono::system_clock::time_point(std::chrono::system_clock::duration(when_microseconds));
    return schedule.addRandomSpread(when, randomness).time_since_epoch().count();
}

constexpr Int64 min_time_point = std::numeric_limits<Int64>::min();
constexpr Int64 max_time_point = std::numeric_limits<Int64>::max();
/// Some instant far from either end of the range, so an ordinary spread cannot reach one.
constexpr Int64 some_time_point = 1786000000000000;

}

TEST(RefreshSchedule, AddRandomSpreadKeepsOrdinarySpreadsExact)
{
    /// Half the window, scaled by randomness/1e9, truncated to whole milliseconds.
    EXPECT_EQ(spreadMicroseconds(0, 1000000000, some_time_point), some_time_point);
    EXPECT_EQ(spreadMicroseconds(3600, 1000000000, some_time_point), some_time_point + 1800000000);
    EXPECT_EQ(spreadMicroseconds(3600, -1000000000, some_time_point), some_time_point - 1800000000);
    EXPECT_EQ(spreadMicroseconds(3600, 0, some_time_point), some_time_point);
    EXPECT_EQ(spreadMicroseconds(3600, 500000000, some_time_point), some_time_point + 900000000);
    /// 4 DAY 1 HOUR, the spread an existing functional test uses.
    EXPECT_EQ(spreadMicroseconds(349200, 900000000, some_time_point), some_time_point + 157140000000);
    /// Truncation is toward zero on both signs, as it was when the value was computed in double.
    EXPECT_EQ(spreadMicroseconds(1, 1, some_time_point), some_time_point);
    EXPECT_EQ(spreadMicroseconds(1, -1, some_time_point), some_time_point);
}

TEST(RefreshSchedule, AddRandomSpreadIsExactAboveDoublePrecision)
{
    /// The intermediate product leaves the exact range of double above a spread of about five
    /// hours, and above about 100 days the rounding changes the truncated result. 9924772 seconds
    /// (115 days) with this randomness produced -4677048804 ms that way; -4677048805 ms is exact.
    EXPECT_EQ(spreadMicroseconds(9924772, -942500000, some_time_point), some_time_point - 4677048805000);
}

TEST(RefreshSchedule, AddRandomSpreadSaturatesInsideTheRange)
{
    /// A spread this wide overshoots the range whichever sign it is drawn with, so the result has
    /// to saturate. It must stay strictly inside: time_point::max() means "no refresh scheduled",
    /// and a view whose next refresh reads as that sentinel waits forever.
    const UInt64 huge_spread = 10000000000000000000ULL;
    EXPECT_EQ(spreadMicroseconds(huge_spread, 1000000000, some_time_point), max_time_point - 1);
    EXPECT_EQ(spreadMicroseconds(huge_spread, -1000000000, some_time_point), min_time_point + 1);

    /// Saturation also has to survive a when that is itself at the end of the range, which is
    /// where the addition used to overflow before the spread was even considered.
    EXPECT_EQ(spreadMicroseconds(18000000000000, 900000000, max_time_point - 1), max_time_point - 1);
    EXPECT_EQ(spreadMicroseconds(18000000000000, -900000000, min_time_point + 1), min_time_point + 1);

    /// The widest inputs the parser and the coordination znode can produce between them: the
    /// spread is an unbounded UInt64 and the randomness an unvalidated Int64.
    const UInt64 max_spread = std::numeric_limits<UInt64>::max();
    EXPECT_EQ(spreadMicroseconds(max_spread, max_time_point, some_time_point), max_time_point - 1);
    EXPECT_EQ(spreadMicroseconds(max_spread, min_time_point, some_time_point), min_time_point + 1);
}
