#include <gtest/gtest.h>

#include <Storages/MaterializedView/RefreshTimers.h>
#include <Parsers/ASTTimeInterval.h>

using namespace DB;

TEST(Timers, AfterTimer)
{
    using namespace std::chrono;

    auto interval = std::make_shared<ASTTimeInterval>();
    interval->kinds = {
        {IntervalKind::Week, 2},
        {IntervalKind::Day, 3},
        {IntervalKind::Minute, 15},
    };
    RefreshAfterTimer timer(interval.get());

    sys_days date_in = 2023y / January / 18d;
    auto secs_in = date_in + 23h + 57min;

    sys_days date_out = 2023y / February / 5d;
    auto secs_out = date_out + 0h + 12min;

    ASSERT_EQ(secs_out, timer.after(secs_in));
}
