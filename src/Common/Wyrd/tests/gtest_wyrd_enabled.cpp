#include <Common/Wyrd/Facade/Enabled.h>

#include <gtest/gtest.h>

using namespace DB::Wyrd;

TEST(Wyrd, EnabledOnlyUnderAStrategy)
{
#if defined(WYRD_SIMULATION) || defined(WYRD_ADVERSARY)
    EXPECT_TRUE(enabled());
#else
    EXPECT_FALSE(enabled());
#endif
}
