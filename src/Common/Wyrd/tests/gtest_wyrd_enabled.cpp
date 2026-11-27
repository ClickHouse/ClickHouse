#include <Common/Wyrd/Facade/Enabled.h>
#include <Common/Wyrd/Testing/Test.h>

using namespace DB::Wyrd;

WYRD_TEST(Wyrd, EnabledOnlyUnderAStrategy, .runs = 1)
{
#if defined(WYRD_ENABLED)
    EXPECT_TRUE(enabled());
#else
    EXPECT_FALSE(enabled());
#endif
}
