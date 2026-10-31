#include <Common/tests/gtest_libcxx_hardening.h>

#include <algorithm>

#include <gtest/gtest.h>

namespace
{

/// `std::clamp` with `low > high` is checked only by `_LIBCPP_ASSERT_ARGUMENT_WITHIN_DOMAIN`, which `FAST` does not enable.
int clampWithBuildLibcxxHardening(int value, int low, int high)
{
    return std::clamp(value, low, high);
}

}

TEST(LibcxxHardening, Modes)
{
    EXPECT_EQ(_LIBCPP_HARDENING_MODE, _LIBCPP_HARDENING_MODE_EXTENSIVE);
    EXPECT_EQ(libcxxHardeningModeOfFastTU(), _LIBCPP_HARDENING_MODE_FAST);
}

TEST(LibcxxHardening, ExtensiveTrapsOnBadClampBounds)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(clampWithBuildLibcxxHardening(5, 10, 1), "Bad bounds passed to std::clamp");
}

TEST(LibcxxHardening, FastDoesNotCheckClampBounds)
{
    EXPECT_EQ(clampWithFastLibcxxHardening(5, 10, 1), 10);
}
