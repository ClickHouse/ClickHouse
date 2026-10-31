#include <Common/tests/gtest_libcxx_hardening.h>

#include <algorithm>

/// Compiled in the libc++ `FAST` hardening mode whatever the mode of the build, see `src/CMakeLists.txt`.
int clampWithFastLibcxxHardening(int value, int low, int high)
{
    return std::clamp(value, low, high);
}

int libcxxHardeningModeOfFastTU()
{
    return _LIBCPP_HARDENING_MODE;
}
