#pragma once

#if defined(_MSC_VER)
#    include <intrin.h>
#else
#    include <cpuid.h>
#endif

namespace DB
{

inline bool supportsF16C()
{
    int info[4];
    const int feature_flag_ecx = 1;

#if defined(_MSC_VER)
    __cpuid(info, featureFlagECX);
#else
    __cpuid(feature_flag_ecx, info[0], info[1], info[2], info[3]);
#endif

    const int f16_c_bit = 1 << 29;
    return (info[2] & f16_c_bit) != 0;
}

static const bool F16C_SUPPORTED = supportsF16C();

}
