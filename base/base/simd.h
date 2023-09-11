#pragma once

#if defined(__aarch64__) && defined(__ARM_NEON)

#    include <arm_neon.h>
#      pragma clang diagnostic ignored "-Wreserved-identifier"

/// Returns a 64 bit mask of nibbles (4 bits for each byte).
inline uint64_t getNibbleMask(uint8x16_t res)
{
    return vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(res), 4)), 0);
}

#endif
