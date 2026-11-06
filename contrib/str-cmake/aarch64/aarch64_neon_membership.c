/* Selected kernels from the pg83/str AArch64 workbench. See provenance.json. */
#include "aarch64_helpers.h"

static inline unsigned table_index(u8 c) {
    return (c >> 4) | ((c & 8) << 1);
}

static inline size_t scan_table(const char *text, const char *set, int accept) {
    u8 map[32] = {0};
    const u8 *p = (const u8 *)set;

    while (*p) {
        map[table_index(*p)] |= 1u << (*p & 7);
        ++p;
    }

    uint8x16x2_t table = {{vld1q_u8(map), vld1q_u8(map + 16)}};
    const u8 *s = (const u8 *)text;
    size_t i = 0;

    for (;;) {
        if ((((uintptr_t)s + i) & 4095) <= 4080) {
            uint8x16_t v = vld1q_u8(s + i);
            uint8x16_t indices = vorrq_u8(vshrq_n_u8(v, 4), vshlq_n_u8(vandq_u8(v, vdupq_n_u8(8)), 1));
            uint8x16_t mask = vshlq_u8(vdupq_n_u8(1), vreinterpretq_s8_u8(vandq_u8(v, vdupq_n_u8(7))));
            uint8x16_t member = vtstq_u8(vqtbl2q_u8(table, indices), mask);
            uint8x16_t stop = accept ? vmvnq_u8(member) : vorrq_u8(member, vceqq_u8(v, vdupq_n_u8(0)));
            uint64_t bits = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(stop), 4)), 0);

            if (bits) {
                return i + kl_first(bits);
            }

            i += 16;
        } else {
            u8 c = s[i];
            int member = (map[table_index(c)] >> (c & 7)) & 1;

            if (!c || member != accept) {
                return i;
            }

            ++i;
        }
    }
}

size_t kl_strspn_neon_table(const char *s, const char *set) {
    return scan_table(s, set, 1);
}

size_t kl_strcspn_neon_table(const char *s, const char *set) {
    return scan_table(s, set, 0);
}
