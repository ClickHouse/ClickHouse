#include "aarch64_helpers.h"
#include <arm_sve.h>

void *ch_str_sve_memchr_aligned16(const void *src, int c, size_t n)
{
    const u8 *s = src;
    size_t step = 16 - ((uintptr_t)s & 15);
    for (;;)
    {
        /* The prefix reaches the next 16-byte boundary; later groups start
           aligned. Every group fits within a page. Limit active lanes to 16
           even when the thread uses a larger VL. */
        size_t count = n < step ? n : step;
        svbool_t p = svwhilelt_b8_u64(0, count);
        svuint8_t value = svld1_u8(p, s);
        svbool_t hit = svcmpeq_n_u8(p, value, (u8)c);
        if (svptest_any(svptrue_b8(), hit))
        {
            svbool_t all = svptrue_b8();
            size_t first = svcntp_b8(all, svbrkb_z(all, hit));
            return (void *)(s + first);
        }
        if (n <= count)
            return 0;
        s = (const u8 *)((uintptr_t)s + count);
        n -= count;
        step = 16;
    }
}
