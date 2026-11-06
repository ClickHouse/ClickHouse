/* Selected kernels from the pg83/str AArch64 workbench. See provenance.json. */
#include "aarch64_helpers.h"
#include <arm_sve.h>
extern void *kl_memcpy_neon_bulk64(void *, const void *, size_t);
extern void *kl_memcpy_neon_bulk128(void *, const void *, size_t);
extern void *kl_memmove_neon_bulk64(void *, const void *, size_t);
extern void *kl_memset_neon_bulk64(void *, int, size_t);
extern void *kl_memset_neon_bulk128(void *, int, size_t);

static inline __attribute__((always_inline)) void *copy_prefix(void *dst, const void *src, size_t n, unsigned blocks) {
    size_t vl = svcntb();
    svbool_t p0 = svwhilelt_b8_u64(0, n), p1 = svwhilelt_b8_u64(vl, n);
    svbool_t p2 = svwhilelt_b8_u64(2 * vl, n), p3 = svwhilelt_b8_u64(3 * vl, n);
    svbool_t p4 = svwhilelt_b8_u64(4 * vl, n), p5 = svwhilelt_b8_u64(5 * vl, n);
    svbool_t p6 = svwhilelt_b8_u64(6 * vl, n), p7 = svwhilelt_b8_u64(7 * vl, n);
    svuint8_t a0 = svld1_u8(p0, src), a1 = svld1_u8(p1, (const u8 *)((uintptr_t)src + vl));
    svuint8_t a2 = svld1_u8(p2, (const u8 *)((uintptr_t)src + 2 * vl)), a3 = svld1_u8(p3, (const u8 *)((uintptr_t)src + 3 * vl));
    svuint8_t a4 = svdup_n_u8(0), a5 = a4, a6 = a4, a7 = a4;

    if (blocks == 8) {
        a4 = svld1_u8(p4, (const u8 *)((uintptr_t)src + 4 * vl));
        a5 = svld1_u8(p5, (const u8 *)((uintptr_t)src + 5 * vl));
        a6 = svld1_u8(p6, (const u8 *)((uintptr_t)src + 6 * vl));
        a7 = svld1_u8(p7, (const u8 *)((uintptr_t)src + 7 * vl));
    }

    svst1_u8(p0, dst, a0); svst1_u8(p1, (u8 *)((uintptr_t)dst + vl), a1);
    svst1_u8(p2, (u8 *)((uintptr_t)dst + 2 * vl), a2); svst1_u8(p3, (u8 *)((uintptr_t)dst + 3 * vl), a3);

    if (blocks == 8) {
        svst1_u8(p4, (u8 *)((uintptr_t)dst + 4 * vl), a4); svst1_u8(p5, (u8 *)((uintptr_t)dst + 5 * vl), a5);
        svst1_u8(p6, (u8 *)((uintptr_t)dst + 6 * vl), a6); svst1_u8(p7, (u8 *)((uintptr_t)dst + 7 * vl), a7);
    }

    return dst;
}

static inline __attribute__((always_inline)) void *fill_prefix(void *dst, int c, size_t n, unsigned blocks) {
    size_t vl = svcntb();
    svuint8_t value = svdup_n_u8((u8)c);
    svst1_u8(svwhilelt_b8_u64(0, n), dst, value); svst1_u8(svwhilelt_b8_u64(vl, n), (u8 *)((uintptr_t)dst + vl), value);
    svst1_u8(svwhilelt_b8_u64(2 * vl, n), (u8 *)((uintptr_t)dst + 2 * vl), value);
    svst1_u8(svwhilelt_b8_u64(3 * vl, n), (u8 *)((uintptr_t)dst + 3 * vl), value);

    if (blocks == 8) {
        svst1_u8(svwhilelt_b8_u64(4 * vl, n), (u8 *)((uintptr_t)dst + 4 * vl), value);
        svst1_u8(svwhilelt_b8_u64(5 * vl, n), (u8 *)((uintptr_t)dst + 5 * vl), value);
        svst1_u8(svwhilelt_b8_u64(6 * vl, n), (u8 *)((uintptr_t)dst + 6 * vl), value);
        svst1_u8(svwhilelt_b8_u64(7 * vl, n), (u8 *)((uintptr_t)dst + 7 * vl), value);
    }

    return dst;
}

void *kl_memcpy_sve_bulk128(void *d, const void *s, size_t n) {
    return n > 4 * svcntb() ? kl_memcpy_neon_bulk128(d, s, n) : copy_prefix(d, s, n, 4);
}

void *kl_memmove_sve_bulk64(void *d, const void *s, size_t n) {
    return n > 4 * svcntb() ? kl_memmove_neon_bulk64(d, s, n) : copy_prefix(d, s, n, 4);
}

void *kl_memset_sve_bulk128(void *d, int c, size_t n) {
    return n > 4 * svcntb() ? kl_memset_neon_bulk128(d, c, n) : fill_prefix(d, c, n, 4);
}
