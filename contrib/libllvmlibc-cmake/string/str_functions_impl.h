// Bodies of the x86_64 `memchr` and `strlen`, included once per vector tier by
// str_functions.cpp. The includer defines the vector macros (VEC, vec_t, mask_t,
// LOAD, ..., see there), CHSTR_AVX512 for the mask-register tier, CHSTR_ATTR for
// the function attributes and FN(name) for the symbol to emit.
//
// Shared shape (the same as glibc's memchr-evex / memchr-avx2):
//   head      one vector at `s`: an unaligned load when it stays within the page,
//             otherwise (the next page is not known to be mapped, `n` may overstate
//             the object) an aligned load with the leading bytes shifted out, or a
//             masked load on AVX-512.
//   3 steps   three more aligned vectors, straight-line, each exiting when the
//             bytes run out: covers n <= 4 * VEC without a loop and moves far enough
//             in that realigning down to the 4 * VEC stride stays above `s`.
//   loop      4 * VEC bytes per iteration, one branch on the OR / min of the four
//             compares, aligned to its own stride so a group never crosses a page:
//             bytes past the first match are only ever read on the page holding it.
//   tail      the last < 4 * VEC bytes: four masked loads and one branch on
//             AVX-512 (masked-off lanes never fault); bounded straight-line steps
//             on SSE2 / AVX2.
// `memchr` tracks the remaining byte count, never `s + n`: `n` may be SIZE_MAX
// (musl's strnlen forwards the caller's limit, realpath's is PATH_MAX past a
// shorter string) and the search must still stop at the first match (C11
// 7.24.5.1p2).

#if defined(CHSTR_AVX512)

CHSTR_ATTR static void *FN(memchr)(const void *s, int c, size_t n) noexcept {
  if (__builtin_expect(n == 0, 0))
    return nullptr;
  const char *p = static_cast<const char *>(s);
  const vec_t needle = SPLAT(c);
  mask_t m;
  const uintptr_t in_page = PAGE - (reinterpret_cast<uintptr_t>(p) & (PAGE - 1));
  if (n <= VEC && n <= in_page) {
    // One masked compare: lanes past n are never read. Fault suppression only
    // covers masked-off lanes, so this needs the n bytes to be on this page: an
    // `n` that overstates the object by one byte across the page end
    // (memchr(s, c, strlen(s) + 1) at a page end) takes the paths below instead.
    const mask_t hm = MASK_LO(n);
    m = CMPEQ_MM(hm, MLOADZ(hm, p), needle);
    return m ? const_cast<char *>(p + CTZ(m)) : nullptr;
  }
  if (__builtin_expect(in_page >= VEC, 1)) {
    // n > VEC here: a whole vector within the page.
    m = CMPEQ_M(LOADU(p), needle);
    if (m)
      return const_cast<char *>(p + CTZ(m));
  } else {
    const mask_t hm = MASK_LO(in_page);
    m = CMPEQ_MM(hm, MLOADZ(hm, p), needle);
    if (m)
      return const_cast<char *>(p + CTZ(m));
    if (n <= in_page)
      return nullptr;
  }
  const char *a = ALIGN_DOWN(p) + VEC;
  size_t remaining = n - static_cast<size_t>(a - p); // >= 1
#define CHSTR_STEP()                                                           \
  do {                                                                         \
    m = CMPEQ_M(LOAD(a), needle);                                              \
    if (m)                                                                     \
      return CTZ(m) < remaining ? const_cast<char *>(a + CTZ(m)) : nullptr;    \
    if (remaining <= VEC)                                                      \
      return nullptr;                                                          \
    a += VEC;                                                                  \
    remaining -= VEC;                                                          \
  } while (0)
  CHSTR_STEP();
  CHSTR_STEP();
  CHSTR_STEP();
#undef CHSTR_STEP
  {
    const char *a4 = ALIGN_DOWN4(a);
    remaining += static_cast<size_t>(a - a4);
    a = a4;
  }
  const vec_t zero = SPLAT(0);
  for (; remaining >= 4 * VEC; a += 4 * VEC, remaining -= 4 * VEC) {
    // v XOR needle is zero where it matches: min over the four, one compare.
    const vec_t x0 = XOR(LOAD(a), needle);
    const vec_t x1 = XOR(LOAD(a + VEC), needle);
    const vec_t x2 = XOR(LOAD(a + 2 * VEC), needle);
    const vec_t x3 = XOR(LOAD(a + 3 * VEC), needle);
    const vec_t mn = MINU(MINU(MINU(x0, x1), x2), x3);
    if (__builtin_expect(CMPEQ_M(mn, zero) != 0, 0)) {
      if (const mask_t m0 = CMPEQ_M(x0, zero))
        return const_cast<char *>(a + CTZ(m0));
      if (const mask_t m1 = CMPEQ_M(x1, zero))
        return const_cast<char *>(a + VEC + CTZ(m1));
      if (const mask_t m2 = CMPEQ_M(x2, zero))
        return const_cast<char *>(a + 2 * VEC + CTZ(m2));
      return const_cast<char *>(a + 3 * VEC + CTZ(CMPEQ_M(x3, zero)));
    }
  }
  if (remaining) {
    const size_t r1 = remaining > VEC ? remaining - VEC : 0;
    const size_t r2 = r1 > VEC ? r1 - VEC : 0;
    const size_t r3 = r2 > VEC ? r2 - VEC : 0;
    const mask_t k0 = remaining >= VEC ? MASK_ALL : MASK_LO(remaining);
    const mask_t k1 = r1 >= VEC ? MASK_ALL : MASK_LO(r1);
    const mask_t k2 = r2 >= VEC ? MASK_ALL : MASK_LO(r2);
    const mask_t k3 = MASK_LO(r3); // r3 < VEC
    const mask_t m0 = CMPEQ_MM(k0, MLOADZ(k0, a), needle);
    const mask_t m1 = CMPEQ_MM(k1, MLOADZ(k1, a + VEC), needle);
    const mask_t m2 = CMPEQ_MM(k2, MLOADZ(k2, a + 2 * VEC), needle);
    const mask_t m3 = CMPEQ_MM(k3, MLOADZ(k3, a + 3 * VEC), needle);
    if ((m0 | m1) | (m2 | m3)) {
      if (m0)
        return const_cast<char *>(a + CTZ(m0));
      if (m1)
        return const_cast<char *>(a + VEC + CTZ(m1));
      if (m2)
        return const_cast<char *>(a + 2 * VEC + CTZ(m2));
      return const_cast<char *>(a + 3 * VEC + CTZ(m3));
    }
  }
  return nullptr;
}

CHSTR_ATTR static size_t FN(strlen)(const char *s) noexcept {
  const vec_t zero = SPLAT(0);
  mask_t m;
  if (__builtin_expect((reinterpret_cast<uintptr_t>(s) & (PAGE - 1)) <= PAGE - VEC, 1)) {
    m = CMPEQ_M(LOADU(s), zero);
  } else {
    const mask_t hm = MASK_LO(PAGE - (reinterpret_cast<uintptr_t>(s) & (PAGE - 1)));
    m = CMPEQ_MM(hm, MLOADZ(hm, s), zero);
  }
  if (m)
    return CTZ(m);
  const char *a = ALIGN_DOWN(s) + VEC;
  for (; reinterpret_cast<uintptr_t>(a) & (4 * VEC - 1); a += VEC) {
    m = CMPEQ_M(LOAD(a), zero);
    if (m)
      return static_cast<size_t>(a - s) + CTZ(m);
  }
  for (;; a += 4 * VEC) {
    const vec_t v0 = LOAD(a);
    const vec_t v1 = LOAD(a + VEC);
    const vec_t v2 = LOAD(a + 2 * VEC);
    const vec_t v3 = LOAD(a + 3 * VEC);
    const vec_t mn = MINU(MINU(MINU(v0, v1), v2), v3);
    if (__builtin_expect(CMPEQ_M(mn, zero) != 0, 0)) {
      if (const mask_t m0 = CMPEQ_M(v0, zero))
        return static_cast<size_t>(a - s) + CTZ(m0);
      if (const mask_t m1 = CMPEQ_M(v1, zero))
        return static_cast<size_t>(a - s) + VEC + CTZ(m1);
      if (const mask_t m2 = CMPEQ_M(v2, zero))
        return static_cast<size_t>(a - s) + 2 * VEC + CTZ(m2);
      return static_cast<size_t>(a - s) + 3 * VEC + CTZ(CMPEQ_M(v3, zero));
    }
  }
}

#else // SSE2 / AVX2

// The first VEC bytes at p as an eq-mask against needle, bit 0 = p[0]. An unaligned
// load when it stays within the page; otherwise the aligned load shifted, which
// leaves zero bits at the top: *valid tells how many low bits are meaningful.
#define CHSTR_HEAD(p, needle, m, valid)                                        \
  do {                                                                         \
    if (__builtin_expect(                                                      \
            (reinterpret_cast<uintptr_t>(p) & (PAGE - 1)) <= PAGE - VEC, 1)) { \
      valid = VEC;                                                             \
      m = MOVEMASK(CMPEQ(LOADU(p), needle));                                   \
    } else {                                                                   \
      const char *ha = ALIGN_DOWN(p);                                          \
      const size_t off = static_cast<size_t>((p) - ha);                        \
      valid = VEC - off;                                                       \
      m = MOVEMASK(CMPEQ(LOAD(ha), needle)) >> off;                            \
    }                                                                          \
  } while (0)

CHSTR_ATTR static void *FN(memchr)(const void *s, int c, size_t n) noexcept {
  if (__builtin_expect(n == 0, 0))
    return nullptr;
  const char *p = static_cast<const char *>(s);
  const vec_t needle = SPLAT(c);
  size_t valid;
  mask_t m;
  CHSTR_HEAD(p, needle, m, valid);
  if (n < valid)
    m &= MASK_LO(n);
  if (m)
    return const_cast<char *>(p + CTZ(m));
  if (n <= valid)
    return nullptr;
  const char *a = ALIGN_DOWN(p) + VEC;
  size_t remaining = n - static_cast<size_t>(a - p); // >= 1
#define CHSTR_STEP()                                                           \
  do {                                                                         \
    m = MOVEMASK(CMPEQ(LOAD(a), needle));                                      \
    if (m)                                                                     \
      return CTZ(m) < remaining ? const_cast<char *>(a + CTZ(m)) : nullptr;    \
    if (remaining <= VEC)                                                      \
      return nullptr;                                                          \
    a += VEC;                                                                  \
    remaining -= VEC;                                                          \
  } while (0)
  CHSTR_STEP();
  CHSTR_STEP();
  CHSTR_STEP();
  {
    const char *a4 = ALIGN_DOWN4(a);
    remaining += static_cast<size_t>(a - a4);
    a = a4;
  }
  // A countdown instead of comparing `remaining` keeps the loop body under 64
  // bytes; a 68-byte body whose back-edge straddled a cache line ran at half the
  // IPC on Zen 5 with the same instruction count.
  for (size_t iters = remaining / (4 * VEC); iters; --iters, a += 4 * VEC) {
    const vec_t e0 = CMPEQ(LOAD(a), needle);
    const vec_t e1 = CMPEQ(LOAD(a + VEC), needle);
    const vec_t e2 = CMPEQ(LOAD(a + 2 * VEC), needle);
    const vec_t e3 = CMPEQ(LOAD(a + 3 * VEC), needle);
    if (__builtin_expect(MOVEMASK(OR(OR(OR(e0, e1), e2), e3)) != 0, 0)) {
      // Reload (L1-hot) rather than keep e0..e3 live across the OR tree, which
      // costs register copies with destructive SSE2 ops; the barrier stops the
      // compiler from merging the reloads with the loads above.
      __asm__("" : "+r"(a));
      if (const mask_t m0 = MOVEMASK(CMPEQ(LOAD(a), needle)))
        return const_cast<char *>(a + CTZ(m0));
      if (const mask_t m1 = MOVEMASK(CMPEQ(LOAD(a + VEC), needle)))
        return const_cast<char *>(a + VEC + CTZ(m1));
      if (const mask_t m2 = MOVEMASK(CMPEQ(LOAD(a + 2 * VEC), needle)))
        return const_cast<char *>(a + 2 * VEC + CTZ(m2));
      return const_cast<char *>(a + 3 * VEC +
                                CTZ(MOVEMASK(CMPEQ(LOAD(a + 3 * VEC), needle))));
    }
  }
  remaining &= 4 * VEC - 1;
  if (remaining == 0)
    return nullptr;
  CHSTR_STEP();
  CHSTR_STEP();
  CHSTR_STEP();
#undef CHSTR_STEP
  m = MOVEMASK(CMPEQ(LOAD(a), needle));
  if (m && CTZ(m) < remaining)
    return const_cast<char *>(a + CTZ(m));
  return nullptr;
}

CHSTR_ATTR static size_t FN(strlen)(const char *s) noexcept {
  const vec_t zero = SPLAT(0);
  size_t valid;
  mask_t m;
  CHSTR_HEAD(s, zero, m, valid);
  (void)valid; // a string has no limit to clamp
  if (m)
    return CTZ(m);
  const char *a = ALIGN_DOWN(s) + VEC;
  for (; reinterpret_cast<uintptr_t>(a) & (4 * VEC - 1); a += VEC) {
    m = MOVEMASK(CMPEQ(LOAD(a), zero));
    if (m)
      return static_cast<size_t>(a - s) + CTZ(m);
  }
  for (;; a += 4 * VEC) {
    // min of the four vectors is zero iff one of them holds a NUL: one compare.
    const vec_t v0 = LOAD(a);
    const vec_t v1 = LOAD(a + VEC);
    const vec_t v2 = LOAD(a + 2 * VEC);
    const vec_t v3 = LOAD(a + 3 * VEC);
    const vec_t mn = MINU(MINU(MINU(v0, v1), v2), v3);
    if (__builtin_expect(MOVEMASK(CMPEQ(mn, zero)) != 0, 0)) {
      __asm__("" : "+r"(a));
      if (const mask_t m0 = MOVEMASK(CMPEQ(LOAD(a), zero)))
        return static_cast<size_t>(a - s) + CTZ(m0);
      if (const mask_t m1 = MOVEMASK(CMPEQ(LOAD(a + VEC), zero)))
        return static_cast<size_t>(a - s) + VEC + CTZ(m1);
      if (const mask_t m2 = MOVEMASK(CMPEQ(LOAD(a + 2 * VEC), zero)))
        return static_cast<size_t>(a - s) + 2 * VEC + CTZ(m2);
      return static_cast<size_t>(a - s) + 3 * VEC +
             CTZ(MOVEMASK(CMPEQ(LOAD(a + 3 * VEC), zero)));
    }
  }
}

#undef CHSTR_HEAD
#endif
