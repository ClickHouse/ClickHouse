/// Runtime selection between the SVE and the AdvSIMD `memcpy`/`memmove` of ARM's
/// optimized-routines, for both glibc and musl builds.
///
/// `__attribute__((ifunc))` is not an option: a static musl binary has nothing that
/// processes the `R_AARCH64_IRELATIVE` relocations lld would emit, so the first call
/// would jump to garbage. Instead each public symbol tail-calls through a function
/// pointer that initially points at a resolver. The resolver picks the implementation,
/// stores it (the store is idempotent, so a race between threads is harmless), and
/// forwards the call. After that the trampoline costs what an IPLT entry costs: one
/// load and one indirect branch.
///
/// SVE presence is read from `ID_AA64PFR0_EL1`. User space cannot access that register
/// directly, but the Linux kernel traps and emulates the read with the sanitized,
/// system-wide value since 4.11, and reports SVE only when it has enabled it for user
/// space. That makes the probe independent of `getauxval`, which in a static musl
/// binary is not usable until `__init_libc` has run.

#include <stddef.h>
#include <stdint.h>

void * __memcpy_aarch64_simd(void *, const void *, size_t);
void * __memmove_aarch64_simd(void *, const void *, size_t);
void * __memcpy_aarch64_sve(void *, const void *, size_t);
void * __memmove_aarch64_sve(void *, const void *, size_t);

typedef void * (*mem_fn)(void *, const void *, size_t);

static void * memcpy_resolve(void *, const void *, size_t);
static void * memmove_resolve(void *, const void *, size_t);

static mem_fn memcpy_impl = memcpy_resolve;
static mem_fn memmove_impl = memmove_resolve;

static int has_sve(void)
{
    uint64_t pfr0;
    __asm__("mrs %0, ID_AA64PFR0_EL1" : "=r"(pfr0));
    /// ID_AA64PFR0_EL1.SVE, bits [35:32]; 0 means not implemented.
    return ((pfr0 >> 32) & 0xF) != 0;
}

static void * memcpy_resolve(void * dst, const void * src, size_t n)
{
    mem_fn f = has_sve() ? __memcpy_aarch64_sve : __memcpy_aarch64_simd;
    __atomic_store_n(&memcpy_impl, f, __ATOMIC_RELAXED);
    return f(dst, src, n);
}

static void * memmove_resolve(void * dst, const void * src, size_t n)
{
    mem_fn f = has_sve() ? __memmove_aarch64_sve : __memmove_aarch64_simd;
    __atomic_store_n(&memmove_impl, f, __ATOMIC_RELAXED);
    return f(dst, src, n);
}

void * memcpy(void * dst, const void * src, size_t n)
{
    return __atomic_load_n(&memcpy_impl, __ATOMIC_RELAXED)(dst, src, n);
}

void * memmove(void * dst, const void * src, size_t n)
{
    return __atomic_load_n(&memmove_impl, __ATOMIC_RELAXED)(dst, src, n);
}
