/// Runtime selection between the variants of ARM's optimized-routines `memcpy` and
/// `memmove`, for both glibc and musl builds.
///
/// `__attribute__((ifunc))` is not an option: a static musl binary has nothing that
/// processes the `R_AARCH64_IRELATIVE` relocations lld would emit, so the first call
/// would jump to garbage. Instead each public symbol tail-calls through a function
/// pointer, which costs what an IPLT entry costs: one load and one indirect branch.
///
/// The pointers start out at the AdvSIMD variants, which run on every AArch64 core, so
/// `memcpy` works from the first instruction of the process, including inside the
/// libc's own startup. A constructor then switches to the best variant according to
/// the capabilities the kernel advertises in the auxiliary vector: `HWCAP_SVE` and
/// `HWCAP2_MOPS` are set only when the kernel has enabled the feature for user space,
/// which is the documented way to find out. Reading the ID registers instead would
/// depend on the kernel trapping and emulating `mrs`; kernels before 4.11 do not, and
/// the instruction raises `SIGILL` there. Constructors run after `__init_libc` in musl
/// and after `_dl_aux_init` in glibc, so `getauxval` is usable by then.
///
/// The selection order follows glibc's ifunc resolvers: MOPS (`cpyf*`/`cpy*`
/// instructions, Armv8.8/9.3), then SVE, then AdvSIMD.

#include <stddef.h>
#include <sys/auxv.h>

/// From <asm/hwcap.h>, spelled out so the build does not depend on the sysroot's
/// kernel headers knowing them (`HWCAP2_MOPS` was added in Linux 6.5).
#define AOR_HWCAP_SVE (1UL << 22)
#define AOR_HWCAP2_MOPS (1UL << 43)

void * __memcpy_aarch64_simd(void *, const void *, size_t);
void * __memmove_aarch64_simd(void *, const void *, size_t);
void * __memcpy_aarch64_sve(void *, const void *, size_t);
void * __memmove_aarch64_sve(void *, const void *, size_t);
void * __memcpy_aarch64_mops(void *, const void *, size_t);
void * __memmove_aarch64_mops(void *, const void *, size_t);

typedef void * (*mem_fn)(void *, const void *, size_t);

static mem_fn memcpy_impl = __memcpy_aarch64_simd;
static mem_fn memmove_impl = __memmove_aarch64_simd;

/// Runs before `main`, but a static initializer may already have started a thread,
/// hence the atomic accesses: a torn function pointer must never be observed.
__attribute__((constructor(101))) static void select_implementations(void)
{
    unsigned long hwcap = getauxval(AT_HWCAP);
    unsigned long hwcap2 = getauxval(AT_HWCAP2);

    mem_fn cpy = __memcpy_aarch64_simd;
    mem_fn mov = __memmove_aarch64_simd;
    if (hwcap2 & AOR_HWCAP2_MOPS)
    {
        cpy = __memcpy_aarch64_mops;
        mov = __memmove_aarch64_mops;
    }
    else if (hwcap & AOR_HWCAP_SVE)
    {
        cpy = __memcpy_aarch64_sve;
        mov = __memmove_aarch64_sve;
    }

    __atomic_store_n(&memcpy_impl, cpy, __ATOMIC_RELAXED);
    __atomic_store_n(&memmove_impl, mov, __ATOMIC_RELAXED);
}

void * memcpy(void * dst, const void * src, size_t n)
{
    return __atomic_load_n(&memcpy_impl, __ATOMIC_RELAXED)(dst, src, n);
}

void * memmove(void * dst, const void * src, size_t n)
{
    return __atomic_load_n(&memmove_impl, __ATOMIC_RELAXED)(dst, src, n);
}
