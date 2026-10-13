/// Runtime selection between several variants of ARM's optimized-routines for
/// `memcpy`, `memmove` and `strchr`, for both glibc and musl builds.
///
/// `__attribute__((ifunc))` is not an option: a static musl binary has nothing that
/// processes the `R_AARCH64_IRELATIVE` relocations lld would emit, so the first call
/// would jump to garbage. Instead each public symbol tail-calls through a function
/// pointer that initially points at a resolver. The resolver picks the implementation,
/// stores it (the store is idempotent, so a race between threads is harmless), and
/// forwards the call. After that the trampoline costs what an IPLT entry costs: one
/// load and one indirect branch.
///
/// Features are read from the ID registers. User space cannot access them directly,
/// but the Linux kernel traps and emulates the reads with the sanitized, system-wide
/// values since 4.11, reports a feature only when it has enabled it for user space,
/// and returns zero for ID registers it does not know. That makes the probe independent
/// of `getauxval`, which in a static musl binary is not usable until `__init_libc` has
/// run. The registers are named by their encodings because the assembler accepts the
/// symbolic names of `ID_AA64ZFR0_EL1` and `ID_AA64ISAR2_EL1` only with the matching
/// `-march` extensions enabled.
///
/// The selection order follows glibc's ifunc resolvers:
///  - `memcpy`/`memmove`: MOPS (`cpyf*`/`cpy*` instructions, Armv8.8/9.3), then SVE,
///    then AdvSIMD.
///  - `strchr`: SVE2 (uses the `match` instruction), then the AdvSIMD/MTE variant.

#include <stddef.h>
#include <stdint.h>

void * __memcpy_aarch64_simd(void *, const void *, size_t);
void * __memmove_aarch64_simd(void *, const void *, size_t);
void * __memcpy_aarch64_sve(void *, const void *, size_t);
void * __memmove_aarch64_sve(void *, const void *, size_t);
void * __memcpy_aarch64_mops(void *, const void *, size_t);
void * __memmove_aarch64_mops(void *, const void *, size_t);
char * __strchr_aarch64_mte(const char *, int);
char * __strchr_aarch64_sve2(const char *, int);

typedef void * (*mem_fn)(void *, const void *, size_t);
typedef char * (*strchr_fn)(const char *, int);

static void * memcpy_resolve(void *, const void *, size_t);
static void * memmove_resolve(void *, const void *, size_t);
static char * strchr_resolve(const char *, int);

static mem_fn memcpy_impl = memcpy_resolve;
static mem_fn memmove_impl = memmove_resolve;
static strchr_fn strchr_impl = strchr_resolve;

static int has_sve(void)
{
    uint64_t pfr0;
    /// ID_AA64PFR0_EL1
    __asm__("mrs %0, S3_0_C0_C4_0" : "=r"(pfr0));
    /// ID_AA64PFR0_EL1.SVE, bits [35:32]; 0 means not implemented.
    return ((pfr0 >> 32) & 0xF) != 0;
}

static int has_sve2(void)
{
    if (!has_sve())
        return 0;
    uint64_t zfr0;
    /// ID_AA64ZFR0_EL1; reads as zero when SVE is not implemented.
    __asm__("mrs %0, S3_0_C0_C4_4" : "=r"(zfr0));
    /// ID_AA64ZFR0_EL1.SVEver, bits [3:0]; 0 is SVE, 1 is SVE2, 2 is SVE2.1.
    return (zfr0 & 0xF) >= 1;
}

static int has_mops(void)
{
    uint64_t isar2;
    /// ID_AA64ISAR2_EL1
    __asm__("mrs %0, S3_0_C0_C6_2" : "=r"(isar2));
    /// ID_AA64ISAR2_EL1.MOPS, bits [19:16]; 0 means not implemented.
    return ((isar2 >> 16) & 0xF) != 0;
}

static void * memcpy_resolve(void * dst, const void * src, size_t n)
{
    mem_fn f = has_mops() ? __memcpy_aarch64_mops : has_sve() ? __memcpy_aarch64_sve : __memcpy_aarch64_simd;
    __atomic_store_n(&memcpy_impl, f, __ATOMIC_RELAXED);
    return f(dst, src, n);
}

static void * memmove_resolve(void * dst, const void * src, size_t n)
{
    mem_fn f = has_mops() ? __memmove_aarch64_mops : has_sve() ? __memmove_aarch64_sve : __memmove_aarch64_simd;
    __atomic_store_n(&memmove_impl, f, __ATOMIC_RELAXED);
    return f(dst, src, n);
}

static char * strchr_resolve(const char * s, int c)
{
    strchr_fn f = has_sve2() ? __strchr_aarch64_sve2 : __strchr_aarch64_mte;
    __atomic_store_n(&strchr_impl, f, __ATOMIC_RELAXED);
    return f(s, c);
}

void * memcpy(void * dst, const void * src, size_t n)
{
    return __atomic_load_n(&memcpy_impl, __ATOMIC_RELAXED)(dst, src, n);
}

void * memmove(void * dst, const void * src, size_t n)
{
    return __atomic_load_n(&memmove_impl, __ATOMIC_RELAXED)(dst, src, n);
}

char * strchr(const char * s, int c)
{
    return __atomic_load_n(&strchr_impl, __ATOMIC_RELAXED)(s, c);
}
