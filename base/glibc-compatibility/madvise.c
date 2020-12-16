#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/syscall.h>
#include <sys/mman.h>

#if !defined(unlikely)
#define unlikely(x) (__builtin_expect(!!(x), 0))
#endif

static int MADV_DONTNEED_works = -1;

#if defined (__cplusplus)
extern "C" {
#endif

static int __madvise(void * addr, size_t len, int advice)
{
    return syscall(SYS_madvise, addr, len, advice);
}

/**
 * Check for working MADV_DONTNEED at runtime.
 * If MADV_DONTNEED will not work propertly, madvise will be "patched" to return ENOSYS.
 *
 * TL;DR;
 *
 * jemalloc relies on working MADV_DONTNEED (that fact that after
 * madvise(MADV_DONTNEED) returns success, after subsequent access to those
 * pages they will be zeroed).
 *
 * However qemu does not support this, yet [1], and you can get very tricky
 * assert if you will run clickhouse-server under qemu:
 *
 *     <jemalloc>: ../contrib/jemalloc/src/extent.c:1195: Failed assertion: "p[i] == 0"
 *
 *   [1]: https://patchwork.kernel.org/patch/10576637/
 *
 * And "not supports this" means that it returns 0, not ENOSYS or some other
 * error.
 *
 * Refs: https://gist.github.com/azat/12ba2c825b710653ece34dba7f926ece
 * Refs: https://github.com/ClickHouse/ClickHouse/pull/15590
 */
static int madvise_MADV_DONTNEED_works()
{
    int works = -1;
    size_t size = 1 << 16;

    void * addr1 = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    void * addr2 = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);

    if (addr1 == MAP_FAILED || addr2 == MAP_FAILED)
    {
        abort();
    }

    memset(addr1, 'A', size);
    if (__madvise(addr1, size, MADV_DONTNEED) == 0)
    {
        works = memcmp(addr1, addr2, size) == 0;
    }
    else
    {
        /* Avoid checking one more time in case of MADV_DONTNEED does not actually supported. */
        works = -2;
    }

    if (munmap(addr1, size) != 0 || munmap(addr2, size) != 0)
    {
        abort();
    }

    return works;
}

static int __madvise_safe(void * addr, size_t len, int advice)
{
    if (unlikely(MADV_DONTNEED_works == -1))
    {
        MADV_DONTNEED_works = madvise_MADV_DONTNEED_works();
    }
    /* "Patch" to return ENOSYS */
    if (advice == MADV_DONTNEED && !MADV_DONTNEED_works)
    {
        errno = ENOSYS;
        return -1;
    }
    return __madvise(addr, len, advice);
}

int madvise(void * addr, size_t len, int advice)
{
    return __madvise_safe(addr, len, advice);
}

#if defined (__cplusplus)
}
#endif
