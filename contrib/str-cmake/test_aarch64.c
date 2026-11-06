/* Link the prefixed archive against an independent host libc with -fno-builtin. */
#define _GNU_SOURCE
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/auxv.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <unistd.h>

#ifdef STR_TEST_NO_SVE
unsigned long __real_getauxval(unsigned long);
unsigned long __wrap_getauxval(unsigned long type)
{
    unsigned long value = __real_getauxval(type);
    return type == AT_HWCAP ? value & ~(1UL << 22) : value;
}
#endif

void *str_memcpy(void *, const void *, size_t);
void *str_memmove(void *, const void *, size_t);
void *str_memset(void *, int, size_t);
void *str_memchr(const void *, int, size_t);
int str_memcmp(const void *, const void *, size_t);
size_t str_strlen(const char *);
char *str_strcpy(char *, const char *);
char *str_strcat(char *, const char *);
int str_strcmp(const char *, const char *);
size_t str_strspn(const char *, const char *);
size_t str_strcspn(const char *, const char *);

static void before_constructors(void)
{
    char buffer[64];
    const char expected[] = "before constructors";
    str_memset(buffer, 0, sizeof(buffer));
    str_memcpy(buffer, expected, sizeof(expected));
    if (str_memcmp(buffer, expected, sizeof(expected)) != 0
        || str_strlen(buffer) != sizeof(expected) - 1
        || str_strcmp(buffer, expected) != 0)
        _exit(90);
}

__attribute__((section(".preinit_array"), used))
static void (*const preinit_check)(void) = before_constructors;

static unsigned long random_state = 0x71317431;
static unsigned next_byte(void)
{
    random_state ^= random_state << 13;
    random_state ^= random_state >> 7;
    random_state ^= random_state << 17;
    return random_state & 255;
}

static int sign(int n)
{
    return (n > 0) - (n < 0);
}

static unsigned char *guarded(size_t page)
{
    unsigned char *p = mmap(NULL, 3 * page, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    assert(p != MAP_FAILED);
    assert(mprotect(p + page, page, PROT_READ | PROT_WRITE) == 0);
    return p + page;
}

int main(int argc, char **argv)
{
    if (argc == 2)
    {
        int requested = atoi(argv[1]);
        assert(requested > 0);
        int result = prctl(PR_SVE_SET_VL, requested, 0, 0, 0);
        if (result < 0 || (result & PR_SVE_VL_LEN_MASK) != requested)
        {
            fprintf(stderr, "Requested SVE VL %d is unavailable\n", requested);
            return 77;
        }
    }
    printf("HWCAP=%lx SVE_VL=%d\n", getauxval(AT_HWCAP), prctl(PR_SVE_GET_VL, 0, 0, 0, 0));
    unsigned char a[16384], b[16384], c[16384], source[16384];
    for (size_t i = 0; i < sizeof(source); ++i)
        source[i] = next_byte();

    for (size_t n = 0; n <= 4096; ++n)
    {
        size_t d = (n * 13) & 63, s = (n * 17) & 63;
        memset(a, 0xa5, sizeof(a));
        memset(b, 0xa5, sizeof(b));
        assert(str_memcpy(a + d, source + s, n) == a + d);
        memcpy(b + d, source + s, n);
        assert(memcmp(a, b, sizeof(a)) == 0);
        int value = (int)n - 2048;
        assert(str_memset(a + d, value, n) == a + d);
        memset(b + d, value, n);
        assert(memcmp(a, b, sizeof(a)) == 0);
        assert(str_memcmp(source + s, source + s, n) == 0);
        memcpy(c, source, sizeof(c));
        if (n)
            c[s + n / 2] ^= 255;
        assert(sign(str_memcmp(source + s, c + s, n)) == sign(memcmp(source + s, c + s, n)));
        for (int shift = -65; shift <= 65; shift += 13)
        {
            memcpy(a, source, sizeof(a));
            memcpy(b, source, sizeof(b));
            assert(str_memmove(a + 128 + shift, a + 128, n) == a + 128 + shift);
            memmove(b + 128 + shift, b + 128, n);
            assert(memcmp(a, b, sizeof(a)) == 0);
        }
        for (unsigned k = 0; k < 5; ++k)
        {
            int value = (int)next_byte() + 256;
            assert(str_memchr(source + s, value, n) == memchr(source + s, value, n));
        }
    }

    size_t page = (size_t)sysconf(_SC_PAGESIZE);
    assert(page >= 4096 && page % 4096 == 0);
    unsigned char *g = guarded(page), *h = guarded(page), *dest = guarded(page);
    assert(str_memchr(g + page, 1, 0) == NULL);
    assert(str_memcpy(dest + page, g + page, 0) == dest + page);
    assert(str_memmove(dest + page, g + page, 0) == dest + page);
    assert(str_memset(dest + page, 1, 0) == dest + page);
    assert(str_memcmp(g + page, h + page, 0) == 0);
    for (size_t n = 1; n <= 1024; ++n)
    {
        unsigned char *s = g + page - n;
        memset(s, 0xab, n);
        s[n - 1] = 0xcd;
        assert(str_memchr(s, 0xcd, n + 1) == s + n - 1);
        assert(str_memchr(s, 0xcd, n + 64) == s + n - 1);
        assert(str_memchr(s, 0xcd, SIZE_MAX) == s + n - 1);
        assert(str_memchr(s, 0xac, n) == NULL);
        assert(str_memcpy(dest + page - n, s, n) == dest + page - n);
        assert(memcmp(dest + page - n, s, n) == 0);

        for (size_t i = 0; i + 1 < n; ++i)
            s[i] = (unsigned char)(1 + next_byte() % 255);
        s[n - 1] = 0;
        assert(str_strlen((char *)s) == n - 1);
        memset(dest, 0xa5, page);
        assert(str_strcpy((char *)dest + page - n, (char *)s) == (char *)dest + page - n);
        assert(memcmp(dest + page - n, s, n) == 0);
        if (n < page)
            assert(dest[page - n - 1] == 0xa5);
        memcpy(h + page - n, s, n);
        assert(str_strcmp((char *)s, (char *)h + page - n) == 0);
        if (n > 1)
            h[page - 2] ^= 0x80;
        assert(sign(str_strcmp((char *)s, (char *)h + page - n)) == sign(strcmp((char *)s, (char *)h + page - n)));
        memset(a, 0xa5, sizeof(a));
        memset(b, 0xa5, sizeof(b));
        a[31] = b[31] = 'x';
        a[32] = b[32] = 0;
        assert(str_strcat((char *)a + 31, (char *)s) == (char *)a + 31);
        strcat((char *)b + 31, (char *)s);
        assert(memcmp(a, b, sizeof(a)) == 0);

        size_t set_length = n % 49;
        char *set = (char *)h + page - set_length - 1;
        for (size_t i = 0; i < set_length; ++i)
            set[i] = (char)(1 + next_byte() % 255);
        set[set_length] = 0;
        assert(str_strspn((char *)s, set) == strspn((char *)s, set));
        assert(str_strcspn((char *)s, set) == strcspn((char *)s, set));
        memset(a, set_length ? (unsigned char)set[0] : 'x', 2047);
        a[2047] = 0;
        assert(str_strspn((char *)a, set) == strspn((char *)a, set));
        assert(str_strcspn((char *)a, set) == strcspn((char *)a, set));
    }
    assert(munmap(g - page, 3 * page) == 0);
    assert(munmap(h - page, 3 * page) == 0);
    assert(munmap(dest - page, 3 * page) == 0);
    puts("PASS: eleven public entry points, overlap, guard pages, early-match oversized memchr");
    return 0;
}
