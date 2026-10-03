#include <gtest/gtest.h>

#include <climits>
#include <cstdint>
#include <cstring>
#include <random>
#include <stdexcept>

#include <sys/mman.h>
#include <unistd.h>

/// `memchr` and `strlen` are overridden process-wide (see `contrib/libllvmlibc-cmake/string/str_functions.cpp`
/// on x86_64). These tests pin down that they never read outside the page holding the buffer, even when
/// the `memchr` limit overstates the buffer up to `SIZE_MAX`, as `strnlen(path, PATH_MAX + 1)` callers do.

namespace
{

/// Called through volatile pointers so that the compiler cannot replace the calls with its builtins.
/// The glibc headers declare const-overloaded `memchr` for C++, so it is wrapped rather than taken by address.
const void * (* volatile memchr_ptr)(const void *, int, size_t)
    = [](const void * s, int c, size_t n) -> const void * { return memchr(s, c, n); };
size_t (* volatile strlen_ptr)(const char *) = &strlen;

/// One readable page surrounded by two `PROT_NONE` pages: any access outside it faults.
class GuardedPage
{
public:
    GuardedPage()
        : page_size(static_cast<size_t>(sysconf(_SC_PAGESIZE)))
    {
        void * mapping = mmap(nullptr, 3 * page_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (mapping == MAP_FAILED)
            throw std::runtime_error("mmap failed");
        base = static_cast<char *>(mapping);
        if (0 != mprotect(begin(), page_size, PROT_READ | PROT_WRITE))
            throw std::runtime_error("mprotect failed");
    }

    ~GuardedPage() { munmap(base, 3 * page_size); }

    char * begin() const { return base + page_size; }
    char * end() const { return base + 2 * page_size; }
    void fill(char c) const { memset(begin(), c, page_size); }

    const size_t page_size;

private:
    char * base;
};

/// Longer than the unrolled stride of the widest vector implementation (4 * 64 bytes), so every head,
/// re-alignment, unrolled and tail path is crossed at every misalignment.
constexpr size_t max_len = 600;

const size_t oversized_limits[] = {PATH_MAX - 1, PATH_MAX, PATH_MAX + 1, SIZE_MAX / 2, SIZE_MAX - 1, SIZE_MAX};

}

TEST(MemchrStrlenPageBoundary, StrlenTerminatorAtPageEnd)
{
    GuardedPage page;
    page.fill('x');
    page.end()[-1] = '\0';
    for (size_t len = 0; len < max_len; ++len)
        ASSERT_EQ(len, strlen_ptr(page.end() - 1 - len)) << "len " << len;
}

TEST(MemchrStrlenPageBoundary, StrlenAtPageBegin)
{
    GuardedPage page;
    page.fill('x');
    for (size_t offset = 0; offset < 128; ++offset)
    {
        for (size_t len = 0; len < max_len; ++len)
        {
            page.begin()[offset + len] = '\0';
            ASSERT_EQ(len, strlen_ptr(page.begin() + offset)) << "offset " << offset << ", len " << len;
            page.begin()[offset + len] = 'x';
        }
    }
}

TEST(MemchrStrlenPageBoundary, MemchrExactLimitAtPageEnd)
{
    GuardedPage page;
    page.fill('x');
    for (size_t len = 0; len < max_len; ++len)
    {
        const char * s = page.end() - len;
        ASSERT_EQ(nullptr, memchr_ptr(s, 'y', len)) << "len " << len;
        if (len > 0)
        {
            page.end()[-1] = 'y';
            ASSERT_EQ(page.end() - 1, memchr_ptr(s, 'y', len)) << "len " << len;
            page.end()[-1] = 'x';
        }
    }
}

TEST(MemchrStrlenPageBoundary, MemchrOversizedLimitStopsAtMatchAtPageEnd)
{
    GuardedPage page;
    page.fill('x');
    page.end()[-1] = '\0';
    for (size_t len = 1; len < max_len; ++len)
    {
        const char * s = page.end() - len;
        for (size_t n : oversized_limits)
            ASSERT_EQ(page.end() - 1, memchr_ptr(s, '\0', n)) << "len " << len << ", n " << n;
        ASSERT_EQ(page.end() - 1, memchr_ptr(s, '\0', len + 1)) << "len " << len;
    }
}

TEST(MemchrStrlenPageBoundary, MemchrAtPageBegin)
{
    GuardedPage page;
    page.fill('x');
    for (size_t offset = 0; offset < 128; ++offset)
    {
        const char * s = page.begin() + offset;
        for (size_t pos = 0; pos < max_len; ++pos)
        {
            page.begin()[offset + pos] = 'y';
            for (size_t n : oversized_limits)
                ASSERT_EQ(s + pos, memchr_ptr(s, 'y', n)) << "offset " << offset << ", pos " << pos << ", n " << n;
            ASSERT_EQ(s + pos, memchr_ptr(s, 'y', pos + 1)) << "offset " << offset << ", pos " << pos;
            ASSERT_EQ(nullptr, memchr_ptr(s, 'y', pos)) << "offset " << offset << ", pos " << pos;
            page.begin()[offset + pos] = 'x';
        }
    }
}

TEST(MemchrStrlenPageBoundary, MemchrMatchesReference)
{
    GuardedPage page;
    std::mt19937_64 rng(42);
    /// A small alphabet, so that matches are frequent and appear at every position.
    for (size_t i = 0; i < page.page_size; ++i)
        page.begin()[i] = static_cast<char>('a' + rng() % 64);
    for (size_t iteration = 0; iteration < 100000; ++iteration)
    {
        const size_t offset = rng() % page.page_size;
        const size_t n = rng() % (page.page_size - offset + 1);
        const char c = static_cast<char>('a' + rng() % 64);
        const char * s = page.begin() + offset;

        const char * expected = nullptr;
        for (size_t i = 0; i < n; ++i)
        {
            if (s[i] == c)
            {
                expected = s + i;
                break;
            }
        }
        ASSERT_EQ(expected, memchr_ptr(s, c, n)) << "offset " << offset << ", n " << n;
    }
}
