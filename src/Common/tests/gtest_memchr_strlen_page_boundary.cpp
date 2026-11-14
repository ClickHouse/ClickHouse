#include <gtest/gtest.h>

#include <Common/CPUID.h>

#include <climits>
#include <cstdint>
#include <cstring>
#include <random>
#include <stdexcept>
#include <ostream>
#include <string>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

/// `memchr` and `strlen` are overridden process-wide (see `contrib/libllvmlibc-cmake/string/str_functions.cpp`
/// on x86_64; other architectures run the libc versions through the same tests). Each test runs against the
/// libc functions the binary links, and on x86_64 also against every vector tier of the override separately
/// (`str_functions_test_candidates.cpp`): the process-wide functions run only the tier the CPU selects, and
/// sanitizer builds, which run the unit tests in CI, do not override them at all. These tests pin down the
/// ISO C contract plus the two properties callers rely on: the functions never read outside the page holding
/// the buffer, even when the `memchr` limit overstates the buffer up to `SIZE_MAX` as `strnlen(path, PATH_MAX + 1)`
/// callers do, and the result does not depend on the bytes next to the match (SWAR and vector implementations
/// compare whole words at once).

#if defined(__x86_64__)
extern "C"
{
void * ch_test_memchr_sse2(const void * s, int c, size_t n) noexcept;
size_t ch_test_strlen_sse2(const char * s) noexcept;
void * ch_test_memchr_avx2(const void * s, int c, size_t n) noexcept;
size_t ch_test_strlen_avx2(const char * s) noexcept;
void * ch_test_memchr_avx512(const void * s, int c, size_t n) noexcept;
size_t ch_test_strlen_avx512(const char * s) noexcept;
}
#endif

namespace
{

using MemchrFunction = const void * (*)(const void *, int, size_t);
using StrlenFunction = size_t (*)(const char *);

/// Called through volatile pointers so that the compiler cannot replace the calls with its builtins.
/// The glibc headers declare const-overloaded `memchr` for C++, so it is wrapped rather than taken by address.
const void * (* volatile libc_memchr)(const void *, int, size_t)
    = [](const void * s, int c, size_t n) -> const void * { return memchr(s, c, n); };
size_t (* volatile libc_strlen)(const char *) = &strlen;

struct Implementation
{
    const char * name;
    MemchrFunction memchr_function;
    StrlenFunction strlen_function;
    bool (*supported)();
};

void PrintTo(const Implementation & implementation, std::ostream * os)
{
    *os << implementation.name;
}

std::vector<Implementation> implementations()
{
    std::vector<Implementation> result{
        {"libc",
         [](const void * s, int c, size_t n) { return libc_memchr(s, c, n); },
         [](const char * s) { return libc_strlen(s); },
         [] { return true; }},
    };
#if defined(__x86_64__)
    result.push_back(
        {"sse2",
         [](const void * s, int c, size_t n) -> const void * { return ch_test_memchr_sse2(s, c, n); },
         &ch_test_strlen_sse2,
         [] { return true; }});
    result.push_back(
        {"avx2",
         [](const void * s, int c, size_t n) -> const void * { return ch_test_memchr_avx2(s, c, n); },
         &ch_test_strlen_avx2,
         [] { return DB::CPU::CPUFlagsCache::have_AVX2; }});
    /// The same features as `detect_cpu_level` in `x86_cpu_dispatch.h` checks, plus BMI2 for `bzhi`,
    /// which every AVX-512 CPU has.
    result.push_back(
        {"avx512",
         [](const void * s, int c, size_t n) -> const void * { return ch_test_memchr_avx512(s, c, n); },
         &ch_test_strlen_avx512,
         []
         {
             using DB::CPU::CPUFlagsCache;
             return CPUFlagsCache::have_AVX512F && CPUFlagsCache::have_AVX512DQ && CPUFlagsCache::have_AVX512CD
                 && CPUFlagsCache::have_AVX512BW && CPUFlagsCache::have_AVX512VL && CPUFlagsCache::have_BMI2;
         }});
#endif
    return result;
}

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

/// Longer than the unrolled stride of the widest vector implementation (4 * 64 bytes) plus the three
/// straight-line vectors before it, so every head, re-alignment, unrolled and tail path is crossed at
/// every misalignment.
constexpr size_t max_len = 600;

const size_t oversized_limits[] = {PATH_MAX - 1, PATH_MAX, PATH_MAX + 1, SIZE_MAX / 2, SIZE_MAX - 1, SIZE_MAX};

/// Byte values that trip naive "has byte" tricks: 0, the sign bit, all ones, and their neighbours.
const unsigned char tricky_values[] = {0x00, 0x01, 0x7f, 0x80, 0x81, 0xfe, 0xff, 0x5a};

const void * reference_memchr(const void * s, int c, size_t n)
{
    const auto * p = static_cast<const unsigned char *>(s);
    for (size_t i = 0; i < n; ++i)
        if (p[i] == static_cast<unsigned char>(c))
            return p + i;
    return nullptr;
}

}

class MemchrStrlenPageBoundary : public ::testing::TestWithParam<Implementation>
{
protected:
    void SetUp() override
    {
        if (!GetParam().supported())
            GTEST_SKIP() << "the CPU does not support " << GetParam().name;
    }

    const void * memchr_ptr(const void * s, int c, size_t n) const { return GetParam().memchr_function(s, c, n); }
    size_t strlen_ptr(const char * s) const { return GetParam().strlen_function(s); }
};

TEST_P(MemchrStrlenPageBoundary, StrlenTerminatorAtPageEnd)
{
    GuardedPage page;
    page.fill('x');
    page.end()[-1] = '\0';
    for (size_t len = 0; len < max_len; ++len)
        ASSERT_EQ(len, strlen_ptr(page.end() - 1 - len)) << "len " << len;
}

TEST_P(MemchrStrlenPageBoundary, StrlenAtPageBegin)
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

/// Bytes with the high bit set and bytes next to 0 must not be mistaken for the terminator, and the
/// garbage after the terminator must not matter.
TEST_P(MemchrStrlenPageBoundary, StrlenHighBytesAndGarbageAfterTerminator)
{
    GuardedPage page;
    for (unsigned char fill : tricky_values)
    {
        if (fill == 0)
            continue;
        page.fill(static_cast<char>(fill));
        for (size_t offset = 0; offset < 64; ++offset)
        {
            for (size_t len = 0; len < max_len; ++len)
            {
                char * s = page.begin() + offset;
                s[len] = '\0';
                /// Garbage after the terminator, including more terminators.
                s[len + 1] = '\0';
                s[len + 2] = static_cast<char>(0x80);
                ASSERT_EQ(len, strlen_ptr(s)) << "fill " << int(fill) << ", offset " << offset << ", len " << len;
                s[len] = s[len + 1] = s[len + 2] = static_cast<char>(fill);
            }
        }
    }
}

TEST_P(MemchrStrlenPageBoundary, MemchrExactLimitAtPageEnd)
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

TEST_P(MemchrStrlenPageBoundary, MemchrOversizedLimitStopsAtMatchAtPageEnd)
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

/// C11 7.24.5.1p2: the search behaves as if the bytes were read one at a time and stops at the first match,
/// so with an overstated limit nothing after the match may be touched, wherever the match is in the buffer.
TEST_P(MemchrStrlenPageBoundary, MemchrOversizedLimitStopsAtMatchAnywhere)
{
    GuardedPage page;
    page.fill('x');
    for (size_t bytes_after_match = 0; bytes_after_match < 70; ++bytes_after_match)
    {
        for (size_t pos = 0; pos < 300; ++pos)
        {
            char * s = page.end() - (pos + 1 + bytes_after_match);
            s[pos] = 'y';
            for (size_t n : oversized_limits)
                ASSERT_EQ(s + pos, memchr_ptr(s, 'y', n)) << "pos " << pos << ", after " << bytes_after_match << ", n " << n;
            s[pos] = 'x';
        }
    }
}

TEST_P(MemchrStrlenPageBoundary, MemchrAtPageBegin)
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

/// The first of several matches is returned.
TEST_P(MemchrStrlenPageBoundary, MemchrFirstMatchWins)
{
    GuardedPage page;
    page.fill('x');
    for (size_t offset = 0; offset < 64; ++offset)
    {
        char * s = page.begin() + offset;
        for (size_t first = 0; first < 300; ++first)
        {
            for (size_t gap : {size_t(1), size_t(15), size_t(16), size_t(31), size_t(32), size_t(63), size_t(64), size_t(65), size_t(255),})
            {
                s[first] = 'y';
                s[first + gap] = 'y';
                ASSERT_EQ(s + first, memchr_ptr(s, 'y', max_len)) << "offset " << offset << ", first " << first << ", gap " << gap;
                s[first] = 'x';
                s[first + gap] = 'x';
            }
        }
    }
}

/// `c` is converted to `unsigned char`: the high bits of the `int` are ignored, and -1 means 0xff.
TEST_P(MemchrStrlenPageBoundary, MemchrConvertsCharacterToUnsignedChar)
{
    GuardedPage page;
    page.fill('x');
    for (unsigned char value : tricky_values)
    {
        char * s = page.begin() + 3;
        s[100] = static_cast<char>(value);
        for (int c : {int(value), int(value) + 0x100, int(value) - 0x100, int(value) | 0x7fffff00, int(static_cast<signed char>(value))})
            ASSERT_EQ(s + 100, memchr_ptr(s, c, 200)) << "value " << int(value) << ", c " << c;
        s[100] = 'x';
    }
}

/// Only the byte itself may match, not a byte that differs in a single bit or in the sign bit,
/// and such neighbours around the match must not hide it.
TEST_P(MemchrStrlenPageBoundary, MemchrIgnoresNeighbourValues)
{
    GuardedPage page;
    for (unsigned char c : tricky_values)
    {
        const unsigned char neighbours[] = {
            static_cast<unsigned char>(c ^ 0x80), static_cast<unsigned char>(c ^ 0x01), static_cast<unsigned char>(c + 1),
            static_cast<unsigned char>(c - 1), static_cast<unsigned char>(~c), 0x80, 0x00, 0xff, 0x01, 0x7f};
        for (size_t n = 1; n <= 300; ++n)
        {
            for (size_t offset = 0; offset < 64; offset += 7)
            {
                char * s = page.begin() + offset;
                for (size_t i = 0; i < n; ++i)
                {
                    unsigned char x = neighbours[(i * 7 + n) % std::size(neighbours)];
                    s[i] = static_cast<char>(x == c ? static_cast<unsigned char>(c ^ 0x40) : x);
                }
                ASSERT_EQ(nullptr, memchr_ptr(s, c, n)) << "c " << int(c) << ", n " << n << ", offset " << offset;
                size_t pos = (n * 13) % n;
                char saved = s[pos];
                s[pos] = static_cast<char>(c);
                ASSERT_EQ(s + pos, memchr_ptr(s, c, n)) << "c " << int(c) << ", n " << n << ", offset " << offset << ", pos " << pos;
                s[pos] = saved;
            }
        }
    }
}

TEST_P(MemchrStrlenPageBoundary, MemchrMatchesReference)
{
    GuardedPage page;
    std::mt19937_64 rng(42); /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible
    /// A small alphabet including the tricky values, so that matches are frequent and appear at every position.
    std::vector<unsigned char> alphabet(tricky_values, tricky_values + std::size(tricky_values));
    for (unsigned char v = 'a'; v < 'a' + 56; ++v)
        alphabet.push_back(v);
    for (size_t i = 0; i < page.page_size; ++i)
        page.begin()[i] = static_cast<char>(alphabet[rng() % alphabet.size()]);
    for (size_t iteration = 0; iteration < 200000; ++iteration)
    {
        const size_t offset = rng() % page.page_size;
        const size_t n = rng() % (page.page_size - offset + 1);
        const int c = alphabet[rng() % alphabet.size()];
        const char * s = page.begin() + offset;
        ASSERT_EQ(reference_memchr(s, c, n), memchr_ptr(s, c, n)) << "offset " << offset << ", n " << n << ", c " << c;
    }
}

/// A buffer far longer than any vector stride, with the match at the very end.
TEST_P(MemchrStrlenPageBoundary, LargeBuffer)
{
    const size_t n = size_t(32) << 20;
    std::vector<char> buffer(n + 1, 'x');
    char * s = buffer.data();
    s[n] = 'y';
    ASSERT_EQ(nullptr, memchr_ptr(s, 'y', n));
    s[n - 1] = 'y';
    ASSERT_EQ(s + n - 1, memchr_ptr(s, 'y', n));
    s[n - 1] = 'x';
    s[n] = '\0';
    ASSERT_EQ(n, strlen_ptr(s));
}

INSTANTIATE_TEST_SUITE_P(
    Implementations,
    MemchrStrlenPageBoundary,
    ::testing::ValuesIn(implementations()),
    [](const ::testing::TestParamInfo<Implementation> & param_info) { return std::string(param_info.param.name); });
