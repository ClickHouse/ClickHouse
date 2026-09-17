#include <algorithm>
#include <exception>
#include <random>
#include <string>
#include <vector>

#include <cstdint>

#include <gtest/gtest.h>

#include <base/sort.h>

/// Regression coverage for the vendored sort engines wired into `base/base/sort.h`:
/// `::sort` (ipnsort) and `::stableSort` (driftsort). The element headers are validated by
/// comparing against `std::sort` / `std::stable_sort` over a range of sizes and input patterns,
/// for trivially-copyable, non-default-constructible trivially-copyable, over-aligned and
/// non-trivially-copyable element types. Stability of `::stableSort` is checked explicitly.

namespace
{

/// Sizes that exercise the small-sort networks, the stack-scratch path and the heap-scratch path.
const std::vector<size_t> test_sizes = {0, 1, 2, 3, 7, 16, 17, 31, 32, 33, 64, 127, 128, 300, 1000, 5000, 40000};

enum class Pattern
{
    Random,
    Sorted,
    Reverse,
    FewUnique,
    OrganPipe,
    AllEqual,
};

const std::vector<Pattern> test_patterns
    = {Pattern::Random, Pattern::Sorted, Pattern::Reverse, Pattern::FewUnique, Pattern::OrganPipe, Pattern::AllEqual};

std::vector<int> makeKeys(size_t n, Pattern pattern, std::mt19937_64 & rng)
{
    std::vector<int> keys(n);
    switch (pattern)
    {
        case Pattern::Random:
            for (auto & k : keys)
                k = static_cast<int>(rng() % 1000000);
            break;
        case Pattern::Sorted:
            for (size_t i = 0; i < n; ++i)
                keys[i] = static_cast<int>(i);
            break;
        case Pattern::Reverse:
            for (size_t i = 0; i < n; ++i)
                keys[i] = static_cast<int>(n - i);
            break;
        case Pattern::FewUnique:
            for (auto & k : keys)
                k = static_cast<int>(rng() % 8);
            break;
        case Pattern::OrganPipe:
            for (size_t i = 0; i < n; ++i)
                keys[i] = static_cast<int>(i < n / 2 ? i : n - i);
            break;
        case Pattern::AllEqual:
            for (auto & k : keys)
                k = 42;
            break;
    }
    return keys;
}

}

TEST(Sort, MatchesStdSortTriviallyCopyable)
{
    std::mt19937_64 rng(12345); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    for (size_t n : test_sizes)
    {
        for (Pattern pattern : test_patterns)
        {
            std::vector<int> keys = makeKeys(n, pattern, rng);

            std::vector<int> a = keys;
            std::vector<int> b = keys;
            ::sort(a.begin(), a.end());
            std::sort(b.begin(), b.end());
            ASSERT_EQ(a, b) << "size=" << n << " pattern=" << static_cast<int>(pattern);

            /// Custom (reverse) comparator.
            std::vector<int> c = keys;
            std::vector<int> d = keys;
            auto greater = [](int x, int y) { return x > y; };
            ::sort(c.begin(), c.end(), greater);
            std::sort(d.begin(), d.end(), greater);
            ASSERT_EQ(c, d) << "size=" << n << " pattern=" << static_cast<int>(pattern) << " (reverse comparator)";
        }
    }
}

TEST(Sort, StableSortMatchesStdAndIsStable)
{
    /// A value carrying a sort key and its original position; equal keys must keep original order.
    struct Item
    {
        int key;
        int original_index;
    };
    static_assert(std::is_trivially_copyable_v<Item>);

    auto less = [](const Item & x, const Item & y) { return x.key < y.key; };

    std::mt19937_64 rng(67890); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    for (size_t n : test_sizes)
    {
        for (Pattern pattern : test_patterns)
        {
            std::vector<int> keys = makeKeys(n, pattern, rng);

            std::vector<Item> a(n);
            std::vector<Item> b(n);
            for (size_t i = 0; i < n; ++i)
            {
                a[i] = Item{keys[i], static_cast<int>(i)};
                b[i] = Item{keys[i], static_cast<int>(i)};
            }

            ::stableSort(a.begin(), a.end(), less);
            std::stable_sort(b.begin(), b.end(), less);

            ASSERT_EQ(a.size(), b.size());
            for (size_t i = 0; i < n; ++i)
            {
                ASSERT_EQ(a[i].key, b[i].key) << "size=" << n << " pattern=" << static_cast<int>(pattern) << " i=" << i;
                /// Stability: identical to the reference stable sort, including equal-key order.
                ASSERT_EQ(a[i].original_index, b[i].original_index)
                    << "size=" << n << " pattern=" << static_cast<int>(pattern) << " i=" << i << " (not stable)";
            }
        }
    }
}

TEST(Sort, NonDefaultConstructibleTriviallyCopyable)
{
    /// Exercises the path that stores scratch elements in uninitialized storage: the element type
    /// is trivially copyable but cannot be default-constructed.
    struct NoDefault
    {
        int key;
        int payload;
        NoDefault() = delete;
        NoDefault(int k, int p) : key(k), payload(p) {}
    };
    static_assert(std::is_trivially_copyable_v<NoDefault>);

    auto less = [](const NoDefault & x, const NoDefault & y) { return x.key < y.key; };

    std::mt19937_64 rng(11111); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    for (size_t n : test_sizes)
    {
        std::vector<int> keys = makeKeys(n, Pattern::Random, rng);

        std::vector<NoDefault> a;
        a.reserve(n);
        for (size_t i = 0; i < n; ++i)
            a.emplace_back(keys[i], static_cast<int>(i));
        std::vector<NoDefault> b = a;

        ::sort(a.begin(), a.end(), less);
        ::stableSort(b.begin(), b.end(), less);

        for (size_t i = 1; i < n; ++i)
        {
            ASSERT_FALSE(less(a[i], a[i - 1])) << "size=" << n << " (::sort not ordered)";
            ASSERT_FALSE(less(b[i], b[i - 1])) << "size=" << n << " (::stableSort not ordered)";
        }
    }
}

TEST(Sort, OverAlignedType)
{
    /// The driftsort heap scratch must honor `alignof(T)`. With a 64-byte element the stack scratch
    /// (4 KiB) only holds 64 elements, so larger inputs take the heap path; the comparator asserts
    /// that it never observes an under-aligned reference.
    struct alignas(64) Aligned
    {
        uint64_t key;
        char padding[56];
    };
    static_assert(std::is_trivially_copyable_v<Aligned>);
    static_assert(alignof(Aligned) == 64);

    auto less = [](const Aligned & x, const Aligned & y)
    {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(&x) % alignof(Aligned), 0u);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(&y) % alignof(Aligned), 0u);
        return x.key < y.key;
    };

    std::mt19937_64 rng(22222); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    for (size_t n : {65u, 128u, 300u, 5000u})
    {
        std::vector<Aligned> a(n);
        for (size_t i = 0; i < n; ++i)
            a[i] = Aligned{rng() % 100000, {}};
        std::vector<Aligned> b = a;

        ::sort(a.begin(), a.end(), less);
        ::stableSort(b.begin(), b.end(), less);

        for (size_t i = 1; i < n; ++i)
        {
            ASSERT_LE(a[i - 1].key, a[i].key) << "size=" << n << " (::sort)";
            ASSERT_LE(b[i - 1].key, b[i].key) << "size=" << n << " (::stableSort)";
        }
    }
}

TEST(Sort, NonTriviallyCopyableFallback)
{
    /// Non-trivially-copyable element type: `::sort` uses the move-based path and `::stableSort`
    /// falls back to `std::stable_sort`.
    struct Item
    {
        std::string key;
        int original_index = 0;
    };

    auto less = [](const Item & x, const Item & y) { return x.key < y.key; };

    std::mt19937_64 rng(33333); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    for (size_t n : test_sizes)
    {
        std::vector<Item> a(n);
        for (size_t i = 0; i < n; ++i)
            a[i] = Item{std::to_string(rng() % 1000), static_cast<int>(i)};
        std::vector<Item> b = a;
        std::vector<Item> ref = a;

        ::sort(a.begin(), a.end(), less);
        std::sort(ref.begin(), ref.end(), less);
        for (size_t i = 0; i < n; ++i)
            ASSERT_EQ(a[i].key, ref[i].key) << "size=" << n << " i=" << i << " (::sort)";

        ::stableSort(b.begin(), b.end(), less);
        for (size_t i = 1; i < n; ++i)
            ASSERT_FALSE(less(b[i], b[i - 1])) << "size=" << n << " i=" << i << " (::stableSort not ordered)";
    }
}

namespace
{

/// A non-trivially-copyable element whose live instances are counted, so a leaked element (one that
/// was constructed but never destroyed) is detectable after the owning container is gone.
struct Tracked
{
    int value = 0;
    static inline int live = 0;

    explicit Tracked(int v) : value(v) { ++live; }
    Tracked(const Tracked & o) : value(o.value) { ++live; }
    Tracked(Tracked && o) noexcept : value(o.value) { ++live; }
    Tracked & operator=(const Tracked &) = default;
    Tracked & operator=(Tracked &&) noexcept = default;
    ~Tracked() { --live; }
};

struct ComparisonLimitReached : std::exception
{
};

}

TEST(Sort, MoveBasedPartitionExceptionSafety)
{
    /// `::sort` on a non-trivially-copyable type uses the move-based Hoare partition, which saves an
    /// in-transit element in uninitialized storage while shuffling. If the comparator throws while
    /// that storage is engaged, the saved element must still be destroyed on unwind — otherwise it
    /// leaks. Throw at every comparison index in turn and verify nothing is left alive once the
    /// vector is destroyed.
    const size_t n = 512;

    std::mt19937_64 rng(44444); // NOLINT(cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures

    for (size_t limit = 1; limit <= 400; ++limit)
    {
        size_t calls = 0;
        auto less = [&](const Tracked & x, const Tracked & y)
        {
            if (++calls > limit)
                throw ComparisonLimitReached{};
            return x.value < y.value;
        };

        {
            std::vector<Tracked> v;
            v.reserve(n);
            for (size_t i = 0; i < n; ++i)
                v.emplace_back(static_cast<int>(rng() % 1000000));

            bool threw = false;
            try
            {
                ::sort(v.begin(), v.end(), less);
            }
            catch (const ComparisonLimitReached &)
            {
                threw = true;
            }
            /// n = 512 unsorted elements always require more than `limit` (<= 400) comparisons.
            ASSERT_TRUE(threw) << "comparator did not throw at comparison " << limit;
        }

        ASSERT_EQ(Tracked::live, 0) << "element leaked when the comparator threw at comparison " << limit;
    }
}
