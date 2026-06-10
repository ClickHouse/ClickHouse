#include <gtest/gtest.h>

#include <base/sort.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <string>
#include <vector>

/** `::sort` (and `::trySort`) is the general-purpose comparison sort behind `ORDER BY`. It is
  * implemented on top of branchless quicksort (`blqsort`) with a pdqsort pattern pre-pass. These
  * tests pin its contract: for every size and input pattern that matters, and for both direct and
  * indirect (permutation) comparators, the result must match `std::sort` element-wise.
  *
  * The boundary sizes (0..13, around 512 and 1024) and the patterned inputs (sorted, reverse,
  * organ-pipe, duplicate-heavy) exercise the small-array, sorting-network and partitioning paths
  * of `blqsort` as well as the pattern pre-pass. The indirect comparator over a `std::iota`
  * permutation reproduces the hot `getPermutation` path; a reverse-sorted permutation there is the
  * case that regressed by an order of magnitude before the pre-pass was added.
  */

namespace
{

template <typename T, typename Compare>
void checkMatchesStdSort(std::vector<T> input, Compare compare)
{
    std::vector<T> expected = input;
    std::stable_sort(expected.begin(), expected.end(), compare);

    std::vector<T> actual = input;
    ::sort(actual.begin(), actual.end(), compare);

    /// `::sort` is not stable, so compare the sorted sequences directly rather than relying on
    /// positions of equal elements. They must be equal because both are sorted by the same order.
    ASSERT_EQ(expected, actual);
    ASSERT_TRUE(std::is_sorted(actual.begin(), actual.end(), compare));
}

std::vector<size_t> interestingSizes()
{
    std::vector<size_t> sizes;
    for (size_t i = 0; i <= 13; ++i)
        sizes.push_back(i);
    for (size_t base : {511, 512, 513, 1023, 1024, 1025, 4096, 100000})
        sizes.push_back(base);
    return sizes;
}

enum class Pattern
{
    Random,
    Sorted,
    Reverse,
    OrganPipe,
    FewUnique,
    AllEqual,
};

std::vector<int64_t> makeInput(size_t size, Pattern pattern, std::mt19937_64 & rng)
{
    std::vector<int64_t> data(size);
    switch (pattern)
    {
        case Pattern::Random:
            for (auto & x : data)
                x = static_cast<int64_t>(rng());
            break;
        case Pattern::Sorted:
            std::iota(data.begin(), data.end(), int64_t(0));
            break;
        case Pattern::Reverse:
            for (size_t i = 0; i < size; ++i)
                data[i] = static_cast<int64_t>(size - i);
            break;
        case Pattern::OrganPipe:
            for (size_t i = 0; i < size; ++i)
                data[i] = static_cast<int64_t>(std::min(i, size - i));
            break;
        case Pattern::FewUnique:
            for (auto & x : data)
                x = static_cast<int64_t>(rng() % 7);
            break;
        case Pattern::AllEqual:
            std::fill(data.begin(), data.end(), int64_t(42));
            break;
    }
    return data;
}

std::string makeString(uint64_t value)
{
    std::string digits = std::to_string(value);
    /// Left-pad to a fixed width so lexicographic order matches numeric order and the strings share
    /// long common prefixes, stressing the comparator that `block_qsort` calls for complex types.
    return "key_" + std::string(20 - std::min<size_t>(digits.size(), 20), '0') + digits;
}

std::vector<std::string> makeStringInput(size_t size, Pattern pattern, std::mt19937_64 & rng)
{
    std::vector<std::string> data(size);
    switch (pattern)
    {
        case Pattern::Random:
            for (auto & x : data)
                x = makeString(rng());
            break;
        case Pattern::Sorted:
            for (size_t i = 0; i < size; ++i)
                data[i] = makeString(i);
            break;
        case Pattern::Reverse:
            for (size_t i = 0; i < size; ++i)
                data[i] = makeString(size - i);
            break;
        case Pattern::OrganPipe:
            for (size_t i = 0; i < size; ++i)
                data[i] = makeString(std::min(i, size - i));
            break;
        case Pattern::FewUnique:
            for (auto & x : data)
                x = makeString(rng() % 7);
            break;
        case Pattern::AllEqual:
            std::fill(data.begin(), data.end(), makeString(42));
            break;
    }
    return data;
}

}

TEST(Sort, MatchesStdSortDirectComparator)
{
    std::mt19937_64 rng(0xC0FFEE); // NOLINT(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    for (Pattern pattern :
         {Pattern::Random, Pattern::Sorted, Pattern::Reverse, Pattern::OrganPipe, Pattern::FewUnique, Pattern::AllEqual})
    {
        for (size_t size : interestingSizes())
        {
            auto data = makeInput(size, pattern, rng);
            checkMatchesStdSort(data, std::less<>{});
            checkMatchesStdSort(data, std::greater<>{});
        }
    }
}

/// 16-byte trivially-copyable type that takes the `sizeof(T) <= 16` branchless path of blqsort,
/// matching `Decimal128`, the type whose `ORDER BY ... DESC` regressed without the pattern pre-pass.
TEST(Sort, MatchesStdSortWideType)
{
    for (size_t size : interestingSizes())
    {
        std::vector<__int128> data(size);
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<__int128>(i); /// already sorted ascending

        auto ascending = [](const __int128 & a, const __int128 & b) { return a < b; };
        auto descending = [](const __int128 & a, const __int128 & b) { return a > b; };

        checkMatchesStdSort(data, ascending);
        checkMatchesStdSort(data, descending); /// reverse-sorted relative to the comparator
    }
}

/// `std::string` is not trivially-copyable, so `::sort` takes the BlockQuicksort (`block_qsort`)
/// path inside `blqsort` rather than the branchless path — this is the path behind the PR's
/// user-visible claim of faster string `ORDER BY`. Cover the same boundary sizes and patterns
/// (high-cardinality, sorted, reverse, organ-pipe, duplicate-heavy, all-equal) as the trivially-
/// copyable cases, with both ascending and descending comparators.
TEST(Sort, MatchesStdSortStrings)
{
    std::mt19937_64 rng(0xBEEF); // NOLINT(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    for (Pattern pattern :
         {Pattern::Random, Pattern::Sorted, Pattern::Reverse, Pattern::OrganPipe, Pattern::FewUnique, Pattern::AllEqual})
    {
        for (size_t size : interestingSizes())
        {
            auto data = makeStringInput(size, pattern, rng);
            checkMatchesStdSort(data, std::less<>{});
            checkMatchesStdSort(data, std::greater<>{});
        }
    }
}

/// Indirect comparator over a permutation, the hot `IColumn::getPermutation` path. A reverse-sorted
/// permutation (ascending values sorted by a descending comparator) is the exact pattern that
/// reaches `::sort` directly for `ColumnDecimal<Decimal128>` and regressed before the pre-pass.
TEST(Sort, MatchesStdSortIndirectPermutation)
{
    std::mt19937_64 rng(0x1234); // NOLINT(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    for (Pattern pattern : {Pattern::Random, Pattern::Sorted, Pattern::Reverse, Pattern::FewUnique})
    {
        for (size_t size : interestingSizes())
        {
            auto values = makeInput(size, pattern, rng);

            auto run = [&](auto compare_values)
            {
                std::vector<uint64_t> permutation(size);
                std::iota(permutation.begin(), permutation.end(), uint64_t(0));

                auto indirect = [&](uint64_t lhs, uint64_t rhs) { return compare_values(values[lhs], values[rhs]); };

                std::vector<uint64_t> expected = permutation;
                std::stable_sort(expected.begin(), expected.end(), indirect);

                ::sort(permutation.begin(), permutation.end(), indirect);

                /// Compare by the values the permutation points at, since equal values may be
                /// permuted differently by an unstable sort.
                ASSERT_EQ(expected.size(), permutation.size());
                for (size_t i = 0; i < size; ++i)
                    ASSERT_EQ(values[expected[i]], values[permutation[i]]);
            };

            run([](int64_t a, int64_t b) { return a < b; });
            run([](int64_t a, int64_t b) { return a > b; });
        }
    }
}

TEST(Sort, TrySortMatchesStdSort)
{
    std::mt19937_64 rng(0x99); // NOLINT(cert-msc32-c,cert-msc51-cpp) — deterministic test fixture
    for (Pattern pattern : {Pattern::Random, Pattern::Sorted, Pattern::Reverse, Pattern::OrganPipe, Pattern::FewUnique})
    {
        for (size_t size : interestingSizes())
        {
            auto data = makeInput(size, pattern, rng);

            std::vector<int64_t> expected = data;
            std::sort(expected.begin(), expected.end());

            std::vector<int64_t> actual = data;
            /// `trySort` may decline to sort (returns false); whenever it claims success the result
            /// must be fully sorted.
            if (::trySort(actual.begin(), actual.end()))
                ASSERT_EQ(expected, actual);
        }
    }
}
