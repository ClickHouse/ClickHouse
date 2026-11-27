#include <algorithm>
#include <iterator>
#include <list>
#include <random>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <base/inplaceMergeWithBuffer.h>
#include <Common/PODArray.h>

/// `inplaceMergeWithOtherBuffer` and `inplaceMergeWithTempBuffer` are compared with `std::merge` and `std::inplace_merge`
/// on random sorted ranges with many equal keys, including empty ranges.

namespace
{

/// The second member is a tag showing where an element came from, so the order of elements with equal keys is checked too.
using Element = std::pair<int, int>;
using Elements = std::vector<Element>;

constexpr auto less_by_key = [](const Element & lhs, const Element & rhs) { return lhs.first < rhs.first; };

constexpr size_t num_iterations = 10000;
constexpr size_t max_range_size = 20;

Elements makeSortedRange(std::mt19937 & rng, int first_tag)
{
    Elements elements(rng() % (max_range_size + 1));
    for (auto & element : elements)
        element.first = static_cast<int>(rng() % 30);
    std::sort(elements.begin(), elements.end());
    for (size_t i = 0; i < elements.size(); ++i)
        elements[i].second = first_tag + static_cast<int>(i);
    return elements;
}

}

TEST(InplaceMerge, WithOtherBufferMatchesStdMerge)
{
    std::mt19937 rng(42); // NOLINT(bugprone-random-generator-seed,cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    for (size_t iteration = 0; iteration < num_iterations; ++iteration)
    {
        const Elements left = makeSortedRange(rng, /* first_tag = */ 0);
        const Elements right = makeSortedRange(rng, /* first_tag = */ 1000);

        Elements expected;
        std::merge(left.begin(), left.end(), right.begin(), right.end(), std::back_inserter(expected), less_by_key);

        Elements grown = left;
        grown.resize(left.size() + right.size());
        inplaceMergeWithOtherBuffer(
            grown.data(), grown.data() + left.size(), grown.data() + grown.size(), right.data(), right.data() + right.size(), less_by_key);
        ASSERT_EQ(grown, expected);

        /// Iterators without random access.
        std::list<Element> grown_list(left.begin(), left.end());
        grown_list.resize(left.size() + right.size());
        const std::list<Element> other_list(right.begin(), right.end());
        inplaceMergeWithOtherBuffer(
            grown_list.begin(), std::next(grown_list.begin(), left.size()), grown_list.end(), other_list.begin(), other_list.end(), less_by_key);
        ASSERT_EQ(Elements(grown_list.begin(), grown_list.end()), expected);
    }
}

TEST(InplaceMerge, WithTempBufferMatchesStdInplaceMerge)
{
    std::mt19937 rng(42); // NOLINT(bugprone-random-generator-seed,cert-msc51-cpp,cert-msc32-c): deterministic seed for reproducible test failures
    /// Each buffer is shared by all the merges.
    DB::PODArray<Element> pod_array_buffer;
    Elements vector_buffer;
    for (size_t iteration = 0; iteration < num_iterations; ++iteration)
    {
        const Elements left = makeSortedRange(rng, /* first_tag = */ 0);
        const Elements right = makeSortedRange(rng, /* first_tag = */ 1000);
        Elements adjacent = left;
        adjacent.insert(adjacent.end(), right.begin(), right.end());

        Elements expected = adjacent;
        std::inplace_merge(expected.begin(), expected.begin() + left.size(), expected.end(), less_by_key);

        Elements merged = adjacent;
        inplaceMergeWithTempBuffer(merged.data(), merged.data() + left.size(), merged.data() + merged.size(), pod_array_buffer, less_by_key);
        ASSERT_EQ(merged, expected);

        /// Iterators without random access.
        std::list<Element> merged_list(adjacent.begin(), adjacent.end());
        inplaceMergeWithTempBuffer(merged_list.begin(), std::next(merged_list.begin(), left.size()), merged_list.end(), vector_buffer, less_by_key);
        ASSERT_EQ(Elements(merged_list.begin(), merged_list.end()), expected);
    }
}

TEST(InplaceMerge, DefaultComparison)
{
    std::vector<int> values = {1, 4, 7};
    const std::vector<int> other = {2, 9};
    const size_t old_values_size = values.size();
    values.resize(old_values_size + other.size());
    inplaceMergeWithOtherBuffer(values.begin(), values.begin() + old_values_size, values.end(), other.begin(), other.end());
    EXPECT_EQ(values, (std::vector<int>{1, 2, 4, 7, 9}));

    std::vector<int> adjacent = {5, 6, 7, 8, 1, 2};
    std::vector<int> temp_buffer;
    inplaceMergeWithTempBuffer(adjacent.begin(), adjacent.begin() + 4, adjacent.end(), temp_buffer);
    EXPECT_EQ(adjacent, (std::vector<int>{1, 2, 5, 6, 7, 8}));
}

TEST(InplaceMerge, TempBufferIsGrownAndNeverShrunk)
{
    std::vector<int> temp_buffer;

    /// The shorter range has 2 elements.
    std::vector<int> values = {5, 6, 7, 8, 1, 2};
    inplaceMergeWithTempBuffer(values.begin(), values.begin() + 4, values.end(), temp_buffer);
    EXPECT_EQ(temp_buffer.size(), 2);

    /// The shorter range has 3 elements, so the buffer is grown.
    values = {1, 5, 9, 2, 3, 4};
    inplaceMergeWithTempBuffer(values.begin(), values.begin() + 3, values.end(), temp_buffer);
    EXPECT_EQ(values, (std::vector<int>{1, 2, 3, 4, 5, 9}));
    EXPECT_EQ(temp_buffer.size(), 3);

    /// The shorter range has 1 element, and the buffer stays as it is.
    values = {2, 3, 4, 1};
    inplaceMergeWithTempBuffer(values.begin(), values.begin() + 3, values.end(), temp_buffer);
    EXPECT_EQ(values, (std::vector<int>{1, 2, 3, 4}));
    EXPECT_EQ(temp_buffer.size(), 3);
}
