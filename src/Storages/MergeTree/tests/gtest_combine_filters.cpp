#include <gtest/gtest.h>
#include <Columns/ColumnVector.h>

// I know that inclusion of .cpp is not good at all
#include <Storages/MergeTree/MergeTreeRangeReader.cpp> // NOLINT

using namespace DB;

/* The combineFilters function from MergeTreeRangeReader.cpp could be optimized with Intel's AVX512VBMI2 intrinsic,
 * _mm512_mask_expandloadu_epi8. And this test is added to ensure that the vectorized code outputs the exact results
 * as the original scalar code when the required hardware feature is supported on the device.
 *
 * To avoid the contingency of the all-one/all-zero sequences, this test fills in the filters with alternating 1s and
 * 0s so that only the 4i-th (i is a non-negative integer) elements in the combined filter equals 1s and others are 0s.
 * For example, given the size of the first filter to be 11, the generated and the output filters are:
 *
 * first_filter:  [1 0 1 0 1 0 1 0 1 0 1]
 * second_filter: [1 0 1 0 1 0]
 * output_filter: [1 0 0 0 1 0 0 0 1 0 0]
 */
bool testCombineFilters(size_t size)
{
    auto generateFilterWithAlternatingOneAndZero = [](size_t len)->ColumnPtr
    {
        auto filter = ColumnUInt8::create(len, 0);
        auto & filter_data = filter->getData();

        for (size_t i = 0; i < len; i += 2)
            filter_data[i] = 1;

        return filter;
    };

    auto first_filter = generateFilterWithAlternatingOneAndZero(size);
    /// The count of 1s in the first_filter is floor((size + 1) / 2), which should be the size of the second_filter.
    auto second_filter = generateFilterWithAlternatingOneAndZero((size + 1) / 2);

    auto result = combineFilters(first_filter, second_filter);

    if (result->size() != size) return false;

    for (size_t i = 0; i < size; i++)
    {
        if (i % 4 == 0)
        {
            if (result->get64(i) != 1) return false;
        }
        else
        {
            if (result->get64(i) != 0) return false;
        }
    }

    return true;
}

TEST(MergeTree, CombineFilters)
{
    EXPECT_TRUE(testCombineFilters(1));
    EXPECT_TRUE(testCombineFilters(2));
    EXPECT_TRUE(testCombineFilters(63));
    EXPECT_TRUE(testCombineFilters(64));
    EXPECT_TRUE(testCombineFilters(65));
    EXPECT_TRUE(testCombineFilters(200));
    EXPECT_TRUE(testCombineFilters(201));
}
