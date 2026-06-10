#include <gtest/gtest.h>
#include <Columns/ColumnVector.h>

// I know that inclusion of .cpp is not good at all
#include <Storages/MergeTree/MergeTreeRangeReader.cpp> // NOLINT

using namespace DB;

/* The combineFilters function from MergeTreeRangeReader.cpp could be optimized with Intel's AVX512VBMI2 intrinsic,
 * _mm512_mask_expandloadu_epi8. And these tests are added to ensure that the vectorized code outputs the exact results
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

    if (result->size() != size)
    {
        return false;
    }

    for (size_t i = 0; i < size; i++)
    {
        if (i % 4 == 0)
        {
            if (result->get64(i) != 1)
            {
                return false;
            }
        }
        else
        {
            if (result->get64(i) != 0)
            {
                return false;
            }
        }
    }

    return true;
}

/* This test is to further test DB::combineFilters by combining two UInt8 columns. Given the implementation of
 * DB::combineFilters, the non-zero values in the first column are contiguously replaced with the elements in the
 * second column. And to validate the first column with arbitrary intervals, this test constructs its values in
 * the following manner: the count of 0s between two consecutive 1s increases in step of 1. An example column
 * with the size of 16 looks like:
 * [1 1 0 1 0 0 1 0 0 0 1 0 0 0 0 1]
 *
 * The second column contains the consecutively incremented UInt8 integers between 0x00 and 0xFF, and when the overflow
 * occurs, the value would reset to 0x00 and increment again.
 */
bool testCombineColumns(size_t size)
{
    auto generateFirstColumn = [] (size_t len, size_t & non_zero_count)->ColumnPtr
    {
        auto column = ColumnUInt8::create(len, 0);
        auto & column_data = column->getData();

        non_zero_count = 0;
        for (size_t i = 0; i < len; non_zero_count++, i += non_zero_count)
        {
            column_data[i] = 1;
        }

        return column;
    };

    auto generateSecondColumn = [] (size_t len)->ColumnPtr
    {
        auto column = ColumnUInt8::create(len, 0);
        auto & column_data = column->getData();

        for (size_t i = 0; i < len; i++)
        {
            column_data[i] = static_cast<UInt8>(i);
        }

        return column;
    };

    size_t non_zero_count = 0;
    auto first_column = generateFirstColumn(size, non_zero_count);
    const auto & first_column_data = typeid_cast<const ColumnUInt8 *>(first_column.get())->getData();

    /// The count of non-zero values in the first column should be the size of the second column.
    auto second_column = generateSecondColumn(non_zero_count);

    auto result = combineFilters(first_column, second_column);
    const auto & result_data = typeid_cast<const ColumnUInt8 *>(result.get())->getData();

    if (result->size() != size) return false;

    UInt8 expected = 0;
    for (size_t i = 0; i < size; ++i)
    {
        if (first_column_data[i])
        {
            if (result_data[i] != expected)
            {
                return false;
            }
            /// Integer overflow is speculated during the integer increments. It is the expected behavior.
            expected++;
        }
        else
        {
            if (result_data[i] != 0)
            {
                return false;
            }
        }
    }

    return true;
}

/* To ensure the vectorized DB::andFilters works as its scalar implementation, this test validates the AND (&&)
 * of any combinations of the UInt8 values.
 */
bool testAndFilters(size_t size)
{
    auto generateFastIncrementColumn = [](size_t len)->ColumnPtr
    {
        auto filter = ColumnUInt8::create(len);
        auto & filter_data = filter->getData();

        for (size_t i = 0; i < len; ++i)
            filter_data[i] = static_cast<UInt8>(i & 0xFF);

        return filter;
    };

    auto generateSlowIncrementColumn = [](size_t len)->ColumnPtr
    {
        auto filter = ColumnUInt8::create(len);
        auto & filter_data = filter->getData();

        for (size_t i = 0; i < len; ++i)
            filter_data[i] = static_cast<UInt8>((i >> 8) & 0xFF);

        return filter;
    };

    auto first_filter = generateFastIncrementColumn(size);
    auto second_filter = generateSlowIncrementColumn(size);

    auto result = andFilters(first_filter, second_filter);

    const auto & first_filter_data = typeid_cast<const ColumnUInt8 *>(first_filter.get())->getData();
    const auto & second_filter_data = typeid_cast<const ColumnUInt8 *>(second_filter.get())->getData();
    const auto & result_data = typeid_cast<const ColumnUInt8 *>(result.get())->getData();

    if (result->size() != size)
    {
        return false;
    }

    for (size_t i = 0; i < size; i++)
    {
        UInt8 expected = first_filter_data[i] && second_filter_data[i];
        if (result_data[i] != expected)
            return false;
    }

    return true;
}

TEST(MergeTree, CombineFilters)
{
    /// Tests with only 0/1 and fixed intervals.
    EXPECT_TRUE(testCombineFilters(1));
    EXPECT_TRUE(testCombineFilters(2));
    EXPECT_TRUE(testCombineFilters(63));
    EXPECT_TRUE(testCombineFilters(64));
    EXPECT_TRUE(testCombineFilters(65));
    EXPECT_TRUE(testCombineFilters(200));
    EXPECT_TRUE(testCombineFilters(201));
    EXPECT_TRUE(testCombineFilters(300));
    /// Extended tests: combination of two UInt8 columns.
    EXPECT_TRUE(testCombineColumns(1));
    EXPECT_TRUE(testCombineColumns(2));
    EXPECT_TRUE(testCombineColumns(63));
    EXPECT_TRUE(testCombineColumns(64));
    EXPECT_TRUE(testCombineColumns(200));
    EXPECT_TRUE(testCombineColumns(201));
    EXPECT_TRUE(testCombineColumns(2000));
    EXPECT_TRUE(testCombineColumns(200000));
}

TEST(MergeTree, AndFilters)
{
    EXPECT_TRUE(testAndFilters(1));
    EXPECT_TRUE(testAndFilters(2));
    EXPECT_TRUE(testAndFilters(15));
    EXPECT_TRUE(testAndFilters(16));
    EXPECT_TRUE(testAndFilters(200));
    EXPECT_TRUE(testAndFilters(201));
    EXPECT_TRUE(testAndFilters(2000));
    EXPECT_TRUE(testAndFilters(65535));
    EXPECT_TRUE(testAndFilters(65536));
    EXPECT_TRUE(testAndFilters(65537));
    EXPECT_TRUE(testAndFilters(200000));
}
