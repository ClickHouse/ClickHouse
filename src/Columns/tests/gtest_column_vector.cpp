#include <limits>
#include <type_traits>
#include <typeinfo>
#include <vector>
#include <Columns/ColumnsNumber.h>
#include <Common/randomSeed.h>
#include <Common/thread_local_rng.h>
#include <gtest/gtest.h>

using namespace DB;

static pcg64 rng(randomSeed());
static constexpr int error_code = 12345;
static constexpr size_t TEST_RUNS = 500;
static constexpr size_t MAX_ROWS = 10000;
static const std::vector<size_t> filter_ratios = {1, 2, 5, 11, 32, 64, 100, 1000};
static const size_t K = filter_ratios.size();

template <typename, typename = void >
struct HasUnderlyingType : std::false_type {};

template <typename T>
struct HasUnderlyingType<T, std::void_t<typename T::UnderlyingType>> : std::true_type {};

template <typename T>
static MutableColumnPtr createColumn(size_t n)
{
    auto column = ColumnVector<T>::create();
    auto & values = column->getData();

    for (size_t i = 0; i < n; ++i)
        if constexpr (HasUnderlyingType<T>::value)
            values.push_back(static_cast<typename T::UnderlyingType>(i));
        else
            values.push_back(static_cast<T>(i));

    return column;
}

bool checkFilter(const PaddedPODArray<UInt8> &flit, const IColumn & src, const IColumn & dst)
{
    size_t n = flit.size();
    size_t dst_size = dst.size();
    size_t j = 0;   /// index of dest
    for (size_t i = 0; i < n; ++i)
    {
        if (flit[i] != 0)
        {
            if ((dst_size <= j) || (src.compareAt(i, j, dst, 0) != 0))
                return false;
            j++;
        }
    }
    return dst_size == j;   /// filtered size check
}

template <typename T>
static void testFilter()
{
    auto test_case = [&](size_t rows, size_t filter_ratio)
    {
        auto vector_column = createColumn<T>(rows);
        PaddedPODArray<UInt8> flit(rows);
        for (size_t i = 0; i < rows; ++i)
            flit[i] = rng() % filter_ratio == 0;
        auto res_column = vector_column->filter(flit, -1);

        if (!checkFilter(flit, *vector_column, *res_column))
            throw Exception(error_code, "VectorColumn filter failure, type: {}", typeid(T).name());
    };

    try
    {
        for (size_t i = 0; i < TEST_RUNS; ++i)
        {
            size_t rows = rng() % MAX_ROWS + 1;
            size_t filter_ratio = filter_ratios[rng() % K];

            test_case(rows, filter_ratio);
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

TEST(ColumnVector, Filter)
{
    testFilter<UInt8>();
    testFilter<Int16>();
    testFilter<UInt32>();
    testFilter<Int64>();
    testFilter<UInt128>();
    testFilter<Int256>();
    testFilter<Float32>();
    testFilter<Float64>();
    testFilter<UUID>();
    testFilter<IPv4>();
    testFilter<IPv6>();
}

template <typename T>
static MutableColumnPtr createIndexColumn(size_t limit, size_t rows)
{
    auto column = ColumnVector<T>::create();
    auto & values = column->getData();
    auto max = std::numeric_limits<T>::max();
    limit = limit > max ? max : limit;

    for (size_t i = 0; i < rows; ++i)
    {
        T val = rng() % limit;
        values.push_back(val);
    }

    return column;
}

template <typename T, typename IndexType>
static void testIndex()
{
    static const std::vector<size_t> column_sizes = {64, 128, 196, 256, 512};

    auto test_case = [&](size_t rows, size_t index_rows, size_t limit)
    {
        auto vector_column = createColumn<T>(rows);
        auto index_column = createIndexColumn<IndexType>(rows, index_rows);
        auto res_column = vector_column->index(*index_column, limit);
        if (limit == 0)
            limit = index_column->size();

        /// check results
        if (limit != res_column->size())
            throw Exception(error_code, "ColumnVector index size not match to limit: {} {}", typeid(T).name(), typeid(IndexType).name());
        for (size_t i = 0; i < limit; ++i)
        {
            /// vector_column data is the same as index, so indexed column's value will equals to index_column.
            if (res_column->get64(i) != index_column->get64(i))
                throw Exception(error_code, "ColumnVector index fail: {} {}", typeid(T).name(), typeid(IndexType).name());
        }
    };

    try
    {
        test_case(0, 0, 0);   /// test for zero length index
        for (size_t i = 0; i < TEST_RUNS; ++i)
        {
            /// make sure rows distribute in (column_sizes[r-1], colulmn_sizes[r]]
            size_t row_idx = rng() % column_sizes.size();
            size_t row_base = row_idx > 0 ? column_sizes[row_idx - 1] : 0;
            size_t rows = row_base + (rng() % (column_sizes[row_idx] - row_base) + 1);
            size_t index_rows = rng() % MAX_ROWS + 1;

            test_case(rows, index_rows, 0);
            test_case(rows, index_rows, static_cast<size_t>(0.5 * index_rows));
        }
    }
    catch (const Exception & e)
    {
        FAIL() << e.displayText();
    }
}

TEST(ColumnVector, Index)
{
    testIndex<UInt8, UInt8>();
    testIndex<UInt16, UInt8>();
    testIndex<UInt16, UInt16>();
}
