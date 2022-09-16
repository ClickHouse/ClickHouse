#include <typeinfo>
#include <vector>
#include <Columns/ColumnsNumber.h>
#include <Common/randomSeed.h>
#include <gtest/gtest.h>


using namespace DB;

static pcg64 rng(randomSeed());
static constexpr int error_code = 12345;
static constexpr size_t TEST_RUNS = 500;
static constexpr size_t MAX_ROWS = 10000;
static const std::vector<size_t> filter_ratios = {1, 2, 5, 11, 32, 64, 100, 1000};
static const size_t K = filter_ratios.size();

template <typename T>
static MutableColumnPtr createColumn(size_t n)
{
    auto column = ColumnVector<T>::create();
    auto & values = column->getData();

    for (size_t i = 0; i < n; ++i)
    {
        values.push_back(i);
    }

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
}
