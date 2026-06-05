#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include <Columns/Collator.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <Core/SortCursor.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <base/types.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

using RunLengths = std::vector<size_t>;

size_t oracleRangeEnd(const IColumn & col, size_t begin, size_t end, int hint)
{
    if (begin >= end)
        return begin;
    size_t r = begin + 1;
    while (r < end && col.compareAt(r, begin, col, hint) == 0)
        ++r;
    return r;
}

void checkAgainstOracle(const IColumn & col, int hint, const std::string & label)
{
    const size_t n = col.size();

    for (size_t end : {n / 2, n})
    {
        for (size_t begin = 0; begin <= end; ++begin)
        {
            const size_t got = col.getEqualRangeEndAssumeSorted(begin, end, hint);
            const size_t want = oracleRangeEnd(col, begin, end, hint);
            ASSERT_EQ(got, want) << label << ": getEqualRangeEndAssumeSorted begin=" << begin << " end=" << end << " hint=" << hint;
        }
    }
}

std::vector<RunLengths> makePatterns()
{
    return {
        {},
        {1},
        {5},
        {600},
        RunLengths(600, 1),
        {1, 1, 1, 5, 2, 1, 300, 1, 1, 260, 1, 1},
        {255, 1},
        {256, 1},
        {257, 1},
        {1, 256},
        {300, 300},
    };
}

template <typename T>
ColumnPtr makeSortedVector(const RunLengths & runs)
{
    auto col = ColumnVector<T>::create();
    auto & data = col->getData();
    size_t value = 0;
    for (size_t rl : runs)
    {
        for (size_t k = 0; k < rl; ++k)
            data.push_back(static_cast<T>(value));
        ++value;
    }
    return col;
}

template <typename T>
ColumnPtr makeSortedFloatWithNaN(const RunLengths & finite_runs, size_t nan_run, bool nan_first)
{
    auto col = ColumnVector<T>::create();
    auto & data = col->getData();
    auto push_finite = [&]
    {
        size_t value = 0;
        for (size_t rl : finite_runs)
        {
            for (size_t k = 0; k < rl; ++k)
                data.push_back(static_cast<T>(value));
            ++value;
        }
    };
    auto push_nan = [&]
    {
        for (size_t k = 0; k < nan_run; ++k)
            data.push_back(std::numeric_limits<T>::quiet_NaN());
    };
    if (nan_first)
    {
        push_nan();
        push_finite();
    }
    else
    {
        push_finite();
        push_nan();
    }
    return col;
}

ColumnPtr makeSortedDecimal64(const RunLengths & runs, UInt32 scale)
{
    auto col = ColumnDecimal<Decimal64>::create(0, scale);
    auto & data = col->getData();
    size_t value = 0;
    for (size_t rl : runs)
    {
        for (size_t k = 0; k < rl; ++k)
            data.push_back(Decimal64(static_cast<Int64>(value)));
        ++value;
    }
    return col;
}

ColumnPtr makeSortedString(const RunLengths & runs)
{
    auto col = ColumnString::create();
    size_t value = 0;
    for (size_t rl : runs)
    {
        char buf[16];
        (void)snprintf(buf, sizeof(buf), "%08zu", value);
        const size_t len = strlen(buf);
        for (size_t k = 0; k < rl; ++k)
            col->insertData(buf, len);
        ++value;
    }
    return col;
}

ColumnPtr makeSortedNullable(const RunLengths & finite_runs, size_t null_run, bool null_first)
{
    auto nested = ColumnUInt32::create();
    auto null_map = ColumnUInt8::create();
    auto & nd = nested->getData();
    auto & nm = null_map->getData();
    auto push_finite = [&]
    {
        size_t value = 0;
        for (size_t rl : finite_runs)
        {
            for (size_t k = 0; k < rl; ++k)
            {
                nd.push_back(static_cast<UInt32>(value));
                nm.push_back(static_cast<UInt8>(0));
            }
            ++value;
        }
    };
    auto push_null = [&]
    {
        for (size_t k = 0; k < null_run; ++k)
        {
            nd.push_back(0);
            nm.push_back(static_cast<UInt8>(1));
        }
    };
    if (null_first)
    {
        push_null();
        push_finite();
    }
    else
    {
        push_finite();
        push_null();
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr makeSortedFixedString(const RunLengths & runs, size_t n)
{
    if (n < 8 && runs.size() > (size_t(1) << (8 * n)))
        return nullptr;

    auto col = ColumnFixedString::create(n);
    std::string buf(n, '\0');
    size_t value = 0;
    for (size_t rl : runs)
    {
        for (size_t b = 0; b < n && b < sizeof(value); ++b)
            buf[n - 1 - b] = static_cast<char>((value >> (8 * b)) & 0xFF);
        for (size_t k = 0; k < rl; ++k)
            col->insertData(buf.data(), n);
        ++value;
    }
    return col;
}

ColumnPtr makeSortedSparse(const RunLengths & runs)
{
    auto values = ColumnUInt64::create();
    auto offsets = ColumnUInt64::create();
    values->getData().push_back(0);
    size_t row = 0;
    size_t value = 0;
    for (size_t rl : runs)
    {
        for (size_t k = 0; k < rl; ++k)
        {
            if (value != 0)
            {
                values->getData().push_back(value);
                offsets->getData().push_back(row);
            }
            ++row;
        }
        ++value;
    }
    return ColumnSparse::create(std::move(values), std::move(offsets), row);
}

ColumnPtr makeSortedLowCardinality(const RunLengths & runs)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto col = type->createColumn();
    size_t value = 0;
    for (size_t rl : runs)
    {
        char buf[16];
        (void)snprintf(buf, sizeof(buf), "%08zu", value);
        Field f = String(buf);
        for (size_t k = 0; k < rl; ++k)
            col->insert(f);
        ++value;
    }
    return col;
}

template <typename T>
void runVectorTests(const char * name)
{
    for (const auto & p : makePatterns())
    {
        auto col = makeSortedVector<T>(p);
        checkAgainstOracle(*col, 1, std::string("vector<") + name + "> hint=1");
        checkAgainstOracle(*col, -1, std::string("vector<") + name + "> hint=-1");
    }
}

}

TEST(SortedEqualRuns, ColumnVectorIntegers)
{
    runVectorTests<UInt16>("UInt16");
    runVectorTests<UInt32>("UInt32");
    runVectorTests<UInt64>("UInt64");
    runVectorTests<Int32>("Int32");
    runVectorTests<Int64>("Int64");
    runVectorTests<Int128>("Int128");
}

TEST(SortedEqualRuns, ColumnVectorFloatsFinite)
{
    for (const auto & p : makePatterns())
    {
        auto c32 = makeSortedVector<Float32>(p);
        checkAgainstOracle(*c32, 1, "vector<Float32> finite");
        auto c64 = makeSortedVector<Float64>(p);
        checkAgainstOracle(*c64, 1, "vector<Float64> finite");
    }
}

TEST(SortedEqualRuns, ColumnVectorFloatsWithNaN)
{
    auto nan_last = makeSortedFloatWithNaN<Float64>({1, 3, 260, 1}, 5, false);
    checkAgainstOracle(*nan_last, 1, "vector<Float64> NaN-last");

    auto nan_first = makeSortedFloatWithNaN<Float64>({1, 3, 260, 1}, 5, true);
    checkAgainstOracle(*nan_first, -1, "vector<Float64> NaN-first");

    auto all_nan = makeSortedFloatWithNaN<Float64>({}, 300, false);
    checkAgainstOracle(*all_nan, 1, "vector<Float64> all-NaN");

    auto f32_nan_last = makeSortedFloatWithNaN<Float32>({2, 300}, 4, false);
    checkAgainstOracle(*f32_nan_last, 1, "vector<Float32> NaN-last");
}

TEST(SortedEqualRuns, ColumnDecimal)
{
    for (const auto & p : makePatterns())
    {
        auto col = makeSortedDecimal64(p, 3);
        checkAgainstOracle(*col, 1, "ColumnDecimal<Decimal64>");
    }
}

TEST(SortedEqualRuns, ColumnString)
{
    for (const auto & p : makePatterns())
    {
        auto col = makeSortedString(p);
        checkAgainstOracle(*col, 1, "ColumnString");
    }
}

TEST(SortedEqualRuns, ColumnLowCardinality)
{
    for (const auto & p : makePatterns())
    {
        auto col = makeSortedLowCardinality(p);
        checkAgainstOracle(*col, 1, "ColumnLowCardinality (value-ordered indices)");
    }

    {
        auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        auto builder = type->createColumn();
        builder->insert(Field(String("b")));
        builder->insert(Field(String("a")));
        for (size_t k = 0; k < 400; ++k)
            builder->insert(Field(String("a")));
        for (size_t k = 0; k < 400; ++k)
            builder->insert(Field(String("b")));
        builder->insert(Field(String("c")));
        auto sorted = builder->cut(2, builder->size() - 2);
        checkAgainstOracle(*sorted, 1, "ColumnLowCardinality (non-value-ordered indices, long runs)");
    }
}

TEST(SortedEqualRuns, ColumnFixedString)
{
    for (size_t n : {size_t(1), size_t(8), size_t(15), size_t(16), size_t(17), size_t(24), size_t(33)})
        for (const auto & p : makePatterns())
            if (auto col = makeSortedFixedString(p, n))
                checkAgainstOracle(*col, 1, "ColumnFixedString n=" + std::to_string(n));
}

TEST(SortedEqualRuns, ColumnSparse)
{
    for (const auto & p : makePatterns())
    {
        auto col = makeSortedSparse(p);
        ASSERT_TRUE(col->isSparse()) << "expected a ColumnSparse (must not densify)";
        checkAgainstOracle(*col, 1, "ColumnSparse (base default)");
    }
}

TEST(SortedEqualRuns, ColumnConstDefaultPath)
{
    auto inner = ColumnUInt32::create();
    inner->getData().push_back(42);
    for (size_t n : {size_t(1), size_t(5), size_t(300), size_t(1000)})
    {
        auto col = ColumnConst::create(inner->cloneResized(1), n);
        checkAgainstOracle(*col, 1, "ColumnConst");
    }
}

TEST(SortedEqualRuns, MultiColumnHelperIgnoresCollation)
{
    auto col = ColumnString::create();
    col->insertData("A", 1);
    col->insertData("a", 1);
    col->insertData("b", 1);

    auto collator = std::make_shared<Collator>("en-u-ks-level2");

    ASSERT_EQ(col->compareAtWithCollation(0, 1, *col, 1, *collator), 0);
    ASSERT_NE(col->compareAt(0, 1, *col, 1), 0);

    SortDescription descr;
    descr.emplace_back("s", 1, 1, collator);
    const ColumnRawPtrs cols{col.get()};

    EXPECT_EQ(getEqualRangeEndAssumeSorted(cols, descr, 0, col->size()), 1u);
}

TEST(SortedEqualRuns, ColumnNullableDefaultPath)
{
    auto null_last = makeSortedNullable({1, 3, 260, 1}, 5, false);
    checkAgainstOracle(*null_last, 1, "ColumnNullable NULLs-last");

    auto null_first = makeSortedNullable({1, 3, 260, 1}, 5, true);
    checkAgainstOracle(*null_first, -1, "ColumnNullable NULLs-first");

    auto all_null = makeSortedNullable({}, 300, false);
    checkAgainstOracle(*all_null, 1, "ColumnNullable all-NULL");

    for (const auto & p : makePatterns())
    {
        auto col = makeSortedNullable(p, 0, false);
        checkAgainstOracle(*col, 1, "ColumnNullable no-NULL");
    }
}
