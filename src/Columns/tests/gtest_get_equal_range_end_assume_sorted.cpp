#include <algorithm>
#include <cstdio>
#include <cstring>
#include <limits>
#include <set>
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

/// Builds a sorted float column: an optional run of -inf, the finite runs, an optional run of +inf,
/// and a run of NaNs at the front or at the back depending on `nan_first` (matching the sort order
/// for nan_direction_hint = -1 or 1 respectively).
template <typename T>
ColumnPtr makeSortedFloatWithNaN(
    const RunLengths & finite_runs, size_t nan_run, bool nan_first, size_t neg_inf_run = 0, size_t pos_inf_run = 0)
{
    auto col = ColumnVector<T>::create();
    auto & data = col->getData();
    auto push_repeated = [&](T value, size_t count)
    {
        for (size_t k = 0; k < count; ++k)
            data.push_back(value);
    };
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
    if (nan_first)
        push_repeated(std::numeric_limits<T>::quiet_NaN(), nan_run);
    push_repeated(-std::numeric_limits<T>::infinity(), neg_inf_run);
    push_finite();
    push_repeated(std::numeric_limits<T>::infinity(), pos_inf_run);
    if (!nan_first)
        push_repeated(std::numeric_limits<T>::quiet_NaN(), nan_run);
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

/// With `canonical_null_map` unset, the NULL rows get an arbitrary mix of non-zero bytes: any non-zero
/// byte in the null map means NULL, so such a column is still validly sorted.
ColumnPtr makeSortedNullable(const RunLengths & finite_runs, size_t null_run, bool null_first, bool canonical_null_map = true)
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
            nm.push_back(canonical_null_map ? static_cast<UInt8>(1) : static_cast<UInt8>(1 + k % 255));
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

/// Values of a sorted multi-column key: column 0 has runs of `leading_run` rows, and column k + 1 restarts at 0
/// in every run of column k and has runs of `sub_runs[k]` rows there (0 means unique values).
std::vector<std::vector<UInt64>> makeSortedKeyValues(size_t rows, size_t leading_run, const std::vector<size_t> & sub_runs)
{
    std::vector<std::vector<UInt64>> values(sub_runs.size() + 1, std::vector<UInt64>(rows));
    for (size_t row = 0; row < rows; ++row)
    {
        values[0][row] = row / leading_run;
        size_t offset = row % leading_run;
        for (size_t k = 0; k < sub_runs.size(); ++k)
        {
            if (sub_runs[k] == 0)
            {
                values[k + 1][row] = row;
                offset = 0;
            }
            else
            {
                values[k + 1][row] = offset / sub_runs[k];
                offset %= sub_runs[k];
            }
        }
    }
    return values;
}

enum class KeyType : uint8_t
{
    UInt32,
    UInt64,
    String,
    /// 0 is NULL (with arbitrary nested values and non-zero null map bytes), so it sorts first for hint -1.
    NullableString,
    /// 0 is NaN and 1 is -0.0 or +0.0 by row parity, so it sorts as the values for hint -1.
    Float64WithNaNAndZeros,
    /// Dictionary indices are in the reverse order of the values.
    LowCardinalityString,
};

String sortableString(UInt64 value)
{
    char buf[32];
    (void)snprintf(buf, sizeof(buf), "%08llu", static_cast<unsigned long long>(value));
    return buf;
}

ColumnPtr makeKeyColumn(KeyType type, const std::vector<UInt64> & values)
{
    switch (type)
    {
        case KeyType::UInt32:
        {
            auto col = ColumnUInt32::create();
            for (UInt64 v : values)
                col->getData().push_back(static_cast<UInt32>(v));
            return col;
        }
        case KeyType::UInt64:
        {
            auto col = ColumnUInt64::create();
            for (UInt64 v : values)
                col->getData().push_back(v);
            return col;
        }
        case KeyType::String:
        {
            auto col = ColumnString::create();
            for (UInt64 v : values)
                col->insert(Field(sortableString(v)));
            return col;
        }
        case KeyType::NullableString:
        {
            auto nested = ColumnString::create();
            auto null_map = ColumnUInt8::create();
            for (size_t row = 0; row < values.size(); ++row)
            {
                const bool is_null = values[row] == 0;
                nested->insert(Field(sortableString(is_null ? (row * 7919) % 1000 : values[row])));
                null_map->getData().push_back(is_null ? static_cast<UInt8>(1 + row % 255) : static_cast<UInt8>(0));
            }
            return ColumnNullable::create(std::move(nested), std::move(null_map));
        }
        case KeyType::Float64WithNaNAndZeros:
        {
            auto col = ColumnFloat64::create();
            for (size_t row = 0; row < values.size(); ++row)
            {
                Float64 x = static_cast<Float64>(values[row]);
                if (values[row] == 0)
                    x = std::numeric_limits<Float64>::quiet_NaN();
                else if (values[row] == 1)
                    x = row % 2 ? -0.0 : 0.0;
                col->getData().push_back(x);
            }
            return col;
        }
        case KeyType::LowCardinalityString:
        {
            auto type_lc = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
            auto builder = type_lc->createColumn();
            const UInt64 max_value = values.empty() ? 0 : *std::max_element(values.begin(), values.end());
            for (UInt64 v = max_value + 1; v > 0; --v)
                builder->insert(Field(sortableString(v - 1)));
            for (UInt64 v : values)
                builder->insert(Field(sortableString(v)));
            return builder->cut(max_value + 1, values.size());
        }
    }
    UNREACHABLE();
}

bool keyEquals(const ColumnRawPtrs & key, size_t lhs, size_t rhs, int hint)
{
    return std::all_of(key.begin(), key.end(), [&](const IColumn * col) { return col->compareAt(lhs, rhs, *col, hint) == 0; });
}

size_t oracleKeyRangeEnd(const ColumnRawPtrs & key, size_t begin, size_t end, int hint)
{
    if (begin >= end)
        return begin;
    size_t r = begin + 1;
    while (r < end && keyEquals(key, r, begin, hint))
        ++r;
    return r;
}

/// Checks the three multi-column overloads against `oracleKeyRangeEnd` for every `begin` and two ends, and adds
/// the lengths of the runs it sees to `run_lengths`.
void checkKeyAgainstOracle(const Columns & key_columns, int hint, const std::string & label, std::set<size_t> & run_lengths)
{
    const size_t n = key_columns.front()->size();
    ColumnRawPtrs key;
    for (const auto & col : key_columns)
        key.push_back(col.get());

    for (size_t row = 0; row + 1 < n; ++row)
    {
        int cmp = 0;
        for (size_t i = 0; i < key.size() && cmp == 0; ++i)
            cmp = key[i]->compareAt(row, row + 1, *key[i], hint);
        ASSERT_LE(cmp, 0) << label << ": the key is not sorted at row " << row;
    }

    /// The key columns in reverse order after an unsorted column, selected back by `positions`.
    auto unsorted = ColumnUInt64::create();
    for (size_t row = 0; row < n; ++row)
        unsorted->getData().push_back(row % 2);
    ColumnRawPtrs with_other_columns{unsorted.get()};
    std::vector<size_t> positions;
    for (size_t i = key.size(); i > 0; --i)
    {
        with_other_columns.push_back(key[i - 1]);
        positions.push_back(i);
    }

    SortDescription descr;
    for (size_t i = 0; i < key.size(); ++i)
        descr.emplace_back("k" + std::to_string(i), 1, hint);

    for (size_t end : {n / 2, n})
    {
        for (size_t begin = 0; begin <= end; ++begin)
        {
            const size_t want = oracleKeyRangeEnd(key, begin, end, hint);
            if (end == n && begin < end)
                run_lengths.insert(want - begin);

            ASSERT_EQ(getEqualRangeEndAssumeSorted(key, begin, end, hint), want) << label << ": columns begin=" << begin << " end=" << end;
            ASSERT_EQ(getEqualRangeEndAssumeSorted(with_other_columns, positions, begin, end, hint), want)
                << label << ": positions begin=" << begin << " end=" << end;
            ASSERT_EQ(getEqualRangeEndAssumeSorted(key, descr, begin, end), want) << label << ": descr begin=" << begin << " end=" << end;
        }
    }
}

void runKeyTests(const std::vector<KeyType> & types, int hint, const std::vector<size_t> & last_sub_runs, std::set<size_t> & run_lengths)
{
    const std::vector<size_t> leading_runs{1, 7, 8, 9, 600};
    const std::vector<size_t> sub_runs{1, 2, 7, 8, 9, 300, 0};

    for (size_t leading_run : leading_runs)
    {
        for (size_t sub_run : sub_runs)
        {
            for (size_t last_sub_run : last_sub_runs)
            {
                std::vector<size_t> key_sub_runs{sub_run};
                if (types.size() > 2)
                    key_sub_runs.push_back(last_sub_run);

                const size_t rows = std::max<size_t>(3 * leading_run, 40);
                const auto values = makeSortedKeyValues(rows, leading_run, key_sub_runs);
                Columns key_columns;
                for (size_t i = 0; i < types.size(); ++i)
                    key_columns.push_back(makeKeyColumn(types[i], values[i]));

                const std::string label = "types=" + std::to_string(static_cast<int>(types[0])) + "," + std::to_string(static_cast<int>(types[1]))
                    + " leading_run=" + std::to_string(leading_run) + " sub_runs=" + std::to_string(sub_run) + "," + std::to_string(last_sub_run);
                checkKeyAgainstOracle(key_columns, hint, label, run_lengths);
                if (::testing::Test::HasFatalFailure())
                    return;
            }
        }
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

TEST(SortedEqualRuns, ColumnVectorFloatsWithInfinities)
{
    /// Runs of -inf and +inf long enough to engage the galloping search.
    auto with_inf = makeSortedFloatWithNaN<Float64>({1, 3, 260, 1}, 0, false, 300, 300);
    checkAgainstOracle(*with_inf, 1, "vector<Float64> with infinities");

    /// All the special values together: -inf, finite, +inf and a NaN run at either end.
    auto nan_last = makeSortedFloatWithNaN<Float64>({2, 300}, 5, false, 17, 9);
    checkAgainstOracle(*nan_last, 1, "vector<Float64> -inf/+inf/NaN-last");

    auto nan_first = makeSortedFloatWithNaN<Float64>({2, 300}, 5, true, 17, 9);
    checkAgainstOracle(*nan_first, -1, "vector<Float64> NaN-first/-inf/+inf");

    auto f32 = makeSortedFloatWithNaN<Float32>({2, 300}, 4, false, 300, 3);
    checkAgainstOracle(*f32, 1, "vector<Float32> with infinities");

    auto only_inf = makeSortedFloatWithNaN<Float64>({}, 0, false, 300, 300);
    checkAgainstOracle(*only_inf, 1, "vector<Float64> only infinities");
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

TEST(SortedEqualRuns, ColumnNullableNonCanonicalNullMap)
{
    /// The null run is longer than 256 rows, so every non-zero byte value occurs in the null map and
    /// the run is long enough to engage the galloping search.
    auto null_last = makeSortedNullable({1, 3, 260, 1}, 300, false, false);
    checkAgainstOracle(*null_last, 1, "ColumnNullable non-canonical NULLs-last");

    auto null_first = makeSortedNullable({1, 3, 260, 1}, 300, true, false);
    checkAgainstOracle(*null_first, -1, "ColumnNullable non-canonical NULLs-first");

    auto all_null = makeSortedNullable({}, 300, false, false);
    checkAgainstOracle(*all_null, 1, "ColumnNullable non-canonical all-NULL");
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

namespace
{

/// Runs shorter than, equal to and longer than the linear probe of the whole-key search must all occur.
void runKeyTestsWithCoverage(const std::vector<KeyType> & types, int hint, const std::vector<size_t> & last_sub_runs)
{
    std::set<size_t> run_lengths;
    runKeyTests(types, hint, last_sub_runs, run_lengths);
    for (size_t len : {1, 2, 7, 8, 9, 300})
        EXPECT_TRUE(run_lengths.contains(len)) << "no run of length " << len;
}

}

TEST(SortedEqualRuns, MultiColumnKeyUInt64String)
{
    runKeyTestsWithCoverage({KeyType::UInt64, KeyType::String}, 1, {1});
}

TEST(SortedEqualRuns, MultiColumnKeyStringString)
{
    runKeyTestsWithCoverage({KeyType::String, KeyType::String}, 1, {1});
}

TEST(SortedEqualRuns, MultiColumnKeyNullableStringFloat64)
{
    runKeyTestsWithCoverage({KeyType::NullableString, KeyType::Float64WithNaNAndZeros}, -1, {1});
}

TEST(SortedEqualRuns, MultiColumnKeyLowCardinalityStringUInt32)
{
    runKeyTestsWithCoverage({KeyType::LowCardinalityString, KeyType::UInt32}, 1, {1});
}

TEST(SortedEqualRuns, MultiColumnKeyThreeColumns)
{
    runKeyTestsWithCoverage({KeyType::UInt64, KeyType::String, KeyType::UInt32}, 1, {1, 3, 8, 300});
}

TEST(SortedEqualRuns, MultiColumnHelperSingleAndNoColumns)
{
    for (const auto & p : makePatterns())
    {
        auto col = makeSortedString(p);
        const ColumnRawPtrs key{col.get()};
        const std::vector<size_t> positions{0};
        SortDescription descr;
        descr.emplace_back("s", 1, 1);

        const size_t n = col->size();
        for (size_t end : {n / 2, n})
        {
            for (size_t begin = 0; begin <= end; ++begin)
            {
                const size_t want = oracleRangeEnd(*col, begin, end, 1);
                ASSERT_EQ(getEqualRangeEndAssumeSorted(key, begin, end, 1), want) << "columns begin=" << begin << " end=" << end;
                ASSERT_EQ(getEqualRangeEndAssumeSorted(key, positions, begin, end, 1), want) << "positions begin=" << begin << " end=" << end;
                ASSERT_EQ(getEqualRangeEndAssumeSorted(key, descr, begin, end), want) << "descr begin=" << begin << " end=" << end;
            }
        }
    }

    /// Without key columns all rows have the same key.
    const ColumnRawPtrs no_key;
    EXPECT_EQ(getEqualRangeEndAssumeSorted(no_key, 3, 10, 1), 10u);
}
