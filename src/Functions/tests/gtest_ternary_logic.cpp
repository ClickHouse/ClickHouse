#include <algorithm>
#include <cstring>
#include <vector>
#include <string>
#include <type_traits>
#include <gtest/gtest.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionsLogical.h>

// I know that inclusion of .cpp is not good at all
#include <Functions/FunctionsLogical.cpp> // NOLINT

using namespace DB;
using TernaryValues = std::vector<Ternary::ResultType>;

struct LinearCongruentialGenerator
{
    /// Constants from `man lrand48_r`.
    static constexpr UInt64 a = 0x5DEECE66D;
    static constexpr UInt64 c = 0xB;

    /// And this is from `head -c8 /dev/urandom | xxd -p`
    UInt64 current = 0x09826f4a081cee35ULL;

    UInt32 next()
    {
        current = current * a + c;
        return static_cast<UInt32>(current >> 16);
    }
};

void generateRandomTernaryValue(LinearCongruentialGenerator & gen, Ternary::ResultType * output, size_t size, double false_ratio, double null_ratio)
{
    /// The LinearCongruentialGenerator generates nonnegative integers uniformly distributed over the interval [0, 2^32).
    /// See https://linux.die.net/man/3/nrand48

    double false_percentile = false_ratio;
    double null_percentile = false_ratio + null_ratio;

    false_percentile = false_percentile > 1 ? 1 : false_percentile;
    null_percentile = null_percentile > 1 ? 1 : null_percentile;

    UInt32 false_threshold = static_cast<UInt32>(static_cast<double>(std::numeric_limits<UInt32>::max()) * false_percentile);
    UInt32 null_threshold = static_cast<UInt32>(static_cast<double>(std::numeric_limits<UInt32>::max()) * null_percentile);

    for (Ternary::ResultType * end = output + size; output != end; ++output)
    {
        UInt32 val = gen.next();
        *output = val < false_threshold ? Ternary::False : (val < null_threshold ? Ternary::Null : Ternary::True);
    }
}

template<typename T>
ColumnPtr createColumnNullable(const Ternary::ResultType * ternary_values, size_t size)
{
    auto nested_column = ColumnVector<T>::create(size);
    auto null_map = ColumnUInt8::create(size);
    auto & nested_column_data = nested_column->getData();
    auto & null_map_data = null_map->getData();

    for (size_t i = 0; i < size; ++i)
    {
        if (ternary_values[i] == Ternary::Null)
        {
            null_map_data[i] = 1;
            nested_column_data[i] = 0;
        }
        else if (ternary_values[i] == Ternary::True)
        {
            null_map_data[i] = 0;
            nested_column_data[i] = 100;
        }
        else
        {
            null_map_data[i] = 0;
            nested_column_data[i] = 0;
        }
    }

    return ColumnNullable::create(std::move(nested_column), std::move(null_map));
}

template<typename T>
ColumnPtr createColumnVector(const Ternary::ResultType * ternary_values, size_t size)
{
    auto column = ColumnVector<T>::create(size);
    auto & column_data = column->getData();

    for (size_t i = 0; i < size; ++i)
    {
        if (ternary_values[i] == Ternary::True)
        {
            column_data[i] = 100;
        }
        else
        {
            column_data[i] = 0;
        }
    }

    return column;
}

template<typename ColumnType, typename T>
ColumnPtr createRandomColumn(LinearCongruentialGenerator & gen, TernaryValues & ternary_values)
{
    size_t size = ternary_values.size();
    Ternary::ResultType * ternary_data = ternary_values.data();

    if constexpr (std::is_same_v<ColumnType, ColumnNullable>)
    {
        generateRandomTernaryValue(gen, ternary_data, size, 0.3, 0.7);
        return createColumnNullable<T>(ternary_data, size);
    }
    else if constexpr (std::is_same_v<ColumnType, ColumnVector<UInt8>>)
    {
        generateRandomTernaryValue(gen, ternary_data, size, 0.5, 0);
        return createColumnVector<T>(ternary_data, size);
    }
    else
    {
        auto nested_col = ColumnNothing::create(size);
        auto null_map = ColumnUInt8::create(size);

        memset(ternary_data, Ternary::Null, size);

        return ColumnNullable::create(std::move(nested_col), std::move(null_map));
    }
}

/* The truth table of ternary And and Or operations:
 * +-------+-------+---------+--------+
 * |   a   |   b   | a And b | a Or b |
 * +-------+-------+---------+--------+
 * | False | False | False   | False  |
 * | False | Null  | False   | Null   |
 * | False | True  | False   | True   |
 * | Null  | False | False   | Null   |
 * | Null  | Null  | Null    | Null   |
 * | Null  | True  | Null    | True   |
 * | True  | False | False   | True   |
 * | True  | Null  | Null    | True   |
 * | True  | True  | True    | True   |
 * +-------+-------+---------+--------+
 *
 * https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
 */
template <typename Op, typename T>
bool testTernaryLogicTruthTable()
{
    constexpr size_t size = 9;

    Ternary::ResultType col_a_ternary[] = {Ternary::False, Ternary::False, Ternary::False, Ternary::Null, Ternary::Null, Ternary::Null, Ternary::True, Ternary::True, Ternary::True};
    Ternary::ResultType col_b_ternary[] = {Ternary::False, Ternary::Null, Ternary::True, Ternary::False, Ternary::Null, Ternary::True,Ternary::False, Ternary::Null, Ternary::True};
    Ternary::ResultType and_expected_ternary[] = {Ternary::False, Ternary::False, Ternary::False, Ternary::False, Ternary::Null, Ternary::Null,Ternary::False, Ternary::Null, Ternary::True};
    Ternary::ResultType or_expected_ternary[] = {Ternary::False, Ternary::Null, Ternary::True, Ternary::Null, Ternary::Null, Ternary::True,Ternary::True, Ternary::True, Ternary::True};
    Ternary::ResultType * expected_ternary;


    if constexpr (std::is_same_v<Op, AndImpl>)
    {
        expected_ternary = and_expected_ternary;
    }
    else
    {
        expected_ternary = or_expected_ternary;
    }

    auto col_a = createColumnNullable<T>(col_a_ternary, size);
    auto col_b = createColumnNullable<T>(col_b_ternary, size);
    ColumnRawPtrs arguments = {col_a.get(), col_b.get()};

    auto col_res = ColumnUInt8::create(size);
    auto & col_res_data = col_res->getData();

    OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, col_res->getData(), false);

    for (size_t i = 0; i < size; ++i)
    {
        if (col_res_data[i] != expected_ternary[i]) return false;
    }

    return true;
}

template <typename Op, typename LeftColumn, typename RightColumn>
bool testTernaryLogicOfTwoColumns(size_t size)
{
    LinearCongruentialGenerator gen;

    TernaryValues left_column_ternary(size);
    TernaryValues right_column_ternary(size);
    TernaryValues expected_ternary(size);

    ColumnPtr left = createRandomColumn<LeftColumn, UInt8>(gen, left_column_ternary);
    ColumnPtr right = createRandomColumn<RightColumn, UInt8>(gen, right_column_ternary);

    for (size_t i = 0; i < size; ++i)
    {
        /// Given that False is less than Null and Null is less than True, the And operation can be implemented
        /// with std::min, and the Or operation can be implemented with std::max.
        if constexpr (std::is_same_v<Op, AndImpl>)
        {
            expected_ternary[i] = std::min(left_column_ternary[i], right_column_ternary[i]);
        }
        else
        {
            expected_ternary[i] = std::max(left_column_ternary[i], right_column_ternary[i]);
        }
    }

    ColumnRawPtrs arguments = {left.get(), right.get()};

    auto col_res = ColumnUInt8::create(size);
    auto & col_res_data = col_res->getData();

    OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, col_res->getData(), false);

    for (size_t i = 0; i < size; ++i)
    {
        if (col_res_data[i] != expected_ternary[i]) return false;
    }

    return true;
}

TEST(TernaryLogicTruthTable, NestedUInt8)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, UInt8>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, UInt8>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedUInt16)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, UInt16>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, UInt16>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedUInt32)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, UInt32>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, UInt32>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedUInt64)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, UInt64>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, UInt64>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedInt8)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, Int8>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, Int8>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedInt16)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, Int16>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, Int16>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedInt32)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, Int32>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, Int32>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedInt64)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, Int64>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, Int64>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedFloat32)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, Float32>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, Float32>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTruthTable, NestedFloat64)
{
    bool test_1 = testTernaryLogicTruthTable<AndImpl, Float64>();
    bool test_2 = testTernaryLogicTruthTable<OrImpl, Float64>();
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTwoColumns, TwoNullable)
{
    bool test_1 = testTernaryLogicOfTwoColumns<AndImpl, ColumnNullable, ColumnNullable>(100 /*size*/);
    bool test_2 = testTernaryLogicOfTwoColumns<OrImpl, ColumnNullable, ColumnNullable>(100 /*size*/);
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTwoColumns, TwoVector)
{
    bool test_1 = testTernaryLogicOfTwoColumns<AndImpl, ColumnUInt8, ColumnUInt8>(100 /*size*/);
    bool test_2 = testTernaryLogicOfTwoColumns<OrImpl, ColumnUInt8, ColumnUInt8>(100 /*size*/);
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTwoColumns, TwoNothing)
{
    bool test_1 = testTernaryLogicOfTwoColumns<AndImpl, ColumnNothing, ColumnNothing>(100 /*size*/);
    bool test_2 = testTernaryLogicOfTwoColumns<OrImpl, ColumnNothing, ColumnNothing>(100 /*size*/);
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTwoColumns, NullableVector)
{
    bool test_1 = testTernaryLogicOfTwoColumns<AndImpl, ColumnNullable, ColumnUInt8>(100 /*size*/);
    bool test_2 = testTernaryLogicOfTwoColumns<OrImpl, ColumnNullable, ColumnUInt8>(100 /*size*/);
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTwoColumns, NullableNothing)
{
    bool test_1 = testTernaryLogicOfTwoColumns<AndImpl, ColumnNullable, ColumnNothing>(100 /*size*/);
    bool test_2 = testTernaryLogicOfTwoColumns<OrImpl, ColumnNullable, ColumnNothing>(100 /*size*/);
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}

TEST(TernaryLogicTwoColumns, VectorNothing)
{
    bool test_1 = testTernaryLogicOfTwoColumns<AndImpl, ColumnUInt8, ColumnNothing>(100 /*size*/);
    bool test_2 = testTernaryLogicOfTwoColumns<OrImpl, ColumnUInt8, ColumnNothing>(100 /*size*/);
    ASSERT_EQ(test_1, true);
    ASSERT_EQ(test_2, true);
}
