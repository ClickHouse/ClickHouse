#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnConst.h>

using namespace DB;

namespace
{

DataTypePtr stringType()
{
    return std::make_shared<DataTypeString>();
}

DataTypePtr lcStringType()
{
    return std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
}

ColumnPtr constString(const String & value, size_t rows)
{
    auto nested = ColumnString::create();
    nested->insert(value);
    return ColumnConst::create(std::move(nested), rows);
}

ColumnPtr fullString(const std::vector<String> & values)
{
    auto col = ColumnString::create();
    for (const auto & v : values)
        col->insert(v);
    return col;
}

ColumnPtr lowCardinalityString(const std::vector<String> & values)
{
    auto col = lcStringType()->createColumn();
    for (const auto & v : values)
        col->insert(v);
    return col;
}

String resultAt(const ColumnPtr & column, size_t row)
{
    Field field;
    column->get(row, field);
    return field.safeGet<String>();
}

/// Build concat over the given argument types and return its (resolver-decided) result type and
/// the executable function. concat uses the default LowCardinality implementation.
FunctionBasePtr buildConcat(const ColumnsWithTypeAndName & build_args)
{
    auto resolver = FunctionFactory::instance().get("concat", getContext().context);
    return resolver->build(build_args);
}

}

/// Regression for the default LowCardinality implementation in
/// IExecutableFunction::executeWithoutSparseColumns: the single-dictionary fast path must be
/// skipped (and all LowCardinality columns materialized to full) whenever the runtime arguments
/// no longer match the invariant getReturnType() used to pick the LowCardinality result type
/// (at most one LowCardinality argument, every other argument constant). Otherwise it aborts in
/// checkFunctionArgumentSizes or replaceLowCardinalityColumnsByNestedAndGetDictionaryIndexes.
class LowCardinalityDefaultImpl : public ::testing::Test
{
protected:
    void SetUp() override
    {
        tryRegisterFunctions();
    }
};

/// A single LowCardinality argument together with a NON-constant ordinary argument, while the
/// declared result type is still LowCardinality. Reproduces the
/// "Expected the argument ... to have 0 rows, but it has 1" abort during header evaluation.
TEST_F(LowCardinalityDefaultImpl, SingleLowCardinalityWithNonConstOrdinaryHeaderEval)
{
    /// Build with (const String, LowCardinality(String)) so the result type is LowCardinality(String).
    ColumnsWithTypeAndName build_args{
        {constString("", 1), stringType(), "a"},
        {nullptr, lcStringType(), "b"},
    };
    auto function = buildConcat(build_args);
    ASSERT_TRUE(typeid_cast<const DataTypeLowCardinality *>(function->getResultType().get()));

    /// Runtime: arg0 lost its constness (non-const empty String, size 0); arg1 empty LowCardinality.
    /// input_rows_count = 0 emulates planner header evaluation.
    ColumnsWithTypeAndName exec_args{
        {ColumnString::create(), stringType(), "a"},
        {lcStringType()->createColumn(), lcStringType(), "b"},
    };
    ColumnPtr res;
    ASSERT_NO_THROW(res = function->execute(exec_args, function->getResultType(), 0, /*dry_run=*/true));
    EXPECT_EQ(res->size(), 0u);
    EXPECT_TRUE(typeid_cast<const ColumnLowCardinality *>(res.get()) || isColumnConst(*res));
}

/// Same invariant violation with real (non-zero) rows: a non-constant ordinary column alongside a
/// single LowCardinality column must produce results identical to executing on fully materialized
/// columns.
TEST_F(LowCardinalityDefaultImpl, SingleLowCardinalityWithNonConstOrdinaryRealRows)
{
    ColumnsWithTypeAndName build_args{
        {constString("", 1), stringType(), "a"},
        {nullptr, lcStringType(), "b"},
    };
    auto function = buildConcat(build_args);
    auto result_type = function->getResultType();
    ASSERT_TRUE(typeid_cast<const DataTypeLowCardinality *>(result_type.get()));

    ColumnsWithTypeAndName exec_args{
        {fullString({"x", "y", "z"}), stringType(), "a"},
        {lowCardinalityString({"1", "2", "3"}), lcStringType(), "b"},
    };
    ColumnPtr res;
    ASSERT_NO_THROW(res = function->execute(exec_args, result_type, 3, /*dry_run=*/false));
    ASSERT_EQ(res->size(), 3u);
    EXPECT_EQ(resultAt(res, 0), "x1");
    EXPECT_EQ(resultAt(res, 1), "y2");
    EXPECT_EQ(resultAt(res, 2), "z3");
}

/// Two LowCardinality columns reaching the path while the result type is LowCardinality (a constant
/// LowCardinality argument arriving as a non-constant column). Must not abort, and must produce
/// results identical to full execution.
TEST_F(LowCardinalityDefaultImpl, TwoLowCardinalityColumnsRealRows)
{
    /// Build with (const LowCardinality, LowCardinality) -> the constant LC is not counted as a full
    /// LowCardinality column, so the result type is LowCardinality(String).
    ColumnsWithTypeAndName build_args{
        {ColumnConst::create(lowCardinalityString({"p"}), 1), lcStringType(), "a"},
        {nullptr, lcStringType(), "b"},
    };
    auto function = buildConcat(build_args);
    auto result_type = function->getResultType();
    ASSERT_TRUE(typeid_cast<const DataTypeLowCardinality *>(result_type.get()));

    /// Runtime: both arguments are non-constant LowCardinality.
    ColumnsWithTypeAndName exec_args{
        {lowCardinalityString({"p", "p", "q"}), lcStringType(), "a"},
        {lowCardinalityString({"1", "2", "3"}), lcStringType(), "b"},
    };
    ColumnPtr res;
    ASSERT_NO_THROW(res = function->execute(exec_args, result_type, 3, /*dry_run=*/false));
    ASSERT_EQ(res->size(), 3u);
    EXPECT_EQ(resultAt(res, 0), "p1");
    EXPECT_EQ(resultAt(res, 1), "p2");
    EXPECT_EQ(resultAt(res, 2), "q3");
}

/// The fast path must still be taken (and produce correct results) in the normal case: a single
/// LowCardinality column with only constant other arguments.
TEST_F(LowCardinalityDefaultImpl, SingleLowCardinalityWithConstOrdinaryFastPath)
{
    ColumnsWithTypeAndName build_args{
        {constString("p", 1), stringType(), "a"},
        {nullptr, lcStringType(), "b"},
    };
    auto function = buildConcat(build_args);
    auto result_type = function->getResultType();
    ASSERT_TRUE(typeid_cast<const DataTypeLowCardinality *>(result_type.get()));

    ColumnsWithTypeAndName exec_args{
        {constString("p", 3), stringType(), "a"},
        {lowCardinalityString({"1", "2", "1"}), lcStringType(), "b"},
    };
    ColumnPtr res;
    ASSERT_NO_THROW(res = function->execute(exec_args, result_type, 3, /*dry_run=*/false));
    ASSERT_EQ(res->size(), 3u);
    EXPECT_EQ(resultAt(res, 0), "p1");
    EXPECT_EQ(resultAt(res, 1), "p2");
    EXPECT_EQ(resultAt(res, 2), "p1");
    /// Result must be a LowCardinality column (dictionary-encoded fast path).
    EXPECT_TRUE(typeid_cast<const ColumnLowCardinality *>(res.get()));
}

/// A LEADING constant argument on the full-materialization fallback path. With
/// (const String, const String, LowCardinality(String)) the result type is LowCardinality, but at
/// header evaluation arg0 stays a size-1 ColumnConst while arg1 lost its constness (empty non-const
/// String) and input_rows_count = 0. The fallback must take the row count from input_rows_count, not
/// from front() (the size-1 constant) which is only meaningful on the dictionary fast path;
/// otherwise checkFunctionArgumentSizes aborts "№2 ... to have 1 rows, but it has 0".
TEST_F(LowCardinalityDefaultImpl, LeadingConstWithNonConstOrdinaryHeaderEval)
{
    ColumnsWithTypeAndName build_args{
        {constString("", 1), stringType(), "a"},
        {constString("", 1), stringType(), "b"},
        {nullptr, lcStringType(), "c"},
    };
    auto function = buildConcat(build_args);
    ASSERT_TRUE(typeid_cast<const DataTypeLowCardinality *>(function->getResultType().get()));

    /// arg0 keeps a size-1 ColumnConst; arg1 lost its constness (empty non-const String, size 0);
    /// arg2 empty LowCardinality. input_rows_count = 0 emulates planner header evaluation.
    ColumnsWithTypeAndName exec_args{
        {constString("", 1), stringType(), "a"},
        {ColumnString::create(), stringType(), "b"},
        {lcStringType()->createColumn(), lcStringType(), "c"},
    };
    ColumnPtr res;
    ASSERT_NO_THROW(res = function->execute(exec_args, function->getResultType(), 0, /*dry_run=*/true));
    EXPECT_EQ(res->size(), 0u);
    EXPECT_TRUE(typeid_cast<const ColumnLowCardinality *>(res.get()) || isColumnConst(*res));
}

/// Same leading-constant fallback shape with real (non-zero) rows: a constant sized to
/// input_rows_count makes the row count happen to agree, so this guards that results stay correct
/// through the fallback path (not just that it no longer aborts).
TEST_F(LowCardinalityDefaultImpl, LeadingConstWithNonConstOrdinaryRealRows)
{
    ColumnsWithTypeAndName build_args{
        {constString("p", 1), stringType(), "a"},
        {constString("", 1), stringType(), "b"},
        {nullptr, lcStringType(), "c"},
    };
    auto function = buildConcat(build_args);
    auto result_type = function->getResultType();
    ASSERT_TRUE(typeid_cast<const DataTypeLowCardinality *>(result_type.get()));

    /// arg0 constant, arg1 non-constant ordinary, arg2 LowCardinality -> fallback path.
    ColumnsWithTypeAndName exec_args{
        {constString("p", 3), stringType(), "a"},
        {fullString({"a", "b", "c"}), stringType(), "b"},
        {lowCardinalityString({"1", "2", "3"}), lcStringType(), "c"},
    };
    ColumnPtr res;
    ASSERT_NO_THROW(res = function->execute(exec_args, result_type, 3, /*dry_run=*/false));
    ASSERT_EQ(res->size(), 3u);
    EXPECT_EQ(resultAt(res, 0), "pa1");
    EXPECT_EQ(resultAt(res, 1), "pb2");
    EXPECT_EQ(resultAt(res, 2), "pc3");
}
