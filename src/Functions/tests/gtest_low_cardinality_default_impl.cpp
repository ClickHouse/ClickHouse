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

}

/// Regression for the default LowCardinality implementation in
/// IExecutableFunction::executeWithoutSparseColumns. getReturnType() picks a LowCardinality result
/// type assuming the single-dictionary fast path applies (at most one full LowCardinality argument,
/// every other argument constant), but a constant argument can lose its constness at a pipeline
/// boundary and arrive as a non-constant column. Then the fast path must be skipped and all
/// arguments materialized to full, using input_rows_count for the row count. This is reproduced
/// deterministically at header evaluation (input_rows_count = 0), which used to abort in
/// checkFunctionArgumentSizes with "Expected the argument ... to have 0 rows, but it has 1".
TEST(LowCardinalityDefaultImpl, NonConstOrdinaryHeaderEval)
{
    tryRegisterFunctions();

    /// Build with (const String, LowCardinality(String)) so the result type is LowCardinality(String).
    ColumnsWithTypeAndName build_args{
        {constString("", 1), stringType(), "a"},
        {nullptr, lcStringType(), "b"},
    };
    auto function = FunctionFactory::instance().get("concat", getContext().context)->build(build_args);
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
}
