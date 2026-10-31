#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyBatchRows.h>

using namespace DB;

namespace
{

/// The walk never reads the columns, it only needs a non-empty argument list.
ColumnsWithTypeAndName oneArgument()
{
    return {ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")};
}

/// A call costs `fixed` bytes plus `per_row` for every row it carries.
WasmBatchMeasurer affineMeasurer(size_t fixed, size_t per_row)
{
    return [=](const ColumnsWithTypeAndName &, size_t, size_t length, const VectorWithMemoryTracking<size_t> &)
    { return fixed + length * per_row; };
}

}

TEST(WasmBatchRows, RowsFillTheBudget)
{
    EXPECT_EQ(chooseWasmBatchRows(oneArgument(), 0, 10000, 32768, 65536, affineMeasurer(1024, 16)), (32768 - 1024) / 16);
}

/// A fixed part over the soft budget makes the rows be budgeted on top of it.
TEST(WasmBatchRows, FixedPartOverBudgetIsUnboundedWithoutCeiling)
{
    EXPECT_EQ(chooseWasmBatchRows(oneArgument(), 0, 10000, 32768, std::nullopt, affineMeasurer(40960, 16)), 32768 / 16);
}

/// A fixed part between the soft budget and the memory ceiling: the rows on top of it must still
/// fit the ceiling, and must not be cut below what does fit.
TEST(WasmBatchRows, FixedPartOverBudgetStaysUnderCeiling)
{
    EXPECT_EQ(chooseWasmBatchRows(oneArgument(), 0, 10000, 32768, 65536, affineMeasurer(40960, 16)), (65536 - 40960) / 16);
}

/// Nothing fits next to a fixed part over the ceiling, and a batch is never below one row.
TEST(WasmBatchRows, FixedPartOverCeilingSendsOneRow)
{
    EXPECT_EQ(chooseWasmBatchRows(oneArgument(), 0, 10000, 32768, 65536, affineMeasurer(70000, 16)), 1);
}
