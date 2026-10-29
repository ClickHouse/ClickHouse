#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <gtest/gtest.h>

using namespace DB;

/// Regression tests for STID 2508 family: type mismatch exception when `MergedData`
/// destination is a regular column but a late-arriving chunk brings in `ColumnReplicated`
/// non-sort columns.
///
/// This can happen when `initialize` set destination types based on the initial inputs
/// (none of which were `ColumnReplicated`), but a later chunk arrives via `consume` with
/// `ColumnReplicated` non-sort columns — for example, from a JOIN executed with
/// `enable_lazy_columns_replication = 1`. The sort-column-only materialization in the
/// merge algorithms' `consume` methods leaves non-sort columns untouched, so the
/// mismatch propagates into `insertRow` / `insertRows` / `insertChunk`.
///
/// The fix detects the mismatch in `MergedData` and WRAPS the destination in
/// `ColumnReplicated`, so `insertFrom` / `insertRangeFrom` consume both regular and
/// replicated sources through `ColumnReplicated`'s optimized path. This preserves the
/// lazy replication optimization instead of eagerly materializing the source.

TEST(MergedDataReplicated, InsertRowsReplicatedSourceRegularDestination)
{
    /// Set up a header with 2 columns: "key" (would be sort) and "value" (non-sort).
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));

    /// Initialize with input 0 having regular columns; input 1 is null (late-arriving).
    /// This means `MergedData` destination columns are regular (not `ColumnReplicated`).
    IMergingAlgorithm::Inputs inputs(2);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(1);
        val_col->insertValue(100);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }
    /// inputs[1] has no chunk — simulates a merge input that arrives later via `consume`.

    MergedData merged_data(false, 1000, 0, {});
    merged_data.initialize(header, inputs);

    /// Simulate `insertRows` with a `ColumnReplicated` value column.
    /// In the real bug scenario, this comes from a JOIN with `enable_lazy_columns_replication = 1`
    /// where `consume` didn't materialize non-sort `ColumnReplicated` columns.
    auto key_src = ColumnInt64::create();
    key_src->insertValue(2);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(200);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    ColumnRawPtrs raw_columns = {key_src.get(), val_replicated.get()};

    /// Before the fix, this would trigger:
    ///   `chassert((isConst() || isSparse() || isReplicated()) ? getDataType() == rhs.getDataType()
    ///            : typeid(*this) == typeid(rhs))`
    /// at `IColumn.h:862` because destination is regular `ColumnInt64` but source is `ColumnReplicated`.
    ASSERT_NO_THROW(merged_data.insertRows(raw_columns, 0, 1, 1));

    /// Verify the data was inserted correctly.
    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    const auto & result_key = assert_cast<const ColumnInt64 &>(*result.getColumns()[0]);
    ASSERT_EQ(result_key.getInt(0), 2);
    ASSERT_EQ(result.getColumns()[1]->getInt(0), 200);
}

TEST(MergedDataReplicated, InsertRowReplicatedSourceRegularDestination)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));

    /// Initialize with only regular columns.
    IMergingAlgorithm::Inputs inputs(1);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(1);
        val_col->insertValue(100);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }

    MergedData merged_data(false, 1000, 0, {});
    merged_data.initialize(header, inputs);

    /// Insert a single row with `ColumnReplicated` source.
    auto key_src = ColumnInt64::create();
    key_src->insertValue(2);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(200);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    ColumnRawPtrs raw_columns = {key_src.get(), val_replicated.get()};
    ASSERT_NO_THROW(merged_data.insertRow(raw_columns, 0, 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    const auto & result_val = *result.getColumns()[1];
    ASSERT_EQ(result_val.getInt(0), 200);
}

/// Verifies that the fix preserves the lazy replication optimization: when the
/// mismatch is detected, the destination is WRAPPED in `ColumnReplicated` rather
/// than materializing the source. Future inserts into the merged data then use
/// `ColumnReplicated::insertFrom`'s optimized path, which avoids copying unique
/// values that are already present.
TEST(MergedDataReplicated, InsertRowsWrapsDestinationPreservingOptimization)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));

    IMergingAlgorithm::Inputs inputs(1);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(1);
        val_col->insertValue(100);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }

    MergedData merged_data(false, 1000, 0, {});
    merged_data.initialize(header, inputs);

    auto key_src = ColumnInt64::create();
    key_src->insertValue(2);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(200);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    ColumnRawPtrs raw_columns = {key_src.get(), val_replicated.get()};
    merged_data.insertRows(raw_columns, 0, 1, 1);

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    /// After the mismatch is detected, the destination column should be `ColumnReplicated`
    /// (not a regular `ColumnInt64`). This confirms we preserved the optimization rather
    /// than materializing the source.
    ASSERT_TRUE(result.getColumns()[1]->isReplicated());
    ASSERT_EQ(result.getColumns()[1]->getInt(0), 200);
}

TEST(MergedDataReplicated, InsertChunkReplicatedSourceRegularDestination)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));

    /// Initialize with only regular columns — no `ColumnReplicated` seen.
    IMergingAlgorithm::Inputs inputs(2);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(1);
        val_col->insertValue(100);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }

    MergedData merged_data(false, 1000, 0, {});
    merged_data.initialize(header, inputs);

    /// Construct a chunk with `ColumnReplicated` value column.
    auto key_col = ColumnInt64::create();
    key_col->insertValue(3);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(300);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    Chunk chunk(Columns{std::move(key_col), std::move(val_replicated)}, 1);
    ASSERT_NO_THROW(merged_data.insertChunk(std::move(chunk), 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    /// With the fix, `insertChunk`'s simple branch just moves the `ColumnReplicated`
    /// chunk column into the destination. The destination becomes `ColumnReplicated`
    /// — the lazy replication optimization is preserved all the way through.
    ASSERT_TRUE(result.getColumns()[1]->isReplicated());
    ASSERT_EQ(result.getColumns()[1]->getInt(0), 300);
}

/// Regression test for the second code path reported by the automated PR review:
/// `insertChunk`'s `hasDynamicStructure` branch does `cloneEmpty` + `insertRangeFrom`.
/// Without the fix, when the chunk column is `ColumnReplicated(ColumnDynamic)`, the
/// empty destination `ColumnDynamic` receives `ColumnReplicated` via `insertRangeFrom`
/// and `ColumnDynamic::insertRangeFrom` does `assert_cast<const ColumnDynamic &>(src)`,
/// which fails in debug/sanitizer builds and is UB in release.
///
/// The fix wraps the empty destination in `ColumnReplicated` when the chunk is
/// `ColumnReplicated`, so `ColumnReplicated::insertRangeFrom` handles the source
/// through its optimized path and re-inserts values into the nested column with the
/// merged dynamic structure.
TEST(MergedDataReplicated, InsertChunkReplicatedDynamicSourceRegularDestination)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnDynamic::create(254), std::make_shared<DataTypeDynamic>(), "value"));

    /// Initialize with one regular input (no `ColumnReplicated`) — destination is `ColumnDynamic`.
    /// Input 1 is null (late-arriving), so `MergedData` won't see `ColumnReplicated` during init.
    IMergingAlgorithm::Inputs inputs(2);
    {
        auto key_col = ColumnInt64::create();
        key_col->insertValue(1);
        auto val_col = ColumnDynamic::create(254);
        val_col->insert(Field(100));
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }

    MergedData merged_data(false, 1000, 0, 254);
    merged_data.initialize(header, inputs);

    /// Construct a chunk where the Dynamic column is wrapped in `ColumnReplicated`.
    /// This simulates a late-arriving merge input from a JOIN with `enable_lazy_columns_replication = 1`.
    auto key_col = ColumnInt64::create();
    key_col->insertValue(3);
    auto val_dynamic = ColumnDynamic::create(254);
    val_dynamic->insert(Field(300));
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_dynamic)));

    Chunk chunk(Columns{std::move(key_col), std::move(val_replicated)}, 1);

    ASSERT_NO_THROW(merged_data.insertChunk(std::move(chunk), 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    /// The destination should be wrapped as `ColumnReplicated(ColumnDynamic)`,
    /// preserving the optimization.
    ASSERT_TRUE(result.getColumns()[1]->isReplicated());
}
