#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Core/SortCursor.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
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

/// Fast path for the common case (e.g. a plain sort with no JOIN): when the merge cannot
/// receive any `ColumnReplicated` input, `setMayHaveReplicatedColumns(false)` lets
/// `insertRow` / `insertRows` skip the per-row wrapping check. Regular sources must still
/// be inserted correctly and the destination must stay a regular column.
TEST(MergedDataReplicated, FastPathSkipsWrappingForRegularColumns)
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
    /// No source is replicated — the algorithm would enable the fast path.
    merged_data.setMayHaveReplicatedColumns(false);
    ASSERT_FALSE(merged_data.mayHaveReplicatedColumns());

    auto key_src = ColumnInt64::create();
    key_src->insertValue(2);
    auto val_src = ColumnInt64::create();
    val_src->insertValue(200);
    ColumnRawPtrs raw_columns_row = {key_src.get(), val_src.get()};
    ASSERT_NO_THROW(merged_data.insertRow(raw_columns_row, 0, 1));

    auto key_src2 = ColumnInt64::create();
    key_src2->insertValue(3);
    key_src2->insertValue(4);
    auto val_src2 = ColumnInt64::create();
    val_src2->insertValue(300);
    val_src2->insertValue(400);
    ColumnRawPtrs raw_columns_rows = {key_src2.get(), val_src2.get()};
    ASSERT_NO_THROW(merged_data.insertRows(raw_columns_rows, 0, 2, 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 3);
    /// Destination stays regular on the fast path.
    ASSERT_FALSE(result.getColumns()[1]->isReplicated());
    ASSERT_EQ(result.getColumns()[0]->getInt(0), 2);
    ASSERT_EQ(result.getColumns()[1]->getInt(0), 200);
    ASSERT_EQ(result.getColumns()[1]->getInt(2), 400);
}

/// Contract test: once the fast path is re-disabled (as the algorithm does in `consume` when a
/// late chunk brings a `ColumnReplicated` column), `insertRow` again wraps the destination so a
/// replicated source is consumed through the optimized path instead of throwing on type mismatch.
TEST(MergedDataReplicated, RestoringFlagReenablesWrapping)
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
    merged_data.setMayHaveReplicatedColumns(false);
    /// A replicated column arrives — the algorithm restores the flag before inserting these rows.
    merged_data.setMayHaveReplicatedColumns(true);

    auto key_src = ColumnInt64::create();
    key_src->insertValue(2);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(200);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    ColumnRawPtrs raw_columns = {key_src.get(), val_replicated.get()};
    ASSERT_NO_THROW(merged_data.insertRow(raw_columns, 0, 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    ASSERT_TRUE(result.getColumns()[1]->isReplicated());
    ASSERT_EQ(result.getColumns()[1]->getInt(0), 200);
}

/// End-to-end test that drives the ACTUAL `MergingSortedAlgorithm` transition this optimization
/// depends on, not just a manually toggled `MergedData` flag.
///
/// The fast path is enabled by `initialize` because none of the initial inputs are
/// `ColumnReplicated`. A late chunk then arrives for a source via `consume` carrying a
/// non-sort `ColumnReplicated` column (as from a JOIN with `enable_lazy_columns_replication = 1`).
/// `consume` must raise `may_have_replicated_columns` back to `true` BEFORE those rows are fed to
/// `MergedData::insertRow`; otherwise the fast path inserts a `ColumnReplicated` source into a
/// regular destination and hits the type-mismatch assertion.
///
/// Crucially this test never calls `setMayHaveReplicatedColumns` itself, so it fails if a later
/// refactor drops or reorders the `consume` flag update, whereas the `MergedData`-only tests
/// (which toggle the flag manually) would stay green.
///
/// The late replicated chunk carries TWO rows whose keys straddle the other source's remaining
/// key (20 < 50 < 60). That defeats the "current cursor is totally less than the next" direct
/// `insertChunk` fast-forward (which ignores the hint anyway), forcing the replicated rows through
/// the `insertRow` fast path this optimization actually guards.
TEST(MergedDataReplicated, MergingSortedAlgorithmRaisesHintOnLateReplicatedChunk)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));
    auto shared_header = std::make_shared<const Block>(header);

    SortDescription description;
    description.emplace_back(SortColumnDescription("key", 1, 1));

    MergingSortedAlgorithm algorithm(
        shared_header,
        /*num_inputs=*/ 2,
        description,
        /*max_block_size_=*/ 1000,
        /*max_block_size_bytes_=*/ 0,
        /*max_dynamic_subcolumns_=*/ {},
        SortingQueueStrategy::Default);

    /// Initial inputs are all regular (no `ColumnReplicated`), so `initialize` enables the fast path.
    IMergingAlgorithm::Inputs inputs(2);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(5);
        key_col->insertValue(50);
        val_col->insertValue(50);
        val_col->insertValue(500);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 2);
    }
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(10);
        val_col->insertValue(100);
        inputs[1].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }
    algorithm.initialize(std::move(inputs));

    /// Merge what is available; source 1 is exhausted first and its next chunk is requested via `consume`.
    IMergingAlgorithm::Status status = algorithm.merge();

    /// A late chunk arrives for source 1 with a `ColumnReplicated` non-sort column (as from a JOIN
    /// with `enable_lazy_columns_replication = 1`). If `consume` forgets to raise the hint, the
    /// still-enabled fast path in `insertRow` inserts this replicated source into the regular
    /// destination: an assertion failure in debug/sanitizer builds, a wrong value in release.
    IMergingAlgorithm::Input late_input;
    {
        auto key_col = ColumnInt64::create();
        key_col->insertValue(20);
        key_col->insertValue(60);
        auto val_nested = ColumnInt64::create();
        val_nested->insertValue(200);
        val_nested->insertValue(600);
        ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));
        late_input.chunk.setColumns(Columns{std::move(key_col), std::move(val_replicated)}, 2);
    }
    ASSERT_NO_THROW(algorithm.consume(late_input, /*source_num=*/ 1));

    /// Drain the merge and collect the merged output.
    std::vector<Int64> keys;
    std::vector<Int64> values;
    auto collect = [&](const Chunk & chunk)
    {
        for (size_t i = 0; i < chunk.getNumRows(); ++i)
        {
            keys.push_back(chunk.getColumns()[0]->getInt(i));
            values.push_back(chunk.getColumns()[1]->getInt(i));
        }
    };
    if (status.chunk && status.chunk.hasRows())
        collect(status.chunk);

    for (size_t guard = 0; guard < 100; ++guard)
    {
        IMergingAlgorithm::Status next = algorithm.merge();
        if (next.chunk && next.chunk.hasRows())
            collect(next.chunk);
        if (next.is_finished)
            break;
    }

    /// All rows must be present in fully sorted key order with correct values, including the two
    /// values that came from the late `ColumnReplicated` chunk (20 -> 200, 60 -> 600).
    ASSERT_EQ(keys.size(), 5u);
    EXPECT_EQ(keys, (std::vector<Int64>{5, 10, 20, 50, 60}));
    EXPECT_EQ(values, (std::vector<Int64>{50, 100, 200, 500, 600}));
}
