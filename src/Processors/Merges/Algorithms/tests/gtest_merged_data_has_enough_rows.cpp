#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnObject.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Common/assert_cast.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

using namespace DB;

/// Regression tests for issue #113475: a merge spun forever at 100% CPU when
/// `merge_max_block_size_bytes` was below the `byteSize()` of an *empty* column.

namespace
{

constexpr UInt64 MAX_BLOCK_SIZE = 256;
constexpr UInt64 MAX_BLOCK_SIZE_BYTES = 8;
constexpr UInt64 LARGE_MAX_BLOCK_SIZE_BYTES = 1024 * 1024;

/// An empty `LowCardinality` column still carries its dictionary, so it reports a
/// non-zero `byteSize()` at zero rows. That is what lets the byte limit be met
/// before a single row has been merged.
Block makeHeader()
{
    auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "a"));
    header.insert(ColumnWithTypeAndName(lc_type->createColumn(), lc_type, "b"));
    return header;
}

IMergingAlgorithm::Inputs makeInputs(const Block & header)
{
    MutableColumns source = header.cloneEmptyColumns();
    source[0]->insert(Field(UInt64(1)));
    source[1]->insert(Field(String("")));

    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk.setColumns(std::move(source), 1);
    return inputs;
}

size_t totalByteSize(const MutableColumns & columns)
{
    size_t bytes = 0;
    for (const auto & column : columns)
        bytes += column->byteSize();
    return bytes;
}

constexpr UInt64 UNLIMITED_BLOCK_SIZE = 1'000'000;
constexpr UInt64 DEFAULT_MAX_BLOCK_SIZE_BYTES = 10 * 1024 * 1024;

/// One input per part, as in a merge.
IMergingAlgorithm::Inputs makeParts(std::vector<Columns> parts)
{
    IMergingAlgorithm::Inputs inputs(parts.size());
    for (size_t i = 0; i != parts.size(); ++i)
    {
        const size_t rows = parts[i].front()->size();
        inputs[i].chunk.setColumns(std::move(parts[i]), rows);
    }
    return inputs;
}

ColumnRawPtrs rawColumns(const IMergingAlgorithm::Input & input)
{
    ColumnRawPtrs raw_columns;
    for (const auto & column : input.chunk.getColumns())
        raw_columns.push_back(column.get());
    return raw_columns;
}

/// How many rows of part `part` the batch queue inserts at once in a merge of all `parts`.
size_t batchRows(const Block & header, const IMergingAlgorithm::Inputs & parts, size_t part, UInt64 max_block_size_bytes)
{
    MergedData merged_data(false, UNLIMITED_BLOCK_SIZE, max_block_size_bytes, {});
    merged_data.initialize(header, parts);
    const size_t rows = parts[part].chunk.getNumRows();
    return merged_data.rowsToInsertBeforeFlush(rawColumns(parts[part]), 0, rows, rows);
}

/// How many of the `source` rows are inserted as one batch before the byte limit flushes the block.
size_t batchRowsBeforeFlush(const Block & header, MutableColumns source)
{
    const size_t rows = source.front()->size();
    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk.setColumns(std::move(source), rows);
    return batchRows(header, inputs, 0, LARGE_MAX_BLOCK_SIZE_BYTES);
}

/// After how many rows of part `part`, inserted one by one, the byte limit flushes the block.
size_t rowByRowRows(const Block & header, const IMergingAlgorithm::Inputs & parts, size_t part, UInt64 max_block_size_bytes)
{
    MergedData merged_data(false, UNLIMITED_BLOCK_SIZE, max_block_size_bytes, {});
    merged_data.initialize(header, parts);
    const size_t rows = parts[part].chunk.getNumRows();
    const auto raw_columns = rawColumns(parts[part]);
    for (size_t row = 0; row != rows; ++row)
    {
        merged_data.insertRow(raw_columns, row, rows);
        if (merged_data.hasEnoughRows())
            return row + 1;
    }
    return rows;
}

/// How much inserting all rows of part `part` at once grows the merged block, and the sum of their `byteSizeAt`.
std::pair<size_t, size_t> growthAndSourceBytes(const Block & header, const IMergingAlgorithm::Inputs & parts, size_t part)
{
    MergedData merged_data(false, UNLIMITED_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE_BYTES, {});
    merged_data.initialize(header, parts);
    const size_t empty_bytes = merged_data.pull().bytes();

    const size_t rows = parts[part].chunk.getNumRows();
    const auto raw_columns = rawColumns(parts[part]);
    merged_data.insertRows(raw_columns, 0, rows, rows);

    size_t source_bytes = 0;
    for (const auto * column : raw_columns)
        for (size_t row = 0; row != rows; ++row)
            source_bytes += column->byteSizeAt(row);
    return {merged_data.pull().bytes() - empty_bytes, source_bytes};
}

Block headerOf(const String & type_name)
{
    auto type = DataTypeFactory::instance().get(type_name);
    return Block{ColumnWithTypeAndName(type->createColumn(), type, "value")};
}

Columns makeColumn(const String & type_name, const std::vector<Field> & values)
{
    auto column = DataTypeFactory::instance().get(type_name)->createColumn();
    for (const auto & value : values)
        column->insert(value);
    return {std::move(column)};
}

/// An array of `size` elements, all but the first `NULL`: 1 byte each in shared data, 9 bytes each in a regular column.
Field mostlyNullArray(size_t size)
{
    Array array(size);
    array[0] = Int64(1);
    return array;
}

}

/// Anti-vacuity precondition: if an empty accumulator ever stopped meeting the byte
/// limit, the assertion in the next test would hold without the fix and test nothing.
TEST(MergedDataHasEnoughRows, EmptyAccumulatorAlreadyMeetsTheByteLimit)
{
    EXPECT_GE(totalByteSize(makeHeader().cloneEmptyColumns()), MAX_BLOCK_SIZE_BYTES);
}

TEST(MergedDataHasEnoughRows, ZeroRowsIsNeverEnoughBecauseOfTheByteLimit)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, MAX_BLOCK_SIZE_BYTES, {});
    merged_data.initialize(header, inputs);

    /// Before the fix the byte branch returned true here, so the algorithm pulled a
    /// 0-row chunk without advancing its queue and was re-entered forever.
    EXPECT_EQ(merged_data.mergedRows(), 0u);
    EXPECT_FALSE(merged_data.hasEnoughRows());
}

/// Positive control: the byte limit must still fire once a row is present, so the fix
/// constrains the predicate at zero rows instead of disabling the limit.
TEST(MergedDataHasEnoughRows, ByteLimitIsStillEnforcedAfterOneRow)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, MAX_BLOCK_SIZE_BYTES, {});
    merged_data.initialize(header, inputs);

    const auto & source_columns = inputs[0].chunk.getColumns();
    ColumnRawPtrs raw_columns{source_columns[0].get(), source_columns[1].get()};
    merged_data.insertRow(raw_columns, 0, 1);

    EXPECT_EQ(merged_data.mergedRows(), 1u);
    EXPECT_TRUE(merged_data.hasEnoughRows());
}

/// The byte limit must stay a real comparison rather than a proxy for "some row exists":
/// one row well below the threshold is not enough.
TEST(MergedDataHasEnoughRows, OneRowBelowTheByteLimitIsNotEnough)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, LARGE_MAX_BLOCK_SIZE_BYTES, {});
    merged_data.initialize(header, inputs);

    const auto & source_columns = inputs[0].chunk.getColumns();
    ColumnRawPtrs raw_columns{source_columns[0].get(), source_columns[1].get()};
    merged_data.insertRow(raw_columns, 0, 1);

    EXPECT_EQ(merged_data.mergedRows(), 1u);
    EXPECT_FALSE(merged_data.hasEnoughRows());
}

/// Control: a zero byte limit disables the branch, so it cannot report enough rows.
TEST(MergedDataHasEnoughRows, ZeroByteLimitDisablesTheBranch)
{
    Block header = makeHeader();
    IMergingAlgorithm::Inputs inputs = makeInputs(header);

    MergedData merged_data(false, MAX_BLOCK_SIZE, 0, {});
    merged_data.initialize(header, inputs);

    EXPECT_FALSE(merged_data.hasEnoughRows());
}

TEST(MergedDataHasEnoughRows, DynamicColumnsUseDestinationSizeForBatching)
{
    auto dynamic_type = std::make_shared<DataTypeDynamic>();
    Block header;
    header.insert(ColumnWithTypeAndName(dynamic_type->createColumn(), dynamic_type, "value"));

    auto source = ColumnDynamic::create(254);
    source->insert(Field(Int64(1)));
    source->insert(Field(Int64(2)));

    IMergingAlgorithm::Inputs inputs(1);
    inputs[0].chunk.setColumns(Columns{source->getPtr()}, 2);

    EXPECT_EQ(batchRows(header, inputs, 0, 32), rowByRowRows(header, inputs, 0, 32));
    EXPECT_GT(rowByRowRows(header, inputs, 0, 32), 1u);
}

/// The bound below relies on these sizes: a default of a dynamic path is a discriminator and an offset.
TEST(MergedDataHasEnoughRows, DefaultOfMissingDynamicPathTakesNineBytes)
{
    auto dynamic = ColumnDynamic::create();
    const size_t empty_bytes = dynamic->byteSize();
    dynamic->insertDefault();
    EXPECT_EQ(dynamic->byteSize() - empty_bytes, 9u);

    std::vector<Field> one_path_rows;
    for (size_t row = 0; row != 100; ++row)
        one_path_rows.push_back(Object{{"a", Field(Int64(row))}});
    auto parts = makeParts(
        {makeColumn("JSON", {Object{{"a", Field(Int64(1))}, {"b", Field(Int64(2))}, {"c", Field("c")}, {"d", Field(Array{Field(Int64(4))})}}}),
         makeColumn("JSON", one_path_rows)});

    auto [growth, source_bytes] = growthAndSourceBytes(headerOf("JSON"), parts, 1);
    EXPECT_EQ(growth, source_bytes + 100 * 3 * 9);
}

/// When the values keep their place, the batch ends at the row where row-by-row insertion flushes the block.
TEST(MergedDataHasEnoughRows, DynamicBatchEndsWhereRowByRowInsertionFlushes)
{
    auto json_type = DataTypeFactory::instance().get("JSON");
    auto dynamic_type = DataTypeFactory::instance().get("Dynamic");
    Block header{ColumnWithTypeAndName(json_type->createColumn(), json_type, "json"),
                 ColumnWithTypeAndName(dynamic_type->createColumn(), dynamic_type, "dynamic")};

    constexpr size_t rows = 2000;
    std::vector<MutableColumns> columns(3);
    for (auto & part_columns : columns)
        part_columns = header.cloneEmptyColumns();
    for (size_t row = 0; row != rows; ++row)
    {
        const auto value = Int64(row);
        Array objects{Object{{"x", Field(value)}}, Object{{"x", Field(value + 1)}}};
        columns[0][0]->insert(Object{{"a", Field(value)}, {"b", Field("value " + std::to_string(row))},
                                     {"c", Field(Array{Field(value), Field(value + 1)})}, {"arr", Field(objects)}});
        columns[0][1]->insert(row % 2 ? Field(value) : Field("value " + std::to_string(row)));
        columns[1][0]->insert(Object{{"a", Field(value)}, {"arr", Field(Array{objects[0]})}});
        columns[1][1]->insert(Field(value));
        columns[2][0]->insert(Object{});
        columns[2][1]->insert(Field());
    }
    std::vector<Columns> part_columns(columns.size());
    for (size_t part = 0; part != columns.size(); ++part)
        for (auto & column : columns[part])
            part_columns[part].push_back(std::move(column));
    auto parts = makeParts(std::move(part_columns));

    for (size_t part = 0; part != parts.size(); ++part)
    {
        for (UInt64 max_block_size_bytes : {1024, 64 * 1024})
        {
            SCOPED_TRACE(fmt::format("part {}, max_block_size_bytes {}", part, max_block_size_bytes));
            const size_t row_by_row_rows = rowByRowRows(header, parts, part, max_block_size_bytes);
            EXPECT_GT(row_by_row_rows, 1u);
            EXPECT_LT(row_by_row_rows, rows);
            EXPECT_EQ(batchRows(header, parts, part, max_block_size_bytes), row_by_row_rows);
        }
    }
}

/// The case that made batching dynamic columns blow up merge memory: 1,000 paths, and many empty objects in another part.
TEST(MergedDataHasEnoughRows, ThousandMissingPathsStillBatch)
{
    Object all_paths;
    for (size_t path = 0; path != 1000; ++path)
        all_paths["path_" + std::to_string(path)] = Field(Int64(path));
    auto parts = makeParts({makeColumn("JSON", {all_paths}), makeColumn("JSON", std::vector<Field>(8000, Object{}))});

    const auto header = headerOf("JSON");
    const size_t row_by_row_rows = rowByRowRows(header, parts, 1, DEFAULT_MAX_BLOCK_SIZE_BYTES);
    EXPECT_GT(row_by_row_rows, 1000u);
    EXPECT_EQ(batchRows(header, parts, 1, DEFAULT_MAX_BLOCK_SIZE_BYTES), row_by_row_rows);
}

/// A value that moves between the regular storage and the shared data can grow the block by much more than
/// its `byteSizeAt` and the defaults of the missing paths, so these rows are still inserted one by one.
TEST(MergedDataHasEnoughRows, DynamicValuesMovedBetweenStoragesAreInsertedOneByOne)
{
    auto expect_row_by_row = [](const Block & header, const IMergingAlgorithm::Inputs & parts, size_t missing_paths)
    {
        auto [growth, source_bytes] = growthAndSourceBytes(header, parts, 0);
        const size_t rows = parts[0].chunk.getNumRows();
        EXPECT_GT(growth, source_bytes + rows * missing_paths * 9);
        EXPECT_EQ(batchRows(header, parts, 0, DEFAULT_MAX_BLOCK_SIZE_BYTES), 1u);
    };

    {
        SCOPED_TRACE("JSON shared data -> dynamic path");
        auto source = DataTypeFactory::instance().get("JSON(max_dynamic_paths = 2)")->createColumn();
        assert_cast<ColumnObject &>(*source).setMaxDynamicPaths(1);
        for (size_t row = 0; row != 4; ++row)
            source->insert(Object{{"a", Field(Int64(row))}, {"x", mostlyNullArray(1000)}});
        expect_row_by_row(
            headerOf("JSON(max_dynamic_paths = 2)"),
            makeParts({Columns{std::move(source)}, makeColumn("JSON(max_dynamic_paths = 2)", {Object{{"x", mostlyNullArray(1000)}}})}),
            1);
    }

    {
        SCOPED_TRACE("Dynamic shared variant -> variant");
        auto source = ColumnDynamic::create(0);
        for (size_t row = 0; row != 4; ++row)
            source->insert(mostlyNullArray(1000));
        expect_row_by_row(headerOf("Dynamic"), makeParts({Columns{std::move(source)}, makeColumn("Dynamic", {mostlyNullArray(1000)})}), 0);
    }

    {
        SCOPED_TRACE("JSON objects in an array without the paths of other parts");
        Object many_paths;
        for (size_t path = 0; path != 100; ++path)
            many_paths["path_" + std::to_string(path)] = Field(Int64(path));
        expect_row_by_row(
            headerOf("JSON"),
            makeParts(
                {makeColumn("JSON", std::vector<Field>(4, Object{{"arr", Field(Array(100, Object{{"a", Field(Int64(1))}}))}})),
                 makeColumn("JSON", {Object{{"arr", Field(Array(100, many_paths))}}})}),
            0);
    }

    {
        SCOPED_TRACE("JSON dynamic path -> shared data");
        const String long_path(60, 'p');
        Object other_paths{{"a", Field(Int64(1))}, {"b", Field(Int64(2))}, {"c", Field(Int64(3))}, {"d", Field(Int64(4))}};
        expect_row_by_row(
            headerOf("JSON(max_dynamic_paths = 4)"),
            makeParts(
                {makeColumn("JSON(max_dynamic_paths = 4)", std::vector<Field>(4, Object{{long_path, Field("v")}})),
                 makeColumn("JSON(max_dynamic_paths = 4)", std::vector<Field>(8, other_paths))}),
            4);
    }
}

/// Repeated `LowCardinality` values far below the byte limit must be inserted as one batch,
/// not row by row: per-row insertion is what the batch queue avoids.
TEST(MergedDataHasEnoughRows, RepeatedLowCardinalityValuesKeepTheFullBatch)
{
    Block header = makeHeader();
    MutableColumns source = header.cloneEmptyColumns();
    for (UInt64 row = 0; row < 100; ++row)
    {
        source[0]->insert(Field(row));
        source[1]->insert(Field(String("repeated value")));
    }
    EXPECT_EQ(batchRowsBeforeFlush(header, std::move(source)), 100u);
}

/// A wide value must cut the batch right after it, as in the row-by-row path, also inside a column
/// whose `byteSizeAt` is only an estimate, such as a `Map` with `LowCardinality` keys.
TEST(MergedDataHasEnoughRows, WideValueInLowCardinalityMapCutsTheBatch)
{
    auto map_type = std::make_shared<DataTypeMap>(
        std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), std::make_shared<DataTypeString>());
    Block header{ColumnWithTypeAndName(map_type->createColumn(), map_type, "attributes")};

    MutableColumns source = header.cloneEmptyColumns();
    for (const auto & value : {String("small"), String(2 * LARGE_MAX_BLOCK_SIZE_BYTES, 'x'), String("small")})
        source[0]->insert(Field(Map{Tuple{String("key"), value}}));
    EXPECT_EQ(batchRowsBeforeFlush(header, std::move(source)), 2u);
}
