#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>

using namespace DB;

namespace
{

Chunk makeChunkWithFileRowNumbers(const std::vector<UInt64> & values, size_t row_num_offset = 0)
{
    auto column = ColumnUInt64::create();
    for (UInt64 value : values)
        column->insert(value);

    Chunk chunk(Columns{std::move(column)}, values.size());
    chunk.getChunkInfos().add(std::make_shared<ChunkInfoRowNumbers>(row_num_offset));
    return chunk;
}

/// Mimics Iceberg equality-delete `FilterTransform`: shrink columns, leave `applied_filter` unset.
void shrinkWithoutAppliedFilter(Chunk & chunk, const IColumn::Filter & filter)
{
    size_t result_size = 0;
    for (UInt8 keep : filter)
        result_size += keep != 0;

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(filter, -1);
    chunk.setColumns(std::move(columns), result_size);
}

std::vector<UInt64> readValues(const Chunk & chunk)
{
    const auto & column = assert_cast<const ColumnUInt64 &>(*chunk.getColumns().at(0));
    std::vector<UInt64> values;
    values.reserve(column.size());
    for (size_t i = 0; i < column.size(); ++i)
        values.push_back(column.getData()[i]);
    return values;
}

std::shared_ptr<DataLakeObjectMetadata::ExcludedRows> makeExcludedRows(std::initializer_list<UInt64> positions)
{
    auto excluded_rows = std::make_shared<DataLakeObjectMetadata::ExcludedRows>();
    for (UInt64 position : positions)
        excluded_rows->add(position);
    return excluded_rows;
}

}

/// Equality removes file row 1; DV deletes file position 2. Survivors must be {0, 3}.
TEST(DeletionVectorBeforeEqualityFilter, CorrectOrderKeepsFileRowMapping)
{
    Chunk chunk = makeChunkWithFileRowNumbers({0, 1, 2, 3});

    DeletionVectorTransform::transform(chunk, *makeExcludedRows({2}));
    ASSERT_EQ(readValues(chunk), (std::vector<UInt64>{0, 1, 3}));

    /// Drop equality-deleted value 1 without updating applied_filter (as FilterTransform does).
    shrinkWithoutAppliedFilter(chunk, IColumn::Filter{1, 0, 1});
    EXPECT_EQ(readValues(chunk), (std::vector<UInt64>{0, 3}));
}
