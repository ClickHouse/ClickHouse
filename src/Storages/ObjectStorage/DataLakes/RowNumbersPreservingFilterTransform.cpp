#include <Storages/ObjectStorage/DataLakes/RowNumbersPreservingFilterTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Formats/IInputFormat.h>
#include <Columns/FilterDescription.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RowNumbersPreservingFilterTransform::RowNumbersPreservingFilterTransform(
    const SharedHeader & header_,
    ExpressionActionsPtr expression_,
    String filter_column_name_,
    bool remove_filter_column_)
    : ISimpleTransform(
        header_,
        std::make_shared<const Block>(FilterTransform::transformHeader(
            *header_, &expression_->getActionsDAG(), filter_column_name_, remove_filter_column_)),
        /*skip_empty_chunks=*/ true)
    , expression(std::move(expression_))
    , filter_column_name(std::move(filter_column_name_))
    , remove_filter_column(remove_filter_column_)
{
}

void RowNumbersPreservingFilterTransform::transform(Chunk & chunk)
{
    const size_t num_rows_before = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    expression->execute(block);

    ColumnPtr filter_column = block.getByName(filter_column_name).column;
    if (remove_filter_column)
        block.erase(filter_column_name);

    filter_column = FilterDescription::preprocessFilterColumn(std::move(filter_column));
    const IColumnFilter & filter = assert_cast<const ColumnUInt8 &>(*filter_column).getData();
    if (filter.size() != num_rows_before)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Filter size {} does not match the number of rows {}", filter.size(), num_rows_before);

    const size_t num_rows_after = countBytesInFilter(filter.data(), 0, filter.size());

    /// Update the physical row numbers of the chunk to account for the removed rows.
    if (auto chunk_info = chunk.getChunkInfos().get<ChunkInfoRowNumbers>())
    {
        auto & applied_filter = chunk_info->applied_filter;
        if (applied_filter.has_value())
        {
            size_t idx_in_chunk = 0;
            for (size_t i = 0; i < applied_filter->size(); ++i)
            {
                if ((*applied_filter)[i])
                {
                    if (!filter[idx_in_chunk])
                        (*applied_filter)[i] = 0;
                    ++idx_in_chunk;
                }
            }
            chassert(idx_in_chunk == num_rows_before);
        }
        else if (num_rows_after != num_rows_before)
        {
            applied_filter.emplace(filter.begin(), filter.end());
        }
    }

    auto columns = block.getColumns();
    if (num_rows_after != num_rows_before)
    {
        for (auto & column : columns)
            column = column->filter(filter, num_rows_after);
    }

    chunk.setColumns(std::move(columns), num_rows_after);
}

}
