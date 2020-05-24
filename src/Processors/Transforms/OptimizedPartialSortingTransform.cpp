#include <Processors/Transforms/OptimizedPartialSortingTransform.h>
#include <Interpreters/sortBlock.h>
#include <Common/PODArray.h>

namespace DB
{

OptimizedPartialSortingTransform::OptimizedPartialSortingTransform(
    const Block & header_, SortDescription & description_, UInt64 limit_)
    : ISimpleTransform(header_, header_, false)
    , description(description_), limit(limit_)
    , threshold_shared_block(nullptr)
{
}

static ColumnRawPtrs extractColumns(const Block & block, const SortDescription& description)
{
    size_t size = description.size();
    ColumnRawPtrs res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i)
    {
        const IColumn * column = !description[i].column_name.empty()
            ? block.getByName(description[i].column_name).column.get()
            : block.safeGetByPosition(description[i].column_number).column.get();
        res.emplace_back(column);
    }

    return res;
}

void OptimizedPartialSortingTransform::transform(Chunk & chunk)
{
    if (read_rows)
        read_rows->add(chunk.getNumRows());

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    chunk.clear();

    SharedBlockPtr shared_block = new detail::SharedBlock(std::move(block));
    UInt64 rows_num = shared_block->rows();


    if (threshold_shared_block) {
        SharedBlockRowWithSortDescriptionRef row;
        IColumn::Filter filter(rows_num);
        ColumnRawPtrs shared_block_columns = extractColumns(*shared_block, description);
        size_t filtered_count = 0;

        for (UInt64 i = 0; i < rows_num; ++i) {
            row.set(shared_block, &shared_block_columns, i, &description);

            if (threshold_row < row)
            {
                ++filtered_count;
                filter[i] = 1;
            }
        }

        if (filtered_count)
        {
            for (auto & column : shared_block->getColumns())
            {
                column = column->filter(filter, filtered_count);
            }
        }
    }

    sortBlock(*shared_block, description, limit);

    if (!threshold_shared_block && limit && limit < rows_num)
    {
        Block threshold_block = shared_block->cloneWithColumns(shared_block->getColumns());
        threshold_shared_block = new detail::SharedBlock(std::move(threshold_block));
        threshold_block_columns = extractColumns(*threshold_shared_block, description);
        threshold_row.set(threshold_shared_block, &threshold_block_columns, limit - 1, &description);
    }

    chunk.setColumns(shared_block->getColumns(), shared_block->rows());
}

}
