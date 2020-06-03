#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergingSortedAlgorithm::MergingSortedAlgorithm(
    const Block & header,
    size_t num_inputs,
    SortDescription description_,
    size_t max_block_size,
    UInt64 limit_,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes)
    : merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
    , description(std::move(description_))
    , limit(limit_)
    , out_row_sources_buf(out_row_sources_buf_)
    , source_chunks(num_inputs)
    , cursors(num_inputs)
{
    /// Replace column names in description to positions.
    for (auto & column_description : description)
    {
        has_collation |= column_description.collator != nullptr;
        if (!column_description.column_name.empty())
        {
            column_description.column_number = header.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }
}

void MergingSortedAlgorithm::addInput()
{
    source_chunks.emplace_back();
    cursors.emplace_back();
}

static void prepareChunk(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void MergingSortedAlgorithm::initialize(Chunks chunks)
{
    source_chunks = std::move(chunks);

    for (size_t source_num = 0; source_num < source_chunks.size(); ++source_num)
    {
        auto & chunk = source_chunks[source_num];

        if (!chunk)
            continue;

        prepareChunk(chunk);
        cursors[source_num] = SortCursorImpl(chunk.getColumns(), description, source_num);
    }

    if (has_collation)
        queue_with_collation = SortingHeap<SortCursorWithCollation>(cursors);
    else
        queue_without_collation = SortingHeap<SortCursor>(cursors);
}

void MergingSortedAlgorithm::consume(Chunk & chunk, size_t source_num)
{
    prepareChunk(chunk);
    source_chunks[source_num].swap(chunk);
    cursors[source_num].reset(source_chunks[source_num].getColumns(), {});

    if (has_collation)
        queue_with_collation.push(cursors[source_num]);
    else
        queue_without_collation.push(cursors[source_num]);
}

IMergingAlgorithm::Status MergingSortedAlgorithm::merge()
{
    if (has_collation)
        return mergeImpl(queue_with_collation);
    else
        return mergeImpl(queue_without_collation);
}

template <typename TSortingHeap>
IMergingAlgorithm::Status MergingSortedAlgorithm::mergeImpl(TSortingHeap & queue)
{
    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        if (merged_data.hasEnoughRows())
            return Status(merged_data.pull());

        auto current = queue.current();

        /** And what if the block is totally less or equal than the rest for the current cursor?
            * Or is there only one data source left in the queue? Then you can take the entire block on current cursor.
            */
        if (current.impl->isFirst()
            && (queue.size() == 1
                || (queue.size() >= 2 && current.totallyLessOrEquals(queue.nextChild()))))
        {
            //std::cerr << "current block is totally less or equals\n";

            /// If there are already data in the current block, we first return it.
            /// We'll get here again the next time we call the merge function.
            if (merged_data.mergedRows() != 0)
            {
                //std::cerr << "merged rows is non-zero\n";
                // merged_data.flush();
                return Status(merged_data.pull());
            }

            /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
            size_t source_num = current.impl->order;
            queue.removeTop();
            return insertFromChunk(source_num);
        }

        //std::cerr << "total_merged_rows: " << total_merged_rows << ", merged_rows: " << merged_rows << "\n";
        //std::cerr << "Inserting row\n";
        merged_data.insertRow(current->all_columns, current->pos, current->rows);

        if (out_row_sources_buf)
        {
            /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
            RowSourcePart row_source(current.impl->order);
            out_row_sources_buf->write(row_source.data);
        }

        if (limit && merged_data.totalMergedRows() >= limit)
            return Status(merged_data.pull(), true);

        if (!current->isLast())
        {
            //std::cerr << "moving to next row\n";
            queue.next();
        }
        else
        {
            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();
            //std::cerr << "It was last row, fetching next block\n";
            return Status(current.impl->order);
        }
    }

    return Status(merged_data.pull(), true);
}

IMergingAlgorithm::Status MergingSortedAlgorithm::insertFromChunk(size_t source_num)
{
    if (source_num >= cursors.size())
        throw Exception("Logical error in MergingSortedTransform", ErrorCodes::LOGICAL_ERROR);

    //std::cerr << "copied columns\n";

    auto num_rows = source_chunks[source_num].getNumRows();

    UInt64 total_merged_rows_after_insertion = merged_data.mergedRows() + num_rows;
    bool is_finished = limit && total_merged_rows_after_insertion >= limit;

    if (limit && total_merged_rows_after_insertion > limit)
    {
        num_rows -= total_merged_rows_after_insertion - limit;
        merged_data.insertFromChunk(std::move(source_chunks[source_num]), num_rows);
    }
    else
        merged_data.insertFromChunk(std::move(source_chunks[source_num]), 0);

    source_chunks[source_num] = Chunk();

    /// Write order of rows for other columns
    /// this data will be used in gather stream
    if (out_row_sources_buf)
    {
        RowSourcePart row_source(source_num);
        for (size_t i = 0; i < num_rows; ++i)
            out_row_sources_buf->write(row_source.data);
    }

    auto status = Status(merged_data.pull(), is_finished);

    if (!is_finished)
        status.required_source = source_num;

    return status;
}

}
