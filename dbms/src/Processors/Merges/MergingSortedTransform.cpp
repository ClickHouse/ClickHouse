#include <Processors/Merges/MergingSortedTransform.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBuffer.h>
#include <DataStreams/materializeBlock.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergingSortedTransform::MergingSortedTransform(
    const Block & header,
    size_t num_inputs,
    SortDescription  description_,
    size_t max_block_size,
    UInt64 limit_,
    bool quiet_,
    bool use_average_block_sizes,
    bool have_all_inputs)
    : IMergingTransform(num_inputs, header, header, max_block_size, use_average_block_sizes, have_all_inputs)
    , description(std::move(description_))
    , limit(limit_)
    , quiet(quiet_)
    , source_chunks(num_inputs)
    , cursors(num_inputs)
{
    auto & sample = outputs.front().getHeader();
    /// Replace column names in description to positions.
    for (auto & column_description : description)
    {
        has_collation |= column_description.collator != nullptr;
        if (!column_description.column_name.empty())
        {
            column_description.column_number = sample.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }
}

void MergingSortedTransform::onNewInput()
{
    source_chunks.emplace_back();
    cursors.emplace_back();
}

void MergingSortedTransform::initializeInputs()
{
    if (has_collation)
        queue_with_collation = SortingHeap<SortCursorWithCollation>(cursors);
    else
        queue_without_collation = SortingHeap<SortCursor>(cursors);

    is_queue_initialized = true;
}

void MergingSortedTransform::consume(Chunk chunk, size_t input_number)
{
    updateCursor(std::move(chunk), input_number);

    if (is_queue_initialized)
    {
        if (has_collation)
            queue_with_collation.push(cursors[input_number]);
        else
            queue_without_collation.push(cursors[input_number]);
    }
}

void MergingSortedTransform::updateCursor(Chunk chunk, size_t source_num)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);

    auto & source_chunk = source_chunks[source_num];

    if (source_chunk.empty())
    {
        source_chunk = std::move(chunk);
        cursors[source_num] = SortCursorImpl(source_chunk.getColumns(), description, source_num);
        has_collation |= cursors[source_num].has_collation;
    }
    else
    {
        source_chunk = std::move(chunk);
        cursors[source_num].reset(source_chunk.getColumns(), {});
    }
}

void MergingSortedTransform::work()
{
    if (has_collation)
        merge(queue_with_collation);
    else
        merge(queue_without_collation);
}

template <typename TSortingHeap>
void MergingSortedTransform::merge(TSortingHeap & queue)
{
    /// Returns MergeStatus which we should return if we are going to finish now.
    auto can_read_another_row = [&, this]()
    {
        if (limit && merged_data.totalMergedRows() >= limit)
        {
            //std::cerr << "Limit reached\n";
            finish();
            return false;
        }

        return merged_data.hasEnoughRows();
    };

    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid() && can_read_another_row())
    {
        auto current = queue.current();

        /** And what if the block is totally less or equal than the rest for the current cursor?
            * Or is there only one data source left in the queue? Then you can take the entire block on current cursor.
            */
        if (current.impl->isFirst()
            && (queue.size() == 1
                || (queue.size() >= 2 && current.totallyLessOrEquals(queue.nextChild()))))
        {
            //std::cerr << "current block is totally less or equals\n";

            /// If there are already data in the current block, we first return it. We'll get here again the next time we call the merge function.
            if (merged_data.mergedRows() != 0)
            {
                //std::cerr << "merged rows is non-zero\n";
                return;
            }

            /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
            size_t source_num = current.impl->order;
            insertFromChunk(source_num);
            queue.removeTop();
            return;
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
            requestDataForInput(current.impl->order);

            if (limit && merged_data.totalMergedRows() >= limit)
                finish();

            return;
        }
    }

    finish();
}

void MergingSortedTransform::insertFromChunk(size_t source_num)
{
    if (source_num >= cursors.size())
        throw Exception("Logical error in MergingSortedTrandform", ErrorCodes::LOGICAL_ERROR);

    //std::cerr << "copied columns\n";

    auto num_rows = source_chunks[source_num].getNumRows();

    UInt64 total_merged_rows_after_insertion = merged_data.mergedRows() + num_rows;
    if (limit && total_merged_rows_after_insertion > limit)
    {
        num_rows = total_merged_rows_after_insertion - limit;
        merged_data.insertFromChunk(std::move(source_chunks[source_num]), num_rows);
        finish();
    }
    else
    {
        merged_data.insertFromChunk(std::move(source_chunks[source_num]), 0);
        requestDataForInput(source_num);
    }

    source_chunks[source_num] = Chunk();

    if (out_row_sources_buf)
    {
        RowSourcePart row_source(source_num);
        for (size_t i = 0; i < num_rows; ++i)
            out_row_sources_buf->write(row_source.data);
    }
}

void MergingSortedTransform::onFinish()
{
    if (quiet)
        return;

    auto * log = &Logger::get("MergingSortedBlockInputStream");

    double seconds = total_stopwatch.elapsedSeconds();

    std::stringstream message;
    message << std::fixed << std::setprecision(2)
            << "Merge sorted " << total_chunks << " blocks, " << total_rows << " rows"
            << " in " << seconds << " sec.";

    if (seconds != 0)
        message << ", "
                << total_rows / seconds << " rows/sec., "
                << total_bytes / 1000000.0 / seconds << " MB/sec.";

    LOG_DEBUG(log, message.str());
}

}
