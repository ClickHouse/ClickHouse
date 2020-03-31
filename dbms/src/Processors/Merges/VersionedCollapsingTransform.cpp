#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <IO/WriteBuffer.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

static const size_t MAX_ROWS_IN_MULTIVERSION_QUEUE = 8192;

VersionedCollapsingTransform::VersionedCollapsingTransform(
    size_t num_inputs, const Block & header,
    SortDescription description_, const String & sign_column_,
    size_t max_block_size,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes)
    : IMergingTransform(num_inputs, header, header, true)
    , merged_data(header, use_average_block_sizes, max_block_size)
    , description(std::move(description_))
    , out_row_sources_buf(out_row_sources_buf_)
    , max_rows_in_queue(MAX_ROWS_IN_MULTIVERSION_QUEUE - 1)  /// -1 for +1 in FixedSizeDequeWithGaps's internal buffer
    , current_keys(max_rows_in_queue)
    , chunk_allocator(num_inputs + max_rows_in_queue + 1) /// +1 just in case (for current_row)
{
    sign_column_number = header.getPositionByName(sign_column_);
}

void VersionedCollapsingTransform::initializeInputs()
{
    queue = SortingHeap<SortCursor>(cursors);
    is_queue_initialized = true;
}

void VersionedCollapsingTransform::consume(Chunk chunk, size_t input_number)
{
    updateCursor(std::move(chunk), input_number);

    if (is_queue_initialized)
        queue.push(cursors[input_number]);
}

void VersionedCollapsingTransform::updateCursor(Chunk chunk, size_t source_num)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);

    auto & source_chunk = source_chunks[source_num];

    if (source_chunk)
    {
        source_chunk = chunk_allocator.alloc(std::move(chunk));
        cursors[source_num].reset(source_chunk->getColumns(), {});
    }
    else
    {
        if (cursors[source_num].has_collation)
            throw Exception("Logical error: " + getName() + " does not support collations", ErrorCodes::LOGICAL_ERROR);

        source_chunk = chunk_allocator.alloc(std::move(chunk));
        cursors[source_num] = SortCursorImpl(source_chunk->getColumns(), description, source_num);
    }

    source_chunk->all_columns = cursors[source_num].all_columns;
    source_chunk->sort_columns = cursors[source_num].sort_columns;
}

void VersionedCollapsingTransform::work()
{
    merge();
    prepareOutputChunk(merged_data);
}

inline ALWAYS_INLINE static void writeRowSourcePart(WriteBuffer & buffer, RowSourcePart row_source)
{
    if constexpr (sizeof(RowSourcePart) == 1)
        buffer.write(*reinterpret_cast<const char *>(&row_source));
    else
        buffer.write(reinterpret_cast<const char *>(&row_source), sizeof(RowSourcePart));
}

void VersionedCollapsingTransform::insertGap(size_t gap_size)
{
    if (out_row_sources_buf)
    {
        for (size_t i = 0; i < gap_size; ++i)
        {
            writeRowSourcePart(*out_row_sources_buf, current_row_sources.front());
            current_row_sources.pop();
        }
    }
}

void VersionedCollapsingTransform::insertRow(size_t skip_rows, const RowRef & row)
{
    merged_data.insertRow(*row.all_columns, row.row_num, row.owned_chunk->getNumRows());

    insertGap(skip_rows);

    if (out_row_sources_buf)
    {
        current_row_sources.front().setSkipFlag(false);
        writeRowSourcePart(*out_row_sources_buf, current_row_sources.front());
        current_row_sources.pop();
    }
}

void VersionedCollapsingTransform::merge()
{
    /// Take rows in correct order and put them into `merged_columns` until the rows no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = queue.current();

        RowRef current_row;

        Int8 sign = assert_cast<const ColumnInt8 &>(*current->all_columns[sign_column_number]).getData()[current->pos];

        setRowRef(current_row, current);

        /// At first, let's decide the number of rows needed to insert right now.
        size_t num_rows_to_insert = 0;
        if (!current_keys.empty())
        {
            auto key_differs = !current_row.hasEqualSortColumnsWith(current_keys.back());

            if (key_differs) /// Flush whole queue
                num_rows_to_insert = current_keys.size();
            else if (current_keys.size() >= max_rows_in_queue) /// Flush single row if queue is big
                num_rows_to_insert = 1;
        }

        /// Insert ready roes if any.
        while (num_rows_to_insert)
        {
            const auto & row = current_keys.front();
            auto gap = current_keys.frontGap();

            insertRow(gap, row);

            current_keys.popFront();

            --num_rows_to_insert;

            /// It's ok to return here, because we didn't affect queue.
            if (merged_data.hasEnoughRows())
                return;
        }

        if (current_keys.empty())
        {
            sign_in_queue = sign;
            current_keys.pushBack(current_row);
        }
        else /// If queue is not empty, then current_row has the same key as in current_keys queue
        {
            if (sign == sign_in_queue)
                current_keys.pushBack(current_row);
            else
            {
                current_keys.popBack();
                current_keys.pushGap(2);
            }
        }

        if (out_row_sources_buf)
            current_row_sources.emplace(current->order, true);

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We take next block from the corresponding source, if there is one.
            queue.removeTop();
            requestDataForInput(current.impl->order);
            return;
        }
    }

    while (!current_keys.empty())
    {
        const auto & row = current_keys.front();
        auto gap = current_keys.frontGap();

        insertRow(gap, row);
        current_keys.popFront();

        if (merged_data.hasEnoughRows())
            return;
    }

    /// Write information about last collapsed rows.
    insertGap(current_keys.frontGap());
    is_finished = true;
}


}
