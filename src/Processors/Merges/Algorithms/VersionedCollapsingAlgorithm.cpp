#include <Processors/Merges/Algorithms/VersionedCollapsingAlgorithm.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <IO/WriteBuffer.h>

namespace DB
{

static const size_t MAX_ROWS_IN_MULTIVERSION_QUEUE = 8192;

VersionedCollapsingAlgorithm::VersionedCollapsingAlgorithm(
    SharedHeader header_,
    size_t num_inputs,
    SortDescription description_,
    const String & sign_column_,
    size_t max_block_size_rows_,
    size_t max_block_size_bytes_,
    std::optional<size_t> max_dynamic_subcolumns_,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes)
    : IMergingAlgorithmWithSharedChunks(header_, num_inputs, std::move(description_), out_row_sources_buf_, MAX_ROWS_IN_MULTIVERSION_QUEUE, std::make_unique<MergedData>(use_average_block_sizes, max_block_size_rows_, max_block_size_bytes_, max_dynamic_subcolumns_))
    /// -1 for +1 in FixedSizeDequeWithGaps's internal buffer. 3 is a reasonable minimum size to collapse anything.
    , max_rows_in_queue(std::min(std::max<size_t>(3, max_block_size_rows_), MAX_ROWS_IN_MULTIVERSION_QUEUE) - 1)
    , current_keys(max_rows_in_queue)
{
    sign_column_number = header_->getPositionByName(sign_column_);
}

inline ALWAYS_INLINE static void writeRowSourcePart(WriteBuffer & buffer, RowSourcePart row_source)
{
    if constexpr (sizeof(RowSourcePart) == 1)
        buffer.write(*reinterpret_cast<const char *>(&row_source));
    else
        buffer.write(reinterpret_cast<const char *>(&row_source), sizeof(RowSourcePart));
}

void VersionedCollapsingAlgorithm::insertGap(size_t gap_size)
{
    if (out_row_sources_buf)
    {
        for (size_t i = 0; i < gap_size; ++i)
        {
            /// In a gap only the sources of buffered invalid-sign rows are unskipped.
            if (!current_row_sources.front().getSkipFlag())
                insertBufferedInvalidSignRow();
            writeRowSourcePart(*out_row_sources_buf, current_row_sources.front());
            current_row_sources.pop();
        }
    }
}

void VersionedCollapsingAlgorithm::insertRow(size_t skip_rows, const RowRef & row)
{
    insertGap(skip_rows);

    merged_data->insertRow(*row.all_columns, row.row_num, row.owned_chunk->getNumRows());

    if (out_row_sources_buf)
    {
        current_row_sources.front().setSkipFlag(false);
        writeRowSourcePart(*out_row_sources_buf, current_row_sources.front());
        current_row_sources.pop();
    }
}

void VersionedCollapsingAlgorithm::bufferInvalidSignRow(const RowRef & row)
{
    const auto & columns = *row.all_columns;

    if (invalid_sign_columns.empty())
    {
        invalid_sign_columns.reserve(columns.size());
        for (const auto * column : columns)
            invalid_sign_columns.push_back(column->cloneEmpty());
    }

    for (size_t i = 0, size = columns.size(); i < size; ++i)
        invalid_sign_columns[i]->insertFrom(*columns[i], row.row_num);

    invalid_sign_block_sizes.push_back(row.owned_chunk->getNumRows());
}

void VersionedCollapsingAlgorithm::insertBufferedInvalidSignRow()
{
    ColumnRawPtrs raw_columns;
    raw_columns.reserve(invalid_sign_columns.size());
    for (const auto & column : invalid_sign_columns)
        raw_columns.push_back(column.get());

    merged_data->insertRow(raw_columns, next_invalid_sign_row, invalid_sign_block_sizes[next_invalid_sign_row]);

    if (++next_invalid_sign_row == invalid_sign_block_sizes.size())
    {
        invalid_sign_columns.clear();
        invalid_sign_block_sizes.clear();
        next_invalid_sign_row = 0;
    }
}

IMergingAlgorithm::Status VersionedCollapsingAlgorithm::merge()
{
    /// Take rows in correct order and put them into `merged_columns` until the rows no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = *queue.current().first;

        if (current->isLast() && skipLastRowFor(current->order))
        {
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        RowRef current_row;

        Int8 sign = assert_cast<const ColumnInt8 &>(*current->all_columns[sign_column_number]).getData()[current->getRow()];

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

        /// Insert ready rows if any.
        while (num_rows_to_insert)
        {
            const auto & row = current_keys.front();
            auto gap = current_keys.frontGap();

            insertRow(gap, row);

            current_keys.popFront();

            --num_rows_to_insert;

            /// It's ok to return here, because we didn't affect queue.
            if (merged_data->hasEnoughRows())
                return Status(merged_data->pull());
        }

        bool inserted_rows = false;

        /// With the queue empty, every row read before the buffered ones is already resolved.
        if (current_keys.empty() && !invalid_sign_block_sizes.empty())
        {
            insertGap(current_keys.frontGap());
            current_keys.frontGap() = 0;
            inserted_rows = true;
        }

        const bool valid_sign = sign == 1 || sign == -1;
        const bool keep_invalid_sign_row = !valid_sign;

        if (!valid_sign)
        {
            /// Only 1 and -1 cancel each other: a row with another sign is kept as is.
            if (out_row_sources_buf)
                bufferInvalidSignRow(current_row);
            else
            {
                merged_data->insertRow(*current_row.all_columns, current_row.row_num, current_row.owned_chunk->getNumRows());
                inserted_rows = true;
            }
            current_keys.pushGap(1);
        }
        else if (current_keys.empty())
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
            current_row_sources.emplace(current->order, /*skip_flag=*/ !keep_invalid_sign_row);

        if (!current->isLast())
        {
            queue.next(1);
            if (inserted_rows && merged_data->hasEnoughRows())
                return Status(merged_data->pull());
        }
        else
        {
            /// We take next block from the corresponding source, if there is one.
            queue.removeTop();
            Status status(current.impl->order);
            if (inserted_rows && merged_data->hasEnoughRows())
                status.chunk = merged_data->pull();
            return status;
        }
    }

    while (!current_keys.empty())
    {
        const auto & row = current_keys.front();
        auto gap = current_keys.frontGap();

        insertRow(gap, row);
        current_keys.popFront();

        if (merged_data->hasEnoughRows())
            return Status(merged_data->pull());
    }

    /// Write information about last collapsed rows.
    insertGap(current_keys.frontGap());
    return Status(merged_data->pull(), true);
}

}
