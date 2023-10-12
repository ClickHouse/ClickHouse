#include <memory>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>

#include <Columns/ColumnsNumber.h>
#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <Processors/Merges/Algorithms/RowRef.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

ReplacingSortedAlgorithm::ReplacingSortedAlgorithm(
    const Block & header_,
    size_t num_inputs,
    SortDescription description_,
    const String & is_deleted_column,
    const String & version_column,
    size_t max_block_size_rows,
    size_t max_block_size_bytes,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes,
    bool cleanup_,
    size_t * cleanedup_rows_count_,
    bool use_skipping_final_)
    : IMergingAlgorithmWithSharedChunks(header_, num_inputs, std::move(description_), out_row_sources_buf_, use_skipping_final_ ? 2*max_row_refs : max_row_refs)
    , merged_data(header_.cloneEmptyColumns(), use_average_block_sizes, max_block_size_rows, max_block_size_bytes)
    , cleanup(cleanup_)
    , cleanedup_rows_count(cleanedup_rows_count_)
    , use_skipping_final(use_skipping_final_)
{
    if (!is_deleted_column.empty())
        is_deleted_column_number = header_.getPositionByName(is_deleted_column);
    if (!version_column.empty())
        version_column_number = header_.getPositionByName(version_column);
}

detail::SharedChunkPtr ReplacingSortedAlgorithm::insertRow()
{
    detail::SharedChunkPtr res;
    if (out_row_sources_buf)
    {
        /// true flag value means "skip row"
        current_row_sources[max_pos].setSkipFlag(false);

        out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                   current_row_sources.size() * sizeof(RowSourcePart));
        current_row_sources.resize(0);
    }
    if (use_skipping_final)
    {
        /// We just record the position to be selected in the chunk
        if (!selected_row.owned_chunk->replace_final_selection)
            selected_row.owned_chunk->replace_final_selection = ColumnUInt64::create();
        // fmt::print(stderr, "Adding row {} for chunk {}\n", selected_row.row_num, static_cast<void *>(selected_row.owned_chunk.get()));
        selected_row.owned_chunk->replace_final_selection->insert(selected_row.row_num);
        if (selected_row.current_cursor == nullptr) /// This is the "lonely" chunk w/o cursor, we keep and then emit it later
            res = std::move(selected_row.owned_chunk);
    }
    else
        merged_data.insertRow(*selected_row.all_columns, selected_row.row_num, selected_row.owned_chunk->getNumRows());

    selected_row.clear();
    return res;
}

IMergingAlgorithm::Status ReplacingSortedAlgorithm::merge()
{
    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = queue.current();

        if (sources[current.impl->order].chunk->empty() || (current->isLast() && skipLastRowFor(current->order)))
        {
            auto & chunk = sources[current.impl->order].chunk;
            if (!chunk->empty() && use_skipping_final)
            {
                if (selected_row.owned_chunk.get() == chunk.get())
                {
                    /// selected_row points to current source chunk but the chunk will be destroy soon, either in emitChunk() or queue.removeTop()
                    /// In the first case, we create a cloned chunk with only one row from `selected_row.row_num` and let selected_row point to it
                    /// In the second case, we mark selected_row.owned_chunk = nullptr
                    /// In either case, the chunk is "lonely" and if later selected_row is inserted to final result, the chunk will be emitted
                    /// immediately. This will create some blocks with only single row, but it's not a big problem.
                    if (chunk->replace_final_selection)
                        selected_row.set(selected_row.owned_chunk->cloneForSelectedRow(selected_row.row_num), 0);
                    else
                        selected_row.current_cursor = nullptr;
                }

                if (chunk->replace_final_selection)
                    return emitChunk(chunk);
            }

            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        RowRef current_row;
        setRowRef(current_row, current);
        // fmt::print(stderr, "Current row owned chunk: {}, selected row owned chunk: {}\n", static_cast<void *>(current_row.owned_chunk.get()), static_cast<void *>(selected_row.owned_chunk.get()));

        bool key_differs = selected_row.empty() || !current_row.hasEqualSortColumnsWith(selected_row);
        if (key_differs)
        {
            /// if there are enough rows and the last one is calculated completely
            if (merged_data.hasEnoughRows())
                return Status(merged_data.pull());

            detail::SharedChunkPtr chunk_to_emit;

            /// Write the data for the previous primary key.
            if (!selected_row.empty())
            {
                if (is_deleted_column_number != -1)
                {
                    uint8_t value = assert_cast<const ColumnUInt8 &>(*(*selected_row.all_columns)[is_deleted_column_number]).getData()[selected_row.row_num];
                    if (!cleanup || !value)
                        chunk_to_emit = insertRow();
                    else if (cleanup && cleanedup_rows_count != nullptr)
                    {
                        *cleanedup_rows_count += current_row_sources.size();
                        current_row_sources.resize(0);
                    }
                }
                else
                    chunk_to_emit = insertRow();
            }

            selected_row.clear();

            if (chunk_to_emit)
                return emitChunk(chunk_to_emit);
        }

        /// Initially, skip all rows. Unskip last on insert.
        size_t current_pos = current_row_sources.size();
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        if (is_deleted_column_number != -1)
        {
            const UInt8 is_deleted = assert_cast<const ColumnUInt8 &>(*current->all_columns[is_deleted_column_number]).getData()[current->getRow()];
            if ((is_deleted != 1) && (is_deleted != 0))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect data: is_deleted = {} (must be 1 or 0).", toString(is_deleted));
        }

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version_column_number == -1
            || selected_row.empty()
            || current->all_columns[version_column_number]->compareAt(
                current->getRow(), selected_row.row_num,
                *(*selected_row.all_columns)[version_column_number],
                /* nan_direction_hint = */ 1) >= 0)
        {
            max_pos = current_pos;
            setRowRef(selected_row, current);
        }

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            auto & chunk = sources[current.impl->order].chunk;
            if (use_skipping_final && !chunk->empty())
            {
                if (selected_row.owned_chunk.get() == chunk.get())
                {
                    /// selected_row points to current source chunk but the chunk will be destroy soon, either in emitChunk() or queue.removeTop()
                    /// In the first case, we create a cloned chunk with only one row from `selected_row.row_num` and let selected_row point to it
                    /// In the second case, we mark selected_row.owned_chunk = nullptr
                    /// In either case, the chunk is "lonely" and if later selected_row is inserted to final result, the chunk will be emitted
                    /// immediately. This will create some blocks with only single row, but it's not a big problem.
                    if (chunk->replace_final_selection)
                        selected_row.set(selected_row.owned_chunk->cloneForSelectedRow(selected_row.row_num), 0);
                    else
                        selected_row.current_cursor = nullptr;
                }

                if (chunk->replace_final_selection)
                    return emitChunk(chunk);
            }

            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    /// If have enough rows, return block, because it prohibited to overflow requested number of rows.
    if (merged_data.hasEnoughRows())
        return Status(merged_data.pull());

    /// We will write the data for the last primary key.
    if (!selected_row.empty())
    {
        detail::SharedChunkPtr chunk;
        if (is_deleted_column_number != -1)
        {
            uint8_t value = assert_cast<const ColumnUInt8 &>(*(*selected_row.all_columns)[is_deleted_column_number]).getData()[selected_row.row_num];
            if (!cleanup || !value)
                chunk = insertRow();
            else if (cleanup && cleanedup_rows_count != nullptr)
            {
                *cleanedup_rows_count += current_row_sources.size();
                current_row_sources.resize(0);
            }
        }
        else
            chunk = insertRow();

        if (chunk)
            return emitChunk(chunk, true);
    }

    return Status(merged_data.pull(), true);
}

IMergingAlgorithm::Status ReplacingSortedAlgorithm::emitChunk(detail::SharedChunkPtr & chunk, bool finished)
{
    chunk->setChunkInfo(std::make_shared<ChunkSelectFinalIndices>(std::move(chunk->replace_final_selection)));
    return Status(std::move(*chunk), finished);
}

}
