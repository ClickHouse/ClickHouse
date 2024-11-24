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
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

static IMergingAlgorithm::Status emitChunk(detail::SharedChunkPtr & chunk, bool finished = false)
{
    chunk->getChunkInfos().add(std::make_shared<ChunkSelectFinalIndices>(std::move(chunk->replace_final_selection)));
    return IMergingAlgorithm::Status(std::move(*chunk), finished);
}

ChunkSelectFinalIndices::ChunkSelectFinalIndices(MutableColumnPtr select_final_indices_)
    : column_holder(std::move(select_final_indices_))
    , select_final_indices(typeid_cast<const ColumnUInt64 *>(column_holder.get()))
{
    if (!select_final_indices)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column passed to 'ChunkSelectFinalIndices' must be ColumnUInt64");
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
    bool enable_vertical_final_)
    : IMergingAlgorithmWithSharedChunks(header_, num_inputs, std::move(description_), out_row_sources_buf_, max_row_refs, std::make_unique<MergedData>(use_average_block_sizes, max_block_size_rows, max_block_size_bytes))
    , cleanup(cleanup_), enable_vertical_final(enable_vertical_final_)
{
    if (!is_deleted_column.empty())
        is_deleted_column_number = header_.getPositionByName(is_deleted_column);

    if (!version_column.empty())
        version_column_number = header_.getPositionByName(version_column);
}

void ReplacingSortedAlgorithm::insertRow()
{
    if (is_deleted_column_number != -1)
    {
        if (!(cleanup && assert_cast<const ColumnUInt8 &>(*(*selected_row.all_columns)[is_deleted_column_number]).getData()[selected_row.row_num]))
            insertRowImpl();
    }
    else
    {
        insertRowImpl();
    }

    /// insertRowImpl() may has not been called
    saveChunkForSkippingFinalFromSelectedRow();
}

void ReplacingSortedAlgorithm::insertRowImpl()
{
    if (out_row_sources_buf)
    {
        /// true flag value means "skip row"
        current_row_sources[max_pos].setSkipFlag(false);

        out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                   current_row_sources.size() * sizeof(RowSourcePart));
        current_row_sources.resize(0);
    }

    if (enable_vertical_final)
    {
        /// We just record the position to be selected in the chunk
        if (!selected_row.owned_chunk->replace_final_selection)
            selected_row.owned_chunk->replace_final_selection = ColumnUInt64::create();

        selected_row.owned_chunk->replace_final_selection->insert(selected_row.row_num);

        /// This is the last row we can select from `selected_row.owned_chunk`, keep it to emit later
        if (selected_row.current_cursor == nullptr)
            to_be_emitted.push(std::move(selected_row.owned_chunk));
    }
    else
    {
        merged_data->insertRow(*selected_row.all_columns, selected_row.row_num, selected_row.owned_chunk->getNumRows());
    }

    selected_row.clear();
}

IMergingAlgorithm::Status ReplacingSortedAlgorithm::merge()
{
    /// Skipping final: we've done processing some chunk and can emit them
    if (!to_be_emitted.empty())
    {
        auto chunk = std::move(to_be_emitted.front());
        to_be_emitted.pop();
        return emitChunk(chunk);
    }

    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = queue.current();
        if (current->isLast() && skipLastRowFor(current->order))
        {
            saveChunkForSkippingFinalFromSource(current.impl->order);
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        RowRef current_row;
        setRowRef(current_row, current);

        bool key_differs = selected_row.empty() || rowsHaveDifferentSortColumns(selected_row, current_row);
        if (key_differs)
        {
            /// If there are enough rows and the last one is calculated completely
            if (merged_data->hasEnoughRows())
                return Status(merged_data->pull());

            /// Write the data for the previous primary key.
            if (!selected_row.empty())
                insertRow();

            selected_row.clear();
        }

        if (current->isFirst()
            && key_differs
            && is_deleted_column_number == -1 /// Ignore optimization if we need to filter deleted rows.
            && sources_origin_merge_tree_part_level[current->order] > 0
            && !skipLastRowFor(current->order) /// Ignore optimization if last row should be skipped.
            && (queue.size() == 1 || (queue.size() >= 2 && current.totallyLess(queue.nextChild()))))
        {
            /// This is special optimization if current cursor is totally less than next cursor
            /// and current chunk has no duplicates (we assume that parts with non-zero level have no duplicates)
            /// We want to insert current cursor chunk directly in merged data.

            /// First if merged_data is not empty we need to flush it.
            /// We will get into the same condition on next merge call.
            if (merged_data->mergedRows() != 0)
                return Status(merged_data->pull());

            size_t source_num = current->order;
            auto current_chunk = std::move(*sources[source_num].chunk);
            size_t chunk_num_rows = current_chunk.getNumRows();

            /// We will get the next block from the corresponding source, if there is one.
            queue.removeTop();

            if (enable_vertical_final)
            {
                current_chunk.getChunkInfos().add(std::make_shared<ChunkSelectFinalAllRows>());
                Status status(std::move(current_chunk));
                status.required_source = source_num;
                return status;
            }

            merged_data->insertChunk(std::move(current_chunk), chunk_num_rows);
            sources[source_num].chunk = {};

            /// Write order of rows for other columns this data will be used in gather stream
            if (out_row_sources_buf)
            {
                /// All rows are not skipped.
                RowSourcePart row_source(source_num);
                for (size_t i = 0; i < chunk_num_rows; ++i)
                    out_row_sources_buf->write(row_source.data);
            }

            Status status(merged_data->pull());
            status.required_source = source_num;
            return status;
        }

        /// Initially, skip all rows. Unskip last on insert.
        size_t current_pos = current_row_sources.size();
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        if (is_deleted_column_number != -1)
        {
            const UInt8 is_deleted = assert_cast<const ColumnUInt8 &>(*current->all_columns[is_deleted_column_number]).getData()[current->getRow()];
            if (is_deleted > 1)
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
            saveChunkForSkippingFinalFromSelectedRow();
            setRowRef(selected_row, current);
        }

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            saveChunkForSkippingFinalFromSource(current.impl->order);
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    /// If have enough rows, return block, because it prohibited to overflow requested number of rows.
    if (merged_data->hasEnoughRows())
        return Status(merged_data->pull());

    /// We will write the data for the last primary key.
    if (!selected_row.empty())
        insertRow();

    /// Skipping final: emit the remaining chunks
    if (!to_be_emitted.empty())
    {
        auto chunk = std::move(to_be_emitted.front());
        to_be_emitted.pop();
        return emitChunk(chunk, to_be_emitted.empty());
    }

    return Status(merged_data->pull(), true);
}

void ReplacingSortedAlgorithm::saveChunkForSkippingFinalFromSelectedRow()
{
    if (selected_row.owned_chunk && selected_row.owned_chunk->replace_final_selection && selected_row.current_cursor == nullptr)
        to_be_emitted.push(std::move(selected_row.owned_chunk));
}

void ReplacingSortedAlgorithm::saveChunkForSkippingFinalFromSource(size_t current_source_index)
{
    if (enable_vertical_final)
    {
        auto & chunk = sources[current_source_index].chunk;
        if (selected_row.owned_chunk.get() == chunk.get())
        {
            /// selected_row is the last row (or the row before last row) of chunk, so we cannot emit the chunk now.
            /// But after this function, queue.removeTop() will destroy the chunk's cursor, so we mark `selected_row.current_cursor` to `nullptr`
            /// to indicate that `selected_row` is now the sole owner of the chunk
            /// Later when we change the value of `selected_row`, if `selected_row` is the sole owner of its chunk and the chunk has selected rows,
            /// we will emit it
            selected_row.current_cursor = nullptr;
        }
        else
        {
            /// Otherwise, its safe to emit the chunk
            if (chunk->replace_final_selection)
                to_be_emitted.push(std::move(chunk));
        }
    }
}

}
