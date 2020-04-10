#include <Processors/Merges/ReplacingSortedAlgorithm.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

ReplacingSortedAlgorithm::ReplacingSortedAlgorithm(
        const Block & header, size_t num_inputs,
        SortDescription description_, const String & version_column,
        size_t max_block_size,
        WriteBuffer * out_row_sources_buf_,
        bool use_average_block_sizes)
        : IMergingAlgorithmWithSharedChunks(num_inputs, std::move(description_), out_row_sources_buf_, max_row_refs)
        , merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
{
    if (!version_column.empty())
        version_column_number = header.getPositionByName(version_column);
}

void ReplacingSortedAlgorithm::insertRow()
{
    if (out_row_sources_buf)
    {
        /// true flag value means "skip row"
        current_row_sources[max_pos].setSkipFlag(false);

        out_row_sources_buf->write(reinterpret_cast<const char *>(current_row_sources.data()),
                                   current_row_sources.size() * sizeof(RowSourcePart));
        current_row_sources.resize(0);
    }

    merged_data.insertRow(*selected_row.all_columns, selected_row.row_num, selected_row.owned_chunk->getNumRows());
    selected_row.clear();
}

IMergingAlgorithm::Status ReplacingSortedAlgorithm::merge()
{
    /// Take the rows in needed order and put them into `merged_columns` until rows no more than `max_block_size`
    while (queue.isValid())
    {
        SortCursor current = queue.current();

        if (last_row.empty())
            setRowRef(last_row, current);

        RowRef current_row;
        setRowRef(current_row, current);

        bool key_differs = !current_row.hasEqualSortColumnsWith(last_row);

        /// if there are enough rows and the last one is calculated completely
        if (key_differs && merged_data.hasEnoughRows())
            return Status(merged_data.pull());

        if (key_differs)
        {
            /// Write the data for the previous primary key.
            insertRow();
            last_row.swap(current_row);
        }

        /// Initially, skip all rows. Unskip last on insert.
        size_t current_pos = current_row_sources.size();
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        /// A non-strict comparison, since we select the last row for the same version values.
        if (version_column_number == -1
            || selected_row.empty()
            || current->all_columns[version_column_number]->compareAt(
                current->pos, selected_row.row_num,
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
            /// We get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    /// We will write the data for the last primary key.
    if (!selected_row.empty())
        insertRow();

    return Status(merged_data.pull(), true);
}

}
