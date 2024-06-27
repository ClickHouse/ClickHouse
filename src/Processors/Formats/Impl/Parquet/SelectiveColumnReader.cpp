#include "SelectiveColumnReader.h"

namespace DB
{
Chunk RowGroupChunkReader::readChunk(int rows)
{
    // compute row_set;
    RowSet row_set;
    std::ranges::for_each(filter_columns, [&](auto & column) { reader_columns_mapping[column]->computeRowSet(row_set, rows); });

    // read column
    Columns columns;
    std::ranges::for_each(column_readers, [&](auto & reader) {columns.push_back(reader->read(row_set, rows));});

    return Chunk(columns, columns[0]->size());
}

void Int64ColumnDirectReader::computeRowSet(RowSet row_set, size_t& rows_to_read)
{
    if (!state.page || state.offset >= state.page->size())
        state.page = page_reader->NextPage();
    size_t remain_rows = (state.page->size() - state.offset) / sizeof(Int64);
    rows_to_read = std::min(remain_rows, rows_to_read);
    size_t row_set_offset = row_set.offset();
    const Int64 * start = reinterpret_cast<const Int64 *>(state.page->data() + state.offset);
    for (size_t i = 0; i < rows_to_read; i++)
    {
        row_set.set(i, scan_spec.filter->testInt64(start[i]));
    }

}
}
