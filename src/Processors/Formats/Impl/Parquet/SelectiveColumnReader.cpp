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
}
