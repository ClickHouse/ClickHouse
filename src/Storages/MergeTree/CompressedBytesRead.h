#pragma once

#include <Core/ColumnWithTypeAndName.h>

#include <vector>


namespace DB
{

/// What a reader read from a part, as it is stored, since the previous call of `IMergeTreeReader::takeCompressedBytesRead`.
struct CompressedBytesRead
{
    /// The compressed size of the blocks read from the part.
    size_t bytes = 0;

    /// Rows of a column, `offset` and `rows` of the column of an entry of the columns cache, that the reader took from the cache
    /// instead of reading them from the part, so `bytes` misses them. The column is named as in the part.
    struct RowsFromColumnsCache
    {
        ColumnWithTypeAndName column;
        size_t offset = 0;
        size_t rows = 0;
    };
    std::vector<RowsFromColumnsCache> rows_from_columns_cache;
};

}
