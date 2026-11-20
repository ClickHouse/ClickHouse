#include <Columns/ColumnsHash128.h>

#include <algorithm>


namespace DB::ColumnsHashing
{

Hash128InOrder::Hash128InOrder(const ColumnRawPtrs & key_columns, size_t end_, const UInt8 * skip_)
    : key(key_columns)
    , skip(skip_)
    , end(end_)
{
    if (key.buildsManyRowsAtOnce())
        hashes = std::make_unique<UInt128[]>(chunk_rows);
}

UInt128 Hash128InOrder::hashNext(size_t row)
{
    if (row < ready_begin + ready_size || row >= end)
        return key.build(row);
    ready_begin = row;
    ready_size = std::min(chunk_rows, end - row);
    key.buildRange(row, row + ready_size, hashes.get(), skip);
    return hashes[0];
}

}
