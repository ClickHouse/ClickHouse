#pragma once

#include <Columns/HashedJoinKey.h>

#include <memory>


namespace DB::ColumnsHashing
{

/// The `hash128` of the rows [0, end) of `key_columns` for a caller that reads them in ascending order: `get` hashes the
/// next `chunk_rows` rows through `HashedJoinKey::buildRange` when it reaches a row past the hashed ones. A row below the
/// current chunk, and every row of a key that `HashedJoinKey` hashes per row, is hashed alone. The caller must not `get`
/// a row that `skip` marks, see `HashedJoinKey::buildRange`.
class Hash128InOrder
{
public:
    static constexpr size_t chunk_rows = 256;

    Hash128InOrder(const ColumnRawPtrs & key_columns, size_t end_, const UInt8 * skip_ = nullptr);

    UInt128 get(size_t row)
    {
        if (row - ready_begin < ready_size)
            return hashes[row - ready_begin];
        if (!hashes)
            return key.build(row);
        return hashNext(row);
    }

private:
    UInt128 hashNext(size_t row);

    HashedJoinKey key;
    const UInt8 * skip;
    size_t end;
    size_t ready_begin = 0;
    size_t ready_size = 0;
    /// `chunk_rows` keys, or null if `key` hashes each row alone.
    std::unique_ptr<UInt128[]> hashes;
};

}
