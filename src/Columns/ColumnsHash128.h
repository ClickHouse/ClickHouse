#pragma once

#include <Columns/IColumn_fwd.h>
#include <base/extended_types.h>

#include <memory>


namespace DB::ColumnsHashing
{

/// `hash128Range` and `Hash128InOrder` give the `hash128` of many rows of `key_columns` at once, the same values. A key for
/// which `canHash128InPlace` is true is hashed many rows at a time, every other key one row at a time.

/// True for a key of one number, `Decimal` or `FixedString` column, where SipHash of many rows at once is vectorized.
bool canHash128InPlace(const ColumnRawPtrs & key_columns);

/// `hashes[i]` = `hash128(begin + i, key_columns.size(), key_columns)`, for i in [0, end - begin).
void hash128Range(const ColumnRawPtrs & key_columns, size_t begin, size_t end, UInt128 * hashes);

/// The hashes of the rows [begin, end) of `key_columns` for a caller that reads them in ascending order: `get` hashes the
/// next `chunk_rows` rows when it reaches a row past the hashed ones. A row outside [begin, end), or below the current
/// chunk, is hashed alone, and so is every row of a key for which `canHash128InPlace` is false.
class Hash128InOrder
{
public:
    static constexpr size_t chunk_rows = 256;

    Hash128InOrder(ColumnRawPtrs key_columns_, size_t begin, size_t end_);

    UInt128 get(size_t row)
    {
        if (row - ready_begin < ready_size)
            return hashes[row - ready_begin];
        return hashNext(row);
    }

private:
    UInt128 hashNext(size_t row);

    ColumnRawPtrs key_columns;
    size_t end;
    size_t ready_begin;
    size_t ready_size = 0;
    /// `chunk_rows` hashes, or null if the key is hashed one row at a time.
    std::unique_ptr<UInt128[]> hashes;
};

}
