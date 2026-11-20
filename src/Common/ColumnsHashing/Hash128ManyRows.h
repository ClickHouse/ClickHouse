#pragma once

#include <Columns/IColumn.h>
#include <Common/ColumnsHashing/HashMethod.h>
#include <Common/PODArray.h>

#include <memory>
#include <vector>


namespace DB::ColumnsHashing
{

/// `hash128` of the rows of `key_columns`, many rows at once where the bytes that `hash128` hashes can be copied.
class Hash128ManyRows
{
public:
    explicit Hash128ManyRows(const ColumnRawPtrs & key_columns_);

    [[nodiscard]] UInt128 build(size_t row) const { return hash128(row, key_columns.size(), key_columns); }

    /// Sets `keys[i - begin]` to `build(i)` for the rows [begin, end). The keys of the rows `i` with `skip[i] != 0` may be
    /// left unset. `skip` may be nullptr, then no row is skipped.
    void buildRange(size_t begin, size_t end, UInt128 * __restrict keys, const UInt8 * skip);

    /// False if `buildRange` calls `build` for each row.
    [[nodiscard]] bool buildsManyRowsAtOnce() const { return layout != Layout::PerRow; }

private:
    enum class Layout : uint8_t
    {
        /// One fixed-width column, hashed where it is.
        Fixed,
        /// Several fixed-width columns, copied into rows of `fixed_size` bytes.
        FixedRows,
        /// The bytes that `hash128` hashes for each row, copied into a buffer laid out like the chars of a `ColumnString`.
        Rows,
        /// `build` for each row.
        PerRow,
    };

    /// A column whose `updateHashWithValue` hashes `size` bytes at `data + row * size`, or, when `offsets` is not
    /// nullptr, a `String` column with these chars and offsets. With `null_map`, it is the nested column of a `Nullable`.
    struct Part
    {
        const UInt8 * data = nullptr;
        size_t size = 0;
        const IColumn::Offset * offsets = nullptr;
        const UInt8 * null_map = nullptr;
    };

    /// Appends the parts of `column`. Returns false if `buildRange` cannot copy the bytes that `hash128` hashes for it.
    static bool addParts(const IColumn & column, std::vector<Part> & parts);
    /// These set `keys[i]` to `build(row_at(i))` for `i` in [0, num_rows), for at most 256 rows.
    template <typename RowAt>
    void buildFixedRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys);
    template <typename RowAt>
    void buildRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys);

    ColumnRawPtrs key_columns;
    Layout layout = Layout::PerRow;
    std::vector<Part> parts;
    /// The total size of the fixed-width parts.
    size_t fixed_size = 0;
    /// The rows that `buildFixedRows` and `buildRows` copy. The kernel reads up to 8 bytes before a row shorter than 8
    /// bytes and up to 7 bytes past a row, and the padding of a `PaddedPODArray` allows that.
    PaddedPODArray<UInt8> buffer;
};

/// The `hash128` of the rows [0, end) of `key_columns` for a caller that reads them in ascending order: `get` hashes the
/// next `chunk_rows` rows through `Hash128ManyRows::buildRange` when it reaches a row past the hashed ones. A row below the
/// current chunk, and every row of a key that `Hash128ManyRows` hashes per row, is hashed alone. The caller must not `get`
/// a row that `skip` marks, see `Hash128ManyRows::buildRange`.
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

    Hash128ManyRows key;
    const UInt8 * skip;
    size_t end;
    size_t ready_begin = 0;
    size_t ready_size = 0;
    /// `chunk_rows` keys, or null if `key` hashes each row alone.
    std::unique_ptr<UInt128[]> hashes;
};

}
