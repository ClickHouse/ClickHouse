#pragma once

#include <Columns/IColumn.h>
#include <Common/ColumnsHashing/HashMethod.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

/// The key of a row in the `hashed` and `two_level_hashed` maps: `ColumnsHashing::hash128` of its key columns. The build
/// and the probe of one map can see different column types for one key, for example `Tuple(LowCardinality(String))` and
/// `Tuple(String)`, so every way of building a key must give exactly `hash128`.
class HashedJoinKey
{
public:
    explicit HashedJoinKey(const ColumnRawPtrs & key_columns_);

    [[nodiscard]] UInt128 build(size_t row) const { return ColumnsHashing::hash128(row, key_columns.size(), key_columns); }

    /// Sets `keys[i - begin]` to `build(i)` for the rows [begin, end). The keys of the rows `i` with `skip[i] != 0` may be
    /// left unset.
    void buildRange(size_t begin, size_t end, UInt128 * __restrict keys, const UInt8 * skip);

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
    static bool addParts(const IColumn & column, VectorWithMemoryTracking<Part> & parts);
    /// These set `keys[i]` to `build(row_at(i))` for `i` in [0, num_rows), for at most 256 rows.
    template <typename RowAt>
    void buildFixedRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys);
    template <typename RowAt>
    void buildRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys);

    ColumnRawPtrs key_columns;
    Layout layout = Layout::PerRow;
    VectorWithMemoryTracking<Part> parts;
    /// The total size of the fixed-width parts.
    size_t fixed_size = 0;
    /// The rows that `buildFixedRows` and `buildRows` copy. The kernel reads up to 8 bytes before a row shorter than 8
    /// bytes and up to 7 bytes past a row, and the padding of a `PaddedPODArray` allows that.
    PaddedPODArray<UInt8> buffer;
};

}
