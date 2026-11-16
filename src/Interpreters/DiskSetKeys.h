#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/SetVariants.h>
#include <Common/SipHash.h>


namespace DB
{

/// In `x IN (subquery)`, the rows of the right-hand side (RHS) are the keys of the set, and each value of the
/// left-hand side (LHS) is looked up among them. A set on disk stores each RHS key as a disk key, an unsigned
/// integer of a fixed width, in sorted blocks. An LHS value is converted to a disk key in the same way, and
/// its lookup reads and decodes the one block that can hold it, so the cost of the lookups is mostly the
/// number of blocks that they read.
///
/// LHS values often arrive sorted: a table is read in the order of its sorting key, values such as dates
/// and identifiers grow with time, and a subquery can sort them. If the conversion keeps the order, so that
/// a smaller value always gets a smaller disk key, a chunk of sorted LHS values falls into a few neighboring
/// disk blocks and reads only those. Otherwise the values of a chunk scatter over the disk files, and the
/// chunk reads about one block per value.
///
/// So a fixed-size key, such as a number, a date, a short `FixedString` or a tuple of them, is converted to
/// a disk key of the same width in a way that keeps the sort order (see `encodeFixedSizeKeys`). These are the
/// keys of the methods `key8` to `key64`, `keys32` to `keys256`, `nullable_keys128` and `nullable_keys256`.
///
/// A string, the key of the methods `key_string` and `key_fixed_string`, does not fit a disk key of a fixed
/// width. Its disk key keeps the order of its first 4 bytes, and a hash of the whole string tells apart the
/// strings that share them (see `getPrefixedHash`). So sorted strings that differ early, such as identifiers,
/// tokens and UUIDs, still fall into neighboring blocks, while strings with a longer common prefix, such as
/// URLs, scatter as hashes do. The disk key keeps 96 bits of the hash, which make the chance that different
/// strings get equal disk keys negligible.
///
/// Any other key, such as a tuple with a `String`, becomes the 128-bit hash that the table of the `hashed`
/// method keeps: the hash fits the fixed width of a disk key, but it does not keep the order.

/// The type of the disk keys of a set with `Method`.
template <typename Method>
using DiskSetKey = std::conditional_t<set_method_has_fixed_size_keys<Method>, typename Method::Key, UInt128>;

/// Converts the rows of fixed-size key columns, RHS keys or LHS values, to disk keys of type `Key` in a way
/// that keeps the order of the rows, compared as tuples (see above). Skips the rows marked in `null_map`. The
/// first column is the most significant one.
template <typename Key>
ColumnPtr encodeFixedSizeKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map);

/// Returns the disk key of the string `value` (see above): its first 4 bytes, big-endian and padded with zero
/// bytes, above the low 96 bits of its `sipHash128`.
inline UInt128 getPrefixedHash(std::string_view value)
{
    /// An empty string can have a null data pointer, which `memcpy` must not get, so the bytes are read one
    /// at a time.
    UInt32 prefix = 0;
    for (size_t i = 0; i < sizeof(prefix); ++i)
        prefix = (prefix << 8) | (i < value.size() ? static_cast<UInt8>(value[i]) : 0);

    const UInt128 hash = sipHash128(value.data(), value.size());
    return (UInt128(prefix) << 96) | (hash & ((UInt128(1) << 96) - 1));
}

/// Computes the disk keys of the rows of a `String` or `FixedString` column, RHS keys or LHS values, with
/// `getPrefixedHash`. Skips the rows marked in `null_map`.
ColumnPtr computePrefixedHashes(const IColumn & column, size_t rows, ConstNullMapPtr null_map);

/// Computes the 128-bit hashes that the `hashed` method computes for the rows of key columns, so that the
/// disk keys of LHS values match the hashes of the RHS keys that the table of the `hashed` method keeps.
/// Skips the rows marked in `null_map`.
ColumnPtr computeHashedKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map);

/// Computes the disk keys of the rows of key columns, RHS keys or LHS values, for a set with `Method`,
/// skipping the rows marked in `null_map`. Without a null map, the disk keys retain their input row positions
/// for batched lookups.
template <typename Method>
ColumnPtr computeDiskSetKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map)
{
    if constexpr (set_method_has_fixed_size_keys<Method>)
        return encodeFixedSizeKeys<typename Method::Key>(key_columns, key_sizes, rows, null_map);
    else if constexpr (set_method_has_string_keys<Method>)
    {
        chassert(key_columns.size() == 1);
        return computePrefixedHashes(*key_columns.front(), rows, null_map);
    }
    else
        return computeHashedKeys(key_columns, key_sizes, rows, null_map);
}

/// Computes the disk keys of the RHS keys in the table of `method`, when the set spills, and passes them to
/// `consume` in batches of at most `max_rows` keys. The tables of the methods with fixed-size keys keep the
/// RHS keys packed as in memory, which are read back into columns of `key_types` and converted as LHS values
/// are, so that equal values get equal disk keys. These columns take at most twice the width of a key per
/// row: each column takes at least a byte of the packed key, and its null map one more byte. The table of the
/// `hashed` method keeps the disk keys themselves, and the strings of `key_string` and `key_fixed_string` are
/// converted in place: copying them out of the arena would take memory in proportion to their length, while
/// the set spills because memory is short.
template <typename Method, typename Consume>
void computeDiskSetKeysOfTable(
    const Method & method, const DataTypes & key_types, const Sizes & key_sizes, size_t max_rows, Consume && consume)
{
    if constexpr (set_method_has_fixed_size_keys<Method>)
    {
        SetMethodKeyReader<Method> reader(method, key_types, key_sizes);
        while (!reader.atEnd())
        {
            const auto columns = reader.read(max_rows, /*max_bytes=*/0);
            ColumnRawPtrs key_columns;
            for (const auto & column : columns)
                key_columns.push_back(column.get());
            consume(computeDiskSetKeys<Method>(key_columns, key_sizes, columns.front()->size(), /*null_map=*/nullptr));
        }
    }
    else
    {
        using KeysColumn = ColumnVector<DiskSetKey<Method>>;
        auto keys = KeysColumn::create();
        for (auto it = method.data.begin(); it != method.data.end(); ++it)
        {
            if constexpr (set_method_has_string_keys<Method>)
                keys->getData().push_back(getPrefixedHash(it->getValue()));
            else
                keys->getData().push_back(it->getValue());

            if (keys->size() == max_rows)
                consume(std::exchange(keys, KeysColumn::create()));
        }

        if (!keys->empty())
            consume(std::move(keys));
    }
}

/// Returns the memory that `computeDiskSetKeysOfTable` holds besides the table for a batch of `max_rows`
/// keys: the column of their disk keys and, for fixed-size keys, the columns read back from the table, which
/// take at most twice as much.
template <typename Method>
size_t estimateMemoryToComputeDiskSetKeysOfTable(size_t max_rows)
{
    const size_t disk_keys_bytes = max_rows * sizeof(DiskSetKey<Method>);
    const size_t read_back_bytes = set_method_has_fixed_size_keys<Method> ? 2 * disk_keys_bytes : 0;
    return disk_keys_bytes + read_back_bytes;
}

}
