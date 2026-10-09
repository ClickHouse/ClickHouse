#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/SetVariants.h>


namespace DB
{

/// In `x IN (subquery)`, the rows of the right-hand side (RHS) are the keys of the set, and each value of
/// the left-hand side (LHS) is looked up among them. A set on disk stores each RHS key as a disk key, an
/// unsigned integer of a fixed width, in sorted, compressed blocks. An LHS value is converted to a disk key
/// in the same way, and its lookup reads and decompresses the one block that can hold it, so the cost of the
/// lookups is mostly the number of blocks that they read.
///
/// LHS values often arrive sorted: a table is read in the order of its sorting key, values such as dates
/// and identifiers grow with time, and a subquery can sort them. If the conversion keeps the order, so that
/// a smaller value always gets a smaller disk key, a chunk of sorted LHS values falls into a few neighboring
/// disk blocks and reads only those. Otherwise the values of a chunk scatter over the disk files, and the chunk
/// reads about one block per value.
///
/// So a fixed-size key, such as a number, a date, a short `FixedString` or a tuple of them, is converted to
/// a disk key of the same width in a way that keeps the sort order (see `encodeFixedSizeKeys`). These are the
/// keys of the methods `key8` to `key64`, `keys32` to `keys256`, `nullable_keys128` and `nullable_keys256`.
/// Any other key, such as a `String`, becomes the 128-bit hash that the `hashed` method computes: the hash
/// fits the fixed width of a disk key, but it does not keep the order.

/// The type of the disk keys of a set with `Method`.
template <typename Method>
using DiskSetKey = std::conditional_t<set_method_has_fixed_size_keys<Method>, typename Method::Key, UInt128>;

/// Converts the rows of fixed-size key columns, RHS keys or LHS values, to disk keys of type `Key` in a way
/// that keeps the order of the rows, compared as tuples (see above). Skips the rows marked in `null_map`. The
/// first column is the most significant one.
template <typename Key>
ColumnPtr encodeFixedSizeKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map);

/// Computes the 128-bit hashes that the `hashed` method computes for the rows of key columns, so that the
/// disk keys of LHS values match the hashes of the RHS keys: those that the table of the `hashed` method
/// keeps, and those that `getHashedKey` computes for strings. Skips the rows marked in `null_map`.
ColumnPtr computeHashedKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map);

/// Computes the disk keys of the rows of key columns, RHS keys or LHS values, for a set with `Method`,
/// skipping the rows marked in `null_map`. Without a null map, the disk keys retain their input row positions
/// for batched lookups.
template <typename Method>
ColumnPtr computeDiskSetKeys(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, ConstNullMapPtr null_map)
{
    if constexpr (set_method_has_fixed_size_keys<Method>)
        return encodeFixedSizeKeys<typename Method::Key>(key_columns, key_sizes, rows, null_map);
    else
        return computeHashedKeys(key_columns, key_sizes, rows, null_map);
}

/// Computes the disk keys of the RHS keys in the table of `method`, when the set spills, and passes them to
/// `consume` in batches of at most `max_rows` keys. The tables of the methods with fixed-size keys keep the
/// RHS keys packed as in memory, which are read back into columns of `key_types` and converted as LHS values
/// are, so that equal values get equal disk keys. These columns take at most `max_rows` times the width of a
/// key. The table of the `hashed` method keeps the disk keys themselves, and the strings of `key_string` and
/// `key_fixed_string` are hashed in place: copying them out of the arena would take memory in proportion to
/// their length, while the set spills because memory is short.
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
            if constexpr (std::is_same_v<Method, decltype(SetVariants::hashed)::element_type>)
                keys->getData().push_back(it->getValue());
            else
                keys->getData().push_back(Method::getHashedKey(it->getValue()));

            if (keys->size() == max_rows)
                consume(std::exchange(keys, KeysColumn::create()));
        }

        if (!keys->empty())
            consume(std::move(keys));
    }
}

}
