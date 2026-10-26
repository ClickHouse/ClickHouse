#pragma once

#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <base/PackedStringRef.h>
#include <base/defines.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/memory.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>

/// The representation of the records the adaptive aggregation stages for its merge: their formats, the layout of
/// the aggregate arguments they carry, and the reconstruction of their keys. Producers write the records and the
/// merge reads them, so both sides follow the definitions here.

namespace DB
{

/// The record layout of the aggregate arguments that general payloads stage, fixed for the query by the header.
/// Fixed-size arguments have offsets within their argument area and use the `RowDataStore` representation:
/// a `Nullable` field is a null byte followed by the value. The drain rebuilds each of these arguments with
/// one `IColumn::fillFromRowStorePtrs` over the records. Variable-size arguments follow the key, serialized
/// one after another in their layout order. A column read by several aggregates is staged once. A single
/// numeric key used as an argument shares its key bytes instead of occupying another field.
struct AdaptiveArgumentLayout
{
    /// The producers stage the normalized argument values (`LowCardinality` removed, constant, sparse and replicated
    /// columns read as full ones), so the layout follows the types without `LowCardinality`.
    static std::unique_ptr<const AdaptiveArgumentLayout>
    build(const Block & header, const ColumnNumbers & keys_positions, const ColumnNumbersList & aggregates_positions);

    struct FixedField
    {
        size_t position;
        DataTypePtr type;
        size_t offset;
        size_t size;
    };

    struct Field
    {
        size_t position;
        DataTypePtr type;
    };

    std::vector<FixedField> fixed_fields;
    std::vector<Field> variable_fields;
    /// Whether every variable-size argument is staged as a `String` (see `isStagedString`), which the records of a
    /// fixed-width key stage without a header (see `StagedStringArgumentRecord`).
    bool variable_fields_are_strings = false;
    /// A single numeric grouping key used as an argument is read from the key's bytes in the record.
    std::optional<Field> key_field;
    size_t fixed_bytes = 0;
    /// One past the highest argument position, the size of the column vectors the positions index.
    size_t num_positions = 0;
};

/// String-like keys stage their bytes: a packed reference copied as a plain value would
/// carry a pointer into the source block, which is released once the block is consumed. The
/// staged form of both string kinds is the raw characters, and the drain rebuilds the table's
/// key from them, pointing into the record.
template <typename Key>
constexpr bool adaptive_key_stages_bytes = std::is_same_v<Key, std::string_view> || std::is_same_v<Key, PackedStringRef>;

/// Records retain the key holder's width. A numeric table may widen a key for its cell layout,
/// but staging and rehashing need only the original value.
template <typename Method>
using AdaptiveKeyHolder = decltype(
    std::declval<typename Method::StateNoCache &>().getKeyHolder(0, std::declval<DB::Arena &>()));

template <typename Method>
using AdaptiveRecordKey = std::remove_cvref_t<decltype(keyHolderGetKey(std::declval<AdaptiveKeyHolder<Method> &>()))>;

/// Reads a staged key back from its record. String-like keys were staged as raw characters and are rebuilt pointing
/// into the record, so a table holding such a key needs the record until the table is converted or written: the merge
/// frees a partition's records only after that, and copies the keys of the records it reads back from a spill, whose
/// blocks go before.
template <typename Key>
Key ALWAYS_INLINE readStagedKey(const char * key_pos, [[maybe_unused]] size_t key_size, [[maybe_unused]] size_t routing_hash)
{
    if constexpr (std::is_same_v<Key, std::string_view>)
    {
        return std::string_view(key_pos, key_size);
    }
    else if constexpr (std::is_same_v<Key, PackedStringRef>)
    {
        /// The staged routing hash IS the packed key's cached content hash
        /// (`DefaultHash<PackedStringRef>` returns it), so the rebuild reuses it instead of
        /// re-hashing the key bytes; `build` consults the functor only for lengths that
        /// store a hash, which is exactly the range the staged hash was derived from.
        return PackedStringRef::build(key_pos, key_size, [routing_hash](const char *, size_t) { return static_cast<UInt32>(routing_hash); });
    }
    else
    {
        return unalignedLoad<Key>(key_pos);
    }
}

/// The staged record formats. A record never straddles two chunks, and nothing in it points
/// outside the record, so a chunk can go to disk and come back byte for byte. Fields are read
/// and written unaligned. The argument records of a fixed-width key need no padding; the other
/// formats keep their sizes a multiple of the narrowest header field.
///
/// The merge emplaces a staged key with its routing hash, the table's own hash of the key (see
/// `emplaceStagedKey`). A record of a byte-staged key, and a general record, starts with the
/// hash, because rehashing the key bytes would take a pass over them. An argument record of a
/// fixed-width key stores no hash: the merge rehashes the key in a few instructions, which costs
/// less than eight more bytes in a record of a dozen or two, written by the staging, read by the
/// merge, and written to disk and read back by a spill.
inline size_t alignStagedRecord(size_t bytes)
{
    return ::Memory::alignUp(bytes, alignof(UInt32));
}

/// Zeroes the padding `alignStagedRecord` put after the last field of a record, which ends at `fields_end`. The merge
/// never reads it, but a spill writes the records to disk as they are, and a key copied with overflow may have left
/// bytes of its source there. The padding is shorter than 4 bytes, and the 4 zero bytes may run past the record:
/// into the next record of the chunk, written after this one, or into the chunk's tail padding.
inline void ALWAYS_INLINE zeroStagedRecordPadding(char * fields_end)
{
    unalignedStore<UInt32>(fields_end, 0);
}

/// A staged `String` argument: a UInt32 length and the bytes, copied straight from and to the column without its
/// serialization's virtual calls. The length fits every value, because the buffer rejects a record whose chunk does
/// not fit 32 bits (see `PartitionedRecordBuffer::append`).
inline size_t ALWAYS_INLINE stagedStringBytes(std::string_view value)
{
    return sizeof(UInt32) + value.size();
}

/// Writes a staged `String` at `to` and returns the position after it.
inline char * ALWAYS_INLINE writeStagedString(char * to, std::string_view value)
{
    chassert(value.size() <= std::numeric_limits<UInt32>::max());
    unalignedStore<UInt32>(to, static_cast<UInt32>(value.size()));
    memcpy(to + sizeof(UInt32), value.data(), value.size());
    return to + stagedStringBytes(value);
}

/// Reads the staged `String` at `from` and moves `from` past it.
inline std::string_view ALWAYS_INLINE readStagedString(const char *& from)
{
    const std::string_view value(from + sizeof(UInt32), unalignedLoad<UInt32>(from));
    from += stagedStringBytes(value);
    return value;
}

/// Whether a variable-size argument is staged by `writeStagedString` rather than in its column's serialization. The
/// writer and the drain both decide by it, so they agree on the encoding of every field.
inline bool isStagedString(const AdaptiveArgumentLayout::Field & field)
{
    return isString(field.type);
}

/// Every format offers the readers the same interface: `bytes(record)`, the size of the record; `key(record)`, the
/// position and the size of its key; and `hash(table, record)`, the routing hash of its key, which `table` hashes as
/// the producer's table did. A format of argument records also names the offsets of the key and of the fixed-size
/// arguments, `key_offset` and `fixed_arguments_offset`. The key formats are empty types, and the argument formats
/// carry the size of the query's fixed-size argument area.

/// Count and key-only records: {[UInt64 hash,] [UInt32 count,] [UInt32 size,] key}. The count is
/// a contribution within one block, which a UInt32 holds. A key whose width varies is staged with
/// the hash and its UInt32 size; a fixed-width key has the compile-time width, so its records
/// have a fixed stride, and carries no hash.
template <typename Key, bool with_count_>
struct StagedKeyRecord
{
    static constexpr bool with_count = with_count_;
    static constexpr bool with_arguments = false;
    static constexpr bool variable_width = adaptive_key_stages_bytes<Key>;
    static constexpr size_t count_offset = variable_width ? sizeof(UInt64) : 0;
    static constexpr size_t size_offset = count_offset + (with_count ? sizeof(UInt32) : 0);
    static constexpr size_t key_offset = size_offset + (variable_width ? sizeof(UInt32) : 0);

    /// The size of the record of a key of `key_size` bytes.
    static size_t bytesForKey(size_t key_size) { return alignStagedRecord(key_offset + key_size); }

    static void writeHeader(char * record, [[maybe_unused]] UInt64 routing_hash, UInt32 count, [[maybe_unused]] size_t key_size)
    {
        if constexpr (variable_width)
        {
            unalignedStore<UInt64>(record, routing_hash);
            unalignedStore<UInt32>(record + size_offset, static_cast<UInt32>(key_size));
        }
        if constexpr (with_count)
            unalignedStore<UInt32>(record + count_offset, count);
    }

    static size_t bytes(const char * record) { return bytesForKey(keySize(record)); }

    static std::pair<const char *, size_t> key(const char * record) { return {record + key_offset, keySize(record)}; }

    template <typename Table>
    static UInt64 hash([[maybe_unused]] const Table & table, const char * record)
    {
        if constexpr (variable_width)
            return unalignedLoad<UInt64>(record);
        else
            return table.hash(unalignedLoad<Key>(record + key_offset));
    }

    static UInt32 count(const char * record) { return unalignedLoad<UInt32>(record + count_offset); }

private:
    static size_t keySize(const char * record)
    {
        if constexpr (variable_width)
            return unalignedLoad<UInt32>(record + size_offset);
        else
            return sizeof(Key);
    }
};

/// General records with a fixed-width key and only fixed-size arguments: {key, arguments}, the arguments laid out
/// by `AdaptiveArgumentLayout`, so every record of the query has the same stride.
template <typename Key>
struct StagedFixedArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr size_t key_offset = 0;
    static constexpr size_t fixed_arguments_offset = key_offset + sizeof(Key);

    explicit StagedFixedArgumentRecord(const AdaptiveArgumentLayout & layout)
        : stride(fixed_arguments_offset + layout.fixed_bytes)
    {
    }

    size_t bytes(const char *) const { return stride; }

    static std::pair<const char *, size_t> key(const char * record) { return {record + key_offset, sizeof(Key)}; }

    template <typename Table>
    static UInt64 hash(const Table & table, const char * record) { return table.hash(unalignedLoad<Key>(record + key_offset)); }

    /// The size of every record of the query.
    const size_t stride;
};

/// General records with a fixed-width key whose variable-size arguments are all `String`s: {fixed-size arguments, key,
/// then each `String` argument as `writeStagedString` stages it}, with no header. The key has the compile-time width,
/// and a record's size follows from the lengths.
template <typename Key>
struct StagedStringArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr size_t fixed_arguments_offset = 0;

    explicit StagedStringArgumentRecord(const AdaptiveArgumentLayout & layout)
        : key_offset(layout.fixed_bytes)
        , strings(layout.variable_fields.size())
    {
    }

    size_t bytes(const char * record) const
    {
        const char * value = record + key_offset + sizeof(Key);
        for (size_t i = 0; i < strings; ++i)
            readStagedString(value);
        return static_cast<size_t>(value - record);
    }

    std::pair<const char *, size_t> key(const char * record) const { return {record + key_offset, sizeof(Key)}; }

    template <typename Table>
    UInt64 hash(const Table & table, const char * record) const { return table.hash(unalignedLoad<Key>(record + key_offset)); }

    /// The key follows the fixed-size arguments, whose area has one size for the query.
    const size_t key_offset;
    /// The number of `String` arguments, which follow the key.
    const size_t strings;
};

/// General records otherwise: {UInt64 hash, UInt32 size, UInt32 key size, fixed-size arguments, key,
/// variable-size arguments}, the arguments laid out by `AdaptiveArgumentLayout`. `size` is the
/// whole padded record. A `String` argument is staged by `writeStagedString`; another variable-size argument is in its
/// column's serialization.
struct StagedArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr size_t fixed_arguments_offset = 16;

    explicit StagedArgumentRecord(const AdaptiveArgumentLayout & layout)
        : key_offset(fixed_arguments_offset + layout.fixed_bytes)
    {
    }

    static void writeHeader(char * record, UInt64 routing_hash, size_t bytes, size_t key_size)
    {
        unalignedStore<UInt64>(record, routing_hash);
        unalignedStore<UInt32>(record + 8, static_cast<UInt32>(bytes));
        unalignedStore<UInt32>(record + 12, static_cast<UInt32>(key_size));
    }

    static size_t bytes(const char * record) { return unalignedLoad<UInt32>(record + 8); }

    std::pair<const char *, size_t> key(const char * record) const { return {record + key_offset, unalignedLoad<UInt32>(record + 12)}; }

    template <typename Table>
    static UInt64 hash(const Table &, const char * record) { return unalignedLoad<UInt64>(record); }

    /// The key follows the fixed-size arguments, whose area has one size for the query.
    const size_t key_offset;
};

/// Calls `f` with the format of the records that `Key` and the layout produce: count records for an aggregation by a
/// lone `count`, key-only records for one without aggregate functions, which has no layout, and argument records
/// otherwise. For a fixed-width key the argument records have a fixed stride when every argument has a fixed size, and
/// no header when the variable-size arguments are all `String`s. The writer and every reader of the records dispatch
/// through it, so they agree on the format. It is inlined with `f` into the caller, so the caller's loops keep their
/// state in registers.
template <typename Key, typename F>
ALWAYS_INLINE decltype(auto) visitStagedRecordFormat(const AdaptiveArgumentLayout * layout, bool is_simple_count, F && f)
{
    if (is_simple_count)
        return f(StagedKeyRecord<Key, /*with_count_=*/true>{});
    if (!layout)
        return f(StagedKeyRecord<Key, /*with_count_=*/false>{});
    if constexpr (!adaptive_key_stages_bytes<Key>)
    {
        if (layout->variable_fields.empty())
            return f(StagedFixedArgumentRecord<Key>(*layout));
        if (layout->variable_fields_are_strings)
            return f(StagedStringArgumentRecord<Key>(*layout));
    }
    return f(StagedArgumentRecord(*layout));
}

}
