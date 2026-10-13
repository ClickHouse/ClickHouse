#pragma once

#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <vector>

#include <base/PackedStringRef.h>
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
    /// A single numeric grouping key used as an argument is read from the key's bytes in the record.
    std::optional<Field> key_field;
    size_t fixed_bytes = 0;
    /// One past the highest argument position, the size of the column vectors the positions index.
    size_t num_positions = 0;
};

/// See `AdaptiveArgumentLayout`. The producers stage the normalized argument columns (`LowCardinality` removed,
/// constant, sparse and replicated columns materialized), so the layout follows the types without `LowCardinality`.
std::unique_ptr<const AdaptiveArgumentLayout>
buildAdaptiveArgumentLayout(const Block & header, const ColumnNumbers & keys_positions, const ColumnNumbersList & aggregates_positions);

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
/// into the record: the records of a partition are freed only after every table holding its keys is converted or
/// written.
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
/// and written unaligned. Fixed-stride argument records need no padding; the other formats
/// keep their sizes a multiple of the narrowest header field.
///
/// The merge emplaces a staged key with its routing hash, the table's own hash of the key (see
/// `emplaceStagedKey`). A record of a byte-staged key, and a general record, starts with the
/// hash, because rehashing the key bytes would take a pass over them. A record of a fixed stride
/// has a fixed-width key and stores no hash: the merge rehashes the key in a few instructions,
/// which costs less than eight more bytes in a record of a dozen or two, written by the staging,
/// read by the merge, and written to disk and read back by a spill.
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

/// Count and key-only records: {[UInt64 hash,] [UInt32 count,] [UInt32 size,] key}. The count is
/// a contribution within one block, which a UInt32 holds. A key whose width varies is staged with
/// the hash and its UInt32 size; a fixed-width key has the compile-time width, so its records
/// have a fixed stride, and carries no hash.
template <typename Key, bool with_count>
struct StagedKeyRecord
{
    static constexpr bool variable_width = adaptive_key_stages_bytes<Key>;
    static constexpr size_t count_offset = variable_width ? sizeof(UInt64) : 0;
    static constexpr size_t size_offset = count_offset + (with_count ? sizeof(UInt32) : 0);
    static constexpr size_t key_offset = size_offset + (variable_width ? sizeof(UInt32) : 0);

    static size_t bytes(size_t key_size) { return alignStagedRecord(key_offset + key_size); }

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

    /// The routing hash of the record's key, which `table` hashes as the producer's table did.
    template <typename Table>
    static UInt64 hash([[maybe_unused]] const Table & table, const char * record)
    {
        if constexpr (variable_width)
            return unalignedLoad<UInt64>(record);
        else
            return table.hash(unalignedLoad<Key>(record + key_offset));
    }

    static UInt32 count(const char * record) { return unalignedLoad<UInt32>(record + count_offset); }

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
    static constexpr size_t key_offset = 0;
    static constexpr size_t arguments_offset = key_offset + sizeof(Key);

    static size_t bytes(size_t fixed_argument_bytes) { return arguments_offset + fixed_argument_bytes; }

    /// The routing hash of the record's key, which `table` hashes as the producer's table did.
    template <typename Table>
    static UInt64 hash(const Table & table, const char * record) { return table.hash(unalignedLoad<Key>(record + key_offset)); }
};

/// General records otherwise: {UInt64 hash, UInt32 size, UInt32 key size, fixed-size arguments, key,
/// variable-size arguments}, the arguments laid out by `AdaptiveArgumentLayout`. `size` is the
/// whole padded record.
struct StagedArgumentRecord
{
    static constexpr size_t header_bytes = 16;

    static void writeHeader(char * record, UInt64 routing_hash, size_t bytes, size_t key_size)
    {
        unalignedStore<UInt64>(record, routing_hash);
        unalignedStore<UInt32>(record + 8, static_cast<UInt32>(bytes));
        unalignedStore<UInt32>(record + 12, static_cast<UInt32>(key_size));
    }

    static UInt64 hash(const char * record) { return unalignedLoad<UInt64>(record); }
    static size_t bytes(const char * record) { return unalignedLoad<UInt32>(record + 8); }
    static size_t keySize(const char * record) { return unalignedLoad<UInt32>(record + 12); }
    static size_t keyOffset(size_t fixed_argument_bytes) { return header_bytes + fixed_argument_bytes; }
};

}
