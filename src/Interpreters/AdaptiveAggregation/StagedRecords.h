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
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Core/Block_fwd.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AggregationCommon.h>

/// The representation of the records the adaptive aggregation stages for its merge: their formats, the layout of
/// the aggregate arguments they carry, and the reconstruction of their keys. Producers write the records and the
/// merge reads them, so both sides follow the definitions here.

namespace DB
{

/// The record layout of the aggregate arguments that general payloads stage, fixed for the query by the header.
/// Fixed-size arguments open the record at their offsets and use the `RowDataStore` representation: a
/// `Nullable` field is a null byte followed by the value. The drain rebuilds each of these arguments with
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
    /// Whether every variable-size argument, if any, is staged as a `String` (see `isStagedString`), so that a record's
    /// size follows from the lengths in it (see `StagedFixedKeyStringArgumentRecord` and
    /// `StagedByteKeyStringArgumentRecord`).
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

/// The two-level bucket table of `Method`, into which the merge emplaces the staged keys. The staging and the merge
/// both take from it the form in which the records store the hash (see `AdaptiveStagedHash`), so they agree on it.
template <typename Method>
using AdaptiveBucketTable = typename Method::Data::Impl;

/// Reads a byte-staged key back from its record. The key is rebuilt pointing into the record, so a table holding it
/// needs the record until the table is converted or written: the merge frees a partition's records only after that,
/// and copies the keys of the records it reads back from a spill, whose blocks go before.
template <typename Key>
Key ALWAYS_INLINE readStagedKey(const char * key_pos, size_t key_size, [[maybe_unused]] size_t routing_hash)
{
    if constexpr (std::is_same_v<Key, std::string_view>)
    {
        return std::string_view(key_pos, key_size);
    }
    else
    {
        static_assert(std::is_same_v<Key, PackedStringRef>);
        /// The staged routing hash IS the packed key's cached content hash
        /// (`DefaultHash<PackedStringRef>` returns it), so the rebuild reuses it instead of
        /// re-hashing the key bytes; `build` consults the functor only for lengths that
        /// store a hash, which is exactly the range the staged hash was derived from.
        return PackedStringRef::build(key_pos, key_size, [routing_hash](const char *, size_t) { return static_cast<UInt32>(routing_hash); });
    }
}

/// A length in a record: the size of a byte-staged key or of a `String` argument. One byte holds a length below
/// `staged_length_escape`, which nearly every length is; a longer one is that byte followed by the length as a
/// `UInt32`. The `UInt32` holds every length, because the buffer rejects a record whose chunk does not fit 32 bits
/// (see `PartitionedRecordBuffer::append`). A `VarUInt` takes a second byte from 128 on, which keys such as URLs often
/// reach, and its read needs a bound and a check of it for every byte; this length takes one comparison.
inline constexpr UInt8 staged_length_escape = 255;

inline size_t ALWAYS_INLINE stagedLengthBytes(size_t length)
{
    return length < staged_length_escape ? 1 : 1 + sizeof(UInt32);
}

/// Writes `length` at `to` and returns the position after it.
inline char * ALWAYS_INLINE writeStagedLength(char * to, size_t length)
{
    chassert(length <= std::numeric_limits<UInt32>::max());
    if (length < staged_length_escape)
    {
        *to = static_cast<char>(length);
        return to + 1;
    }
    *to = static_cast<char>(staged_length_escape);
    unalignedStore<UInt32>(to + 1, static_cast<UInt32>(length));
    return to + 1 + sizeof(UInt32);
}

/// Reads the length at `from` and moves `from` past it.
inline size_t ALWAYS_INLINE readStagedLength(const char *& from)
{
    const auto first = static_cast<UInt8>(*from);
    if (first < staged_length_escape) [[likely]]
    {
        ++from;
        return first;
    }
    const size_t length = unalignedLoad<UInt32>(from + 1);
    from += 1 + sizeof(UInt32);
    return length;
}

/// A staged `String` argument: its length and the bytes, copied straight from and to the column without its
/// serialization's virtual calls.
inline size_t ALWAYS_INLINE stagedStringBytes(std::string_view value)
{
    return stagedLengthBytes(value.size()) + value.size();
}

/// Writes a staged `String` at `to` and returns the position after it.
inline char * ALWAYS_INLINE writeStagedString(char * to, std::string_view value)
{
    char * bytes = writeStagedLength(to, value.size());
    memcpy(bytes, value.data(), value.size());
    return bytes + value.size();
}

/// Reads the staged `String` at `from` and moves `from` past it.
inline std::string_view ALWAYS_INLINE readStagedString(const char *& from)
{
    const size_t size = readStagedLength(from);
    const std::string_view value(from, size);
    from += size;
    return value;
}

/// Whether a variable-size argument is staged by `writeStagedString` rather than in its column's serialization. The
/// writer and the drain both decide by it, so they agree on the encoding of every field.
inline bool isStagedString(const AdaptiveArgumentLayout::Field & field)
{
    return isString(field.type);
}

/// The routing hash in the records of a byte-staged key, with which the merge emplaces the key: rehashing the key
/// bytes would take a pass over them. A record stores what the hash function of the bucket table computes beyond what
/// the key determines. The hash functions of byte keys mostly compute 32 bits, and their 64-bit value is the 32 bits
/// either zero-extended or repeated in the high half, as the key's size and last byte decide, so a record stores the 32
/// bits and the merge restores the rest. The staging checks that the restored hash is the computed one. Each form below
/// is named after the hash function whose value it stores.

/// Any other hash function: the whole 64-bit hash.
struct StagedFullHash
{
    using Stored = UInt64;
    static Stored store(UInt64 hash) { return hash; }
    static UInt64 restore(Stored stored, std::string_view) { return stored; }
};

/// `DefaultHash<PackedStringRef>`: the packed key's 32-bit content hash, zero-extended to 64 bits.
struct StagedPackedStringHash
{
    using Stored = UInt32;
    static Stored store(UInt64 hash) { return static_cast<UInt32>(hash); }
    static UInt64 restore(Stored stored, std::string_view) { return stored; }
};

/// `CRC32Hash`: the CRC repeats in the high half for a key of at least 8 bytes.
struct StagedCRC32Hash
{
    using Stored = UInt32;
    static Stored store(UInt64 hash) { return static_cast<UInt32>(hash); }
    static UInt64 restore(Stored stored, std::string_view key)
    {
        return key.size() >= 8 ? (UInt64{stored} << 32) | stored : stored;
    }
};

/// `StringHashTableHash`, the hash of `StringHashTable`: a key of up to 24 bytes that does not end with a zero byte
/// takes the 32-bit CRC of its fixed-size form, and the other keys take `CRC32Hash` (see
/// `StringHashTable::dispatchOnKeyClass`).
struct StagedStringHashTableHash
{
    using Stored = UInt32;
    static Stored store(UInt64 hash) { return static_cast<UInt32>(hash); }
    static UInt64 restore(Stored stored, std::string_view key)
    {
        const size_t size = key.size();
        const bool repeated = size >= 8 && (size > 24 || key[size - 1] == 0);
        return repeated ? (UInt64{stored} << 32) | stored : stored;
    }
};

namespace staged_records_detail
{
    template <typename Key, typename Table>
    auto stagedHashOf()
    {
        /// The CRC forms hold on the platforms with a CRC instruction, where `StringViewHash` is `CRC32Hash`. A table
        /// of another hash function stores the whole hash.
        constexpr bool crc = std::is_base_of_v<CRC32Hash, StringViewHash>;
        if constexpr (std::is_same_v<Key, PackedStringRef> && std::is_base_of_v<DefaultHash<PackedStringRef>, Table>)
            return StagedPackedStringHash{};
        else if constexpr (std::is_same_v<Key, std::string_view> && crc && requires(Table & table) { table.emptyStringSlot(); })
            return StagedStringHashTableHash{};
        else if constexpr (std::is_same_v<Key, std::string_view> && crc && std::is_base_of_v<CRC32Hash, Table>)
            return StagedCRC32Hash{};
        else
            return StagedFullHash{};
    }
}

/// The form in which the records of `Method`'s byte-staged keys store their hash.
template <typename Method>
using AdaptiveStagedHash = decltype(staged_records_detail::stagedHashOf<AdaptiveRecordKey<Method>, AdaptiveBucketTable<Method>>());

/// The bytes of a fixed-width key that its records store. A key packed from several columns fills only its first
/// `bytes`, its columns' values and the null flags of nullable ones, and leaves the rest zero in either byte order (see
/// `packFixed`), so the records keep those bytes and the reader zeroes the rest again. Any other fixed-width key stores
/// all its bytes.
template <typename Key>
struct StagedFixedKey
{
    explicit StagedFixedKey(size_t bytes_)
        : bytes(bytes_)
    {
        chassert(bytes > 0 && bytes <= sizeof(Key));
        memset(&mask, 0, sizeof(mask));
        memset(&mask, 0xFF, bytes);
    }

    /// Reads the key at `pos`, loading `sizeof(Key)` bytes: past the stored ones it reads what follows them, which ends
    /// in padding both in a chunk and in a block of records read back from a spill (a `PaddedPODArray`).
    Key ALWAYS_INLINE read(const char * pos) const { return static_cast<Key>(unalignedLoad<Key>(pos) & mask); }

    size_t bytes;
    Key mask;
};

/// The stored width of `Method`'s fixed-width keys (see `StagedFixedKey`), where `packed_key_bytes` is the sum of the
/// sizes of the key columns. Only `AggregationMethodKeysFixed` packs its keys, and the method is chosen only where they
/// fit in its key together with their null flags.
template <typename Method>
size_t stagedFixedKeyBytes(size_t packed_key_bytes)
{
    using Key = AdaptiveRecordKey<Method>;
    if constexpr (requires { Method::has_nullable_keys; })
        return packed_key_bytes + (Method::has_nullable_keys ? std::tuple_size_v<KeysNullMap<Key>> : 0);
    else
        return sizeof(Key);
}

/// The staged record formats. A record never straddles two chunks, and nothing in it points outside the record, so a
/// chunk can go to disk and come back byte for byte. Fields are read and written unaligned, and records follow each
/// other without padding. The fixed-size aggregate arguments open an argument record at the offsets of
/// `AdaptiveArgumentLayout`, so each lies at one offset in all the records of the query, from which the drain rebuilds
/// it with one `IColumn::fillFromRowStorePtrs`.
///
/// A record of a fixed-width key stores no hash: the merge rehashes the key in a few instructions, which costs less
/// than the bytes of a hash in a record of a dozen or two, written by the staging, read by the merge, and written to
/// disk and read back by a spill. A record of a byte-staged key stores the hash in the form of its `Hash` (see
/// `AdaptiveStagedHash`) and the size of the key as a staged length.
///
/// Every format offers the readers the same interface: `bytes(record)`, the size of the record; `key(record)`, the
/// position and the size of its stored key; `readKey(record, hash)`, the key as the table takes it; and
/// `hash(table, record)`, the routing hash of the key, which `table` hashes as the producer's table did. A count format
/// also has `count(record)`. The writer reserves the size that `bytesForKey` (key formats) or `bytesFor` (argument
/// formats) gives and fills the record from `writeHeader(record, hash, key_size, ...)`, whose last argument is the
/// count of a count record or the size of an argument record. It returns the position where the writer stores the
/// `keyBytes(key_size)` bytes of the key. The formats of a fixed-width key ignore `key_size`: they know the stored
/// width. An argument format with `with_variable_arguments` may hold variable-size arguments after the key; the others
/// hold none, so the writer compiles their loop without the code for them. An argument format with `fixed_width_key`
/// stores the key's `fixed_key.bytes` at `key_offset` in every record, from which the drain rebuilds a key that is also
/// an argument (see `AdaptiveArgumentLayout::key_field`).

/// Count and key-only records of a byte-staged key: {hash, [UInt8 count,] key size, key}. The count is a contribution
/// within one block; the staging splits a larger one into several records, which the merge adds up.
template <typename Key, typename Hash, bool with_count_>
struct StagedByteKeyRecord
{
    static constexpr bool with_count = with_count_;
    static constexpr bool with_arguments = false;
    using StoredHash = typename Hash::Stored;
    static constexpr size_t count_offset = sizeof(StoredHash);
    static constexpr size_t size_offset = count_offset + (with_count ? 1 : 0);

    static size_t bytesForKey(size_t key_size) { return size_offset + stagedLengthBytes(key_size) + key_size; }

    static size_t keyBytes(size_t key_size) { return key_size; }

    static char * writeHeader(char * record, UInt64 routing_hash, size_t key_size, UInt8 count)
    {
        unalignedStore<StoredHash>(record, Hash::store(routing_hash));
        if constexpr (with_count)
            record[count_offset] = static_cast<char>(count);
        return writeStagedLength(record + size_offset, key_size);
    }

    static std::pair<const char *, size_t> key(const char * record)
    {
        const char * pos = record + size_offset;
        const size_t size = readStagedLength(pos);
        return {pos, size};
    }

    static size_t bytes(const char * record)
    {
        const auto [pos, size] = key(record);
        return static_cast<size_t>(pos + size - record);
    }

    static UInt64 routingHash(const char * record)
    {
        const auto [pos, size] = key(record);
        return Hash::restore(unalignedLoad<StoredHash>(record), std::string_view(pos, size));
    }

    template <typename Table>
    static UInt64 hash(const Table &, const char * record) { return routingHash(record); }

    static Key readKey(const char * record, UInt64 routing_hash)
    {
        const auto [pos, size] = key(record);
        return readStagedKey<Key>(pos, size, routing_hash);
    }

    static UInt8 count(const char * record) requires with_count { return static_cast<UInt8>(record[count_offset]); }
};

/// Count and key-only records of a fixed-width key: {[UInt8 count,] key}, of one size for the query. The count is
/// split as in `StagedByteKeyRecord`.
template <typename Key, bool with_count_>
struct StagedFixedKeyRecord
{
    static constexpr bool with_count = with_count_;
    static constexpr bool with_arguments = false;
    static constexpr size_t key_offset = with_count ? 1 : 0;

    explicit StagedFixedKeyRecord(size_t key_bytes)
        : fixed_key(key_bytes)
        , stride(key_offset + key_bytes)
    {
    }

    size_t bytesForKey(size_t) const { return stride; }

    size_t keyBytes(size_t) const { return fixed_key.bytes; }

    static char * writeHeader(char * record, UInt64, size_t, UInt8 count)
    {
        if constexpr (with_count)
            record[0] = static_cast<char>(count);
        return record + key_offset;
    }

    std::pair<const char *, size_t> key(const char * record) const { return {record + key_offset, fixed_key.bytes}; }

    size_t bytes(const char *) const { return stride; }

    Key readKey(const char * record, UInt64) const { return fixed_key.read(record + key_offset); }

    template <typename Table>
    UInt64 hash(const Table & table, const char * record) const { return table.hash(readKey(record, 0)); }

    static UInt8 count(const char * record) requires with_count { return static_cast<UInt8>(record[0]); }

    const StagedFixedKey<Key> fixed_key;
    /// The size of every record of the query.
    const size_t stride;
};

/// Argument records of a fixed-width key with only fixed-size arguments: {fixed-size arguments, key}, of one size for
/// the query.
template <typename Key>
struct StagedFixedKeyFixedArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr bool with_variable_arguments = false;
    static constexpr bool fixed_width_key = true;

    StagedFixedKeyFixedArgumentRecord(const AdaptiveArgumentLayout & layout, size_t key_bytes)
        : key_offset(layout.fixed_bytes)
        , fixed_key(key_bytes)
        , stride(key_offset + key_bytes)
    {
    }

    size_t bytesFor(size_t, size_t) const { return stride; }

    size_t keyBytes(size_t) const { return fixed_key.bytes; }

    char * writeHeader(char * record, UInt64, size_t, size_t) const { return record + key_offset; }

    std::pair<const char *, size_t> key(const char * record) const { return {record + key_offset, fixed_key.bytes}; }

    size_t bytes(const char *) const { return stride; }

    Key readKey(const char * record, UInt64) const { return fixed_key.read(record + key_offset); }

    template <typename Table>
    UInt64 hash(const Table & table, const char * record) const { return table.hash(readKey(record, 0)); }

    /// The key follows the fixed-size arguments, whose area has one size for the query.
    const size_t key_offset;
    const StagedFixedKey<Key> fixed_key;
    /// The size of every record of the query.
    const size_t stride;
};

/// Argument records of a fixed-width key whose variable-size arguments are all `String`s: {fixed-size arguments, key,
/// each `String` argument as `writeStagedString` stages it}. A record's size follows from the lengths.
template <typename Key>
struct StagedFixedKeyStringArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr bool with_variable_arguments = true;
    static constexpr bool fixed_width_key = true;

    StagedFixedKeyStringArgumentRecord(const AdaptiveArgumentLayout & layout, size_t key_bytes)
        : key_offset(layout.fixed_bytes)
        , fixed_key(key_bytes)
        , strings(layout.variable_fields.size())
    {
    }

    size_t bytesFor(size_t, size_t variable_bytes) const { return key_offset + fixed_key.bytes + variable_bytes; }

    size_t keyBytes(size_t) const { return fixed_key.bytes; }

    char * writeHeader(char * record, UInt64, size_t, size_t) const { return record + key_offset; }

    std::pair<const char *, size_t> key(const char * record) const { return {record + key_offset, fixed_key.bytes}; }

    size_t bytes(const char * record) const
    {
        const char * value = record + key_offset + fixed_key.bytes;
        for (size_t i = 0; i < strings; ++i)
            readStagedString(value);
        return static_cast<size_t>(value - record);
    }

    Key readKey(const char * record, UInt64) const { return fixed_key.read(record + key_offset); }

    template <typename Table>
    UInt64 hash(const Table & table, const char * record) const { return table.hash(readKey(record, 0)); }

    const size_t key_offset;
    const StagedFixedKey<Key> fixed_key;
    /// The number of `String` arguments, which follow the key.
    const size_t strings;
};

/// Argument records of a fixed-width key with another variable-size argument: {fixed-size arguments, UInt32 record
/// size, key, variable-size arguments}. The serialization of such an argument keeps its length to itself, so the record
/// stores its own size.
template <typename Key>
struct StagedFixedKeyGeneralArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr bool with_variable_arguments = true;
    static constexpr bool fixed_width_key = true;

    StagedFixedKeyGeneralArgumentRecord(const AdaptiveArgumentLayout & layout, size_t key_bytes)
        : size_offset(layout.fixed_bytes)
        , key_offset(size_offset + sizeof(UInt32))
        , fixed_key(key_bytes)
    {
    }

    size_t bytesFor(size_t, size_t variable_bytes) const { return key_offset + fixed_key.bytes + variable_bytes; }

    size_t keyBytes(size_t) const { return fixed_key.bytes; }

    char * writeHeader(char * record, UInt64, size_t, size_t record_bytes) const
    {
        unalignedStore<UInt32>(record + size_offset, static_cast<UInt32>(record_bytes));
        return record + key_offset;
    }

    std::pair<const char *, size_t> key(const char * record) const { return {record + key_offset, fixed_key.bytes}; }

    size_t bytes(const char * record) const { return unalignedLoad<UInt32>(record + size_offset); }

    Key readKey(const char * record, UInt64) const { return fixed_key.read(record + key_offset); }

    template <typename Table>
    UInt64 hash(const Table & table, const char * record) const { return table.hash(readKey(record, 0)); }

    const size_t size_offset;
    const size_t key_offset;
    const StagedFixedKey<Key> fixed_key;
};

/// Argument records of a byte-staged key whose variable-size arguments, if any, are all `String`s: {fixed-size
/// arguments, hash, key size, key, each `String` argument as `writeStagedString` stages it}.
template <typename Key, typename Hash>
struct StagedByteKeyStringArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr bool with_variable_arguments = true;
    static constexpr bool fixed_width_key = false;
    using StoredHash = typename Hash::Stored;

    explicit StagedByteKeyStringArgumentRecord(const AdaptiveArgumentLayout & layout)
        : hash_offset(layout.fixed_bytes)
        , size_offset(hash_offset + sizeof(StoredHash))
        , strings(layout.variable_fields.size())
    {
    }

    size_t bytesFor(size_t key_size, size_t variable_bytes) const
    {
        return size_offset + stagedLengthBytes(key_size) + key_size + variable_bytes;
    }

    static size_t keyBytes(size_t key_size) { return key_size; }

    char * writeHeader(char * record, UInt64 routing_hash, size_t key_size, size_t) const
    {
        unalignedStore<StoredHash>(record + hash_offset, Hash::store(routing_hash));
        return writeStagedLength(record + size_offset, key_size);
    }

    std::pair<const char *, size_t> key(const char * record) const
    {
        const char * pos = record + size_offset;
        const size_t size = readStagedLength(pos);
        return {pos, size};
    }

    size_t bytes(const char * record) const
    {
        const auto [pos, size] = key(record);
        const char * value = pos + size;
        for (size_t i = 0; i < strings; ++i)
            readStagedString(value);
        return static_cast<size_t>(value - record);
    }

    UInt64 routingHash(const char * record) const
    {
        const auto [pos, size] = key(record);
        return Hash::restore(unalignedLoad<StoredHash>(record + hash_offset), std::string_view(pos, size));
    }

    template <typename Table>
    UInt64 hash(const Table &, const char * record) const { return routingHash(record); }

    Key readKey(const char * record, UInt64 routing_hash) const
    {
        const auto [pos, size] = key(record);
        return readStagedKey<Key>(pos, size, routing_hash);
    }

    /// The hash follows the fixed-size arguments, whose area has one size for the query.
    const size_t hash_offset;
    const size_t size_offset;
    /// The number of `String` arguments, which follow the key.
    const size_t strings;
};

/// Argument records of a byte-staged key with another variable-size argument: {fixed-size arguments, hash, UInt32
/// record size, key size, key, variable-size arguments}. The record stores its own size, as in
/// `StagedFixedKeyGeneralArgumentRecord`.
template <typename Key, typename Hash>
struct StagedByteKeyGeneralArgumentRecord
{
    static constexpr bool with_count = false;
    static constexpr bool with_arguments = true;
    static constexpr bool with_variable_arguments = true;
    static constexpr bool fixed_width_key = false;
    using StoredHash = typename Hash::Stored;

    explicit StagedByteKeyGeneralArgumentRecord(const AdaptiveArgumentLayout & layout)
        : hash_offset(layout.fixed_bytes)
        , record_size_offset(hash_offset + sizeof(StoredHash))
        , size_offset(record_size_offset + sizeof(UInt32))
    {
    }

    size_t bytesFor(size_t key_size, size_t variable_bytes) const
    {
        return size_offset + stagedLengthBytes(key_size) + key_size + variable_bytes;
    }

    static size_t keyBytes(size_t key_size) { return key_size; }

    char * writeHeader(char * record, UInt64 routing_hash, size_t key_size, size_t record_bytes) const
    {
        unalignedStore<StoredHash>(record + hash_offset, Hash::store(routing_hash));
        unalignedStore<UInt32>(record + record_size_offset, static_cast<UInt32>(record_bytes));
        return writeStagedLength(record + size_offset, key_size);
    }

    std::pair<const char *, size_t> key(const char * record) const
    {
        const char * pos = record + size_offset;
        const size_t size = readStagedLength(pos);
        return {pos, size};
    }

    size_t bytes(const char * record) const { return unalignedLoad<UInt32>(record + record_size_offset); }

    UInt64 routingHash(const char * record) const
    {
        const auto [pos, size] = key(record);
        return Hash::restore(unalignedLoad<StoredHash>(record + hash_offset), std::string_view(pos, size));
    }

    template <typename Table>
    UInt64 hash(const Table &, const char * record) const { return routingHash(record); }

    Key readKey(const char * record, UInt64 routing_hash) const
    {
        const auto [pos, size] = key(record);
        return readStagedKey<Key>(pos, size, routing_hash);
    }

    const size_t hash_offset;
    const size_t record_size_offset;
    const size_t size_offset;
};

/// Calls `f` with the format of the records of `Method`'s keys: count records for an aggregation by a lone `count`,
/// key-only records for one without aggregate functions, which has no layout, and argument records otherwise. The
/// argument records of a fixed-width key have one size when every argument has a fixed size, and those of any key need
/// no stored record size when the variable-size arguments are all `String`s. `packed_key_bytes` is the sum of the
/// sizes of the key columns (see `stagedFixedKeyBytes`). The writer and every reader of the records dispatch through
/// it, so they agree on the format. It is inlined with `f` into the caller, so the caller's loops keep their state in
/// registers.
template <typename Method, typename F>
ALWAYS_INLINE decltype(auto) visitStagedRecordFormat(
    const AdaptiveArgumentLayout * layout, bool is_simple_count, size_t packed_key_bytes, F && f)
{
    using Key = AdaptiveRecordKey<Method>;
    if constexpr (adaptive_key_stages_bytes<Key>)
    {
        using Hash = AdaptiveStagedHash<Method>;
        if (is_simple_count)
            return f(StagedByteKeyRecord<Key, Hash, /*with_count_=*/true>{});
        if (!layout)
            return f(StagedByteKeyRecord<Key, Hash, /*with_count_=*/false>{});
        if (layout->variable_fields_are_strings)
            return f(StagedByteKeyStringArgumentRecord<Key, Hash>(*layout));
        return f(StagedByteKeyGeneralArgumentRecord<Key, Hash>(*layout));
    }
    else
    {
        const size_t key_bytes = stagedFixedKeyBytes<Method>(packed_key_bytes);
        if (is_simple_count)
            return f(StagedFixedKeyRecord<Key, /*with_count_=*/true>(key_bytes));
        if (!layout)
            return f(StagedFixedKeyRecord<Key, /*with_count_=*/false>(key_bytes));
        if (layout->variable_fields.empty())
            return f(StagedFixedKeyFixedArgumentRecord<Key>(*layout, key_bytes));
        if (layout->variable_fields_are_strings)
            return f(StagedFixedKeyStringArgumentRecord<Key>(*layout, key_bytes));
        return f(StagedFixedKeyGeneralArgumentRecord<Key>(*layout, key_bytes));
    }
}

}
