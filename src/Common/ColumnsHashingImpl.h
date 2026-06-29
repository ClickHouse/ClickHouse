#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Interpreters/KeysNullMap.h>
#include <Common/HashTable/Prefetching.h>

#include <cstring>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ColumnsHashing
{

struct HashMethodContextSettings
{
    size_t max_threads{};
    bool serialize_string_with_zero_byte = false;

    /// Whether software prefetching of hash-table buckets is enabled for this run.
    /// Controls the precomputed-hash prefetch path in `HashMethodSerialized<prealloc=true>`
    /// (mirrors `enable_software_prefetch_in_aggregation`).
    bool enable_prefetch = true;
    /// Threshold on the hash table's buffer size below which prefetching is skipped
    /// because the table fits into caches. Zero disables the threshold.
    size_t min_bytes_for_prefetch = 0;
};

/// Generic context for HashMethod. Context is shared between multiple threads, all methods must be thread-safe.
/// Is used for caching.
class HashMethodContext
{
public:
    virtual ~HashMethodContext() = default;

    using Settings = HashMethodContextSettings;
};

using HashMethodContextPtr = std::shared_ptr<HashMethodContext>;

struct LastElementCacheStats
{
    UInt64 hits = 0;
    UInt64 misses = 0;

    void update(size_t num_tries, size_t num_misses)
    {
        hits += num_tries - num_misses;
        misses += num_misses;
    }
};

namespace columns_hashing_impl
{

struct LastElementCacheBase
{
    bool empty = true;
    bool found = false;
    UInt64 misses = 0;

    void onNewValue(bool is_found)
    {
        empty = false;
        found = is_found;
        ++misses;
    }

    bool hasOnlyOneValue() const { return found && misses == 1; }
};

template <typename Value, bool nullable> struct LastElementCache;

template <typename Value>
struct LastElementCache<Value, true> : public LastElementCacheBase
{
    Value value{};
    bool is_null = false;

    template <typename Key>
    bool check(const Key & key) const { return !is_null && value.first == key; }

    bool check(const Value & rhs) const { return !is_null && value == rhs; }
};

template <typename Value>
struct LastElementCache<Value, false> : public LastElementCacheBase
{
    Value value{};

    template <typename Key>
    bool check(const Key & key) const { return value.first == key; }

    bool check(const Value & rhs) const { return value == rhs; }
};

template <typename Mapped>
class EmplaceResultImpl
{
    Mapped & value;
    Mapped & cached_value;
    bool inserted;

public:
    EmplaceResultImpl(Mapped & value_, Mapped & cached_value_, bool inserted_)
            : value(value_), cached_value(cached_value_), inserted(inserted_) {}

    bool isInserted() const { return inserted; }
    auto & getMapped() const { return value; }

    void setMapped(const Mapped & mapped)
    {
        cached_value = mapped;
        value = mapped;
    }
};

template <>
class EmplaceResultImpl<void>
{
    bool inserted;

public:
    explicit EmplaceResultImpl(bool inserted_) : inserted(inserted_) {}
    bool isInserted() const { return inserted; }
};

/// FindResult optionally may contain pointer to value and offset in hashtable buffer.
/// Only bool found is required.
/// So we will have 4 different specializations for FindResultImpl
class FindResultImplBase
{
    bool found;

public:
    explicit FindResultImplBase(bool found_) : found(found_) {}
    bool isFound() const { return found; }
};

template <bool need_offset = false>
class FindResultImplOffsetBase
{
public:
    constexpr static bool has_offset = need_offset;
    explicit FindResultImplOffsetBase(size_t /* off */) {}
};

template <>
class FindResultImplOffsetBase<true>
{
    size_t offset;
public:
    constexpr static bool has_offset = true;

    explicit FindResultImplOffsetBase(size_t off) : offset(off) {}
    ALWAYS_INLINE size_t getOffset() const { return offset; }
};

template <typename Mapped, bool need_offset = false>
class FindResultImpl : public FindResultImplBase, public FindResultImplOffsetBase<need_offset>
{
    Mapped * value;

public:
    FindResultImpl()
        : FindResultImplBase(false), FindResultImplOffsetBase<need_offset>(0) // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)  intentionally allow uninitialized value here
    {}

    FindResultImpl(Mapped * value_, bool found_, size_t off)
        : FindResultImplBase(found_), FindResultImplOffsetBase<need_offset>(off), value(value_) {}
    Mapped & getMapped() const { return *value; }  /// NOLINT(clang-analyzer-core.uninitialized.UndefReturn)
};

template <bool need_offset>
class FindResultImpl<void, need_offset> : public FindResultImplBase, public FindResultImplOffsetBase<need_offset>
{
public:
    FindResultImpl(bool found_, size_t off) : FindResultImplBase(found_), FindResultImplOffsetBase<need_offset>(off) {}
};

template <typename Derived, typename Value, typename Mapped, bool consecutive_keys_optimization, bool need_offset = false, bool nullable = false>
class HashMethodBase
{
public:
    using EmplaceResult = EmplaceResultImpl<Mapped>;
    using FindResult = FindResultImpl<Mapped, need_offset>;
    static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
    using Cache = LastElementCache<Value, nullable>;
    static constexpr bool has_range_check = false;
    static constexpr bool has_pre_computed_hashes = false;

    static HashMethodContextPtr createContext(const HashMethodContextSettings &) { return nullptr; }

    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row, Arena & pool)
    {
        if constexpr (nullable)
        {
            /// Per-block fast path: if a one-time `memchr` at construction proved that the block
            /// contains no nulls, the compiler can fold this branch away entirely. Otherwise we
            /// load the cached `null_map_data` directly, avoiding the virtual `IColumn::getBool`.
            if (!block_has_no_nulls && null_map_data[row]) [[unlikely]]
            {
                if constexpr (consecutive_keys_optimization)
                {
                    if (!cache.is_null)
                    {
                        cache.onNewValue(true);
                        cache.is_null = true;
                    }
                }

                bool has_null_key = data.hasNullKeyData();
                data.hasNullKeyData() = true;

                if constexpr (has_mapped)
                    return EmplaceResult(data.getNullKeyData(), data.getNullKeyData(), !has_null_key);
                else
                    return EmplaceResult(!has_null_key);
            }
        }

        auto & derived = static_cast<Derived &>(*this);
        auto key_holder = derived.getKeyHolder(row, pool);
        if constexpr (Derived::has_pre_computed_hashes)
        {
            /// Single gate in the hot path: `precomputed_hashes_initialized` is set to `true`
            /// after the first call (regardless of whether hashes were actually computed), so
            /// subsequent rows only do the one `can_precompute_hashes` check below.
            if (!derived.precomputed_hashes_initialized) [[unlikely]]
                derived.initPrecomputedHashes(data, row);

            if (derived.can_precompute_hashes)
            {
                if (row == derived.calibration_row)
                    derived.prefetch_look_ahead = derived.prefetching->calcPrefetchLookAhead();
                const auto & hashes = derived.precomputed_hashes;
                if (row + derived.prefetch_look_ahead < hashes.size())
                    data.prefetchByHash(hashes[row + derived.prefetch_look_ahead]);
                return emplaceImpl<false>(key_holder, data, hashes[row]);
            }
        }
        return emplaceImpl<true>(key_holder, data, 0);
    }

    template <typename Data>
    ALWAYS_INLINE FindResult findKey(Data & data, size_t row, Arena & pool)
    {
        if constexpr (nullable)
        {
            /// See note in `emplaceKey` about `block_has_no_nulls` and the cached `null_map_data`.
            if (!block_has_no_nulls && null_map_data[row]) [[unlikely]]
            {
                bool has_null_key = data.hasNullKeyData();

                if constexpr (consecutive_keys_optimization)
                {
                    if (!cache.is_null)
                    {
                        cache.onNewValue(has_null_key);
                        cache.is_null = true;
                    }
                }

                if constexpr (has_mapped)
                    return FindResult(&data.getNullKeyData(), has_null_key, 0);
                else
                    return FindResult(has_null_key, 0);
            }
        }

        auto & derived = static_cast<Derived &>(*this);
        if constexpr (Derived::has_pre_computed_hashes)
        {
            /// See note in `emplaceKey`: single gate via `precomputed_hashes_initialized`.
            if (!derived.precomputed_hashes_initialized) [[unlikely]]
                derived.initPrecomputedHashes(data, row);

            if (derived.can_precompute_hashes)
            {
                if (row == derived.calibration_row)
                    derived.prefetch_look_ahead = derived.prefetching->calcPrefetchLookAhead();
                const auto & hashes = derived.precomputed_hashes;
                if (row + derived.prefetch_look_ahead < hashes.size())
                    data.prefetchByHash(hashes[row + derived.prefetch_look_ahead]);

                if (data.isEmptyCell(hashes[row]))
                {
                    if constexpr (has_mapped)
                        return FindResult(nullptr, false, 0);
                    else
                        return FindResult(false, 0);
                }
            }
        }

        if constexpr (Derived::has_range_check)
        {
            auto [key_holder, in_range] = static_cast<const Derived &>(*this).getKeyHolderInRange(row, pool);
            if (!in_range)
            {
                if constexpr (has_mapped)
                    return FindResult(nullptr, false, 0);
                else
                    return FindResult(false, 0);
            }
            return findKeyImpl(keyHolderGetKey(key_holder), data);
        }
        else
        {
            auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
            return findKeyImpl(keyHolderGetKey(key_holder), data);
        }
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & pool)
    {
        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return data.hash(keyHolderGetKey(key_holder));
    }

    ALWAYS_INLINE void resetCache()
    {
        if constexpr (consecutive_keys_optimization)
        {
            cache.empty = true;
            cache.found = false;
            cache.misses = 0;
        }
    }

    ALWAYS_INLINE bool hasOnlyOneValueSinceLastReset() const
    {
        if constexpr (consecutive_keys_optimization)
            return cache.hasOnlyOneValue();
        return false;
    }

    ALWAYS_INLINE UInt64 getCacheMissesSinceLastReset() const
    {
        if constexpr (consecutive_keys_optimization)
            return cache.misses;
        return 0;
    }

    ALWAYS_INLINE bool isNullAt(size_t row) const
    {
        if constexpr (nullable)
        {
            /// Use the cached raw pointer; avoids the virtual `IColumn::getBool` per call.
            return !block_has_no_nulls && null_map_data[row];
        }
        else
        {
            return false;
        }
    }

protected:
    Cache cache;
    /// Cached raw pointer to the null map bytes for the current block. Each element is 0/1.
    /// Bypasses the virtual `IColumn` dispatch on the per-row hot path in `emplaceKey` / `findKey`.
    const UInt8 * null_map_data = nullptr;
    /// Per-block flag set by a single `memchr` at construction time. When true, every row in the
    /// block has a zero null-map byte, so the per-row null check can be statically skipped.
    bool block_has_no_nulls = true;
    bool has_null_data = false;

    /// column argument only for nullable column
    explicit HashMethodBase(const IColumn * column = nullptr)
    {
        if constexpr (consecutive_keys_optimization)
        {
            if constexpr (has_mapped)
            {
                /// Init PairNoInit elements.
                cache.value.second = Mapped();
                cache.value.first = {};
            }
            else
                cache.value = Value();
        }

        if constexpr (nullable)
        {
            const auto & null_map_column = checkAndGetColumn<ColumnNullable>(*column).getNullMapColumn();
            const auto & null_map_container = null_map_column.getData();
            null_map_data = null_map_container.data();
            /// Scan the null map once per block. `PaddedPODArray<UInt8>` stores 0/1 bytes, so
            /// finding a single 0x01 byte is enough to know the block contains a null. We use
            /// `memchr` which is typically vectorized in libc and amortizes well for blocks of
            /// the usual aggregation size (`max_block_size` = 65505). For tiny blocks the cost
            /// is dominated by the function-call overhead, but the per-row payload saves a
            /// virtual call and a branch, so the break-even is small.
            const size_t size = null_map_container.size();
            block_has_no_nulls = (size == 0) || (std::memchr(null_map_data, 1, size) == nullptr);
        }
    }

    template <bool compute_hash, typename Data, typename KeyHolder>
    ALWAYS_INLINE EmplaceResult emplaceImpl(KeyHolder & key_holder, Data & data, [[maybe_unused]] size_t hash_value)
    {
        if constexpr (consecutive_keys_optimization)
        {
            if (cache.found && cache.check(keyHolderGetKey(key_holder)))
            {
                if constexpr (has_mapped)
                    return EmplaceResult(cache.value.second, cache.value.second, false);
                else
                    return EmplaceResult(false);
            }
        }

        typename Data::LookupResult it;
        bool inserted = false;

        if constexpr (compute_hash)
            data.emplace(key_holder, it, inserted);
        else
            data.emplace(key_holder, it, inserted, hash_value);

        [[maybe_unused]] Mapped * cached = nullptr;
        if constexpr (has_mapped)
            cached = &it->getMapped();

        if constexpr (has_mapped)
        {
            if (inserted)
            {
                new (&it->getMapped()) Mapped();
            }
        }

        if constexpr (consecutive_keys_optimization)
        {
            cache.onNewValue(true);

            if constexpr (nullable)
                cache.is_null = false;

            if constexpr (has_mapped)
            {
                cache.value.first = it->getKey();
                cache.value.second = it->getMapped();
                cached = &cache.value.second;
            }
            else
            {
                cache.value = it->getKey();
            }
        }

        if constexpr (has_mapped)
            return EmplaceResult(it->getMapped(), *cached, inserted);
        else
            return EmplaceResult(inserted);
    }

    template <typename Data, typename Key>
    ALWAYS_INLINE FindResult findKeyImpl(Key key, Data & data)
    {
        if constexpr (consecutive_keys_optimization)
        {
            /// It's possible to support such combination, but code will became more complex.
            /// Now there's not place where we need this options enabled together
            static_assert(!FindResult::has_offset, "`consecutive_keys_optimization` and `has_offset` are conflicting options");
            if (likely(!cache.empty) && cache.check(key))
            {
                if constexpr (has_mapped)
                    return FindResult(&cache.value.second, cache.found, 0);
                else
                    return FindResult(cache.found, 0);
            }
        }

        auto it = data.find(key);

        if constexpr (consecutive_keys_optimization)
        {
            cache.onNewValue(it != nullptr);

            if constexpr (nullable)
                cache.is_null = false;

            if constexpr (has_mapped)
            {
                cache.value.first = key;
                if (it)
                    cache.value.second = it->getMapped();
            }
            else
            {
                cache.value = key;
            }
        }

        size_t offset = 0;
        if constexpr (FindResult::has_offset)
            offset = it ? data.offsetInternal(it) : 0;

        if constexpr (has_mapped)
            return FindResult(it ? &it->getMapped() : nullptr, it != nullptr, offset);
        else
            return FindResult(it != nullptr, offset);
    }
};

template <typename T>
struct MappedCache : public PaddedPODArray<T> {};

template <>
struct MappedCache<void> {};


/// This class is designed to provide the functionality that is required for
/// supporting nullable keys in HashMethodKeysFixed. If there are
/// no nullable keys, this class is merely implemented as an empty shell.
template <typename Key, bool has_nullable_keys>
class BaseStateKeysFixed;

/// Case where nullable keys are supported.
template <typename Key>
class BaseStateKeysFixed<Key, true>
{
protected:
    explicit BaseStateKeysFixed(const ColumnRawPtrs & key_columns)
    {
        null_maps.reserve(key_columns.size());
        actual_columns.reserve(key_columns.size());

        for (const auto & col : key_columns)
        {
            if (const auto * nullable_col = checkAndGetColumn<ColumnNullable>(col))
            {
                actual_columns.push_back(&nullable_col->getNestedColumn());
                null_maps.push_back(&nullable_col->getNullMapColumn());
            }
            else
            {
                actual_columns.push_back(col);
                null_maps.push_back(nullptr);
            }
        }
    }

    /// Return the columns which actually contain the values of the keys.
    /// For a given key column, if it is nullable, we return its nested
    /// column. Otherwise we return the key column itself.
    const ColumnRawPtrs & getActualColumns() const
    {
        return actual_columns;
    }

    /// Create a bitmap that indicates whether, for a particular row,
    /// a key column bears a null value or not.
    KeysNullMap<Key> createBitmap(size_t row) const
    {
        KeysNullMap<Key> bitmap{};

        for (size_t k = 0; k < null_maps.size(); ++k)
        {
            if (null_maps[k] != nullptr)
            {
                const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_maps[k]).getData();
                if (null_map[row] == 1)
                {
                    size_t bucket = k / 8;
                    size_t offset = k % 8;
                    bitmap[bucket] |= UInt8(1) << offset;
                }
            }
        }

        return bitmap;
    }

private:
    ColumnRawPtrs actual_columns;
    ColumnRawPtrs null_maps;
};

/// Case where nullable keys are not supported.
template <typename Key>
class BaseStateKeysFixed<Key, false>
{
protected:
    explicit BaseStateKeysFixed(const ColumnRawPtrs & columns) : actual_columns(columns) {}

    const ColumnRawPtrs & getActualColumns() const { return actual_columns; }

    KeysNullMap<Key> createBitmap(size_t) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Internal error: calling createBitmap() for non-nullable keys is forbidden");
    }

private:
    ColumnRawPtrs actual_columns;
};

}

}

}
