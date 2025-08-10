#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include "Common/HashTable/FixedHashSet.h"
#include "Common/HashTable/HashMap.h"
#include "Common/HashTable/HashTable.h"
#include "Common/HashTable/StringHashMap.h"
#include "Common/HashTable/TwoLevelStringHashMap.h"
#include <Common/assert_cast.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include "Interpreters/AggregatedData.h"
#include <Interpreters/AggregationCommon.h>
#include <typeinfo>

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
    size_t max_threads;
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

    static HashMethodContextPtr createContext(const HashMethodContextSettings &) { return nullptr; }

    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row, Arena & pool) {
        return emplaceKey(data, row, pool, 9223372036854775807ll);
    }

    // 72610: data -- мап (или сет)
    // 72610: по row достается key_holder (указатель на ключ?) из выкачанных с диска данных
    // 72610: вызывается emplaceImpl, которая возвращает результат попытки вставки ключа в мап
    // 72610: если ключ не нужен благодаря оптимизации, корректно ли вернуть EmplaceResult(false)?
    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row, Arena & pool, size_t limit_length)
    {
        std::cout << "@@@@@@@@@@@@@@@@@ emplaceKey2 called" << std::endl;
        if constexpr (nullable)
        {
            if (isNullAt(row))
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

        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return emplaceImpl(key_holder, data, limit_length);
    }

    template <typename Data>
    ALWAYS_INLINE FindResult findKey(Data & data, size_t row, Arena & pool)
    {
        if constexpr (nullable)
        {
            if (isNullAt(row))
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

        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return findKeyImpl(keyHolderGetKey(key_holder), data);
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
            return null_map->getBool(row);
        }
        else
        {
            return false;
        }
    }

protected:
    Cache cache;
    const IColumn * null_map = nullptr;
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
            null_map = &checkAndGetColumn<ColumnNullable>(*column).getNullMapColumn();
    }

    template <typename T, typename = void>
    struct HasBegin : std::false_type {};
    
    template <typename T>
    struct HasBegin<T, std::void_t<decltype(std::declval<const T&>().begin())>> : std::true_type {};

    template <typename T, typename = void>
    struct HasForEachMapped : std::false_type {};
    
    template <typename T>
    struct HasForEachMapped<T, std::void_t<decltype(std::declval<const T&>().forEachMapped())>> : std::true_type {};

    // 72610: key_holder -- указатель на ключ
    // 72610: data -- мап (или сет)
    // 72610: emplaceImpl пытается вставить в мап и возвращает получилось ли и итератор, если да
    // 72610: если ключ не нужен благодаря оптимизации, корректно ли вернуть EmplaceResult(false)?
    template <typename Data, typename KeyHolder>
    ALWAYS_INLINE EmplaceResult emplaceImpl(KeyHolder & key_holder, Data & data, size_t limit_length)
    {
        std::cout << "\t@@@@@@ emplaceImpl called" << std::endl;
        std::cout << "\t@@@@@@ has_mapped: " << has_mapped << std::endl;
        std::cout << "\t@@@@@@ sizeof(has_mapped): " << sizeof(has_mapped) << std::endl;
        std::cout << "\t@@@@@@ consecutive_keys_optimization: " << consecutive_keys_optimization << std::endl;
        std::cout << "\t@@@@@@ sizeof(consecutive_keys_optimization): " << sizeof(consecutive_keys_optimization) << std::endl;
        if constexpr (consecutive_keys_optimization)
        {
            if (cache.found && cache.check(keyHolderGetKey(key_holder)))
            {
                std::cout << "\t@@@@@@ has_mapped: " << has_mapped << std::endl;
                if constexpr (has_mapped) {
                    // std::cout << "\t@@@@@@ cache.value.second: " << cache.value.second << std::endl;
                    return EmplaceResult(cache.value.second, cache.value.second, false);
                }
                else
                    return EmplaceResult(false);
            }
        }

        // StringHashMap<char *> qq;
        // TwoLevelStringHashMap<char *> ww;
        // DB::AggregationDataWithNullKey<StringHashMap<char *>> ee;
        // DB::AggregationDataWithNullKeyTwoLevel<TwoLevelStringHashMap<char *, Allocator<true, true>, DB::StringHashTableWithNullKey>> rr;
        // FixedHashSet<char8_t> tt;

        typename Data::LookupResult it;
        bool inserted = false;
        // std::cout << "\t@@@@@@ before emplace of key_holder=" << key_holder << ", data.size(): " << data.size() << std::endl;
        std::cout << "\t@@@@@@ before emplace data.size(): " << data.size() << std::endl;
        data.emplace(key_holder, it, inserted); // видимо попытка вставки нового ключа (по которому делает group by) в мапу
        std::cout << "\t@@@@@@ after emplace  data.size(): " << data.size() << std::endl;
        // std::cout << "\t@@@@@@ it->getMapped(): " << it->getMapped() << std::endl;
        std::cout << "\t@@@@@@ inserted: " << inserted << std::endl;
    
        // HashMapTable<StringRef, HashMapCellWithSavedHash<StringRef, char*, StringRefHash64, HashTableNoState>, StringRefHash64, HashTableGrowerWithPrecalculation<8ul>, Allocator<true, true> > hhh;
        // HashMapTable<StringRef, HashMapCellWithSavedHash<StringRef, char*, StringRefHash64,HashTableNoState>,StringRefHash64,HashTableGrowerWithPrecalculation<8ul>,Allocator<true, true>> hhh;
        // for (const auto& data_it : hhh) {
        //     (void)data_it;
        // }
        // (void)hhh;

        std::cout << "^^^^^^^^ Type of data: " << typeid(data).name() << std::endl;

        if (data.size() >= limit_length + 1) { // TODO do == and assert <=
            std::cout << "data.size() >= limit_length + 1" << std::endl;
            if constexpr (HasBegin<Data>::value) {
                auto min_key = key_holder;
                (void)min_key;
                for (const auto& data_it : data) {
                    (void)data_it;
                    // if (data_it.getKey() < min_key) {
                    //     min_key = data_it.getKey();
                    // }
                }
                // data.erase(min_key);
                // if (min_key == key_holder) {
                //     return EmplaceResult(false);
                // }    
            } else if constexpr(HasForEachMapped<Data>::value) {
                // data.forEachMapped([](auto el) {
                //     std::cout << "forEachMapped el: " << el << std::endl;
                // });
            }
        }

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

        // std::cout << "@@@@@@@ it->getMapped(): " << it->getMapped() << std::endl;
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
