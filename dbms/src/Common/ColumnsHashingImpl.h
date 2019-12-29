#pragma once

#include <Columns/IColumn.h>
#include <Common/assert_cast.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Interpreters/AggregationCommon.h>


namespace DB
{

namespace ColumnsHashing
{

/// Generic context for HashMethod. Context is shared between multiple threads, all methods must be thread-safe.
/// Is used for caching.
class HashMethodContext
{
public:
    virtual ~HashMethodContext() = default;

    struct Settings
    {
        size_t max_threads;
    };
};

using HashMethodContextPtr = std::shared_ptr<HashMethodContext>;


namespace columns_hashing_impl
{

template <typename Value, bool consecutive_keys_optimization_>
struct LastElementCache
{
    static constexpr bool consecutive_keys_optimization = consecutive_keys_optimization_;
    Value value;
    bool empty = true;
    bool found = false;

    bool check(const Value & value_) { return !empty && value == value_; }

    template <typename Key>
    bool check(const Key & key) { return !empty && value.first == key; }
};

template <typename Data>
struct LastElementCache<Data, false>
{
    static constexpr bool consecutive_keys_optimization = false;
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

template <typename Mapped>
class FindResultImpl
{
    Mapped * value;
    bool found;

public:
    FindResultImpl(Mapped * value_, bool found_) : value(value_), found(found_) {}
    bool isFound() const { return found; }
    Mapped & getMapped() const { return *value; }
};

template <>
class FindResultImpl<void>
{
    bool found;

public:
    explicit FindResultImpl(bool found_) : found(found_) {}
    bool isFound() const { return found; }
};

template <typename Derived, typename Value, typename Mapped, bool consecutive_keys_optimization>
class HashMethodBase
{
public:
    using EmplaceResult = EmplaceResultImpl<Mapped>;
    using FindResult = FindResultImpl<Mapped>;
    static constexpr bool has_mapped = !std::is_same<Mapped, void>::value;
    using Cache = LastElementCache<Value, consecutive_keys_optimization>;

    static HashMethodContextPtr createContext(const HashMethodContext::Settings &) { return nullptr; }

    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row, Arena & pool)
    {
        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return emplaceImpl(key_holder, data);
    }

    template <typename Data>
    ALWAYS_INLINE FindResult findKey(Data & data, size_t row, Arena & pool)
    {
        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return findKeyImpl(keyHolderGetKey(key_holder), data);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & pool)
    {
        auto key_holder = static_cast<Derived &>(*this).getKeyHolder(row, pool);
        return data.hash(keyHolderGetKey(key_holder));
    }

protected:
    Cache cache;

    HashMethodBase()
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
    }

    template <typename Data, typename KeyHolder>
    ALWAYS_INLINE EmplaceResult emplaceImpl(KeyHolder & key_holder, Data & data)
    {
        if constexpr (Cache::consecutive_keys_optimization)
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
        data.emplace(key_holder, it, inserted);

        [[maybe_unused]] Mapped * cached = nullptr;
        if constexpr (has_mapped)
            cached = &it->getMapped();

        if (inserted)
        {
            if constexpr (has_mapped)
            {
                new (&it->getMapped()) Mapped();
            }
        }

        if constexpr (consecutive_keys_optimization)
        {
            cache.found = true;
            cache.empty = false;

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
        if constexpr (Cache::consecutive_keys_optimization)
        {
            if (cache.check(key))
            {
                if constexpr (has_mapped)
                    return FindResult(&cache.value.second, cache.found);
                else
                    return FindResult(cache.found);
            }
        }

        auto it = data.find(key);

        if constexpr (consecutive_keys_optimization)
        {
            cache.found = it != nullptr;
            cache.empty = false;

            if constexpr (has_mapped)
            {
                cache.value.first = key;
                if (it)
                {
                    cache.value.second = it->getMapped();
                }
            }
            else
            {
                cache.value = key;
            }
        }

        if constexpr (has_mapped)
            return FindResult(it ? &it->getMapped() : nullptr, it != nullptr);
        else
            return FindResult(it != nullptr);
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
    BaseStateKeysFixed(const ColumnRawPtrs & key_columns)
    {
        null_maps.reserve(key_columns.size());
        actual_columns.reserve(key_columns.size());

        for (const auto & col : key_columns)
        {
            if (auto * nullable_col = checkAndGetColumn<ColumnNullable>(col))
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
    inline const ColumnRawPtrs & getActualColumns() const
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
    BaseStateKeysFixed(const ColumnRawPtrs & columns) : actual_columns(columns) {}

    const ColumnRawPtrs & getActualColumns() const { return actual_columns; }

    KeysNullMap<Key> createBitmap(size_t) const
    {
        throw Exception{"Internal error: calling createBitmap() for non-nullable keys"
                        " is forbidden", ErrorCodes::LOGICAL_ERROR};
    }

private:
    ColumnRawPtrs actual_columns;
};

}

}

}
