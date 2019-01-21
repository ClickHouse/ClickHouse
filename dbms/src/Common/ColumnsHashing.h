#pragma once
#include <memory>
#include <Core/Defines.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/LRUCache.h>
#include <common/unaligned.h>
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


template <typename T>
struct MappedTraits
{
    using Type = void *;
    static Type getMapped(T &) { return nullptr; }
    static T & getKey(T & key) { return key; }
};

template <typename First, typename Second>
struct MappedTraits<PairNoInit<First, Second>>
{
    using Type = Second *;
    static Type getMapped(PairNoInit<First, Second> & value) { return &value.second; }
    static First & getKey(PairNoInit<First, Second> & value) { return value.first; }
};

template <typename Data>
struct HashTableTraits
{
    using Value = typename Data::value_type;
    using Mapped = typename MappedTraits<Value>::Type;

    static Mapped getMapped(Value & value) { return MappedTraits<Value>::getMapped(value); }
    static auto & getKey(Value & value) { return MappedTraits<Value>::getKey(value); }
};

template <typename Data, bool consecutive_keys_optimization_>
struct LastElementCache
{
    static constexpr bool consecutive_keys_optimization = consecutive_keys_optimization_;
    using Value = typename HashTableTraits<Data>::Value;
    Value value;
    bool empty = true;
    bool found = false;

    auto getMapped() { return HashTableTraits<Data>::getMapped(value); }
    auto & getKey() { return HashTableTraits<Data>::getKey(value); }
};

template <typename Data>
struct LastElementCache<Data, false>
{
    static constexpr bool consecutive_keys_optimization = false;
};

template <typename Data, typename Key, typename Cache>
inline ALWAYS_INLINE typename HashTableTraits<Data>::Value & emplaceKeyImpl(
    Key key, Data & data, bool & inserted, Cache & cache [[maybe_unused]])
{
    if constexpr (Cache::consecutive_keys_optimization)
    {
        if (!cache.empty && cache.found && cache.getKey() == key)
        {
            inserted = false;
            return cache.value;
        }
    }

    typename Data::iterator it;
    data.emplace(key, it, inserted);
    auto & value = *it;

    if constexpr (Cache::consecutive_keys_optimization)
    {
        cache.value = value;
        cache.empty = false;
        cache.found = true;
    }

    return value;
}

template <typename Data, typename Key, typename Cache>
inline ALWAYS_INLINE typename HashTableTraits<Data>::Mapped findKeyImpl(
    Key key, Data & data, bool & found, Cache & cache [[maybe_unused]])
{
    if constexpr (Cache::consecutive_keys_optimization)
    {
        if (!cache.empty && cache.getKey() == key)
        {
            found = cache.found;
            return found ? cache.getMapped() : nullptr;
        }
    }

    auto it = data.find(key);

    found = it != data.end();
    auto mapped = found ? HashTableTraits<Data>::getMapped(*it)
                        : nullptr;

    if constexpr (Cache::consecutive_keys_optimization)
    {
        if (found)
            cache.value = *it;
        else
            cache.getKey() = key;

        cache.empty = false;
        cache.found = found;
    }

    return mapped;
}


/// For the case where there is one numeric key.
template <typename TData, typename FieldType>    /// UInt8/16/32/64 for any type with corresponding bit width.
struct HashMethodOneNumber
{
    const char * vec;
    LastElementCache<TData, true> last_elem_cache;

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        vec = key_columns[0]->getRawData().data;
    }

    /// Creates context. Method is called once and result context is used in all threads.
    static HashMethodContextPtr createContext(const HashMethodContext::Settings &) { return nullptr; }

    FieldType getKey(size_t row) const { return unalignedLoad<FieldType>(vec + row * sizeof(FieldType)); }

    /// Emplace key into HashTable or HashMap. If Data is HashMap, returns ptr to value, otherwise nullptr.
    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped emplaceKey(
        Data & data, /// HashTable
        size_t row, /// From which row of the block insert the key
        bool & inserted,
        Arena & /*pool*/) /// For Serialized method, key may be placed in pool.
    {
        return HashTableTraits<Data>::getMapped(emplaceKeyImpl(getKey(row), data, inserted, last_elem_cache));
    }

    /// Find key into HashTable or HashMap. If Data is HashMap and key was found, returns ptr to value, otherwise nullptr.
    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped findKey(Data & data, size_t row, bool & found, Arena & /*pool*/)
    {
        return findKeyImpl(getKey(row), data, found, last_elem_cache);
    }

    /// Insert the key from the hash table into columns.
    template <typename Value>
    static void insertKeyIntoColumns(const Value & value, MutableColumns & key_columns, const Sizes & /*key_sizes*/)
    {
        static_cast<ColumnVectorHelper *>(key_columns[0].get())->insertRawData<sizeof(FieldType)>(reinterpret_cast<const char *>(&value.first));
    }

    /// Get hash value of row.
    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & /*pool*/)
    {
        return data.hash(getKey(row));
    }

    /// Get StringRef from value which can be inserted into column.
    template <typename Value>
    static StringRef getValueRef(const Value & value)
    {
        return StringRef(reinterpret_cast<const char *>(&value.first), sizeof(value.first));
    }

    /// Cache last result if key was inserted.
    template <typename Mapped>
    ALWAYS_INLINE void cacheData(size_t /*row*/, Mapped mapped)
    {
        *last_elem_cache.getMapped() = mapped;
    }

protected:
    template <typename Value>
    static ALWAYS_INLINE void onNewKey(Value & /*value*/, Arena & /*pool*/) {}
};


/// For the case where there is one string key.
template <typename TData>
struct HashMethodString
{
    const IColumn::Offset * offsets;
    const UInt8 * chars;

    LastElementCache<TData, true> last_elem_cache;

    HashMethodString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        const IColumn & column = *key_columns[0];
        const ColumnString & column_string = static_cast<const ColumnString &>(column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();
    }

    static HashMethodContextPtr createContext(const HashMethodContext::Settings &) { return nullptr; }

    StringRef getKey(size_t row) const { return StringRef(chars + offsets[row - 1], offsets[row] - offsets[row - 1] - 1); }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped emplaceKey(Data & data, size_t row, bool & inserted, Arena & pool)
    {
        auto & value = emplaceKeyImpl(getKey(row), data, inserted, last_elem_cache);
        if (inserted)
        {
            auto & key = HashTableTraits<Data>::getKey(value);
            if (key.size)
                key.data = pool.insert(key.data, key.size);
        }
        return HashTableTraits<Data>::getMapped(value);
    }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped findKey(Data & data, size_t row, bool & found, Arena & /*pool*/)
    {
        return findKeyImpl(getKey(row), data, found, last_elem_cache);
    }

    template <typename Value>
    static void insertKeyIntoColumns(const Value & value, MutableColumns & key_columns, const Sizes & /*key_sizes*/)
    {
        key_columns[0]->insertData(value.first.data, value.first.size);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & /*pool*/)
    {
        return data.hash(getKey(row));
    }

    template <typename Value>
    static StringRef getValueRef(const Value & value)
    {
        return StringRef(value.first.data, value.first.size);
    }

    template <typename Mapped>
    ALWAYS_INLINE void cacheData(size_t /*row*/, Mapped mapped)
    {
        *last_elem_cache.getMapped() = mapped;
    }

protected:
    template <typename Value>
    static ALWAYS_INLINE void onNewKey(Value & value, Arena & pool)
    {
        if (value.first.size)
            value.first.data = pool.insert(value.first.data, value.first.size);
    }
};


/// For the case where there is one fixed-length string key.
template <typename TData>
struct HashMethodFixedString
{
    size_t n;
    const ColumnFixedString::Chars * chars;

    LastElementCache<TData, true> last_elem_cache;

    HashMethodFixedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        const IColumn & column = *key_columns[0];
        const ColumnFixedString & column_string = static_cast<const ColumnFixedString &>(column);
        n = column_string.getN();
        chars = &column_string.getChars();
    }

    static HashMethodContextPtr createContext(const HashMethodContext::Settings &) { return nullptr; }

    StringRef getKey(size_t row) const { return StringRef(&(*chars)[row * n], n); }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped emplaceKey(Data & data, size_t row, bool & inserted, Arena & pool)
    {
        auto & value = emplaceKeyImpl(getKey(row), data, inserted, last_elem_cache);
        if (inserted)
        {
            auto & key = HashTableTraits<Data>::getKey(value);
            key.data = pool.insert(key.data, key.size);
        }
        return HashTableTraits<Data>::getMapped(value);
    }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped findKey(Data & data, size_t row, bool & found, Arena & /*pool*/)
    {
        return findKeyImpl(getKey(row), data, found, last_elem_cache);
    }

    template <typename Value>
    static void insertKeyIntoColumns(const Value & value, MutableColumns & key_columns, const Sizes & /*key_sizes*/)
    {
        key_columns[0]->insertData(value.first.data, value.first.size);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & /*pool*/)
    {
        return data.hash(getKey(row));
    }

    template <typename Value>
    static StringRef getValueRef(const Value & value)
    {
        return StringRef(value.first.data, value.first.size);
    }

    template <typename Mapped>
    ALWAYS_INLINE void cacheData(size_t /*row*/, Mapped mapped)
    {
        *last_elem_cache.getMapped() = mapped;
    }

protected:
    template <typename Value>
    static ALWAYS_INLINE void onNewKey(Value & value, Arena & pool)
    {
        value.first.data = pool.insert(value.first.data, value.first.size);
    }
};


/// Cache stores dictionaries and saved_hash per dictionary key.
class LowCardinalityDictionaryCache : public HashMethodContext
{
public:
    /// Will assume that dictionaries with same hash has the same keys.
    /// Just in case, check that they have also the same size.
    struct DictionaryKey
    {
        UInt128 hash;
        UInt64 size;

        bool operator== (const DictionaryKey & other) const { return hash == other.hash && size == other.size; }
    };

    struct DictionaryKeyHash
    {
        size_t operator()(const DictionaryKey & key) const
        {
            SipHash hash;
            hash.update(key.hash.low);
            hash.update(key.hash.high);
            hash.update(key.size);
            return hash.get64();
        }
    };

    struct CachedValues
    {
        /// Store ptr to dictionary to be sure it won't be deleted.
        ColumnPtr dictionary_holder;
        /// Hashes for dictionary keys.
        const UInt64 * saved_hash = nullptr;
    };

    using CachedValuesPtr = std::shared_ptr<CachedValues>;

    explicit LowCardinalityDictionaryCache(const HashMethodContext::Settings & settings) : cache(settings.max_threads) {}

    CachedValuesPtr get(const DictionaryKey & key) { return cache.get(key); }
    void set(const DictionaryKey & key, const CachedValuesPtr & mapped) { cache.set(key, mapped); }

private:
    using Cache = LRUCache<DictionaryKey, CachedValues, DictionaryKeyHash>;
    Cache cache;
};

/// Single low cardinality column.
template <typename SingleColumnMethod, bool use_cache>
struct HashMethodSingleLowCardinalityColumn : public SingleColumnMethod
{
    using Base = SingleColumnMethod;

    static HashMethodContextPtr createContext(const HashMethodContext::Settings & settings)
    {
        return std::make_shared<LowCardinalityDictionaryCache>(settings);
    }

    ColumnRawPtrs key_columns;
    const IColumn * positions = nullptr;
    size_t size_of_index_type = 0;

    /// saved hash is from current column or from cache.
    const UInt64 * saved_hash = nullptr;
    /// Hold dictionary in case saved_hash is from cache to be sure it won't be deleted.
    ColumnPtr dictionary_holder;

    /// Cache AggregateDataPtr for current column in order to decrease the number of hash table usages.
    PaddedPODArray<AggregateDataPtr> aggregate_data_cache;

    /// If initialized column is nullable.
    bool is_nullable = false;

    static const ColumnLowCardinality & getLowCardinalityColumn(const IColumn * low_cardinality_column)
    {
        auto column = typeid_cast<const ColumnLowCardinality *>(low_cardinality_column);
        if (!column)
            throw Exception("Invalid aggregation key type for HashMethodSingleLowCardinalityColumn method. "
                            "Excepted LowCardinality, got " + column->getName(), ErrorCodes::LOGICAL_ERROR);
        return *column;
    }

    HashMethodSingleLowCardinalityColumn(
        const ColumnRawPtrs & key_columns_low_cardinality, const Sizes & key_sizes, const HashMethodContextPtr & context)
        : Base({getLowCardinalityColumn(key_columns_low_cardinality[0]).getDictionary().getNestedNotNullableColumn().get()}, key_sizes, context)
    {
        auto column = &getLowCardinalityColumn(key_columns_low_cardinality[0]);

        if (!context)
            throw Exception("Cache wasn't created for HashMethodSingleLowCardinalityColumn",
                            ErrorCodes::LOGICAL_ERROR);

        LowCardinalityDictionaryCache * cache;
        if constexpr (use_cache)
        {
            cache = typeid_cast<LowCardinalityDictionaryCache *>(context.get());
            if (!cache)
            {
                const auto & cached_val = *context;
                throw Exception("Invalid type for HashMethodSingleLowCardinalityColumn cache: "
                                + demangle(typeid(cached_val).name()), ErrorCodes::LOGICAL_ERROR);
            }
        }

        auto * dict = column->getDictionary().getNestedNotNullableColumn().get();
        is_nullable = column->getDictionary().nestedColumnIsNullable();
        key_columns = {dict};
        bool is_shared_dict = column->isSharedDictionary();

        typename LowCardinalityDictionaryCache::DictionaryKey dictionary_key;
        typename LowCardinalityDictionaryCache::CachedValuesPtr cached_values;

        if (is_shared_dict)
        {
            dictionary_key = {column->getDictionary().getHash(), dict->size()};
            if constexpr (use_cache)
                cached_values = cache->get(dictionary_key);
        }

        if (cached_values)
        {
            saved_hash = cached_values->saved_hash;
            dictionary_holder = cached_values->dictionary_holder;
        }
        else
        {
            saved_hash = column->getDictionary().tryGetSavedHash();
            dictionary_holder = column->getDictionaryPtr();

            if constexpr (use_cache)
            {
                if (is_shared_dict)
                {
                    cached_values = std::make_shared<typename LowCardinalityDictionaryCache::CachedValues>();
                    cached_values->saved_hash = saved_hash;
                    cached_values->dictionary_holder = dictionary_holder;

                    cache->set(dictionary_key, cached_values);
                }
            }
        }

        AggregateDataPtr default_data = nullptr;
        aggregate_data_cache.assign(key_columns[0]->size(), default_data);

        size_of_index_type = column->getSizeOfIndexType();
        positions = column->getIndexesPtr().get();
    }

    ALWAYS_INLINE size_t getIndexAt(size_t row) const
    {
        switch (size_of_index_type)
        {
            case sizeof(UInt8): return static_cast<const ColumnUInt8 *>(positions)->getElement(row);
            case sizeof(UInt16): return static_cast<const ColumnUInt16 *>(positions)->getElement(row);
            case sizeof(UInt32): return static_cast<const ColumnUInt32 *>(positions)->getElement(row);
            case sizeof(UInt64): return static_cast<const ColumnUInt64 *>(positions)->getElement(row);
            default: throw Exception("Unexpected size of index type for low cardinality column.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    /// Get the key from the key columns for insertion into the hash table.
    ALWAYS_INLINE auto getKey(size_t row) const
    {
        return Base::getKey(getIndexAt(row));
    }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped emplaceKey(Data & data, size_t row_, bool & inserted, Arena & pool)
    {
        size_t row = getIndexAt(row_);

        if (is_nullable && row == 0)
        {
            inserted = !data.hasNullKeyData();
            data.hasNullKeyData() = true;
            return &data.getNullKeyData();
        }

        if constexpr (use_cache)
        {
            if (aggregate_data_cache[row])
            {
                inserted = false;
                return &aggregate_data_cache[row];
            }
        }

        Sizes key_sizes;
        auto key = getKey(row_);

        typename Data::iterator it;
        if (saved_hash)
            data.emplace(key, it, inserted, saved_hash[row]);
        else
            data.emplace(key, it, inserted);

        if (inserted)
            Base::onNewKey(*it, pool);
        else if constexpr (use_cache)
            aggregate_data_cache[row] = it->second;

        return HashTableTraits<Data>::getMapped(*it);
    }

    ALWAYS_INLINE bool isNullAt(size_t i)
    {
        if (!is_nullable)
            return false;

        return getIndexAt(i) == 0;
    }

    template <typename Mapped>
    ALWAYS_INLINE void cacheData(size_t i, Mapped mapped)
    {
        size_t row = getIndexAt(i);
        aggregate_data_cache[row] = mapped;
    }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped findFromRow(Data & data, size_t row_, bool & found, Arena &)
    {
        size_t row = getIndexAt(row_);

        if (is_nullable && row == 0)
            return data.hasNullKeyData() ? &data.getNullKeyData() : nullptr;

        if constexpr (use_cache)
        {
            if (aggregate_data_cache[row])
                return &aggregate_data_cache[row];
        }

        auto key = getKey(row_);

        typename Data::iterator it;
        if (saved_hash)
            it = data.find(key, saved_hash[row]);
        else
            it = data.find(key);

        found = it != data.end();
        if constexpr (use_cache)
        {
            if (found)
                aggregate_data_cache[row] = it->second;
        }

        return typename HashTableTraits<Data>::getMapped(*it);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & pool)
    {
        row = getIndexAt(row);
        if (saved_hash)
            return saved_hash[row];

        return Base::getHash(data, row, pool);
    }

    template <typename Value>
    static void insertKeyIntoColumns(const Value & value, MutableColumns & key_columns_low_cardinality, const Sizes & /*key_sizes*/)
    {
        auto ref = Base::getValueRef(value);
        static_cast<ColumnLowCardinality *>(key_columns_low_cardinality[0].get())->insertData(ref.data, ref.size);
    }
};


namespace columns_hashing_impl
{

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
    void init(const ColumnRawPtrs & key_columns)
    {
        null_maps.reserve(key_columns.size());
        actual_columns.reserve(key_columns.size());

        for (const auto & col : key_columns)
        {
            if (col->isColumnNullable())
            {
                const auto & nullable_col = static_cast<const ColumnNullable &>(*col);
                actual_columns.push_back(&nullable_col.getNestedColumn());
                null_maps.push_back(&nullable_col.getNullMapColumn());
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
                const auto & null_map = static_cast<const ColumnUInt8 &>(*null_maps[k]).getData();
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
    void init(const ColumnRawPtrs & columns) { actual_columns = columns; }

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

// Optional mask for low cardinality columns.
template <bool has_low_cardinality>
struct LowCardinalityKeys
{
    ColumnRawPtrs nested_columns;
    ColumnRawPtrs positions;
    Sizes position_sizes;
};

template <>
struct LowCardinalityKeys<false> {};

/// For the case where all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename TData, bool has_nullable_keys_ = false, bool has_low_cardinality_ = false>
struct HashMethodKeysFixed : private columns_hashing_impl::BaseStateKeysFixed<typename TData::key_type, has_nullable_keys_>
{
    using Key = typename TData::key_type;

    static constexpr bool has_nullable_keys = has_nullable_keys_;
    static constexpr bool has_low_cardinality = has_low_cardinality_;

    LowCardinalityKeys<has_low_cardinality> low_cardinality_keys;
    Sizes key_sizes;
    size_t keys_size;

    LastElementCache<TData, true> last_elem_cache;

    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys>;

    HashMethodKeysFixed(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, const HashMethodContextPtr &)
        : key_sizes(std::move(key_sizes)), keys_size(key_columns.size())
    {
        if constexpr (has_low_cardinality)
        {
            low_cardinality_keys.nested_columns.resize(key_columns.size());
            low_cardinality_keys.positions.assign(key_columns.size(), nullptr);
            low_cardinality_keys.position_sizes.resize(key_columns.size());
            for (size_t i = 0; i < key_columns.size(); ++i)
            {
                if (auto * low_cardinality_col = typeid_cast<const ColumnLowCardinality *>(key_columns[i]))
                {
                    low_cardinality_keys.nested_columns[i] = low_cardinality_col->getDictionary().getNestedColumn().get();
                    low_cardinality_keys.positions[i] = &low_cardinality_col->getIndexes();
                    low_cardinality_keys.position_sizes[i] = low_cardinality_col->getSizeOfIndexType();
                }
                else
                    low_cardinality_keys.nested_columns[i] = key_columns[i];
            }
        }

        Base::init(key_columns);
    }

    static HashMethodContextPtr createContext(const HashMethodContext::Settings &) { return nullptr; }

    ALWAYS_INLINE Key getKey(size_t row) const
    {
        if (has_nullable_keys)
        {
            auto bitmap = Base::createBitmap(row);
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, bitmap);
        }
        else
        {
            if constexpr (has_low_cardinality)
                return packFixed<Key, true>(row, keys_size, low_cardinality_keys.nested_columns, key_sizes,
                                            &low_cardinality_keys.positions, &low_cardinality_keys.position_sizes);

            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes);
        }
    }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped emplaceKey(Data & data, size_t row, bool & inserted, Arena & /*pool*/)
    {
        return HashTableTraits<Data>::getMapped(emplaceKeyImpl(getKey(row), data, inserted, last_elem_cache));
    }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped findKey(Data & data, size_t row, bool & found, Arena & /*pool*/)
    {
        return findKeyImpl(getKey(row), data, found, last_elem_cache);
    }

    template <typename Value>
    static StringRef getValueRef(const Value & value)
    {
        return StringRef(value.first.data, value.first.size);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & /*pool*/)
    {
        return data.hash(getKey(row));
    }

    template <typename Mapped>
    ALWAYS_INLINE void cacheData(size_t /*row*/, Mapped mapped)
    {
        *last_elem_cache.getMapped() = mapped;
    }
};

/** Hash by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData>
struct HashMethodSerialized
{
    ColumnRawPtrs key_columns;
    size_t keys_size;
    LastElementCache<TData, false> last_elem_cache;

    HashMethodSerialized(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
        : key_columns(key_columns), keys_size(key_columns.size()) {}

    static HashMethodContextPtr createContext(const HashMethodContext::Settings &) { return nullptr; }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped emplaceKey(Data & data, size_t row, bool & inserted, Arena & pool)
    {
        auto key = getKey(row, pool);
        auto & value = emplaceKeyImpl(key, data, inserted, last_elem_cache);
        if (!inserted)
            pool.rollback(key.size);

        return HashTableTraits<Data>::getMapped(value);
    }

    template <typename Data>
    ALWAYS_INLINE typename HashTableTraits<Data>::Mapped findKey(Data & data, size_t row, bool & found, Arena & pool)
    {
        auto key = getKey(row, pool);
        auto mapped = findKeyImpl(key, data, found, last_elem_cache);
        pool.rollback(key.size);

        return mapped;
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & pool)
    {
        auto key = getKey(row, pool);
        auto hash = data.hash(key);
        pool.rollback(key.size);

        return hash;
    }

    template <typename Mapped>
    ALWAYS_INLINE void cacheData(size_t /*row*/, Mapped /*mapped*/) {}

protected:
    ALWAYS_INLINE StringRef getKey(size_t row, Arena & pool) const
    {
        return serializeKeysToPoolContiguous(row, keys_size, key_columns, pool);
    }
};

}
}
