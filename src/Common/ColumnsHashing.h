#pragma once

#include <Common/HashTable/HashTable.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ColumnsHashingImpl.h>
#include <Common/Arena.h>
#include <Common/LRUCache.h>
#include <Common/assert_cast.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <base/unaligned.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>

#include <Core/Defines.h>
#include <functional>
#include <memory>
#include <cassert>
#include <stddef.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ColumnsHashing
{

/// For the case when there is one numeric key.
/// UInt8/16/32/64 for any type with corresponding bit width.
template <typename Value, typename Mapped, typename FieldType, bool use_cache = true, bool need_offset = false>
struct HashMethodOneNumber
    : public columns_hashing_impl::HashMethodBase<HashMethodOneNumber<Value, Mapped, FieldType, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodOneNumber<Value, Mapped, FieldType, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    const char * vec;
    FieldType const_value;
    std::function<FieldType(size_t row)> get_key_holder_impl;

    /// If the keys of a fixed length then key_sizes contains their lengths, empty otherwise.
    HashMethodOneNumber(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        vec = key_columns[0]->getRawData().data;
        if (isColumnConst(*key_columns[0]))
        {
            const_value = unalignedLoad<FieldType>(vec);
            get_key_holder_impl = [this](size_t /*row*/) { return const_value; };
        }
        else
        {
            get_key_holder_impl = [this](size_t row) { return unalignedLoad<FieldType>(vec + row * sizeof(FieldType)); };
        }
    }

    explicit HashMethodOneNumber(const IColumn * column)
    {
        vec = column->getRawData().data;
        if (isColumnConst(*column))
        {
            const_value = unalignedLoad<FieldType>(vec);
            get_key_holder_impl = [this](size_t /*row*/) { return const_value; };
        }
        else
        {
            get_key_holder_impl = [this](size_t row) { return unalignedLoad<FieldType>(vec + row * sizeof(FieldType)); };
        }
    }

    /// Creates context. Method is called once and result context is used in all threads.
    using Base::createContext; /// (const HashMethodContext::Settings &) -> HashMethodContextPtr

    /// Emplace key into HashTable or HashMap. If Data is HashMap, returns ptr to value, otherwise nullptr.
    /// Data is a HashTable where to insert key from column's row.
    /// For Serialized method, key may be placed in pool.
    using Base::emplaceKey; /// (Data & data, size_t row, Arena & pool) -> EmplaceResult

    /// Find key into HashTable or HashMap. If Data is HashMap and key was found, returns ptr to value, otherwise nullptr.
    using Base::findKey;  /// (Data & data, size_t row, Arena & pool) -> FindResult

    /// Get hash value of row.
    using Base::getHash; /// (const Data & data, size_t row, Arena & pool) -> size_t

    /// Is used for default implementation in HashMethodBase.
    FieldType getKeyHolder(size_t row, Arena &) const { return get_key_holder_impl(row); }

    const FieldType * getKeyData() const { return reinterpret_cast<const FieldType *>(vec); }
};


/// For the case when there is one string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true, bool use_cache = true, bool need_offset = false>
struct HashMethodString
    : public columns_hashing_impl::HashMethodBase<HashMethodString<Value, Mapped, place_string_to_arena, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodString<Value, Mapped, place_string_to_arena, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    const IColumn::Offset * offsets;
    const UInt8 * chars;
    std::function<StringRef(size_t row)> get_key_holder_impl;

    HashMethodString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        const IColumn * column = key_columns[0];
        bool column_is_const = isColumnConst(*column);
        if (column_is_const)
            column = &assert_cast<const ColumnConst &>(*column).getDataColumn();

        const ColumnString & column_string = assert_cast<const ColumnString &>(*column);
        offsets = column_string.getOffsets().data();
        chars = column_string.getChars().data();

        if (column_is_const)
            get_key_holder_impl = [this](size_t /*row*/) { return StringRef(chars, offsets[0] - 1); };
        else
            get_key_holder_impl = [this](size_t row) { return StringRef(chars + offsets[row - 1], offsets[row] - offsets[row - 1] - 1); };
    }

    auto getKeyHolder(ssize_t row, [[maybe_unused]] Arena & pool) const
    {
        StringRef key = get_key_holder_impl(row);
        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
};


/// For the case when there is one fixed-length string key.
template <typename Value, typename Mapped, bool place_string_to_arena = true, bool use_cache = true, bool need_offset = false>
struct HashMethodFixedString
    : public columns_hashing_impl::
          HashMethodBase<HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodFixedString<Value, Mapped, place_string_to_arena, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    size_t n;
    const ColumnFixedString::Chars * chars;
    std::function<StringRef(size_t row)> get_key_holder_impl;

    HashMethodFixedString(const ColumnRawPtrs & key_columns, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
    {
        const IColumn & column = *key_columns[0];
        const ColumnFixedString & column_string = assert_cast<const ColumnFixedString &>(column);
        n = column_string.getN();
        chars = &column_string.getChars();
        if (isColumnConst(column))
            get_key_holder_impl = [this](size_t /*row*/) { return StringRef(&(*chars)[0], n); };
        else
            get_key_holder_impl = [this](size_t row) { return StringRef(&(*chars)[row * n], n); };
    }

    auto getKeyHolder(size_t row, [[maybe_unused]] Arena & pool) const
    {
        StringRef key = get_key_holder_impl(row);

        if constexpr (place_string_to_arena)
        {
            return ArenaKeyHolder{key, pool};
        }
        else
        {
            return key;
        }
    }

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache>;
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
            hash.update(key.hash);
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
template <typename SingleColumnMethod, typename Mapped, bool use_cache>
struct HashMethodSingleLowCardinalityColumn : public SingleColumnMethod
{
    using Base = SingleColumnMethod;

    enum class VisitValue
    {
        Empty = 0,
        Found = 1,
        NotFound = 2,
    };

    static constexpr bool has_mapped = !std::is_same_v<Mapped, void>;
    using EmplaceResult = columns_hashing_impl::EmplaceResultImpl<Mapped>;
    using FindResult = columns_hashing_impl::FindResultImpl<Mapped>;

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
    columns_hashing_impl::MappedCache<Mapped> mapped_cache;
    PaddedPODArray<VisitValue> visit_cache;

    /// If initialized column is nullable.
    bool is_nullable = false;

    static const ColumnLowCardinality & getLowCardinalityColumn(const IColumn * column)
    {
        const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(column);
        if (!low_cardinality_column)
            throw Exception("Invalid aggregation key type for HashMethodSingleLowCardinalityColumn method. "
                            "Excepted LowCardinality, got " + column->getName(), ErrorCodes::LOGICAL_ERROR);
        return *low_cardinality_column;
    }

    HashMethodSingleLowCardinalityColumn(
        const ColumnRawPtrs & key_columns_low_cardinality, const Sizes & key_sizes, const HashMethodContextPtr & context)
        : Base({getLowCardinalityColumn(key_columns_low_cardinality[0]).getDictionary().getNestedNotNullableColumn().get()}, key_sizes, context)
    {
        const auto * column = &getLowCardinalityColumn(key_columns_low_cardinality[0]);

        if (!context)
            throw Exception("Cache wasn't created for HashMethodSingleLowCardinalityColumn",
                            ErrorCodes::LOGICAL_ERROR);

        LowCardinalityDictionaryCache * lcd_cache;
        if constexpr (use_cache)
        {
            lcd_cache = typeid_cast<LowCardinalityDictionaryCache *>(context.get());
            if (!lcd_cache)
            {
                const auto & cached_val = *context;
                throw Exception("Invalid type for HashMethodSingleLowCardinalityColumn cache: "
                                + demangle(typeid(cached_val).name()), ErrorCodes::LOGICAL_ERROR);
            }
        }

        const auto * dict = column->getDictionary().getNestedNotNullableColumn().get();
        is_nullable = column->getDictionary().nestedColumnIsNullable();
        key_columns = {dict};
        bool is_shared_dict = column->isSharedDictionary();

        typename LowCardinalityDictionaryCache::DictionaryKey dictionary_key;
        typename LowCardinalityDictionaryCache::CachedValuesPtr cached_values;

        if (is_shared_dict)
        {
            dictionary_key = {column->getDictionary().getHash(), dict->size()};
            if constexpr (use_cache)
                cached_values = lcd_cache->get(dictionary_key);
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

                    lcd_cache->set(dictionary_key, cached_values);
                }
            }
        }

        if constexpr (has_mapped)
            mapped_cache.resize(key_columns[0]->size());

        VisitValue empty(VisitValue::Empty);
        visit_cache.assign(key_columns[0]->size(), empty);

        size_of_index_type = column->getSizeOfIndexType();
        positions = column->getIndexesPtr().get();
    }

    ALWAYS_INLINE size_t getIndexAt(size_t row) const
    {
        switch (size_of_index_type)
        {
            case sizeof(UInt8): return assert_cast<const ColumnUInt8 *>(positions)->getElement(row);
            case sizeof(UInt16): return assert_cast<const ColumnUInt16 *>(positions)->getElement(row);
            case sizeof(UInt32): return assert_cast<const ColumnUInt32 *>(positions)->getElement(row);
            case sizeof(UInt64): return assert_cast<const ColumnUInt64 *>(positions)->getElement(row);
            default: throw Exception("Unexpected size of index type for low cardinality column.", ErrorCodes::LOGICAL_ERROR);
        }
    }

    /// Get the key holder from the key columns for insertion into the hash table.
    ALWAYS_INLINE auto getKeyHolder(size_t row, Arena & pool) const
    {
        return Base::getKeyHolder(getIndexAt(row), pool);
    }

    template <typename Data>
    ALWAYS_INLINE EmplaceResult emplaceKey(Data & data, size_t row_, Arena & pool)
    {
        size_t row = getIndexAt(row_);

        if (is_nullable && row == 0)
        {
            visit_cache[row] = VisitValue::Found;
            bool has_null_key = data.hasNullKeyData();
            data.hasNullKeyData() = true;

            if constexpr (has_mapped)
                return EmplaceResult(data.getNullKeyData(), mapped_cache[0], !has_null_key);
            else
                return EmplaceResult(!has_null_key);
        }

        if (visit_cache[row] == VisitValue::Found)
        {
            if constexpr (has_mapped)
                return EmplaceResult(mapped_cache[row], mapped_cache[row], false);
            else
                return EmplaceResult(false);
        }

        auto key_holder = getKeyHolder(row_, pool);

        bool inserted = false;
        typename Data::LookupResult it;
        if (saved_hash)
            data.emplace(key_holder, it, inserted, saved_hash[row]);
        else
            data.emplace(key_holder, it, inserted);

        visit_cache[row] = VisitValue::Found;

        if constexpr (has_mapped)
        {
            auto & mapped = it->getMapped();
            if (inserted)
            {
                new (&mapped) Mapped();
            }
            mapped_cache[row] = mapped;
            return EmplaceResult(mapped, mapped_cache[row], inserted);
        }
        else
            return EmplaceResult(inserted);
    }

    ALWAYS_INLINE bool isNullAt(size_t i)
    {
        if (!is_nullable)
            return false;

        return getIndexAt(i) == 0;
    }

    template <typename Data>
    ALWAYS_INLINE FindResult findKey(Data & data, size_t row_, Arena & pool)
    {
        size_t row = getIndexAt(row_);

        if (is_nullable && row == 0)
        {
            if constexpr (has_mapped)
                return FindResult(data.hasNullKeyData() ? &data.getNullKeyData() : nullptr, data.hasNullKeyData(), 0);
            else
                return FindResult(data.hasNullKeyData(), 0);
        }

        if (visit_cache[row] != VisitValue::Empty)
        {
            if constexpr (has_mapped)
                return FindResult(&mapped_cache[row], visit_cache[row] == VisitValue::Found, 0);
            else
                return FindResult(visit_cache[row] == VisitValue::Found, 0);
        }

        auto key_holder = getKeyHolder(row_, pool);

        typename Data::LookupResult it;
        if (saved_hash)
            it = data.find(keyHolderGetKey(key_holder), saved_hash[row]);
        else
            it = data.find(keyHolderGetKey(key_holder));

        bool found = it;
        visit_cache[row] = found ? VisitValue::Found : VisitValue::NotFound;

        if constexpr (has_mapped)
        {
            if (found)
                mapped_cache[row] = it->getMapped();
        }

        size_t offset = 0;

        if constexpr (FindResult::has_offset)
            offset = found ? data.offsetInternal(it) : 0;

        if constexpr (has_mapped)
            return FindResult(&mapped_cache[row], found, offset);
        else
            return FindResult(found, offset);
    }

    template <typename Data>
    ALWAYS_INLINE size_t getHash(const Data & data, size_t row, Arena & pool)
    {
        row = getIndexAt(row);
        if (saved_hash)
            return saved_hash[row];

        return Base::getHash(data, row, pool);
    }
};


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

/// For the case when all keys are of fixed length, and they fit in N (for example, 128) bits.
template <
    typename Value,
    typename Key,
    typename Mapped,
    bool has_nullable_keys_ = false,
    bool has_low_cardinality_ = false,
    bool use_cache = true,
    bool need_offset = false>
struct HashMethodKeysFixed
    : private columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>
    , public columns_hashing_impl::HashMethodBase<HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Self = HashMethodKeysFixed<Value, Key, Mapped, has_nullable_keys_, has_low_cardinality_, use_cache, need_offset>;
    using BaseHashed = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;
    using Base = columns_hashing_impl::BaseStateKeysFixed<Key, has_nullable_keys_>;

    static constexpr bool has_nullable_keys = has_nullable_keys_;
    static constexpr bool has_low_cardinality = has_low_cardinality_;

    LowCardinalityKeys<has_low_cardinality> low_cardinality_keys;
    Sizes key_sizes;
    size_t keys_size;

    /// SSSE3 shuffle method can be used. Shuffle masks will be calculated and stored here.
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
    std::unique_ptr<uint8_t[]> masks;
    std::unique_ptr<const char*[]> columns_data;
#endif

    PaddedPODArray<Key> prepared_keys;

    static bool usePreparedKeys(const Sizes & key_sizes)
    {
        if (has_low_cardinality || has_nullable_keys || sizeof(Key) > 16)
            return false;

        for (auto size : key_sizes)
            if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
                return false;

        return true;
    }

    HashMethodKeysFixed(const ColumnRawPtrs & key_columns, const Sizes & key_sizes_, const HashMethodContextPtr &)
        : Base(key_columns), key_sizes(key_sizes_), keys_size(key_columns.size())
    {
        if constexpr (has_low_cardinality)
        {
            low_cardinality_keys.nested_columns.resize(key_columns.size());
            low_cardinality_keys.positions.assign(key_columns.size(), nullptr);
            low_cardinality_keys.position_sizes.resize(key_columns.size());
            for (size_t i = 0; i < key_columns.size(); ++i)
            {
                if (const auto * low_cardinality_col = typeid_cast<const ColumnLowCardinality *>(key_columns[i]))
                {
                    low_cardinality_keys.nested_columns[i] = low_cardinality_col->getDictionary().getNestedColumn().get();
                    low_cardinality_keys.positions[i] = &low_cardinality_col->getIndexes();
                    low_cardinality_keys.position_sizes[i] = low_cardinality_col->getSizeOfIndexType();
                }
                else
                    low_cardinality_keys.nested_columns[i] = key_columns[i];
            }
        }

        if (usePreparedKeys(key_sizes))
        {
            packFixedBatch(keys_size, Base::getActualColumns(), key_sizes, prepared_keys);
        }

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
        else if constexpr (!has_low_cardinality && !has_nullable_keys && sizeof(Key) <= 16)
        {
            /** The task is to "pack" multiple fixed-size fields into single larger Key.
              * Example: pack UInt8, UInt32, UInt16, UInt64 into UInt128 key:
              * [- ---- -- -------- -] - the resulting uint128 key
              *  ^  ^   ^   ^       ^
              *  u8 u32 u16 u64    zero
              *
              * We can do it with the help of SSSE3 shuffle instruction.
              *
              * There will be a mask for every GROUP BY element (keys_size masks in total).
              * Every mask has 16 bytes but only sizeof(Key) bytes are used (other we don't care).
              *
              * Every byte in the mask has the following meaning:
              * - if it is 0..15, take the element at this index from source register and place here in the result;
              * - if it is 0xFF - set the elemend in the result to zero.
              *
              * Example:
              * We want to copy UInt32 to offset 1 in the destination and set other bytes in the destination as zero.
              * The corresponding mask will be: FF, 0, 1, 2, 3, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF, FF
              *
              * The max size of destination is 16 bytes, because we cannot process more with SSSE3.
              *
              * The method is disabled under MSan, because it's allowed
              * to load into SSE register and process up to 15 bytes of uninitialized memory in columns padding.
              * We don't use this uninitialized memory but MSan cannot look "into" the shuffle instruction.
              *
              * 16-bytes masks can be placed overlapping, only first sizeof(Key) bytes are relevant in each mask.
              * We initialize them to 0xFF and then set the needed elements.
              */
            size_t total_masks_size = sizeof(Key) * keys_size + (16 - sizeof(Key));
            masks.reset(new uint8_t[total_masks_size]);
            memset(masks.get(), 0xFF, total_masks_size);

            size_t offset = 0;
            for (size_t i = 0; i < keys_size; ++i)
            {
                for (size_t j = 0; j < key_sizes[i]; ++j)
                {
                    masks[i * sizeof(Key) + offset] = j;
                    ++offset;
                }
            }

            columns_data.reset(new const char*[keys_size]);

            for (size_t i = 0; i < keys_size; ++i)
                columns_data[i] = Base::getActualColumns()[i]->getRawData().data;
        }
#endif
    }

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const
    {
        if constexpr (has_nullable_keys)
        {
            auto bitmap = Base::createBitmap(row);
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes, bitmap);
        }
        else
        {
            if constexpr (has_low_cardinality)
                return packFixed<Key, true>(row, keys_size, low_cardinality_keys.nested_columns, key_sizes,
                                            &low_cardinality_keys.positions, &low_cardinality_keys.position_sizes);

            if (!prepared_keys.empty())
                return prepared_keys[row];

#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)
            if constexpr (sizeof(Key) <= 16)
            {
                assert(!has_low_cardinality && !has_nullable_keys);
                return packFixedShuffle<Key>(columns_data.get(), keys_size, key_sizes.data(), row, masks.get());
            }
#endif
            return packFixed<Key>(row, keys_size, Base::getActualColumns(), key_sizes);
        }
    }

    static std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        if (!usePreparedKeys(key_sizes))
            return {};

        std::vector<IColumn *> new_columns;
        new_columns.reserve(key_columns.size());

        Sizes new_sizes;
        auto fill_size = [&](size_t size)
        {
            for (size_t i = 0; i < key_sizes.size(); ++i)
            {
                if (key_sizes[i] == size)
                {
                    new_columns.push_back(key_columns[i]);
                    new_sizes.push_back(size);
                }
            }
        };

        fill_size(16);
        fill_size(8);
        fill_size(4);
        fill_size(2);
        fill_size(1);

        key_columns.swap(new_columns);
        return new_sizes;
    }
};

/** Hash by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename Value, typename Mapped>
struct HashMethodSerialized
    : public columns_hashing_impl::HashMethodBase<HashMethodSerialized<Value, Mapped>, Value, Mapped, false>
{
    using Self = HashMethodSerialized<Value, Mapped>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ColumnRawPtrs key_columns;
    size_t keys_size;

    HashMethodSerialized(const ColumnRawPtrs & key_columns_, const Sizes & /*key_sizes*/, const HashMethodContextPtr &)
        : key_columns(key_columns_), keys_size(key_columns_.size()) {}

protected:
    friend class columns_hashing_impl::HashMethodBase<Self, Value, Mapped, false>;

    ALWAYS_INLINE SerializedKeyHolder getKeyHolder(size_t row, Arena & pool) const
    {
        return SerializedKeyHolder{
            serializeKeysToPoolContiguous(row, keys_size, key_columns, pool),
            pool};
    }
};

/// For the case when there is one string key.
template <typename Value, typename Mapped, bool use_cache = true, bool need_offset = false>
struct HashMethodHashed
    : public columns_hashing_impl::HashMethodBase<HashMethodHashed<Value, Mapped, use_cache, need_offset>, Value, Mapped, use_cache, need_offset>
{
    using Key = UInt128;
    using Self = HashMethodHashed<Value, Mapped, use_cache, need_offset>;
    using Base = columns_hashing_impl::HashMethodBase<Self, Value, Mapped, use_cache, need_offset>;

    ColumnRawPtrs key_columns;

    HashMethodHashed(ColumnRawPtrs key_columns_, const Sizes &, const HashMethodContextPtr &)
        : key_columns(std::move(key_columns_)) {}

    ALWAYS_INLINE Key getKeyHolder(size_t row, Arena &) const
    {
        return hash128(row, key_columns.size(), key_columns);
    }
};

}
}
