#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <tuple>
#include <vector>
#include <shared_mutex>
#include <Columns/ColumnString.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/SmallObjectPool.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <common/StringRef.h>
#include <ext/bit_cast.h>
#include <ext/map.h>
#include <ext/scope_guard.h>
#include <pcg_random.hpp>


namespace ProfileEvents
{
extern const Event DictCacheKeysRequested;
extern const Event DictCacheKeysRequestedMiss;
extern const Event DictCacheKeysRequestedFound;
extern const Event DictCacheKeysExpired;
extern const Event DictCacheKeysNotFound;
extern const Event DictCacheKeysHit;
extern const Event DictCacheRequestTimeNs;
extern const Event DictCacheLockWriteNs;
extern const Event DictCacheLockReadNs;
}

namespace DB
{
class ComplexKeyCacheDictionary final : public IDictionaryBase
{
public:
    ComplexKeyCacheDictionary(const std::string & name,
        const DictionaryStructure & dict_struct,
        DictionarySourcePtr source_ptr,
        const DictionaryLifetime dict_lifetime,
        const size_t size);

    ComplexKeyCacheDictionary(const ComplexKeyCacheDictionary & other);

    std::string getKeyDescription() const
    {
        return key_description;
    };

    std::exception_ptr getCreationException() const override
    {
        return {};
    }

    std::string getName() const override
    {
        return name;
    }

    std::string getTypeName() const override
    {
        return "ComplexKeyCache";
    }

    size_t getBytesAllocated() const override
    {
        return bytes_allocated + (key_size_is_fixed ? fixed_size_keys_pool->size() : keys_pool->size())
            + (string_arena ? string_arena->size() : 0);
    }

    size_t getQueryCount() const override
    {
        return query_count.load(std::memory_order_relaxed);
    }

    double getHitRate() const override
    {
        return static_cast<double>(hit_count.load(std::memory_order_acquire)) / query_count.load(std::memory_order_relaxed);
    }

    size_t getElementCount() const override
    {
        return element_count.load(std::memory_order_relaxed);
    }

    double getLoadFactor() const override
    {
        return static_cast<double>(element_count.load(std::memory_order_relaxed)) / size;
    }

    bool isCached() const override
    {
        return true;
    }

    std::unique_ptr<IExternalLoadable> clone() const override
    {
        return std::make_unique<ComplexKeyCacheDictionary>(*this);
    }

    const IDictionarySource * getSource() const override
    {
        return source_ptr.get();
    }

    const DictionaryLifetime & getLifetime() const override
    {
        return dict_lifetime;
    }

    const DictionaryStructure & getStructure() const override
    {
        return dict_struct;
    }

    std::chrono::time_point<std::chrono::system_clock> getCreationTime() const override
    {
        return creation_time;
    }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
    }

/// In all functions below, key_columns must be full (non-constant) columns.
/// See the requirement in IDataType.h for text-serialization functions.
#define DECLARE(TYPE) \
    void get##TYPE(   \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
#undef DECLARE

    void getString(const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ColumnString * out) const;

#define DECLARE(TYPE)                                  \
    void get##TYPE(const std::string & attribute_name, \
        const Columns & key_columns,                   \
        const DataTypes & key_types,                   \
        const PaddedPODArray<TYPE> & def,              \
        PaddedPODArray<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
#undef DECLARE

    void getString(const std::string & attribute_name,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnString * const def,
        ColumnString * const out) const;

#define DECLARE(TYPE)                                  \
    void get##TYPE(const std::string & attribute_name, \
        const Columns & key_columns,                   \
        const DataTypes & key_types,                   \
        const TYPE def,                                \
        PaddedPODArray<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
#undef DECLARE

    void getString(const std::string & attribute_name,
        const Columns & key_columns,
        const DataTypes & key_types,
        const String & def,
        ColumnString * const out) const;

    void has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    template <typename Value>
    using MapType = HashMapWithSavedHash<StringRef, Value, StringRefHash>;
    template <typename Value>
    using ContainerType = Value[];
    template <typename Value>
    using ContainerPtrType = std::unique_ptr<ContainerType<Value>>;

    struct CellMetadata final
    {
        using time_point_t = std::chrono::system_clock::time_point;
        using time_point_rep_t = time_point_t::rep;
        using time_point_urep_t = std::make_unsigned_t<time_point_rep_t>;

        static constexpr UInt64 EXPIRES_AT_MASK = std::numeric_limits<time_point_rep_t>::max();
        static constexpr UInt64 IS_DEFAULT_MASK = ~EXPIRES_AT_MASK;

        StringRef key;
        decltype(StringRefHash{}(key)) hash;
        /// Stores both expiration time and `is_default` flag in the most significant bit
        time_point_urep_t data;

        /// Sets expiration time, resets `is_default` flag to false
        time_point_t expiresAt() const
        {
            return ext::safe_bit_cast<time_point_t>(data & EXPIRES_AT_MASK);
        }
        void setExpiresAt(const time_point_t & t)
        {
            data = ext::safe_bit_cast<time_point_urep_t>(t);
        }

        bool isDefault() const
        {
            return (data & IS_DEFAULT_MASK) == IS_DEFAULT_MASK;
        }
        void setDefault()
        {
            data |= IS_DEFAULT_MASK;
        }
    };

    struct Attribute final
    {
        AttributeUnderlyingType type;
        std::tuple<UInt8, UInt16, UInt32, UInt64, UInt128, Int8, Int16, Int32, Int64, Float32, Float64, String> null_values;
        std::tuple<ContainerPtrType<UInt8>,
            ContainerPtrType<UInt16>,
            ContainerPtrType<UInt32>,
            ContainerPtrType<UInt64>,
            ContainerPtrType<UInt128>,
            ContainerPtrType<Int8>,
            ContainerPtrType<Int16>,
            ContainerPtrType<Int32>,
            ContainerPtrType<Int64>,
            ContainerPtrType<Float32>,
            ContainerPtrType<Float64>,
            ContainerPtrType<StringRef>>
            arrays;
    };

    void createAttributes();

    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);

    template <typename OutputType, typename DefaultGetter>
    void getItemsNumber(
        Attribute & attribute, const Columns & key_columns, PaddedPODArray<OutputType> & out, DefaultGetter && get_default) const
    {
        if (false)
        {
        }
#define DISPATCH(TYPE)                                        \
    else if (attribute.type == AttributeUnderlyingType::TYPE) \
        getItemsNumberImpl<TYPE, OutputType>(attribute, key_columns, out, std::forward<DefaultGetter>(get_default));
        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Float32)
        DISPATCH(Float64)
#undef DISPATCH
        else throw Exception("Unexpected type of attribute: " + toString(attribute.type), ErrorCodes::LOGICAL_ERROR);
    };

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
        Attribute & attribute, const Columns & key_columns, PaddedPODArray<OutputType> & out, DefaultGetter && get_default) const
    {
        /// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
        MapType<std::vector<size_t>> outdated_keys;
        auto & attribute_array = std::get<ContainerPtrType<AttributeType>>(attribute.arrays);

        const auto rows_num = key_columns.front()->size();
        const auto keys_size = dict_struct.key->size();
        StringRefs keys(keys_size);
        Arena temporary_keys_pool;
        PODArray<StringRef> keys_array(rows_num);

        size_t cache_expired = 0, cache_not_found = 0, cache_hit = 0;
        {
            const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

            const auto now = std::chrono::system_clock::now();
            /// fetch up-to-date values, decide which ones require update
            for (const auto row : ext::range(0, rows_num))
            {
                const StringRef key = placeKeysInPool(row, key_columns, keys, *dict_struct.key, temporary_keys_pool);
                keys_array[row] = key;
                const auto find_result = findCellIdx(key, now);

                /** cell should be updated if either:
                *    1. keys (or hash) do not match,
                *    2. cell has expired,
                *    3. explicit defaults were specified and cell was set default. */

                if (!find_result.valid)
                {
                    outdated_keys[key].push_back(row);
                    if (find_result.outdated)
                        ++cache_expired;
                    else
                        ++cache_not_found;
                }
                else
                {
                    ++cache_hit;
                    const auto & cell_idx = find_result.cell_idx;
                    const auto & cell = cells[cell_idx];
                    out[row] = cell.isDefault() ? get_default(row) : static_cast<OutputType>(attribute_array[cell_idx]);
                }
            }
        }
        ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired);
        ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found);
        ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);
        query_count.fetch_add(rows_num, std::memory_order_relaxed);
        hit_count.fetch_add(rows_num - outdated_keys.size(), std::memory_order_release);

        if (outdated_keys.empty())
            return;

        std::vector<size_t> required_rows(outdated_keys.size());
        std::transform(
            std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows), [](auto & pair) { return pair.second.front(); });

        /// request new values
        update(key_columns,
            keys_array,
            required_rows,
            [&](const StringRef key, const size_t cell_idx)
            {
                for (const auto row : outdated_keys[key])
                    out[row] = static_cast<OutputType>(attribute_array[cell_idx]);
            },
            [&](const StringRef key, const size_t)
            {
                for (const auto row : outdated_keys[key])
                    out[row] = get_default(row);
            });
    };

    template <typename DefaultGetter>
    void getItemsString(Attribute & attribute, const Columns & key_columns, ColumnString * out, DefaultGetter && get_default) const
    {
        const auto rows_num = key_columns.front()->size();
        /// save on some allocations
        out->getOffsets().reserve(rows_num);

        const auto keys_size = dict_struct.key->size();
        StringRefs keys(keys_size);
        Arena temporary_keys_pool;

        auto & attribute_array = std::get<ContainerPtrType<StringRef>>(attribute.arrays);

        auto found_outdated_values = false;

        /// perform optimistic version, fallback to pessimistic if failed
        {
            const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

            const auto now = std::chrono::system_clock::now();
            /// fetch up-to-date values, discard on fail
            for (const auto row : ext::range(0, rows_num))
            {
                const StringRef key = placeKeysInPool(row, key_columns, keys, *dict_struct.key, temporary_keys_pool);
                SCOPE_EXIT(temporary_keys_pool.rollback(key.size));
                const auto find_result = findCellIdx(key, now);

                if (!find_result.valid)
                {
                    found_outdated_values = true;
                    break;
                }
                else
                {
                    const auto & cell_idx = find_result.cell_idx;
                    const auto & cell = cells[cell_idx];
                    const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];
                    out->insertData(string_ref.data, string_ref.size);
                }
            }
        }

        /// optimistic code completed successfully
        if (!found_outdated_values)
        {
            query_count.fetch_add(rows_num, std::memory_order_relaxed);
            hit_count.fetch_add(rows_num, std::memory_order_release);
            return;
        }

        /// now onto the pessimistic one, discard possible partial results from the optimistic path
        out->getChars().resize_assume_reserved(0);
        out->getOffsets().resize_assume_reserved(0);

        /// Mapping: <key> -> { all indices `i` of `key_columns` such that `key_columns[i]` = <key> }
        MapType<std::vector<size_t>> outdated_keys;
        /// we are going to store every string separately
        MapType<StringRef> map;
        PODArray<StringRef> keys_array(rows_num);

        size_t total_length = 0;
        size_t cache_expired = 0, cache_not_found = 0, cache_hit = 0;
        {
            const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

            const auto now = std::chrono::system_clock::now();
            for (const auto row : ext::range(0, rows_num))
            {
                const StringRef key = placeKeysInPool(row, key_columns, keys, *dict_struct.key, temporary_keys_pool);
                keys_array[row] = key;
                const auto find_result = findCellIdx(key, now);

                if (!find_result.valid)
                {
                    outdated_keys[key].push_back(row);
                    if (find_result.outdated)
                        ++cache_expired;
                    else
                        ++cache_not_found;
                }
                else
                {
                    ++cache_hit;
                    const auto & cell_idx = find_result.cell_idx;
                    const auto & cell = cells[cell_idx];
                    const auto string_ref = cell.isDefault() ? get_default(row) : attribute_array[cell_idx];

                    if (!cell.isDefault())
                        map[key] = copyIntoArena(string_ref, temporary_keys_pool);

                    total_length += string_ref.size + 1;
                }
            }
        }
        ProfileEvents::increment(ProfileEvents::DictCacheKeysExpired, cache_expired);
        ProfileEvents::increment(ProfileEvents::DictCacheKeysNotFound, cache_not_found);
        ProfileEvents::increment(ProfileEvents::DictCacheKeysHit, cache_hit);

        query_count.fetch_add(rows_num, std::memory_order_relaxed);
        hit_count.fetch_add(rows_num - outdated_keys.size(), std::memory_order_release);

        /// request new values
        if (!outdated_keys.empty())
        {
            std::vector<size_t> required_rows(outdated_keys.size());
            std::transform(std::begin(outdated_keys), std::end(outdated_keys), std::begin(required_rows), [](auto & pair)
            {
                return pair.second.front();
            });

            update(key_columns,
                keys_array,
                required_rows,
                [&](const StringRef key, const size_t cell_idx)
                {
                    const StringRef attribute_value = attribute_array[cell_idx];

                    /// We must copy key and value to own memory, because it may be replaced with another
                    ///  in next iterations of inner loop of update.
                    const StringRef copied_key = copyIntoArena(key, temporary_keys_pool);
                    const StringRef copied_value = copyIntoArena(attribute_value, temporary_keys_pool);

                    map[copied_key] = copied_value;
                    total_length += (attribute_value.size + 1) * outdated_keys[key].size();
                },
                [&](const StringRef key, const size_t)
                {
                    for (const auto row : outdated_keys[key])
                        total_length += get_default(row).size + 1;
                });
        }

        out->getChars().reserve(total_length);

        for (const auto row : ext::range(0, ext::size(keys_array)))
        {
            const StringRef key = keys_array[row];
            const auto it = map.find(key);
            const auto string_ref = it != std::end(map) ? it->second : get_default(row);
            out->insertData(string_ref.data, string_ref.size);
        }
    };

    template <typename PresentKeyHandler, typename AbsentKeyHandler>
    void update(const Columns & in_key_columns,
        const PODArray<StringRef> & in_keys,
        const std::vector<size_t> & in_requested_rows,
        PresentKeyHandler && on_cell_updated,
        AbsentKeyHandler && on_key_not_found) const
    {
        MapType<bool> remaining_keys{in_requested_rows.size()};
        for (const auto row : in_requested_rows)
            remaining_keys.insert({in_keys[row], false});

        std::uniform_int_distribution<UInt64> distribution(dict_lifetime.min_sec, dict_lifetime.max_sec);

        const ProfilingScopedWriteRWLock write_lock{rw_lock, ProfileEvents::DictCacheLockWriteNs};
        {
            Stopwatch watch;
            auto stream = source_ptr->loadKeys(in_key_columns, in_requested_rows);
            stream->readPrefix();

            const auto keys_size = dict_struct.key->size();
            StringRefs keys(keys_size);

            const auto attributes_size = attributes.size();
            const auto now = std::chrono::system_clock::now();

            while (const auto block = stream->read())
            {
                /// cache column pointers
                const auto key_columns = ext::map<Columns>(
                    ext::range(0, keys_size), [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

                const auto attribute_columns = ext::map<Columns>(ext::range(0, attributes_size),
                    [&](const size_t attribute_idx) { return block.safeGetByPosition(keys_size + attribute_idx).column; });

                const auto rows_num = block.rows();

                for (const auto row : ext::range(0, rows_num))
                {
                    auto key = allocKey(row, key_columns, keys);
                    const auto hash = StringRefHash{}(key);
                    const auto find_result = findCellIdx(key, now, hash);
                    const auto & cell_idx = find_result.cell_idx;
                    auto & cell = cells[cell_idx];

                    for (const auto attribute_idx : ext::range(0, attributes.size()))
                    {
                        const auto & attribute_column = *attribute_columns[attribute_idx];
                        auto & attribute = attributes[attribute_idx];

                        setAttributeValue(attribute, cell_idx, attribute_column[row]);
                    }

                    /// if cell id is zero and zero does not map to this cell, then the cell is unused
                    if (cell.key == StringRef{} && cell_idx != zero_cell_idx)
                        element_count.fetch_add(1, std::memory_order_relaxed);

                    /// handle memory allocated for old key
                    if (key == cell.key)
                    {
                        freeKey(key);
                        key = cell.key;
                    }
                    else
                    {
                        /// new key is different from the old one
                        if (cell.key.data)
                            freeKey(cell.key);

                        cell.key = key;
                    }

                    cell.hash = hash;

                    if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
                        cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
                    else
                        cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

                    /// inform caller
                    on_cell_updated(key, cell_idx);
                    /// mark corresponding id as found
                    remaining_keys[key] = true;
                }
            }

            stream->readSuffix();

            ProfileEvents::increment(ProfileEvents::DictCacheKeysRequested, in_requested_rows.size());
            ProfileEvents::increment(ProfileEvents::DictCacheRequestTimeNs, watch.elapsed());
        }

        size_t found_num = 0;
        size_t not_found_num = 0;

        const auto now = std::chrono::system_clock::now();

        /// Check which ids have not been found and require setting null_value
        for (const auto key_found_pair : remaining_keys)
        {
            if (key_found_pair.second)
            {
                ++found_num;
                continue;
            }

            ++not_found_num;

            auto key = key_found_pair.first;
            const auto hash = StringRefHash{}(key);
            const auto find_result = findCellIdx(key, now, hash);
            const auto & cell_idx = find_result.cell_idx;
            auto & cell = cells[cell_idx];

            /// Set null_value for each attribute
            for (auto & attribute : attributes)
                setDefaultAttributeValue(attribute, cell_idx);

            /// Check if cell had not been occupied before and increment element counter if it hadn't
            if (cell.key == StringRef{} && cell_idx != zero_cell_idx)
                element_count.fetch_add(1, std::memory_order_relaxed);

            if (key == cell.key)
                key = cell.key;
            else
            {
                if (cell.key.data)
                    freeKey(cell.key);

                /// copy key from temporary pool
                key = copyKey(key);
                cell.key = key;
            }

            cell.hash = hash;

            if (dict_lifetime.min_sec != 0 && dict_lifetime.max_sec != 0)
                cell.setExpiresAt(std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)});
            else
                cell.setExpiresAt(std::chrono::time_point<std::chrono::system_clock>::max());

            cell.setDefault();

            /// inform caller that the cell has not been found
            on_key_not_found(key, cell_idx);
        }

        ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, found_num);
        ProfileEvents::increment(ProfileEvents::DictCacheKeysRequestedMiss, not_found_num);
    };

    UInt64 getCellIdx(const StringRef key) const;

    void setDefaultAttributeValue(Attribute & attribute, const size_t idx) const;

    void setAttributeValue(Attribute & attribute, const size_t idx, const Field & value) const;

    Attribute & getAttribute(const std::string & attribute_name) const;

    StringRef allocKey(const size_t row, const Columns & key_columns, StringRefs & keys) const;

    void freeKey(const StringRef key) const;

    template <typename Arena>
    static StringRef placeKeysInPool(const size_t row,
        const Columns & key_columns,
        StringRefs & keys,
        const std::vector<DictionaryAttribute> & key_attributes,
        Arena & pool);

    StringRef placeKeysInFixedSizePool(const size_t row, const Columns & key_columns) const;

    static StringRef copyIntoArena(StringRef src, Arena & arena);
    StringRef copyKey(const StringRef key) const;

    struct FindResult
    {
        const size_t cell_idx;
        const bool valid;
        const bool outdated;
    };

    FindResult findCellIdx(const StringRef & key, const CellMetadata::time_point_t now, const size_t hash) const;
    FindResult findCellIdx(const StringRef & key, const CellMetadata::time_point_t now) const
    {
        const auto hash = StringRefHash{}(key);
        return findCellIdx(key, now, hash);
    };

    bool isEmptyCell(const UInt64 idx) const;

    const std::string name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const std::string key_description{dict_struct.getKeyDescription()};

    mutable std::shared_mutex rw_lock;

    /// Actual size will be increased to match power of 2
    const size_t size;

    /// all bits to 1  mask (size - 1) (0b1000 - 1 = 0b111)
    const size_t size_overlap_mask;

    /// Max tries to find cell, overlaped with mask: if size = 16 and start_cell=10: will try cells: 10,11,12,13,14,15,0,1,2,3
    static constexpr size_t max_collision_length = 10;

    const UInt64 zero_cell_idx{getCellIdx(StringRef{})};
    std::map<std::string, size_t> attribute_index_by_name;
    mutable std::vector<Attribute> attributes;
    mutable std::vector<CellMetadata> cells{size};
    const bool key_size_is_fixed{dict_struct.isKeySizeFixed()};
    size_t key_size{key_size_is_fixed ? dict_struct.getKeySize() : 0};
    std::unique_ptr<ArenaWithFreeLists> keys_pool = key_size_is_fixed ? nullptr : std::make_unique<ArenaWithFreeLists>();
    std::unique_ptr<SmallObjectPool> fixed_size_keys_pool = key_size_is_fixed ? std::make_unique<SmallObjectPool>(key_size) : nullptr;
    std::unique_ptr<ArenaWithFreeLists> string_arena;

    mutable pcg64 rnd_engine;

    mutable size_t bytes_allocated = 0;
    mutable std::atomic<size_t> element_count{0};
    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};

    const std::chrono::time_point<std::chrono::system_clock> creation_time = std::chrono::system_clock::now();
};
}
