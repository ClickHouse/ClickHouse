#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <shared_mutex>
#include <variant>
#include <vector>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <pcg_random.hpp>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/SmallObjectPool.h>
#include <common/StringRef.h>
#include <ext/bit_cast.h>
#include <ext/map.h>
#include <ext/range.h>
#include <ext/size.h>
#include <ext/scope_guard.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include <DataStreams/IBlockInputStream.h>
#include "DictionaryHelpers.h"

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
    ComplexKeyCacheDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        const size_t size_);

    std::string getKeyDescription() const { return key_description; }

    std::string getTypeName() const override { return "ComplexKeyCache"; }

    size_t getBytesAllocated() const override
    {
        return bytes_allocated + (key_size_is_fixed ? fixed_size_keys_pool->size() : keys_pool->size())
            + (string_arena ? string_arena->size() : 0);
    }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override
    {
        return static_cast<double>(hit_count.load(std::memory_order_acquire)) / query_count.load(std::memory_order_relaxed);
    }

    size_t getElementCount() const override { return element_count.load(std::memory_order_relaxed); }

    double getLoadFactor() const override { return static_cast<double>(element_count.load(std::memory_order_relaxed)) / size; }

    bool supportUpdates() const override { return false; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<ComplexKeyCacheDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, size);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
    }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::complex; }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

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
        time_point_t expiresAt() const { return ext::safe_bit_cast<time_point_t>(data & EXPIRES_AT_MASK); }
        void setExpiresAt(const time_point_t & t) { data = ext::safe_bit_cast<time_point_urep_t>(t); }

        bool isDefault() const { return (data & IS_DEFAULT_MASK) == IS_DEFAULT_MASK; }
        void setDefault() { data |= IS_DEFAULT_MASK; }
    };

    struct Attribute final
    {
        AttributeUnderlyingType type;
        std::variant<
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            UInt128,
            Int8,
            Int16,
            Int32,
            Int64,
            Decimal32,
            Decimal64,
            Decimal128,
            Float32,
            Float64,
            String>
            null_values;
        std::variant<
            ContainerPtrType<UInt8>,
            ContainerPtrType<UInt16>,
            ContainerPtrType<UInt32>,
            ContainerPtrType<UInt64>,
            ContainerPtrType<UInt128>,
            ContainerPtrType<Int8>,
            ContainerPtrType<Int16>,
            ContainerPtrType<Int32>,
            ContainerPtrType<Int64>,
            ContainerPtrType<Decimal32>,
            ContainerPtrType<Decimal64>,
            ContainerPtrType<Decimal128>,
            ContainerPtrType<Float32>,
            ContainerPtrType<Float64>,
            ContainerPtrType<StringRef>>
            arrays;
    };

    void createAttributes();

    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);

    template <typename AttributeType, typename OutputType, typename DefaultValueExtractor>
    void getItemsNumberImpl(
        Attribute & attribute,
        const Columns & key_columns,
        PaddedPODArray<OutputType> & out,
        DefaultValueExtractor & default_value_extractor) const;

    void getItemsString(
        Attribute & attribute,
        const Columns & key_columns,
        ColumnString * out,
        DictionaryDefaultValueExtractor<String> & default_value_extractor) const;

    template <typename PresentKeyHandler, typename AbsentKeyHandler>
    void update(
        const Columns & in_key_columns,
        const PODArray<StringRef> & in_keys,
        const std::vector<size_t> & in_requested_rows,
        PresentKeyHandler && on_cell_updated,
        AbsentKeyHandler && on_key_not_found) const;

    UInt64 getCellIdx(const StringRef key) const;

    void setDefaultAttributeValue(Attribute & attribute, const size_t idx) const;

    void setAttributeValue(Attribute & attribute, const size_t idx, const Field & value) const;

    Attribute & getAttribute(const std::string & attribute_name) const;

    StringRef allocKey(const size_t row, const Columns & key_columns, StringRefs & keys) const;

    void freeKey(const StringRef key) const;

    template <typename Arena>
    static StringRef placeKeysInPool(
        const size_t row,
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
    }

    bool isEmptyCell(const UInt64 idx) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const std::string key_description{dict_struct.getKeyDescription()};

    mutable std::shared_mutex rw_lock;

    /// Actual size will be increased to match power of 2
    const size_t size;

    /// all bits to 1  mask (size - 1) (0b1000 - 1 = 0b111)
    const size_t size_overlap_mask;

    /// Max tries to find cell, overlapped with mask: if size = 16 and start_cell=10: will try cells: 10,11,12,13,14,15,0,1,2,3
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
