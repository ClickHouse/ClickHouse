#pragma once

#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/SmallObjectPool.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/ColumnString.h>
#include <common/StringRef.h>
#include <ext/bit_cast.h>
#include <Poco/RWLock.h>
#include <atomic>
#include <chrono>
#include <vector>
#include <map>
#include <tuple>
#include <random>


namespace DB
{


class ComplexKeyCacheDictionary final : public IDictionaryBase
{
public:
    ComplexKeyCacheDictionary(const std::string & name, const DictionaryStructure & dict_struct,
        DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime,
        const std::size_t size);

    ComplexKeyCacheDictionary(const ComplexKeyCacheDictionary & other);

    std::string getKeyDescription() const { return key_description; };

    std::exception_ptr getCreationException() const override { return {}; }

    std::string getName() const override { return name; }

    std::string getTypeName() const override { return "ComplexKeyCache"; }

    std::size_t getBytesAllocated() const override
    {
        return bytes_allocated + (key_size_is_fixed ? fixed_size_keys_pool->size() : keys_pool->size()) +
            (string_arena ? string_arena->size() : 0);
    }

    std::size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override
    {
        return static_cast<double>(hit_count.load(std::memory_order_acquire)) /
            query_count.load(std::memory_order_relaxed);
    }

    std::size_t getElementCount() const override { return element_count.load(std::memory_order_relaxed); }

    double getLoadFactor() const override
    {
        return static_cast<double>(element_count.load(std::memory_order_relaxed)) / size;
    }

    bool isCached() const override { return true; }

    DictionaryPtr clone() const override { return std::make_unique<ComplexKeyCacheDictionary>(*this); }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

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
#define DECLARE(TYPE)\
    void get##TYPE(\
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
        PaddedPODArray<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
#undef DECLARE

    void getString(
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
        ColumnString * out) const;

#define DECLARE(TYPE)\
    void get##TYPE(\
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
        const PaddedPODArray<TYPE> & def, PaddedPODArray<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
#undef DECLARE

    void getString(
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
        const ColumnString * const def, ColumnString * const out) const;

#define DECLARE(TYPE)\
    void get##TYPE(\
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
        const TYPE def, PaddedPODArray<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
#undef DECLARE

    void getString(
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
        const String & def, ColumnString * const out) const;

    void has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    template <typename Value> using MapType = HashMapWithSavedHash<StringRef, Value, StringRefHash>;
    template <typename Value> using ContainerType = Value[];
    template <typename Value> using ContainerPtrType = std::unique_ptr<ContainerType<Value>>;

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
        std::tuple<
            UInt8, UInt16, UInt32, UInt64,
            Int8, Int16, Int32, Int64,
            Float32, Float64,
            String> null_values;
        std::tuple<
            ContainerPtrType<UInt8>, ContainerPtrType<UInt16>, ContainerPtrType<UInt32>, ContainerPtrType<UInt64>,
            ContainerPtrType<Int8>, ContainerPtrType<Int16>, ContainerPtrType<Int32>, ContainerPtrType<Int64>,
            ContainerPtrType<Float32>, ContainerPtrType<Float64>,
            ContainerPtrType<StringRef>> arrays;
    };

    void createAttributes();

    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);

    template <typename OutputType, typename DefaultGetter>
    void getItemsNumber(
        Attribute & attribute,
        const Columns & key_columns,
        PaddedPODArray<OutputType> & out,
        DefaultGetter && get_default) const;

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
        Attribute & attribute,
        const Columns & key_columns,
        PaddedPODArray<OutputType> & out,
        DefaultGetter && get_default) const;

    template <typename DefaultGetter>
    void getItemsString(
        Attribute & attribute, const Columns & key_columns, ColumnString * out,
        DefaultGetter && get_default) const;

    template <typename PresentKeyHandler, typename AbsentKeyHandler>
    void update(
        const Columns & in_key_columns, const PODArray<StringRef> & in_keys,
        const std::vector<std::size_t> & in_requested_rows,
        PresentKeyHandler && on_cell_updated,
        AbsentKeyHandler && on_key_not_found) const;

    UInt64 getCellIdx(const StringRef key) const;

    void setDefaultAttributeValue(Attribute & attribute, const std::size_t idx) const;

    void setAttributeValue(Attribute & attribute, const std::size_t idx, const Field & value) const;

    Attribute & getAttribute(const std::string & attribute_name) const;

    StringRef allocKey(const std::size_t row, const Columns & key_columns, StringRefs & keys) const;

    void freeKey(const StringRef key) const;

    template <typename Arena>
    static StringRef placeKeysInPool(
        const std::size_t row, const Columns & key_columns, StringRefs & keys,
        const std::vector<DictionaryAttribute> & key_attributes, Arena & pool);

    StringRef placeKeysInFixedSizePool(
        const std::size_t row, const Columns & key_columns) const;

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

    mutable Poco::RWLock rw_lock;

    /// Actual size will be increased to match power of 2
    const std::size_t size;

    /// all bits to 1  mask (size - 1) (0b1000 - 1 = 0b111)
    const std::size_t size_overlap_mask;

    /// Max tries to find cell, overlaped with mask: if size = 16 and start_cell=10: will try cells: 10,11,12,13,14,15,0,1,2,3
    static constexpr std::size_t max_collision_length = 10;

    const UInt64 zero_cell_idx{getCellIdx(StringRef{})};
    std::map<std::string, std::size_t> attribute_index_by_name;
    mutable std::vector<Attribute> attributes;
    mutable std::vector<CellMetadata> cells{size};
    const bool key_size_is_fixed{dict_struct.isKeySizeFixed()};
    std::size_t key_size{key_size_is_fixed ? dict_struct.getKeySize() : 0};
    std::unique_ptr<ArenaWithFreeLists> keys_pool = key_size_is_fixed ? nullptr :
        std::make_unique<ArenaWithFreeLists>();
    std::unique_ptr<SmallObjectPool> fixed_size_keys_pool = key_size_is_fixed ?
        std::make_unique<SmallObjectPool>(key_size) : nullptr;
    std::unique_ptr<ArenaWithFreeLists> string_arena;

    mutable std::mt19937_64 rnd_engine;

    mutable std::size_t bytes_allocated = 0;
    mutable std::atomic<std::size_t> element_count{0};
    mutable std::atomic<std::size_t> hit_count{0};
    mutable std::atomic<std::size_t> query_count{0};

    const std::chrono::time_point<std::chrono::system_clock> creation_time = std::chrono::system_clock::now();
};

}
