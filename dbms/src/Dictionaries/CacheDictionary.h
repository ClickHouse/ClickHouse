#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <map>
#include <shared_mutex>
#include <variant>
#include <vector>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <pcg_random.hpp>
#include <Common/ArenaWithFreeLists.h>
#include <Common/CurrentMetrics.h>
#include <ext/bit_cast.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"


namespace DB
{
class CacheDictionary final : public IDictionary
{
public:
    CacheDictionary(
        const std::string & name,
        const DictionaryStructure & dict_struct,
        DictionarySourcePtr source_ptr,
        const DictionaryLifetime dict_lifetime,
        const size_t size);

    std::exception_ptr getCreationException() const override { return {}; }

    std::string getName() const override { return name; }

    std::string getTypeName() const override { return "Cache"; }

    size_t getBytesAllocated() const override { return bytes_allocated + (string_arena ? string_arena->size() : 0); }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override
    {
        return static_cast<double>(hit_count.load(std::memory_order_acquire)) / query_count.load(std::memory_order_relaxed);
    }

    size_t getElementCount() const override { return element_count.load(std::memory_order_relaxed); }

    double getLoadFactor() const override { return static_cast<double>(element_count.load(std::memory_order_relaxed)) / size; }

    bool isCached() const override { return true; }

    std::unique_ptr<IExternalLoadable> clone() const override
    {
        return std::make_unique<CacheDictionary>(name, dict_struct, source_ptr->clone(), dict_lifetime, size);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    std::chrono::time_point<std::chrono::system_clock> getCreationTime() const override { return creation_time; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
    }

    bool hasHierarchy() const override { return hierarchical_attribute; }

    void toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const override;

    void isInVectorVector(
        const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;
    void isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const override;
    void isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

#define DECLARE(TYPE) \
    void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const;
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
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const;
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
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

    void
    getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out)
        const;

#define DECLARE(TYPE) \
    void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def, ResultArrayType<TYPE> & out) const;
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
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const;

    void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
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

        UInt64 id;
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


    template <typename OutputType, typename DefaultGetter>
    void getItemsNumber(
        Attribute & attribute, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const;

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
        Attribute & attribute, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const;

    template <typename DefaultGetter>
    void getItemsString(Attribute & attribute, const PaddedPODArray<Key> & ids, ColumnString * out, DefaultGetter && get_default) const;

    template <typename PresentIdHandler, typename AbsentIdHandler>
    void update(const std::vector<Key> & requested_ids, PresentIdHandler && on_cell_updated, AbsentIdHandler && on_id_not_found) const;

    PaddedPODArray<Key> getCachedIds() const;

    bool isEmptyCell(const UInt64 idx) const;

    size_t getCellIdx(const Key id) const;

    void setDefaultAttributeValue(Attribute & attribute, const Key idx) const;

    void setAttributeValue(Attribute & attribute, const Key idx, const Field & value) const;

    Attribute & getAttribute(const std::string & attribute_name) const;

    struct FindResult
    {
        const size_t cell_idx;
        const bool valid;
        const bool outdated;
    };

    FindResult findCellIdx(const Key & id, const CellMetadata::time_point_t now) const;

    template <typename AncestorType>
    void isInImpl(const PaddedPODArray<Key> & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const;

    const std::string name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    mutable std::shared_mutex rw_lock;

    /// Actual size will be increased to match power of 2
    const size_t size;

    /// all bits to 1  mask (size - 1) (0b1000 - 1 = 0b111)
    const size_t size_overlap_mask;

    /// Max tries to find cell, overlaped with mask: if size = 16 and start_cell=10: will try cells: 10,11,12,13,14,15,0,1,2,3
    static constexpr size_t max_collision_length = 10;

    const size_t zero_cell_idx{getCellIdx(0)};
    std::map<std::string, size_t> attribute_index_by_name;
    mutable std::vector<Attribute> attributes;
    mutable std::vector<CellMetadata> cells;
    Attribute * hierarchical_attribute = nullptr;
    std::unique_ptr<ArenaWithFreeLists> string_arena;

    mutable pcg64 rnd_engine;

    mutable size_t bytes_allocated = 0;
    mutable std::atomic<size_t> element_count{0};
    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};

    const std::chrono::time_point<std::chrono::system_clock> creation_time = std::chrono::system_clock::now();
};

}
