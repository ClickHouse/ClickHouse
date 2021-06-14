#pragma once

#include <atomic>
#include <memory>
#include <variant>
#include <optional>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryHelpers.h"

namespace DB
{
class RangeHashedDictionary final : public IDictionary
{
public:
    RangeHashedDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        bool require_nonempty_);

    std::string getTypeName() const override { return "RangeHashed"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getFoundRate() const override
    {
        size_t queries = query_count.load(std::memory_order_relaxed);
        if (!queries)
            return 0;
        return static_cast<double>(found_count.load(std::memory_order_relaxed)) / queries;
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<RangeHashedDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, require_nonempty);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
    }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::range; }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    using RangeStorageType = Int64;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

    struct Range
    {
        RangeStorageType left;
        RangeStorageType right;

        static bool isCorrectDate(const RangeStorageType & date);
        bool contains(const RangeStorageType & value) const;
    };

private:
    template <typename T>
    struct Value final
    {
        Range range;
        std::optional<T> value;
    };

    template <typename T>
    using Values = std::vector<Value<T>>;
    template <typename T>
    using Collection = HashMap<UInt64, Values<T>>;
    template <typename T>
    using Ptr = std::unique_ptr<Collection<T>>;

    struct Attribute final
    {
    public:
        AttributeUnderlyingType type;
        bool is_nullable;

        std::variant<
            Ptr<UInt8>,
            Ptr<UInt16>,
            Ptr<UInt32>,
            Ptr<UInt64>,
            Ptr<UInt128>,
            Ptr<UInt256>,
            Ptr<Int8>,
            Ptr<Int16>,
            Ptr<Int32>,
            Ptr<Int64>,
            Ptr<Int128>,
            Ptr<Int256>,
            Ptr<Decimal32>,
            Ptr<Decimal64>,
            Ptr<Decimal128>,
            Ptr<Decimal256>,
            Ptr<Float32>,
            Ptr<Float64>,
            Ptr<UUID>,
            Ptr<StringRef>,
            Ptr<Array>>
            maps;
        std::unique_ptr<Arena> string_arena;
    };

    void createAttributes();

    void loadData();

    template <typename T>
    void addAttributeSize(const Attribute & attribute);

    void calculateBytesAllocated();

    static Attribute createAttribute(const DictionaryAttribute & dictionary_attribute);

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const Columns & key_columns,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename AttributeType>
    ColumnUInt8::Ptr hasKeysImpl(
        const Attribute & attribute,
        const PaddedPODArray<UInt64> & ids,
        const PaddedPODArray<RangeStorageType> & dates,
        size_t & keys_found) const;

    template <typename T>
    static void setAttributeValueImpl(Attribute & attribute, const UInt64 id, const Range & range, const Field & value);

    static void setAttributeValue(Attribute & attribute, const UInt64 id, const Range & range, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    const Attribute & getAttributeWithType(const std::string & name, const AttributeUnderlyingType type) const;

    template <typename RangeType>
    void getIdsAndDates(PaddedPODArray<UInt64> & ids, PaddedPODArray<RangeType> & start_dates, PaddedPODArray<RangeType> & end_dates) const;

    template <typename T, typename RangeType>
    void getIdsAndDates(
        const Attribute & attribute,
        PaddedPODArray<UInt64> & ids,
        PaddedPODArray<RangeType> & start_dates,
        PaddedPODArray<RangeType> & end_dates) const;

    template <typename RangeType>
    BlockInputStreamPtr getBlockInputStreamImpl(const Names & column_names, size_t max_block_size) const;

    friend struct RangeHashedDictionaryCallGetBlockInputStreamImpl;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<Attribute> attributes;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};
};

}
