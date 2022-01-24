#pragma once

#include <atomic>
#include <memory>
#include <variant>
#include <optional>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/HashTable/HashMap.h>
#include <Common/IntervalTree.h>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>


namespace DB
{

using RangeStorageType = Int64;

template <DictionaryKeyType dictionary_key_type>
class RangeHashedDictionary final : public IDictionary
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, StringRef>;

    RangeHashedDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        bool require_nonempty_,
        BlockPtr update_field_loaded_block_ = nullptr);

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
        return std::make_shared<RangeHashedDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, require_nonempty, update_field_loaded_block);
    }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override { return dictionary_key_type; }

    DictionarySpecialKeyType getSpecialKeyType() const override { return DictionarySpecialKeyType::Range;}

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

private:

    using RangeInterval = Interval<RangeStorageType>;

    template <typename T>
    using Values = IntervalMap<RangeInterval, std::optional<T>>;

    template <typename Value>
    using CollectionType = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashMap<UInt64, Values<Value>, DefaultHash<UInt64>>,
        HashMapWithSavedHash<StringRef, Values<Value>, DefaultHash<StringRef>>>;

    using NoAttributesCollectionType = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashMap<UInt64, IntervalSet<RangeInterval>>,
        HashMapWithSavedHash<StringRef, IntervalSet<RangeInterval>>>;

    struct Attribute final
    {
    public:
        AttributeUnderlyingType type;
        bool is_nullable;

        std::variant<
            CollectionType<UInt8>,
            CollectionType<UInt16>,
            CollectionType<UInt32>,
            CollectionType<UInt64>,
            CollectionType<UInt128>,
            CollectionType<UInt256>,
            CollectionType<Int8>,
            CollectionType<Int16>,
            CollectionType<Int32>,
            CollectionType<Int64>,
            CollectionType<Int128>,
            CollectionType<Int256>,
            CollectionType<Decimal32>,
            CollectionType<Decimal64>,
            CollectionType<Decimal128>,
            CollectionType<Decimal256>,
            CollectionType<DateTime64>,
            CollectionType<Float32>,
            CollectionType<Float64>,
            CollectionType<UUID>,
            CollectionType<StringRef>,
            CollectionType<Array>>
            maps;
    };

    void createAttributes();

    void loadData();

    void calculateBytesAllocated();

    static Attribute createAttribute(const DictionaryAttribute & dictionary_attribute);

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const Columns & key_columns,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    void updateData();

    void blockToAttributes(const Block & block);

    void buildAttributeIntervalTrees();

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, KeyType key, const RangeInterval & interval, const Field & value);

    void setAttributeValue(Attribute & attribute, KeyType key, const RangeInterval & interval, const Field & value);

    template <typename RangeType>
    void getKeysAndDates(
        PaddedPODArray<KeyType> & keys,
        PaddedPODArray<RangeType> & start_dates,
        PaddedPODArray<RangeType> & end_dates) const;

    template <typename T, typename RangeType>
    void getKeysAndDates(
        const Attribute & attribute,
        PaddedPODArray<KeyType> & keys,
        PaddedPODArray<RangeType> & start_dates,
        PaddedPODArray<RangeType> & end_dates) const;

    template <typename RangeType>
    PaddedPODArray<Int64> makeDateKeys(
        const PaddedPODArray<RangeType> & block_start_dates,
        const PaddedPODArray<RangeType> & block_end_dates) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;
    BlockPtr update_field_loaded_block;

    std::vector<Attribute> attributes;
    Arena complex_key_arena;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};
    Arena string_arena;
    NoAttributesCollectionType no_attributes_container;
};

}
