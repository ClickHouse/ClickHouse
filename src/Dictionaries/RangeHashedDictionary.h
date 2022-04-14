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

enum class RangeHashedDictionaryLookupStrategy : uint8_t
{
    min,
    max
};

struct RangeHashedDictionaryConfiguration
{
    bool convert_null_range_bound_to_open;
    RangeHashedDictionaryLookupStrategy lookup_strategy;
    bool require_nonempty;
};

template <DictionaryKeyType dictionary_key_type>
class RangeHashedDictionary final : public IDictionary
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, StringRef>;

    RangeHashedDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        DictionaryLifetime dict_lifetime_,
        RangeHashedDictionaryConfiguration configuration_,
        BlockPtr update_field_loaded_block_ = nullptr);

    std::string getTypeName() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            return "RangeHashed";
        else
            return "ComplexKeyRangeHashed";
    }

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
        auto result = std::make_shared<RangeHashedDictionary>(
            getDictionaryID(),
            dict_struct,
            source_ptr->clone(),
            dict_lifetime,
            configuration,
            update_field_loaded_block);

        return result;
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
        const std::string & attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

private:

    template <typename RangeStorageType>
    using IntervalMap = IntervalMap<Interval<RangeStorageType>, size_t>;

    template <typename RangeStorageType>
    using KeyAttributeContainerType = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashMap<UInt64, IntervalMap<RangeStorageType>, DefaultHash<UInt64>>,
        HashMapWithSavedHash<StringRef, IntervalMap<RangeStorageType>, DefaultHash<StringRef>>>;

    template <typename Value>
    using AttributeContainerType = std::conditional_t<std::is_same_v<Value, Array>, std::vector<Value>, PaddedPODArray<Value>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;

        std::variant<
            AttributeContainerType<UInt8>,
            AttributeContainerType<UInt16>,
            AttributeContainerType<UInt32>,
            AttributeContainerType<UInt64>,
            AttributeContainerType<UInt128>,
            AttributeContainerType<UInt256>,
            AttributeContainerType<Int8>,
            AttributeContainerType<Int16>,
            AttributeContainerType<Int32>,
            AttributeContainerType<Int64>,
            AttributeContainerType<Int128>,
            AttributeContainerType<Int256>,
            AttributeContainerType<Decimal32>,
            AttributeContainerType<Decimal64>,
            AttributeContainerType<Decimal128>,
            AttributeContainerType<Decimal256>,
            AttributeContainerType<DateTime64>,
            AttributeContainerType<Float32>,
            AttributeContainerType<Float64>,
            AttributeContainerType<UUID>,
            AttributeContainerType<StringRef>,
            AttributeContainerType<Array>>
            container;

        std::optional<std::vector<bool>> is_value_nullable;
    };

    template <typename RangeStorageType>
    struct InvalidIntervalWithKey
    {
        KeyType key;
        Interval<RangeStorageType> interval;
        size_t attribute_value_index;
    };

    template <typename RangeStorageType>
    using InvalidIntervalsContainerType = PaddedPODArray<InvalidIntervalWithKey<RangeStorageType>>;

    template <template<typename> typename ContainerType>
    using RangeStorageTypeContainer = std::variant<
        ContainerType<UInt8>,
        ContainerType<UInt16>,
        ContainerType<UInt32>,
        ContainerType<UInt64>,
        ContainerType<UInt128>,
        ContainerType<UInt256>,
        ContainerType<Int8>,
        ContainerType<Int16>,
        ContainerType<Int32>,
        ContainerType<Int64>,
        ContainerType<Int128>,
        ContainerType<Int256>,
        ContainerType<Decimal32>,
        ContainerType<Decimal64>,
        ContainerType<Decimal128>,
        ContainerType<Decimal256>,
        ContainerType<DateTime64>,
        ContainerType<Float32>,
        ContainerType<Float64>,
        ContainerType<UUID>>;

    struct KeyAttribute final
    {
        RangeStorageTypeContainer<KeyAttributeContainerType> container;

        RangeStorageTypeContainer<InvalidIntervalsContainerType> invalid_intervals_container;

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

    ColumnPtr getColumnInternal(
        const std::string & attribute_name,
        const DataTypePtr & result_type,
        const PaddedPODArray<UInt64> & key_to_index) const;

    template <typename AttributeType, bool is_nullable, typename ValueSetter>
    void getItemsInternalImpl(
        const Attribute & attribute,
        const PaddedPODArray<UInt64> & key_to_index,
        ValueSetter && set_value) const;

    void updateData();

    void blockToAttributes(const Block & block);

    void setAttributeValue(Attribute & attribute, const Field & value);

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const RangeHashedDictionaryConfiguration configuration;
    BlockPtr update_field_loaded_block;

    std::vector<Attribute> attributes;
    KeyAttribute key_attribute;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};
    Arena string_arena;
};

}
