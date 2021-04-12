#pragma once

#include <atomic>
#include <variant>
#include <vector>
#include <optional>

#include <Common/HashTable/HashSet.h>
#include <Common/Arena.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/IDataType.h>
#include <Core/Block.h>
#include <ext/range.h>
#include <ext/size.h>

#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryHelpers.h"

namespace DB
{

class FlatDictionary final : public IDictionary
{
public:
    struct Configuration
    {
        size_t initial_array_size;
        size_t max_array_size;
        bool require_nonempty;
    };

    FlatDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        Configuration configuration_,
        BlockPtr previously_loaded_block_ = nullptr);

    std::string getTypeName() const override { return "Flat"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<FlatDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, configuration, previously_loaded_block);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::simple; }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    bool hasHierarchy() const override { return dict_struct.hierarchical_attribute_index.has_value(); }

    ColumnPtr getHierarchy(ColumnPtr key_column, const DataTypePtr & key_type) const override;

    ColumnUInt8::Ptr isInHierarchy(
        ColumnPtr key_column,
        ColumnPtr in_key_column,
        const DataTypePtr & key_type) const override;

    ColumnPtr getDescendants(
        ColumnPtr key_column,
        const DataTypePtr & key_type,
        size_t level) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    template <typename Value>
    using ContainerType = PaddedPODArray<Value>;

    using NullableSet = HashSet<UInt64, DefaultHash<UInt64>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;
        std::optional<NullableSet> nullable_set;

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
            Decimal256,
            Float32,
            Float64,
            StringRef>
            null_values;
        std::variant<
            ContainerType<UInt8>,
            ContainerType<UInt16>,
            ContainerType<UInt32>,
            ContainerType<UInt64>,
            ContainerType<UInt128>,
            ContainerType<Int8>,
            ContainerType<Int16>,
            ContainerType<Int32>,
            ContainerType<Int64>,
            ContainerType<Decimal32>,
            ContainerType<Decimal64>,
            ContainerType<Decimal128>,
            ContainerType<Decimal256>,
            ContainerType<Float32>,
            ContainerType<Float64>,
            ContainerType<StringRef>>
            container;

        std::unique_ptr<Arena> string_arena;
    };

    void createAttributes();
    void blockToAttributes(const Block & block);
    void updateData();
    void loadData();

    void calculateBytesAllocated();

    Attribute createAttribute(const DictionaryAttribute& attribute, const Field & null_value);

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const PaddedPODArray<UInt64> & keys,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename T>
    void resize(Attribute & attribute, UInt64 key);

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, UInt64 key, const T & value);

    void setAttributeValue(Attribute & attribute, UInt64 key, const Field & value);

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const Configuration configuration;

    std::vector<Attribute> attributes;
    std::vector<bool> loaded_keys;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};

    BlockPtr previously_loaded_block;
};

}
