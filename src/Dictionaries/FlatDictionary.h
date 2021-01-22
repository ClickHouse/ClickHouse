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


namespace DB
{
using BlockPtr = std::shared_ptr<Block>;

template <typename DefaultValue>
class DefaultValueExtractor
{
public:
    using ResultColumnType = std::conditional_t<
        std::is_same_v<DefaultValue, StringRef>,
        ColumnString,
        std::conditional_t<IsDecimalNumber<DefaultValue>, ColumnDecimal<DefaultValue>, ColumnVector<DefaultValue>>>;

    DefaultValueExtractor(DefaultValue default_value_, ColumnPtr default_values_)
    {
        if (default_values_ != nullptr)
        {
            if (const auto * const default_col = checkAndGetColumn<ResultColumnType>(*default_values_))
            {
                default_values = default_col;
            }
            else if (const auto * const default_col_const = checkAndGetColumnConst<ResultColumnType>(default_values_.get()))
            {
                using ConstColumnValue = std::conditional_t<std::is_same_v<DefaultValue, StringRef>, String, DefaultValue>;
                default_value = std::make_optional<DefaultValue>(default_col_const->template getValue<ConstColumnValue>());
            }
            else
                throw Exception{"Type of default column is not the same as result type.", ErrorCodes::TYPE_MISMATCH};
        }
        else
            default_value = std::make_optional<DefaultValue>(default_value_);
    }

    DefaultValue operator[](size_t row)
    {
        if (default_value)
            return *default_value;
        
        if constexpr (std::is_same_v<ResultColumnType, ColumnString>)
            return default_values->getDataAt(row);
        else
            return default_values->getData()[row];
    }
private:
    const ResultColumnType * default_values = nullptr;
    std::optional<DefaultValue> default_value = {};
};

class FlatDictionary final : public IDictionary
{
public:
    FlatDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        bool require_nonempty_,
        BlockPtr saved_block_ = nullptr);

    std::string getTypeName() const override { return "Flat"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<FlatDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, require_nonempty, saved_block);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

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

    DictionaryIdentifierType getIdentifierType() const override { return DictionaryIdentifierType::simple; }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr default_values_column) const override;

    ColumnUInt8::Ptr has(const Columns & key_columns, const DataTypes & key_types) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    template <typename Value>
    using ContainerType = PaddedPODArray<Value>;

    using NullableSet = HashSet<Key, DefaultHash<Key>>;

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
            ContainerType<Float32>,
            ContainerType<Float64>,
            ContainerType<StringRef>>
            arrays;

        std::unique_ptr<Arena> string_arena;
    };

    void createAttributes();
    void blockToAttributes(const Block & block);
    void updateData();
    void loadData();

    template <typename T>
    void addAttributeSize(const Attribute & attribute);

    void calculateBytesAllocated();

    template <typename T>
    void createAttributeImpl(Attribute & attribute, const Field & null_value);

    Attribute createAttribute(const DictionaryAttribute& attribute, const Field & null_value);

    template <typename AttributeType, typename OutputType, typename ValueSetter>
    void getItemsImpl(
        const Attribute & attribute, const PaddedPODArray<Key> & ids, ValueSetter && set_value, DefaultValueExtractor<AttributeType> & default_value_extractor) const;

    template <typename T>
    void resize(Attribute & attribute, const Key id);

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, const Key id, const T & value);

    void setAttributeValue(Attribute & attribute, const Key id, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    template <typename ChildType, typename AncestorType>
    void isInImpl(const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const;

    PaddedPODArray<Key> getIds() const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<Attribute> attributes;
    const Attribute * hierarchical_attribute = nullptr;
    std::vector<bool> loaded_ids;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};

    BlockPtr saved_block;
};

}
