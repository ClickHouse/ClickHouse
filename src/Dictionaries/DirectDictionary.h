#pragma once

#include <atomic>
#include <variant>
#include <vector>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Core/Block.h>
#include <ext/range.h>
#include <ext/size.h>
#include <Common/HashTable/HashMap.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryHelpers.h"

namespace DB
{

class DirectDictionary final : public IDictionary
{
public:
    DirectDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        BlockPtr saved_block_ = nullptr);

    std::string getTypeName() const override { return "Direct"; }

    size_t getBytesAllocated() const override { return 0; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return 0; }

    double getLoadFactor() const override { return 0; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<DirectDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), saved_block);
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

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::simple; }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    struct Attribute final
    {
        AttributeUnderlyingType type;
        bool is_nullable;
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
        std::unique_ptr<Arena> string_arena;
        std::string name;
    };

    void createAttributes();

    template <typename T>
    void addAttributeSize(const Attribute & attribute);

    template <typename T>
    static void createAttributeImpl(Attribute & attribute, const Field & null_value);

    static Attribute createAttribute(const DictionaryAttribute& attribute, const Field & null_value, const std::string & name);

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const PaddedPODArray<Key> & ids,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, const Key id, const T & value);

    void setAttributeValue(Attribute & attribute, const Key id, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    Key getValueOrNullByKey(const Key & to_find) const;

    template <typename ChildType, typename AncestorType>
    void isInImpl(const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    std::map<std::string, size_t> attribute_index_by_name;
    std::map<size_t, std::string> attribute_name_by_index;
    std::vector<Attribute> attributes;
    const Attribute * hierarchical_attribute = nullptr;

    mutable std::atomic<size_t> query_count{0};

    BlockPtr saved_block;
};

}
