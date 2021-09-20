#pragma once

#include <atomic>
#include <variant>
#include <vector>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Core/Block.h>

#include <Common/HashTable/HashMap.h>
#include <ext/range.h>
#include <ext/size.h>
#include <ext/map.h>
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryStructure.h"
#include "DictionaryHelpers.h"

namespace DB
{

class ComplexKeyDirectDictionary final : public IDictionaryBase
{
public:
    ComplexKeyDirectDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        BlockPtr saved_block_ = nullptr);

    std::string getTypeName() const override { return "ComplexKeyDirect"; }

    size_t getBytesAllocated() const override { return 0; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return 0; }

    double getLoadFactor() const override { return 0; }

    std::string getKeyDescription() const { return key_description; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<ComplexKeyDirectDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), saved_block);
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

    static Attribute createAttribute(const DictionaryAttribute & attribute, const Field & null_value, const std::string & name);

    template <typename Pool>
    StringRef placeKeysInPool(
        const size_t row, const Columns & key_columns, StringRefs & keys, const std::vector<DictionaryAttribute> & key_attributes, Pool & pool) const;

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const Columns & key_columns,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, const Key id, const T & value);

    void setAttributeValue(Attribute & attribute, const Key id, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    std::map<std::string, size_t> attribute_index_by_name;
    std::map<size_t, std::string> attribute_name_by_index;
    std::vector<Attribute> attributes;

    mutable std::atomic<size_t> query_count{0};

    BlockPtr saved_block;
    const std::string key_description{dict_struct.getKeyDescription()};
};

}
