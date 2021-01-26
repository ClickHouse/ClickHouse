#pragma once

#include <atomic>
#include <memory>
#include <variant>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Core/Block.h>
#include <common/StringRef.h>
#include <ext/range.h>
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryStructure.h"
#include "DictionaryHelpers.h"

namespace DB
{

class ComplexKeyHashedDictionary final : public IDictionaryBase
{
public:
    ComplexKeyHashedDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        bool require_nonempty_,
        BlockPtr saved_block_ = nullptr);

    std::string getKeyDescription() const { return key_description; }

    std::string getTypeName() const override { return "ComplexKeyHashed"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<ComplexKeyHashedDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, require_nonempty, saved_block);
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
    using ContainerType = HashMapWithSavedHash<StringRef, Value, StringRefHash>;

    using NullableSet = HashSetWithSavedHash<StringRef, StringRefHash>;

    struct Attribute final
    {
        AttributeUnderlyingType type;
        bool is_nullable;
        std::unique_ptr<NullableSet> nullable_set;

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
            maps;
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
    static void createAttributeImpl(Attribute & attribute, const Field & null_value);

    static Attribute createAttribute(const DictionaryAttribute & attribute, const Field & null_value);

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const Columns & key_columns,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename T>
    static bool setAttributeValueImpl(Attribute & attribute, const StringRef key, const T value);

    static bool setAttributeValue(Attribute & attribute, const StringRef key, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    static StringRef placeKeysInPool(const size_t row, const Columns & key_columns, StringRefs & keys, Arena & pool);

    template <typename T>
    void has(const Attribute & attribute, const Columns & key_columns, PaddedPODArray<UInt8> & out) const;

    std::vector<StringRef> getKeys() const;

    template <typename T>
    std::vector<StringRef> getKeys(const Attribute & attribute) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;
    const std::string key_description{dict_struct.getKeyDescription()};

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<Attribute> attributes;
    Arena keys_pool;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};

    BlockPtr saved_block;
};

}
