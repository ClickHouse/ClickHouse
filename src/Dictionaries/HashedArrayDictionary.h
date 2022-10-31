#pragma once

#include <atomic>
#include <memory>
#include <variant>
#include <optional>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Core/Block.h>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>

/** This dictionary stores all attributes in arrays.
  * Key is stored in hash table and value is index into attribute array.
  */

namespace DB
{

struct HashedArrayDictionaryStorageConfiguration
{
    const bool require_nonempty;
    const DictionaryLifetime lifetime;
};

template <DictionaryKeyType dictionary_key_type>
class HashedArrayDictionary final : public IDictionary
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, StringRef>;

    HashedArrayDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const HashedArrayDictionaryStorageConfiguration & configuration_,
        BlockPtr update_field_loaded_block_ = nullptr);

    std::string getTypeName() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            return "HashedArray";
        else
            return "ComplexHashedArray";
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
        return std::make_shared<HashedArrayDictionary<dictionary_key_type>>(getDictionaryID(), dict_struct, source_ptr->clone(), configuration, update_field_loaded_block);
    }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return configuration.lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override { return dictionary_key_type; }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        const Columns & default_values_columns) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    bool hasHierarchy() const override { return dictionary_key_type == DictionaryKeyType::Simple && dict_struct.hierarchical_attribute_index.has_value(); }

    ColumnPtr getHierarchy(ColumnPtr key_column, const DataTypePtr & hierarchy_attribute_type) const override;

    ColumnUInt8::Ptr isInHierarchy(
        ColumnPtr key_column,
        ColumnPtr in_key_column,
        const DataTypePtr & key_type) const override;

    DictionaryHierarchicalParentToChildIndexPtr getHierarchicalIndex() const override;

    size_t getHierarchicalIndexBytesAllocated() const override { return hierarchical_index_bytes_allocated; }

    ColumnPtr getDescendants(
        ColumnPtr key_column,
        const DataTypePtr & key_type,
        size_t level,
        DictionaryHierarchicalParentToChildIndexPtr parent_to_child_index) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

private:

    using KeyContainerType = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashMap<UInt64, size_t>,
        HashMapWithSavedHash<StringRef, size_t, DefaultHash<StringRef>>>;

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

        std::optional<std::vector<bool>> is_index_null;
    };

    struct KeyAttribute final
    {

        KeyContainerType container;

    };

    void createAttributes();

    void blockToAttributes(const Block & block);

    void updateData();

    void loadData();

    void buildHierarchyParentToChildIndexIfNeeded();

    void calculateBytesAllocated();

    template <typename KeysProvider>
    ColumnPtr getAttributeColumn(
        const Attribute & attribute,
        const DictionaryAttribute & dictionary_attribute,
        size_t keys_size,
        ColumnPtr default_values_column,
        KeysProvider && keys_object) const;

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const PaddedPODArray<ssize_t> & key_index_to_element_index,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func);

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func) const;

    void resize(size_t added_rows);

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const HashedArrayDictionaryStorageConfiguration configuration;

    std::vector<Attribute> attributes;

    KeyAttribute key_attribute;

    size_t bytes_allocated = 0;
    size_t hierarchical_index_bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    BlockPtr update_field_loaded_block;
    Arena string_arena;
    DictionaryHierarchicalParentToChildIndexPtr hierarchical_index;
};

extern template class HashedArrayDictionary<DictionaryKeyType::Simple>;
extern template class HashedArrayDictionary<DictionaryKeyType::Complex>;

}
