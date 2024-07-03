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
#include <Dictionaries/HashedDictionaryParallelLoader.h>

/** This dictionary stores all attributes in arrays.
  * Key is stored in hash table and value is index into attribute array.
  */

namespace DB
{

struct HashedArrayDictionaryStorageConfiguration
{
    const bool require_nonempty;
    const DictionaryLifetime lifetime;
    size_t shards = 1;
    size_t shard_load_queue_backlog = 10000;
    bool use_async_executor = false;
    std::chrono::seconds load_timeout{0};
};

template <DictionaryKeyType dictionary_key_type, bool sharded>
class HashedArrayDictionary final : public IDictionary
{
    using DictionaryParallelLoaderType = HashedDictionaryImpl::HashedDictionaryParallelLoader<dictionary_key_type, HashedArrayDictionary<dictionary_key_type, sharded>>;
    friend class HashedDictionaryImpl::HashedDictionaryParallelLoader<dictionary_key_type, HashedArrayDictionary<dictionary_key_type, sharded>>;

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

    size_t getQueryCount() const override { return query_count.load(); }

    double getFoundRate() const override
    {
        size_t queries = query_count.load();
        if (!queries)
            return 0;
        return std::min(1.0, static_cast<double>(found_count.load()) / queries);
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return total_element_count; }

    double getLoadFactor() const override { return static_cast<double>(total_element_count) / bucket_count; }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<HashedArrayDictionary<dictionary_key_type, sharded>>(getDictionaryID(), dict_struct, source_ptr->clone(), configuration, update_field_loaded_block);
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
        const std::string & attribute_name,
        const DataTypePtr & attribute_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultOrFilter default_or_filter) const override;

    Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & attribute_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultsOrFilter defaults_or_filter) const override;

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

    template <typename Value>
    using AttributeContainerShardsType = std::vector<AttributeContainerType<Value>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;

        std::variant<
            AttributeContainerShardsType<UInt8>,
            AttributeContainerShardsType<UInt16>,
            AttributeContainerShardsType<UInt32>,
            AttributeContainerShardsType<UInt64>,
            AttributeContainerShardsType<UInt128>,
            AttributeContainerShardsType<UInt256>,
            AttributeContainerShardsType<Int8>,
            AttributeContainerShardsType<Int16>,
            AttributeContainerShardsType<Int32>,
            AttributeContainerShardsType<Int64>,
            AttributeContainerShardsType<Int128>,
            AttributeContainerShardsType<Int256>,
            AttributeContainerShardsType<Decimal32>,
            AttributeContainerShardsType<Decimal64>,
            AttributeContainerShardsType<Decimal128>,
            AttributeContainerShardsType<Decimal256>,
            AttributeContainerShardsType<DateTime64>,
            AttributeContainerShardsType<Float32>,
            AttributeContainerShardsType<Float64>,
            AttributeContainerShardsType<UUID>,
            AttributeContainerShardsType<IPv4>,
            AttributeContainerShardsType<IPv6>,
            AttributeContainerShardsType<StringRef>,
            AttributeContainerShardsType<Array>>
            containers;

        /// One container per shard
        using RowsMask = std::vector<bool>;
        std::optional<std::vector<RowsMask>> is_index_null;
    };

    struct KeyAttribute final
    {
        /// One container per shard
        std::vector<KeyContainerType> containers;
    };

    void createAttributes();

    void blockToAttributes(const Block & block, DictionaryKeysArenaHolder<dictionary_key_type> & arena_holder, size_t shard);

    void updateData();

    void loadData();

    void buildHierarchyParentToChildIndexIfNeeded();

    void calculateBytesAllocated();

    UInt64 getShard(UInt64 key) const
    {
        if constexpr (!sharded)
            return 0;
        /// NOTE: function here should not match with the DefaultHash<> since
        /// it used for the HashMap/sparse_hash_map.
        return intHashCRC32(key) % configuration.shards;
    }

    UInt64 getShard(StringRef key) const
    {
        if constexpr (!sharded)
            return 0;
        return StringRefHash()(key) % configuration.shards;
    }

    template <typename KeysProvider>
    ColumnPtr getAttributeColumn(
        const Attribute & attribute,
        const DictionaryAttribute & dictionary_attribute,
        size_t keys_size,
        DefaultOrFilter default_or_filter,
        KeysProvider && keys_object) const;

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename AttributeType, bool is_nullable, typename ValueSetter>
    void getItemsShortCircuitImpl(
        const Attribute & attribute,
        DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
        ValueSetter && set_value,
        IColumn::Filter & default_mask) const;

    using KeyIndexToElementIndex = std::conditional_t<sharded, PaddedPODArray<std::pair<ssize_t, UInt8>>, PaddedPODArray<ssize_t>>;

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const KeyIndexToElementIndex & key_index_to_element_index,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename AttributeType, bool is_nullable, typename ValueSetter>
    void getItemsShortCircuitImpl(
        const Attribute & attribute,
        const KeyIndexToElementIndex & key_index_to_element_index,
        ValueSetter && set_value,
        IColumn::Filter & default_mask [[maybe_unused]]) const;

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func);

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func) const;

    void resize(size_t total_rows);

    LoggerPtr log;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const HashedArrayDictionaryStorageConfiguration configuration;

    std::vector<Attribute> attributes;

    KeyAttribute key_attribute;

    size_t bytes_allocated = 0;
    size_t hierarchical_index_bytes_allocated = 0;
    std::atomic<size_t> total_element_count = 0;
    std::vector<size_t> element_counts;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    BlockPtr update_field_loaded_block;
    std::vector<std::unique_ptr<Arena>> string_arenas;
    DictionaryHierarchicalParentToChildIndexPtr hierarchical_index;
};

extern template class HashedArrayDictionary<DictionaryKeyType::Simple, false>;
extern template class HashedArrayDictionary<DictionaryKeyType::Simple, true>;
extern template class HashedArrayDictionary<DictionaryKeyType::Complex, false>;
extern template class HashedArrayDictionary<DictionaryKeyType::Complex, true>;

}
