#pragma once

#include <atomic>
#include <memory>
#include <variant>
#include <optional>
#include <sparsehash/sparse_hash_map>
#include <sparsehash/sparse_hash_set>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Core/Block.h>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>

/** This dictionary stores all content in a hash table in memory
  * (a separate Key -> Value map for each attribute)
  * Two variants of hash table are supported: a fast HashMap and memory efficient sparse_hash_map.
  */

namespace DB
{

struct HashedDictionaryStorageConfiguration
{
    const bool preallocate;
    const bool require_nonempty;
    const DictionaryLifetime lifetime;
};

template <DictionaryKeyType dictionary_key_type, bool sparse>
class HashedDictionary final : public IDictionary
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, StringRef>;

    HashedDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const HashedDictionaryStorageConfiguration & configuration_,
        BlockPtr update_field_loaded_block_ = nullptr);

    std::string getTypeName() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple && sparse)
            return "SparseHashed";
        else if constexpr (dictionary_key_type == DictionaryKeyType::Simple && !sparse)
            return "Hashed";
        else if constexpr (dictionary_key_type == DictionaryKeyType::Complex && sparse)
            return "ComplexKeySparseHashed";
        else
            return "ComplexKeyHashed";
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
        return std::make_shared<HashedDictionary<dictionary_key_type, sparse>>(getDictionaryID(), dict_struct, source_ptr->clone(), configuration, update_field_loaded_block);
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
    template <typename Value>
    using CollectionTypeNonSparse = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashMap<UInt64, Value, DefaultHash<UInt64>>,
        HashMapWithSavedHash<StringRef, Value, DefaultHash<StringRef>>>;

    using NoAttributesCollectionTypeNonSparse = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashSet<UInt64, DefaultHash<UInt64>>,
        HashSetWithSavedHash<StringRef, DefaultHash<StringRef>>>;

    /// Here we use sparse_hash_map with DefaultHash<> for the following reasons:
    ///
    /// - DefaultHash<> is used for HashMap
    /// - DefaultHash<> (from HashTable/Hash.h> works better then std::hash<>
    ///   in case of sequential set of keys, but with random access to this set, i.e.
    ///
    ///       SELECT number FROM numbers(3000000) ORDER BY rand()
    ///
    ///   And even though std::hash<> works better in some other cases,
    ///   DefaultHash<> is preferred since the difference for this particular
    ///   case is significant, i.e. it can be 10x+.
    template <typename Value>
    using CollectionTypeSparse = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        google::sparse_hash_map<UInt64, Value, DefaultHash<KeyType>>,
        google::sparse_hash_map<StringRef, Value, DefaultHash<KeyType>>>;

    using NoAttributesCollectionTypeSparse = google::sparse_hash_set<KeyType, DefaultHash<KeyType>>;

    template <typename Value>
    using CollectionType = std::conditional_t<sparse, CollectionTypeSparse<Value>, CollectionTypeNonSparse<Value>>;

    using NoAttributesCollectionType = std::conditional_t<sparse, NoAttributesCollectionTypeSparse, NoAttributesCollectionTypeNonSparse>;

    using NullableSet = HashSet<KeyType, DefaultHash<KeyType>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;
        std::optional<NullableSet> is_nullable_set;

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
            container;
    };

    void createAttributes();

    void blockToAttributes(const Block & block);

    void updateData();

    void loadData();

    void buildHierarchyParentToChildIndexIfNeeded();

    void calculateBytesAllocated();

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        DictionaryKeysExtractor<dictionary_key_type> & keys_extractor,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func);

    template <typename GetContainerFunc>
    void getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func) const;

    void resize(size_t added_rows);

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const HashedDictionaryStorageConfiguration configuration;

    std::vector<Attribute> attributes;

    size_t bytes_allocated = 0;
    size_t hierarchical_index_bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    BlockPtr update_field_loaded_block;
    Arena string_arena;
    NoAttributesCollectionType no_attributes_container;
    DictionaryHierarchicalParentToChildIndexPtr hierarchical_index;
};

extern template class HashedDictionary<DictionaryKeyType::Simple, false>;
extern template class HashedDictionary<DictionaryKeyType::Simple, true>;

extern template class HashedDictionary<DictionaryKeyType::Complex, false>;
extern template class HashedDictionary<DictionaryKeyType::Complex, true>;

}
