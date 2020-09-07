#pragma once

#include <atomic>
#include <memory>
#include <variant>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Common/HashTable/HashMap.h>
#include <sparsehash/sparse_hash_map>
#include <ext/range.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"

/** This dictionary stores all content in a hash table in memory
  * (a separate Key -> Value map for each attribute)
  * Two variants of hash table are supported: a fast HashMap and memory efficient sparse_hash_map.
  */

namespace DB
{
using BlockPtr = std::shared_ptr<Block>;

class HashedDictionary final : public IDictionary
{
public:
    HashedDictionary(
        const std::string & database_,
        const std::string & name_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        bool require_nonempty_,
        bool sparse_,
        BlockPtr saved_block_ = nullptr);

    const std::string & getDatabase() const override { return database; }
    const std::string & getName() const override { return name; }
    const std::string & getFullName() const override { return full_name; }

    std::string getTypeName() const override { return sparse ? "SparseHashed" : "Hashed"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<HashedDictionary>(database, name, dict_struct, source_ptr->clone(), dict_lifetime, require_nonempty, sparse, saved_block);
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

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

#define DECLARE(TYPE) \
    void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

    void
    getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out)
        const;

#define DECLARE(TYPE) \
    void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE & def, ResultArrayType<TYPE> & out) \
        const;
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const;

    void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const override;

    void isInVectorVector(
        const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;
    void isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const override;
    void isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    template <typename Value>
    using CollectionType = HashMap<UInt64, Value>;
    template <typename Value>
    using CollectionPtrType = std::unique_ptr<CollectionType<Value>>;

#if !defined(ARCADIA_BUILD)
    template <typename Value>
    using SparseCollectionType = google::sparse_hash_map<UInt64, Value, DefaultHash<UInt64>>;
#else
    template <typename Value>
    using SparseCollectionType = google::sparsehash::sparse_hash_map<UInt64, Value, DefaultHash<UInt64>>;
#endif

    template <typename Value>
    using SparseCollectionPtrType = std::unique_ptr<SparseCollectionType<Value>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;
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
            String>
            null_values;
        std::variant<
            CollectionPtrType<UInt8>,
            CollectionPtrType<UInt16>,
            CollectionPtrType<UInt32>,
            CollectionPtrType<UInt64>,
            CollectionPtrType<UInt128>,
            CollectionPtrType<Int8>,
            CollectionPtrType<Int16>,
            CollectionPtrType<Int32>,
            CollectionPtrType<Int64>,
            CollectionPtrType<Decimal32>,
            CollectionPtrType<Decimal64>,
            CollectionPtrType<Decimal128>,
            CollectionPtrType<Float32>,
            CollectionPtrType<Float64>,
            CollectionPtrType<StringRef>>
            maps;
        std::variant<
            SparseCollectionPtrType<UInt8>,
            SparseCollectionPtrType<UInt16>,
            SparseCollectionPtrType<UInt32>,
            SparseCollectionPtrType<UInt64>,
            SparseCollectionPtrType<UInt128>,
            SparseCollectionPtrType<Int8>,
            SparseCollectionPtrType<Int16>,
            SparseCollectionPtrType<Int32>,
            SparseCollectionPtrType<Int64>,
            SparseCollectionPtrType<Decimal32>,
            SparseCollectionPtrType<Decimal64>,
            SparseCollectionPtrType<Decimal128>,
            SparseCollectionPtrType<Float32>,
            SparseCollectionPtrType<Float64>,
            SparseCollectionPtrType<StringRef>>
            sparse_maps;
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

    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);

    template <typename OutputType, typename AttrType, typename ValueSetter, typename DefaultGetter>
    void getItemsAttrImpl(
        const AttrType & attr, const PaddedPODArray<Key> & ids, ValueSetter && set_value, DefaultGetter && get_default) const;
    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
    void getItemsImpl(
        const Attribute & attribute, const PaddedPODArray<Key> & ids, ValueSetter && set_value, DefaultGetter && get_default) const;

    template <typename T>
    bool setAttributeValueImpl(Attribute & attribute, const Key id, const T value);

    bool setAttributeValue(Attribute & attribute, const Key id, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    template <typename T>
    void has(const Attribute & attribute, const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const;

    template <typename T, typename AttrType>
    PaddedPODArray<Key> getIdsAttrImpl(const AttrType & attr) const;
    template <typename T>
    PaddedPODArray<Key> getIds(const Attribute & attribute) const;

    PaddedPODArray<Key> getIds() const;

    template <typename AttrType, typename ChildType, typename AncestorType>
    void isInAttrImpl(const AttrType & attr, const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const;
    template <typename ChildType, typename AncestorType>
    void isInImpl(const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const;

    const std::string database;
    const std::string name;
    const std::string full_name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;
    const bool sparse;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<Attribute> attributes;
    const Attribute * hierarchical_attribute = nullptr;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};

    BlockPtr saved_block;
};

}
