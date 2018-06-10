#pragma once

#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <ext/range.h>
#include <ext/size.h>
#include <atomic>
#include <vector>
#include <tuple>


namespace DB
{

using BlockPtr = std::shared_ptr<Block>;

class FlatDictionary final : public IDictionary
{
public:
    FlatDictionary(const std::string & name, const DictionaryStructure & dict_struct,
        DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime, bool require_nonempty, BlockPtr saved_block = nullptr);

    FlatDictionary(const FlatDictionary & other);

    std::exception_ptr getCreationException() const override { return creation_exception; }

    std::string getName() const override { return name; }

    std::string getTypeName() const override { return "Flat"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    bool isCached() const override { return false; }

    std::unique_ptr<IExternalLoadable> clone() const override { return std::make_unique<FlatDictionary>(*this); }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    std::chrono::time_point<std::chrono::system_clock> getCreationTime() const override
    {
        return creation_time;
    }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
    }

    bool hasHierarchy() const override { return hierarchical_attribute; }

    void toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const override;

    void isInVectorVector(const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;
    void isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const override;
    void isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;

#define DECLARE(TYPE)\
    void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, PaddedPODArray<TYPE> & out) const;
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
#undef DECLARE

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const;

#define DECLARE(TYPE)\
    void get##TYPE(\
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<TYPE> & def,\
        PaddedPODArray<TYPE> & out) const;
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
#undef DECLARE

    void getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def,
        ColumnString * const out) const;

#define DECLARE(TYPE)\
    void get##TYPE(\
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def,\
        PaddedPODArray<TYPE> & out) const;
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
#undef DECLARE

    void getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def,
        ColumnString * const out) const;

    void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    template <typename Value> using ContainerType = PaddedPODArray<Value>;
    template <typename Value> using ContainerPtrType = std::unique_ptr<ContainerType<Value>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;
        std::tuple<
            UInt8, UInt16, UInt32, UInt64,
            UInt128,
            Int8, Int16, Int32, Int64,
            Float32, Float64,
            StringRef> null_values;
        std::tuple<
            ContainerPtrType<UInt8>, ContainerPtrType<UInt16>, ContainerPtrType<UInt32>, ContainerPtrType<UInt64>,
            ContainerPtrType<UInt128>,
            ContainerPtrType<Int8>, ContainerPtrType<Int16>, ContainerPtrType<Int32>, ContainerPtrType<Int64>,
            ContainerPtrType<Float32>, ContainerPtrType<Float64>,
            ContainerPtrType<StringRef>> arrays;
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

    template <typename OutputType, typename ValueSetter, typename DefaultGetter>
    void getItemsNumber(
        const Attribute & attribute,
        const PaddedPODArray<Key> & ids,
        ValueSetter && set_value,
        DefaultGetter && get_default) const;

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
    void getItemsImpl(
        const Attribute & attribute,
        const PaddedPODArray<Key> & ids,
        ValueSetter && set_value,
        DefaultGetter && get_default) const;

    template <typename T>
    void resize(Attribute & attribute, const Key id);

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, const Key id, const T& value);

    void setAttributeValue(Attribute & attribute, const Key id, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    template <typename T>
    void has(const Attribute & attribute, const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const;

    template <typename ChildType, typename AncestorType>
    void isInImpl(
        const ChildType & child_ids,
        const AncestorType & ancestor_ids,
        PaddedPODArray<UInt8> & out) const;

    PaddedPODArray<Key> getIds() const;

    const std::string name;
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

    std::chrono::time_point<std::chrono::system_clock> creation_time;

    std::exception_ptr creation_exception;

    BlockPtr saved_block;
};

}
