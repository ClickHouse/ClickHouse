#pragma once

#include <atomic>
#include <chrono>
#include <shared_mutex>
#include <variant>
#include <vector>
#include <Core/Block.h>
#include <common/logger_useful.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <pcg_random.hpp>
#include <Common/ArenaWithFreeLists.h>
#include <Common/CurrentMetrics.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include <IO/WriteBufferAIO.h>

namespace DB
{

class SSDCacheDictionary;
class CacheStorage;

/*class SimpleSerializer
{
public:
    bool block() const { return false; }

    template <typename T>
    size_t estimateSizeNumber(T number) const;

    size_t estimateSizeString(const String & str) const;

    template <typename T>
    ssize_t writeNumber(T number, WriteBuffer & buffer);

    ssize_t writeString(const String & str, WriteBuffer & buffer);

    template <typename T>
    ssize_t readNumber(T number, WriteBuffer & buffer);

    ssize_t readString(const String & str, WriteBuffer & buffer);
};*/

class CachePartition
{
public:
    using Offset = size_t;
    using Offsets = std::vector<Offset>;

    CachePartition(
            const AttributeUnderlyingType & key_structure, const std::vector<AttributeUnderlyingType> & attributes_structure,
            const std::string & dir_path, const size_t file_id, const size_t max_size, const size_t buffer_size = 4 * 1024 * 1024);

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out, typename Key>
    void getValue(size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found) const;

    // TODO:: getString

    /// 0 -- not found
    /// 1 -- good
    /// 2 -- expired
    void has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out) const;

    struct Attribute
    {
        template <typename T>
        using Container = std::vector<T>;

        AttributeUnderlyingType type;
        std::variant<
                Container<UInt8>,
                Container<UInt16>,
                Container<UInt32>,
                Container<UInt64>,
                Container<UInt128>,
                Container<Int8>,
                Container<Int16>,
                Container<Int32>,
                Container<Int64>,
                Container<Decimal32>,
                Container<Decimal64>,
                Container<Decimal128>,
                Container<Float32>,
                Container<Float64>,
                Container<String>> values;
    };
    using Attributes = std::vector<Attribute>;


    // Key, (Metadata), attributes
    void appendBlock(const Attribute & new_keys, const Attributes & new_attributes);

private:
    void flush();

    void appendValuesToBufferAttribute(Attribute & to, const Attribute & from);

    size_t file_id;
    size_t max_size;
    size_t buffer_size;
    std::string path;

    //mutable std::shared_mutex rw_lock;
    //int index_fd;
    int data_fd;

    std::unique_ptr<WriteBufferAIO> write_data_buffer;
    std::unordered_map<UInt64, size_t> key_to_file_offset;

    Attribute keys_buffer;
    Attributes attributes_buffer;
    //MutableColumns buffer;
    size_t bytes = 0;

    mutable std::atomic<size_t> element_count{0};
};

using CachePartitionPtr = std::unique_ptr<CachePartition>;


class CacheStorage
{
public:
    using Key = IDictionary::Key;

    CacheStorage(SSDCacheDictionary & dictionary_, const std::string & path_,
            const size_t partitions_count_, const size_t partition_max_size_);

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out>
    void getValue(const size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found) const
    {
        partitions[0]->getValue<Out>(attribute_index, ids, out, not_found);
    }

    // getString();

    template <typename PresentIdHandler, typename AbsentIdHandler>
    void update(DictionarySourcePtr & source_ptr, const std::vector<Key> & requested_ids,
            PresentIdHandler && on_updated, AbsentIdHandler && on_id_not_found);

    //BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const;

    std::exception_ptr getLastException() const { return last_update_exception; }

    const std::string & getPath() const { return path; }

private:
    CachePartition::Attributes createAttributesFromBlock(
            const Block & block, const std::vector<AttributeUnderlyingType> & structure);

    SSDCacheDictionary & dictionary;

    // Block structure: Key, (Default + TTL), Attr1, Attr2, ...
    // const Block header;
    const std::string path;
    const size_t partition_max_size;
    std::vector<CachePartitionPtr> partitions;

    Logger * const log;

    mutable pcg64 rnd_engine;

    mutable std::shared_mutex rw_lock;

    mutable std::exception_ptr last_update_exception;
    mutable size_t update_error_count = 0;
    mutable std::chrono::system_clock::time_point backoff_end_time;

    // stats
    mutable std::atomic<size_t> element_count{0};
    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};
};


class SSDCacheDictionary final : public IDictionary
{
public:
    SSDCacheDictionary(
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            const DictionaryLifetime dict_lifetime_,
            const std::string & path,
            const size_t partition_max_size_);

    std::string getName() const override { return name; }

    std::string getTypeName() const override { return "SSDCache"; }

    size_t getBytesAllocated() const override { return 0; } // TODO: ?

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override
    {
        return static_cast<double>(hit_count.load(std::memory_order_acquire)) / query_count.load(std::memory_order_relaxed);
    }

    size_t getElementCount() const override { return element_count.load(std::memory_order_relaxed); }

    double getLoadFactor() const override { return static_cast<double>(element_count.load(std::memory_order_relaxed)) / partition_max_size; } // TODO: fix

    bool supportUpdates() const override { return true; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<SSDCacheDictionary>(name, dict_struct, source_ptr->clone(), dict_lifetime, path, partition_max_size);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[getAttributeIndex(attribute_name)].injective;
    }

    bool hasHierarchy() const override { return false; }

    void toParent(const PaddedPODArray<Key> & /* ids */, PaddedPODArray<Key> & /* out */ ) const override {}

    std::exception_ptr getLastException() const override { return storage.getLastException(); }

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
    void get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def, ResultArrayType<TYPE> & out) const;
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

    void has(const PaddedPODArray<Key> & /* ids */, PaddedPODArray<UInt8> & /* out */) const override {} // TODO

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override // TODO
    {
        UNUSED(column_names);
        UNUSED(max_block_size);
        return nullptr;
    }

    struct Attribute
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
                String> null_value;
    };
    using Attributes = std::vector<Attribute>;

    /// переместить
    const Attributes & getAttributes() const;

private:
    size_t getAttributeIndex(const std::string & attr_name) const;
    Attribute & getAttribute(const std::string & attr_name);
    const Attribute & getAttribute(const std::string & attr_name) const;

    template <typename T>
    Attribute createAttributeWithTypeImpl(const AttributeUnderlyingType type, const Field & null_value);
    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);
    void createAttributes();

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
            const size_t attribute_index, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const;
    template <typename DefaultGetter>
    void getItemsString(const size_t attribute_index, const PaddedPODArray<Key> & ids,
            ColumnString * out, DefaultGetter && get_default) const;
    
    const std::string name;
    const DictionaryStructure dict_struct;
    mutable DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    const std::string path;
    const size_t partition_max_size;
    mutable CacheStorage storage;
    Logger * const log;

    std::map<std::string, size_t> attribute_index_by_name;
    Attributes attributes; // TODO: move to storage

    mutable size_t bytes_allocated = 0;
    mutable std::atomic<size_t> element_count{0};
    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};
};

}


#include "SSDCacheDictionary.inc.h"