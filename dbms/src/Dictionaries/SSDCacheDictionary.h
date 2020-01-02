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
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>


namespace DB
{

class SSDCacheDictionary;
class CacheStorage;

class CachePartition
{
public:
    using Offset = size_t;
    using Offsets = std::vector<Offset>;

    CachePartition(CacheStorage & storage, const size_t file_id, const size_t max_size, const size_t buffer_size = 4 * 1024 * 1024);

    void appendBlock(const Block & block);

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out, typename Key>
    void getValue(const std::string & attribute_name, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found) const;

    // TODO:: getString

    /// 0 -- not found
    /// 1 -- good
    /// 2 -- expired
    void has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out) const;

private:
    void flush();


    CacheStorage & storage;

    size_t file_id;
    size_t max_size;
    size_t buffer_size;

    //mutable std::shared_mutex rw_lock;
    int index_fd;
    int data_fd;

    std::unordered_map<UInt64, size_t> key_to_file_offset;
    MutableColumns buffer;

    mutable std::atomic<size_t> element_count{0};
};

using CachePartitionPtr = std::unique_ptr<CachePartition>;


class CacheStorage
{
    using Key = IDictionary::Key;

    CacheStorage(SSDCacheDictionary & dictionary_, const std::string & path_, const size_t partitions_count_, const size_t partition_max_size_)
        : dictionary(dictionary_)
        , path(path_)
        , partition_max_size(partition_max_size_)
        , log(&Poco::Logger::get("CacheStorage"))
    {
        for (size_t partition_id = 0; partition_id < partitions_count_; ++partition_id)
            partitions.emplace_back(std::make_unique<CachePartition>(partition_id, partition_max_size));
    }

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out, typename Key>
    void getValue(const std::string & attribute_name, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found) const
    {
        partitions[0]->getValue(attribute_name, ids, out, not_found);
    }

    // getString();

    template <typename PresentIdHandler, typename AbsentIdHandler>
    std::exception_ptr update(DictionarySourcePtr & source_ptr, const std::vector<Key> & requested_ids,
            PresentIdHandler && on_updated, AbsentIdHandler && on_id_not_found);

    //BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const;

    std::exception_ptr getLastException() const { return last_update_exception; }

private:
    SSDCacheDictionary & dictionary;

    /// Block structure: Key, (Default + TTL), Attr1, Attr2, ...
    const Block header;
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
        return std::make_shared<SSDCacheDictionary>(name, dict_struct, source_ptr->clone(), dict_lifetime, partition_max_size);
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

    void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override // TODO
    {
        UNUSED(column_names);
        UNUSED(max_block_size);
        return nullptr;
    }

private:
    friend class CacheStorage;

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

    size_t getAttributeIndex(const std::string & attr_name) const;
    Attribute & getAttribute(const std::string & attr_name);
    const Attribute & getAttribute(const std::string & attr_name) const;
    const Attributes & getAttributes() const;

    template <typename T>
    Attribute createAttributeWithTypeImpl(const AttributeUnderlyingType type, const Field & null_value);
    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);
    void createAttributes();

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
            const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const;
    template <typename DefaultGetter>
    void getItemsString(const std::string & attribute_name, const PaddedPODArray<Key> & ids,
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
    Attributes attributes;

    mutable size_t bytes_allocated = 0;
    mutable std::atomic<size_t> element_count{0};
    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};
};

}


#include "SSDCacheDictionary.inc.h"