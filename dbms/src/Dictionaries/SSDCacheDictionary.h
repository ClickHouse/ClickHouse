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

constexpr size_t OFFSET_MASK = ~0xffff000000000000;
constexpr size_t FILE_ID_SIZE = 16;
constexpr size_t FILE_OFFSET_SIZE = sizeof(size_t) * 8 - FILE_ID_SIZE;


class SSDCacheDictionary;

class CachePartition
{
public:
    using Offset = size_t;
    using Offsets = std::vector<Offset>;

    CachePartition(const std::string & file_name, const Block & header = {}, size_t buffer_size = 4 * 1024 * 1024);

    void appendBlock(const Block & block);

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out, typename Key>
    void getValue(const std::string & attribute_name, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found) const;

    // TODO:: getString

private:
    void flush();

    std::string file_name;
    size_t buffer_size;

    WriteBufferFromFile out_file; // 4MB

    /// Block structure: Key, (Default + TTL), Attr1, Attr2, ...
    Block header;

    std::unordered_map<UInt64, size_t> key_to_file_offset;
    MutableColumns buffer;
};


class CacheStorage
{
    CacheStorage(const std::string & path_, size_t partition_max_size_)
        : path(path_)
        , partition_max_size(partition_max_size_)
    {
        partition = std::make_unique<CachePartition>(path);
    }

    void appendBlock(const Block& block)
    {
        partition->appendBlock(block);
    }

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out, typename Key>
    void getValue(const std::string & attribute_name, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found) const
    {
        partition->getValue(attribute_name, ids, out, not_found);
    }

    // getString();

    //BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const;

private:
    const std::string path;
    const size_t partition_max_size;
    std::unique_ptr<CachePartition> partition;
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

    double getLoadFactor() const override { return static_cast<double>(element_count.load(std::memory_order_relaxed)) / max_size; } // TODO: fix

    bool supportUpdates() const override { return true; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<SSDCacheDictionary>(name, dict_struct, source_ptr->clone(), dict_lifetime, max_size);
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

    std::exception_ptr getLastException() const override { return last_exception; }

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

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
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

    template <typename PresentIdHandler, typename AbsentIdHandler>
    void update(const std::vector<Key> & requested_ids, PresentIdHandler && on_updated,
            AbsentIdHandler && on_id_not_found) const;
    
    const std::string name;
    const DictionaryStructure dict_struct;
    mutable DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    CacheStorage storage;
    Logger * const log;

    mutable std::shared_mutex rw_lock;

    std::map<std::string, size_t> attribute_index_by_name;
    Attributes attributes;

    mutable std::exception_ptr last_exception;
    mutable size_t error_count = 0;
    mutable std::chrono::system_clock::time_point backoff_end_time;

    mutable size_t bytes_allocated = 0;
    mutable std::atomic<size_t> element_count{0};
    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};
};

}


#include "SSDCacheDictionary.inc.h"