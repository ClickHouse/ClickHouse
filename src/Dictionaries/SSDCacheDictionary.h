#pragma once

#if defined(__linux__) || defined(__FreeBSD__)

#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include <atomic>
#include <chrono>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/CurrentMetrics.h>
#include <common/logger_useful.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Dictionaries/BucketCache.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferAIO.h>
#include <list>
#include <pcg_random.hpp>
#include <Poco/Logger.h>
#include <shared_mutex>
#include <variant>
#include <vector>

namespace DB
{

using AttributeValueVariant = std::variant<
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
        String>;


/*
    Class for operations with cache file and index.
    Supports GET/SET operations.
*/
class SSDCachePartition
{
public:
    struct Index final
    {
        bool inMemory() const;
        void setInMemory(bool in_memory);

        bool exists() const;
        void setNotExists();

        size_t getAddressInBlock() const;
        void setAddressInBlock(size_t address_in_block);

        size_t getBlockId() const;
        void setBlockId(size_t block_id);

        bool operator< (const Index & rhs) const { return index < rhs.index; }

        /// Stores `is_in_memory` flag, block id, address in uncompressed block
        uint64_t index = 0;
    };

    struct Metadata final
    {
        using time_point_t = std::chrono::system_clock::time_point;
        using time_point_rep_t = time_point_t::rep;
        using time_point_urep_t = std::make_unsigned_t<time_point_rep_t>;

        time_point_t expiresAt() const;
        void setExpiresAt(const time_point_t & t);

        bool isDefault() const;
        void setDefault();

        /// Stores both expiration time and `is_default` flag in the most significant bit
        time_point_urep_t data = 0;
    };

    using Offset = size_t;
    using Offsets = std::vector<Offset>;
    using Key = IDictionary::Key;

    SSDCachePartition(
            const AttributeUnderlyingType & key_structure,
            const std::vector<AttributeUnderlyingType> & attributes_structure,
            const std::string & dir_path,
            size_t file_id,
            size_t max_size,
            size_t block_size,
            size_t read_buffer_size,
            size_t write_buffer_size,
            size_t max_stored_keys);

    ~SSDCachePartition();

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out, typename GetDefault>
    void getValue(size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::vector<bool> & found, GetDefault & get_default,
            std::chrono::system_clock::time_point now) const;

    void getString(size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            StringRefs & refs, ArenaWithFreeLists & arena, std::vector<bool> & found,
            std::vector<size_t> & default_ids, std::chrono::system_clock::time_point now) const;

    void has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out,
            std::vector<bool> & found, std::chrono::system_clock::time_point now) const;

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

    size_t appendBlock(const Attribute & new_keys, const Attributes & new_attributes,
            const PaddedPODArray<Metadata> & metadata, size_t begin);

    size_t appendDefaults(const Attribute & new_keys, const PaddedPODArray<Metadata> & metadata, size_t begin);

    void flush();

    void remove();

    size_t getId() const;

    PaddedPODArray<Key> getCachedIds(std::chrono::system_clock::time_point now) const;

    double getLoadFactor() const;

    size_t getElementCount() const;

    size_t getBytesAllocated() const;

private:
    void clearOldestBlocks();

    template <typename SetFunc>
    void getImpl(const PaddedPODArray<UInt64> & ids, SetFunc & set, std::vector<bool> & found) const;

    template <typename SetFunc>
    void getValueFromMemory(const PaddedPODArray<Index> & indices, SetFunc & set) const;

    template <typename SetFunc>
    void getValueFromStorage(const PaddedPODArray<Index> & indices, SetFunc & set) const;

    void ignoreFromBufferToAttributeIndex(size_t attribute_index, ReadBuffer & buf) const;

    const size_t file_id;
    const size_t max_size;
    const size_t block_size;
    const size_t read_buffer_size;
    const size_t write_buffer_size;
    const size_t max_stored_keys;
    const std::string path;

    mutable std::shared_mutex rw_lock;

    int fd = -1;

    mutable BucketCacheIndex<UInt64, Index, Int64Hasher> key_to_index;

    Attribute keys_buffer;
    const std::vector<AttributeUnderlyingType> attributes_structure;

    std::optional<Memory<>> memory;
    std::optional<WriteBuffer> write_buffer;
    uint32_t keys_in_block = 0;

    size_t current_memory_block_id = 0;
    size_t current_file_block_id = 0;
};

using SSDCachePartitionPtr = std::shared_ptr<SSDCachePartition>;


/*
    Class for managing SSDCachePartition and getting data from source.
*/
class SSDCacheStorage
{
public:
    using AttributeTypes = std::vector<AttributeUnderlyingType>;
    using Key = SSDCachePartition::Key;

    SSDCacheStorage(
            const AttributeTypes & attributes_structure,
            const std::string & path,
            size_t max_partitions_count,
            size_t file_size,
            size_t block_size,
            size_t read_buffer_size,
            size_t write_buffer_size,
            size_t max_stored_keys);

    ~SSDCacheStorage();

    template <typename T>
    using ResultArrayType = SSDCachePartition::ResultArrayType<T>;

    template <typename Out, typename GetDefault>
    void getValue(size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found,
            GetDefault & get_default, std::chrono::system_clock::time_point now) const;

    void getString(size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            StringRefs & refs, ArenaWithFreeLists & arena, std::unordered_map<Key, std::vector<size_t>> & not_found,
            std::vector<size_t> & default_ids, std::chrono::system_clock::time_point now) const;

    void has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out,
             std::unordered_map<Key, std::vector<size_t>> & not_found, std::chrono::system_clock::time_point now) const;

    template <typename PresentIdHandler, typename AbsentIdHandler>
    void update(DictionarySourcePtr & source_ptr, const std::vector<Key> & requested_ids,
            PresentIdHandler && on_updated, AbsentIdHandler && on_id_not_found,
            DictionaryLifetime lifetime);

    PaddedPODArray<Key> getCachedIds() const;

    std::exception_ptr getLastException() const { return last_update_exception; }

    const std::string & getPath() const { return path; }

    size_t getQueryCount() const { return query_count.load(std::memory_order_relaxed); }

    size_t getHitCount() const { return hit_count.load(std::memory_order_acquire); }

    size_t getElementCount() const;

    double getLoadFactor() const;

    size_t getBytesAllocated() const;

private:
    void collectGarbage();

    const AttributeTypes attributes_structure;

    const std::string path;
    const size_t max_partitions_count;
    const size_t file_size;
    const size_t block_size;
    const size_t read_buffer_size;
    const size_t write_buffer_size;
    const size_t max_stored_keys;

    mutable std::shared_mutex rw_lock;
    std::list<SSDCachePartitionPtr> partitions;
    std::list<SSDCachePartitionPtr> partition_delete_queue;

    Poco::Logger * const log;

    mutable pcg64 rnd_engine;

    mutable std::exception_ptr last_update_exception;
    mutable size_t update_error_count = 0;
    mutable std::chrono::system_clock::time_point backoff_end_time;

    mutable std::atomic<size_t> hit_count{0};
    mutable std::atomic<size_t> query_count{0};
};


/*
    Dictionary interface
*/
class SSDCacheDictionary final : public IDictionary
{
public:
    SSDCacheDictionary(
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            DictionaryLifetime dict_lifetime_,
            const std::string & path,
            size_t max_partitions_count_,
            size_t file_size_,
            size_t block_size_,
            size_t read_buffer_size_,
            size_t write_buffer_size_,
            size_t max_stored_keys_);

    const std::string & getDatabase() const override { return name; }
    const std::string & getName() const override { return name; }
    const std::string & getFullName() const override { return getName(); }

    std::string getTypeName() const override { return "SSDCache"; }

    size_t getBytesAllocated() const override { return storage.getBytesAllocated(); }

    size_t getQueryCount() const override { return storage.getQueryCount(); }

    double getHitRate() const override
    {
        return static_cast<double>(storage.getHitCount()) / storage.getQueryCount();
    }

    size_t getElementCount() const override { return storage.getElementCount(); }

    double getLoadFactor() const override { return storage.getLoadFactor(); }

    bool supportUpdates() const override { return false; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<SSDCacheDictionary>(name, dict_struct, source_ptr->clone(), dict_lifetime, path,
                max_partitions_count, file_size, block_size, read_buffer_size, write_buffer_size, max_stored_keys);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[getAttributeIndex(attribute_name)].injective;
    }

    bool hasHierarchy() const override { return false; }

    void toParent(const PaddedPODArray<Key> &, PaddedPODArray<Key> &) const override { }

    std::exception_ptr getLastException() const override { return storage.getLastException(); }

    template <typename T>
    using ResultArrayType = SSDCacheStorage::ResultArrayType<T>;

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
    getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * def, ColumnString * out)
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

    void getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * out) const;

    void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    size_t getAttributeIndex(const std::string & attr_name) const;

    template <typename T>
    AttributeValueVariant createAttributeNullValueWithTypeImpl(const Field & null_value);
    AttributeValueVariant createAttributeNullValueWithType(AttributeUnderlyingType type, const Field & null_value);
    void createAttributes();

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
            size_t attribute_index, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const;

    template <typename DefaultGetter>
    void getItemsStringImpl(size_t attribute_index, const PaddedPODArray<Key> & ids,
            ColumnString * out, DefaultGetter && get_default) const;

    const std::string name;
    const DictionaryStructure dict_struct;
    mutable DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    const std::string path;
    const size_t max_partitions_count;
    const size_t file_size;
    const size_t block_size;
    const size_t read_buffer_size;
    const size_t write_buffer_size;
    const size_t max_stored_keys;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<AttributeValueVariant> null_values;
    mutable SSDCacheStorage storage;
    Poco::Logger * const log;

    mutable size_t bytes_allocated = 0;
};

}

#endif
