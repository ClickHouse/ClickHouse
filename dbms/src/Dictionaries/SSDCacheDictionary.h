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
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>

namespace DB
{

class SSDCacheDictionary;
class CacheStorage;

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

class CachePartition
{
public:
    struct Index final
    {
        bool inMemory() const;
        void setInMemory(const bool in_memory);

        bool exists() const;
        void setNotExists();

        size_t getAddressInBlock() const;
        void setAddressInBlock(const size_t address_in_block);

        size_t getBlockId() const;
        void setBlockId(const size_t block_id);

        bool operator< (const Index & rhs) const { return index < rhs.index; }

        /// Stores `is_in_memory` flag, block id, address in uncompressed block
        size_t index = 0;
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

    CachePartition(
            const AttributeUnderlyingType & key_structure,
            const std::vector<AttributeUnderlyingType> & attributes_structure,
            const std::string & dir_path,
            const size_t file_id,
            const size_t max_size,
            const size_t block_size,
            const size_t read_buffer_size);

    ~CachePartition();

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out>
    void getValue(const size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::vector<bool> & found,
            std::chrono::system_clock::time_point now) const;

    // TODO:: getString

    void has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out, std::chrono::system_clock::time_point now) const;

    struct Attribute
    {
        template <typename T>
        using Container = PaddedPODArray<T>;

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
            const PaddedPODArray<Metadata> & metadata, const size_t begin);

    void flush();

    void remove();

    size_t getId() const;

    PaddedPODArray<Key> getCachedIds(const std::chrono::system_clock::time_point now) const;

    double getLoadFactor() const;

    size_t getElementCount() const;

private:
    template <typename Out>
    void getValueFromMemory(
            const size_t attribute_index, const PaddedPODArray<Index> & indices, ResultArrayType<Out> & out) const;

    template <typename Out>
    void getValueFromStorage(
            const size_t attribute_index, const PaddedPODArray<Index> & indices, ResultArrayType<Out> & out) const;

    template <typename Out>
    void readValueFromBuffer(const size_t attribute_index, Out & dst, ReadBuffer & buf) const;

    const size_t file_id;
    const size_t max_size;
    const size_t block_size;
    const size_t read_buffer_size;
    const std::string path;

    mutable std::shared_mutex rw_lock;

    int fd = -1;

    struct IndexAndMetadata final
    {
        Index index{};
        Metadata metadata{};
    };

    mutable std::unordered_map<UInt64, IndexAndMetadata> key_to_index_and_metadata;

    Attribute keys_buffer;
    const std::vector<AttributeUnderlyingType> attributes_structure;

    std::optional<Memory<>> memory;
    std::optional<WriteBuffer> write_buffer;
    // std::optional<CompressedWriteBuffer> compressed_buffer;
    // std::optional<HashingWriteBuffer> hashing_buffer;
    // CompressionCodecPtr codec;

    size_t current_memory_block_id = 0;
    size_t current_file_block_id = 0;
};

using CachePartitionPtr = std::shared_ptr<CachePartition>;


class CacheStorage
{
public:
    using AttributeTypes = std::vector<AttributeUnderlyingType>;
    using Key = CachePartition::Key;

    CacheStorage(
            const AttributeTypes & attributes_structure_,
            const std::string & path_,
            const size_t max_partitions_count_,
            const size_t partition_size_,
            const size_t block_size_,
            const size_t read_buffer_size_);

    ~CacheStorage();

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out>
    void getValue(const size_t attribute_index, const PaddedPODArray<UInt64> & ids,
            ResultArrayType<Out> & out, std::unordered_map<Key, std::vector<size_t>> & not_found,
            std::chrono::system_clock::time_point now) const;

    // getString();

    void has(const PaddedPODArray<UInt64> & ids, ResultArrayType<UInt8> & out,
             std::unordered_map<Key, std::vector<size_t>> & not_found, std::chrono::system_clock::time_point now) const;

    template <typename PresentIdHandler, typename AbsentIdHandler>
    void update(DictionarySourcePtr & source_ptr, const std::vector<Key> & requested_ids,
            PresentIdHandler && on_updated, AbsentIdHandler && on_id_not_found,
            const DictionaryLifetime lifetime, const std::vector<AttributeValueVariant> & null_values);

    PaddedPODArray<Key> getCachedIds() const;

    //BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const;

    std::exception_ptr getLastException() const { return last_update_exception; }

    const std::string & getPath() const { return path; }

    size_t getQueryCount() const { return query_count.load(std::memory_order_relaxed); }

    size_t getHitCount() const { return hit_count.load(std::memory_order_acquire); }

    size_t getElementCount() const;

    double getLoadFactor() const;

private:
    CachePartition::Attributes createAttributesFromBlock(
            const Block & block, const size_t begin_column, const std::vector<AttributeUnderlyingType> & structure);

    void collectGarbage();

    const AttributeTypes attributes_structure;

    const std::string path;
    const size_t max_partitions_count;
    const size_t partition_size;
    const size_t block_size;
    const size_t read_buffer_size;

    mutable std::shared_mutex rw_lock;
    std::list<CachePartitionPtr> partitions;
    std::list<CachePartitionPtr> partition_delete_queue;

    Logger * const log;

    mutable pcg64 rnd_engine;

    mutable std::exception_ptr last_update_exception;
    mutable size_t update_error_count = 0;
    mutable std::chrono::system_clock::time_point backoff_end_time;

    // stats
    mutable size_t bytes_allocated = 0;

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
            const size_t max_partitions_count_,
            const size_t partition_size_,
            const size_t block_size_,
            const size_t read_buffer_size_);

    std::string getName() const override { return name; }

    std::string getTypeName() const override { return "SSDCache"; }

    size_t getBytesAllocated() const override { return 0; } // TODO: ?

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
                max_partitions_count, partition_size, block_size, read_buffer_size);
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

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    size_t getAttributeIndex(const std::string & attr_name) const;

    template <typename T>
    AttributeValueVariant createAttributeNullValueWithTypeImpl(const Field & null_value);
    AttributeValueVariant createAttributeNullValueWithType(const AttributeUnderlyingType type, const Field & null_value);
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
    const size_t max_partitions_count;
    const size_t partition_size;
    const size_t block_size;
    const size_t read_buffer_size;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<AttributeValueVariant> null_values;
    mutable CacheStorage storage;
    Logger * const log;

    mutable size_t bytes_allocated = 0;
};

}
