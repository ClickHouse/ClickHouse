#pragma once

#if defined(OS_LINUX) || defined(__FreeBSD__)

#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include <atomic>
#include <chrono>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/CurrentMetrics.h>
#include <common/logger_useful.h>
#include <Common/SmallObjectPool.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Dictionaries/BucketCache.h>
#include <ext/scope_guard.h>
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

class KeyRef
{
public:
    explicit KeyRef(char * data) : ptr(data) {}

    KeyRef() : ptr(nullptr) {}

    inline UInt16 size() const
    {
        UInt16 res;
        memcpy(&res, ptr, sizeof(res));
        return res;
    }

    inline size_t fullSize() const
    {
        return static_cast<size_t>(size()) + sizeof(UInt16);
    }

    inline bool isNull() const
    {
        return ptr == nullptr;
    }

    inline char * data() const
    {
        return ptr + sizeof(UInt16);
    }

    inline char * fullData() const
    {
        return ptr;
    }

    inline char * fullData()
    {
        return ptr;
    }

    inline const StringRef getRef() const
    {
        return StringRef(data(), size());
    }

    inline bool operator==(const KeyRef & other) const
    {
        return getRef() == other.getRef();
    }

    inline bool operator!=(const KeyRef & other) const
    {
        return !(*this == other);
    }

    inline bool operator<(const KeyRef & other) const
    {
        return getRef() <  other.getRef();
    }

private:
    char * ptr;
};

using KeyRefs = std::vector<KeyRef>;
}

namespace std
{
    template <>
    struct hash<DB::KeyRef>
    {
        size_t operator() (DB::KeyRef key_ref) const
        {
            return hasher(key_ref.getRef());
        }

        std::hash<StringRef> hasher;
    };
}

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
    The pool for storing complex keys.
*/
template <typename A>
class ComplexKeysPoolImpl
{
public:
    KeyRef allocKey(const size_t row, const Columns & key_columns, StringRefs & keys)
    {
        const auto keys_size = key_columns.size();
        UInt16 sum_keys_size{};

        for (size_t j = 0; j < keys_size; ++j)
        {
            keys[j] = key_columns[j]->getDataAt(row);
            sum_keys_size += keys[j].size;
            if (!key_columns[j]->valuesHaveFixedSize())  // String
                sum_keys_size += sizeof(size_t) + 1;
        }

        auto place = arena.alloc(sum_keys_size + sizeof(sum_keys_size));

        auto key_start = place;
        memcpy(key_start, &sum_keys_size, sizeof(sum_keys_size));
        key_start += sizeof(sum_keys_size);
        for (size_t j = 0; j < keys_size; ++j)
        {
            if (!key_columns[j]->valuesHaveFixedSize())  // String
            {
                auto key_size = keys[j].size + 1;
                memcpy(key_start, &key_size, sizeof(size_t));
                key_start += sizeof(size_t);
                memcpy(key_start, keys[j].data, keys[j].size);
                key_start += keys[j].size;
                *key_start = '\0';
                ++key_start;
            }
            else
            {
                memcpy(key_start, keys[j].data, keys[j].size);
                key_start += keys[j].size;
            }
        }

        return KeyRef(place);
    }

    KeyRef copyKeyFrom(const KeyRef & key)
    {
        char * data = arena.alloc(key.fullSize());
        memcpy(data, key.fullData(), key.fullSize());
        return KeyRef(data);
    }

    void freeKey(const KeyRef & key)
    {
        if constexpr (std::is_same_v<A, ArenaWithFreeLists>)
            arena.free(key.fullData(), key.fullSize());
    }

    void rollback(const KeyRef & key)
    {
        if constexpr (std::is_same_v<A, Arena>)
            arena.rollback(key.fullSize());
    }

    void writeKey(const KeyRef & key, WriteBuffer & buf)
    {
        buf.write(key.fullData(), key.fullSize());
    }

    void readKey(KeyRef & key, ReadBuffer & buf)
    {
        UInt16 sz;
        readBinary(sz, buf);
        char * data = nullptr;
        if constexpr (std::is_same_v<A, SmallObjectPool>)
            data = arena.alloc();
        else
            data = arena.alloc(sz + sizeof(sz));
        memcpy(data, &sz, sizeof(sz));
        buf.read(data + sizeof(sz), sz);
        key = KeyRef(data);
    }

    void ignoreKey(ReadBuffer & buf) const
    {
        UInt16 sz;
        readBinary(sz, buf);
        buf.ignore(sz);
    }

    size_t size() const
    {
        return arena.size();
    }

private:
    A arena;
};

using TemporalComplexKeysPool = ComplexKeysPoolImpl<Arena>;
using ComplexKeysPool = ComplexKeysPoolImpl<ArenaWithFreeLists>;

struct KeyDeleter
{
    KeyDeleter(ComplexKeysPool & keys_pool_) : keys_pool(keys_pool_) {}

    void operator()(const KeyRef key) const
    {
        keys_pool.freeKey(key);
    }

    ComplexKeysPool & keys_pool;
};


/*
    Class for operations with cache file and index.
    Supports GET/SET operations.
*/
class SSDComplexKeyCachePartition
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


    SSDComplexKeyCachePartition(
            const AttributeUnderlyingType & key_structure,
            const std::vector<AttributeUnderlyingType> & attributes_structure,
            const std::string & dir_path,
            const size_t file_id,
            const size_t max_size,
            const size_t block_size,
            const size_t read_buffer_size,
            const size_t write_buffer_size,
            const size_t max_stored_keys);

    ~SSDComplexKeyCachePartition();

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

    template <typename Out, typename GetDefault>
    void getValue(const size_t attribute_index,
            const Columns & key_columns, const DataTypes & key_types,
            ResultArrayType<Out> & out, std::vector<bool> & found, GetDefault & get_default,
            std::chrono::system_clock::time_point now) const;

    void getString(const size_t attribute_index,
            const Columns & key_columns, const DataTypes & key_types,
            StringRefs & refs, ArenaWithFreeLists & arena, std::vector<bool> & found,
            std::vector<size_t> & default_ids, std::chrono::system_clock::time_point now) const;

    void has(const Columns & key_columns, const DataTypes & key_types,
            ResultArrayType<UInt8> & out, std::vector<bool> & found,
            std::chrono::system_clock::time_point now) const;

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

    size_t appendBlock(
        const Columns & key_columns,
        const DataTypes & key_types,
        const Attributes & new_attributes,
        const PaddedPODArray<Metadata> & metadata,
        const size_t begin);

    size_t appendDefaults(
        const KeyRefs & keys,
        const PaddedPODArray<Metadata> & metadata,
        const size_t begin);

    void clearOldestBlocks();

    void flush();

    void remove();

    size_t getId() const;

    double getLoadFactor() const;

    size_t getElementCount() const;

    size_t getBytesAllocated() const;

private:
    size_t append(
        const KeyRefs & keys,
        const Attributes & new_attributes,
        const PaddedPODArray<Metadata> & metadata,
        const size_t begin);

    template <typename SetFunc>
    void getImpl(const Columns & key_columns, const DataTypes & key_types,
        SetFunc & set, std::vector<bool> & found) const;

    template <typename SetFunc>
    void getValueFromMemory(const PaddedPODArray<Index> & indices, SetFunc & set) const;

    template <typename SetFunc>
    void getValueFromStorage(const PaddedPODArray<Index> & indices, SetFunc & set) const;

    void ignoreFromBufferToAttributeIndex(const size_t attribute_index, ReadBuffer & buf) const;

    const size_t file_id;
    const size_t max_size;
    const size_t block_size;
    const size_t read_buffer_size;
    const size_t write_buffer_size;
    const size_t max_stored_keys;
    const std::string path;

    mutable std::shared_mutex rw_lock;

    int fd = -1;

    ComplexKeysPool keys_pool;
    mutable BucketCacheIndex<KeyRef, Index, std::hash<KeyRef>, KeyDeleter> key_to_index;

    std::optional<TemporalComplexKeysPool> keys_buffer_pool;
    KeyRefs keys_buffer;

    const std::vector<AttributeUnderlyingType> attributes_structure;

    std::optional<Memory<>> memory;
    std::optional<WriteBuffer> write_buffer;
    uint32_t keys_in_block = 0;

    size_t current_memory_block_id = 0;
    size_t current_file_block_id = 0;
};

using SSDComplexKeyCachePartitionPtr = std::shared_ptr<SSDComplexKeyCachePartition>;


/*
    Class for managing SSDCachePartition and getting data from source.
*/
class SSDComplexKeyCacheStorage
{
public:
    using AttributeTypes = std::vector<AttributeUnderlyingType>;

    SSDComplexKeyCacheStorage(
            const AttributeTypes & attributes_structure,
            const std::string & path,
            const size_t max_partitions_count,
            const size_t file_size,
            const size_t block_size,
            const size_t read_buffer_size,
            const size_t write_buffer_size,
            const size_t max_stored_keys);

    ~SSDComplexKeyCacheStorage();

    template <typename T>
    using ResultArrayType = SSDComplexKeyCachePartition::ResultArrayType<T>;

    template <typename Out, typename GetDefault>
    void getValue(const size_t attribute_index, const Columns & key_columns, const DataTypes & key_types,
            ResultArrayType<Out> & out, std::unordered_map<KeyRef, std::vector<size_t>> & not_found,
            TemporalComplexKeysPool & not_found_pool,
            GetDefault & get_default, std::chrono::system_clock::time_point now) const;

    void getString(const size_t attribute_index, const Columns & key_columns, const DataTypes & key_types,
            StringRefs & refs, ArenaWithFreeLists & arena, std::unordered_map<KeyRef, std::vector<size_t>> & not_found,
            TemporalComplexKeysPool & not_found_pool,
            std::vector<size_t> & default_ids, std::chrono::system_clock::time_point now) const;

    void has(const Columns & key_columns, const DataTypes & key_types, ResultArrayType<UInt8> & out,
            std::unordered_map<KeyRef, std::vector<size_t>> & not_found,
            TemporalComplexKeysPool & not_found_pool, std::chrono::system_clock::time_point now) const;

    template <typename PresentIdHandler, typename AbsentIdHandler>
    void update(DictionarySourcePtr & source_ptr,
            const Columns & key_columns, const DataTypes & key_types,
            const KeyRefs & required_keys, const std::vector<size_t> & required_rows,
            TemporalComplexKeysPool & tmp_keys_pool,
            PresentIdHandler && on_updated, AbsentIdHandler && on_key_not_found,
            const DictionaryLifetime lifetime);

    std::exception_ptr getLastException() const { return last_update_exception; }

    const std::string & getPath() const { return path; }

    size_t getQueryCount() const { return query_count.load(std::memory_order_relaxed); }

    size_t getHitCount() const { return hit_count.load(std::memory_order_acquire); }

    size_t getElementCount() const;

    double getLoadFactor() const;

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
    std::list<SSDComplexKeyCachePartitionPtr> partitions;
    std::list<SSDComplexKeyCachePartitionPtr> partition_delete_queue;

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
class SSDComplexKeyCacheDictionary final : public IDictionaryBase
{
public:
    SSDComplexKeyCacheDictionary(
            const std::string & name_,
            const DictionaryStructure & dict_struct_,
            DictionarySourcePtr source_ptr_,
            const DictionaryLifetime dict_lifetime_,
            const std::string & path,
            const size_t max_partitions_count_,
            const size_t file_size_,
            const size_t block_size_,
            const size_t read_buffer_size_,
            const size_t write_buffer_size_,
            const size_t max_stored_keys_);

    const std::string & getDatabase() const override { return name; }
    const std::string & getName() const override { return name; }
    const std::string & getFullName() const override { return getName(); }

    std::string getKeyDescription() const { return dict_struct.getKeyDescription(); }

    std::string getTypeName() const override { return "SSDComplexKeyCache"; }

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
        return std::make_shared<SSDComplexKeyCacheDictionary>(name, dict_struct, source_ptr->clone(), dict_lifetime, path,
                max_partitions_count, file_size, block_size, read_buffer_size, write_buffer_size, max_stored_keys);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[getAttributeIndex(attribute_name)].injective;
    }

    std::exception_ptr getLastException() const override { return storage.getLastException(); }

    template <typename T>
    using ResultArrayType = SSDComplexKeyCacheStorage::ResultArrayType<T>;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
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

    void getString(const std::string & attribute_name, const Columns & key_columns,
        const DataTypes & key_types, ColumnString * out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
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

    void getString(const std::string & attribute_name, const Columns & key_columns,
        const DataTypes & key_types, const ColumnString * const def, ColumnString * const out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const TYPE def, \
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

    void getString(const std::string & attribute_name, const Columns & key_columns,
        const DataTypes & key_types, const String & def, ColumnString * const out) const;

    void has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    size_t getAttributeIndex(const std::string & attr_name) const;

    template <typename T>
    AttributeValueVariant createAttributeNullValueWithTypeImpl(const Field & null_value);
    AttributeValueVariant createAttributeNullValueWithType(const AttributeUnderlyingType type, const Field & null_value);
    void createAttributes();

    template <typename AttributeType, typename OutputType, typename DefaultGetter>
    void getItemsNumberImpl(
        const size_t attribute_index,
        const Columns & key_columns, const DataTypes & key_types,
        ResultArrayType<OutputType> & out, DefaultGetter && get_default) const;

    template <typename DefaultGetter>
    void getItemsStringImpl(
        const size_t attribute_index,
        const Columns & key_columns, const DataTypes & key_types,
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
    mutable SSDComplexKeyCacheStorage storage;
    Poco::Logger * const log;

    mutable size_t bytes_allocated = 0;
};

}

#endif
