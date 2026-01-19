#pragma once

#include "config.h"

#if USE_ROCKSDB

#include <Storages/KeyValueStore/IKeyValueStore.h>
#include <Storages/KeyValueStore/IKeyValueIterator.h>
#include <Storages/KeyValueStore/IKeyValueNamespace.h>
#include <base/types.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/status.h>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <Poco/Logger.h>

namespace DB
{

class Exception;

/**
 * RocksDB implementation of IKeyValueNamespace.
 * Wraps a RocksDB ColumnFamilyHandle.
 */
class KeyValueNamespaceRocksDB final : public IKeyValueNamespace
{
public:
    explicit KeyValueNamespaceRocksDB(rocksdb::ColumnFamilyHandle * handle)
        : handle_(handle)
    {
    }

    String getName() const override { return handle_->GetName(); }

    IKeyValueStore * getStore() const override { return store_; }

    void setStore(IKeyValueStore * store) { store_ = store; }

    /// Get underlying ColumnFamilyHandle (for RocksDB-specific operations)
    rocksdb::ColumnFamilyHandle * getHandle() const { return handle_; }

private:
    rocksdb::ColumnFamilyHandle * handle_;
    IKeyValueStore * store_ = nullptr;
};

/**
 * RocksDB implementation of IKeyValueIterator.
 * Wraps a RocksDB Iterator.
 */
class KeyValueIteratorRocksDB final : public IKeyValueIterator
{
public:
    explicit KeyValueIteratorRocksDB(std::unique_ptr<rocksdb::Iterator> it)
        : it_(std::move(it))
    {
    }

    bool valid() const override { return it_ && it_->Valid(); }

    void next() override
    {
        if (it_)
            it_->Next();
    }

    void seekToFirst() override
    {
        if (it_)
            it_->SeekToFirst();
    }

    void seek(const String & target) override
    {
        if (it_)
            it_->Seek(target);
    }

    String key() const override
    {
        if (!it_ || !it_->Valid())
            return {};
        return String(it_->key().data(), it_->key().size());
    }

    String value() const override
    {
        if (!it_ || !it_->Valid())
            return {};
        return String(it_->value().data(), it_->value().size());
    }

    bool status() const override
    {
        return it_ && it_->status().ok();
    }

private:
    std::unique_ptr<rocksdb::Iterator> it_;
};

/**
 * RocksDB implementation of IKeyValueStore.
 * 
 * This class wraps RocksDB operations and provides the IKeyValueStore interface.
 * It manages ColumnFamilyHandles and converts RocksDB Status codes to KVStatus.
 */
class KeyValueStoreRocksDB final : public IKeyValueStore
{
public:
    /// Create a new RocksDB KV store instance
    /// @param db_path - path to RocksDB database directory
    /// @param expected_namespaces - names of namespaces (column families) that should exist
    /// @return shared pointer to the KV store instance
    /// @throws Exception if creation fails
    static IKeyValueStorePtr create(
        const String & db_path,
        const std::vector<std::string> & expected_namespaces = {});

    /// Destructor
    ~KeyValueStoreRocksDB() override;

    // IKeyValueStore interface implementation
    IKeyValueNamespacePtr getOrCreateNamespace(const String & name) override;
    IKeyValueNamespacePtr getNamespace(const String & name) const override;
    KVStatus dropNamespace(const String & name) override;
    std::vector<String> listNamespaces() const override;

    KVStatus put(IKeyValueNamespace * ns, const String & key, const String & value) override;
    KVStatus get(IKeyValueNamespace * ns, const String & key, String & value) const override;
    KVStatus del(IKeyValueNamespace * ns, const String & key) override;
    KVStatus delRange(IKeyValueNamespace * ns, const String & start_key, const String & end_key) override;

    void getByPrefix(
        IKeyValueNamespace * ns,
        const String & prefix,
        std::vector<String> & keys,
        std::vector<String> & values) const override;

    void getValuesByPrefix(
        IKeyValueNamespace * ns,
        const String & prefix,
        std::vector<String> & values) const override;

    void getValuesByRange(
        IKeyValueNamespace * ns,
        const String & begin_key,
        const String & end_key,
        std::vector<String> & values) const override;

    IKeyValueIteratorPtr createIterator(IKeyValueNamespace * ns) const override;

    KVStatus batchWrite(const std::vector<BatchOperation> & operations) override;

    uint64_t getEstimateNumKeys(IKeyValueNamespace * ns) const override;

    std::unordered_map<String, size_t> getProperty(IKeyValueNamespace * ns, const String & property) const override;

    void flush() override;
    void shutdown() override;

    void * getRawHandle() const override { return rocksdb_.get(); }

    /// Get raw RocksDB instance (for advanced use cases)
    /// @warning: Use with caution, this breaks abstraction
    rocksdb::DB * getRawDB() const { return rocksdb_.get(); }

private:
    explicit KeyValueStoreRocksDB(
        std::unique_ptr<rocksdb::DB> db,
        std::vector<rocksdb::ColumnFamilyHandle *> cf_handles);

    static void setupRocksDBEnvThreads();
    static KVStatus convertStatus(const rocksdb::Status & status);

    rocksdb::ColumnFamilyHandle * getColumnFamilyHandle(IKeyValueNamespace * ns) const;

    std::unique_ptr<rocksdb::DB> rocksdb_;
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
    mutable std::mutex namespace_cache_mutex_;
    std::unordered_map<String, IKeyValueNamespacePtr> namespace_cache_;
    std::atomic<bool> shutdown_called_{false};
    Poco::Logger * log = &Poco::Logger::get("KeyValueStoreRocksDB");
};

} // namespace DB

#endif // USE_ROCKSDB

