#pragma once

#include "config.h"

#include <Storages/MergeTree/Manifest/IManifestStorage.h>

#if USE_ROCKSDB

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <memory>
#include <atomic>

namespace DB
{

class ManifestIteratorRocksDB : public IManifestIterator
{
public:
    explicit ManifestIteratorRocksDB(std::unique_ptr<rocksdb::Iterator> it)
        : it_(std::move(it)) {}

    void seek(const String & key) override { it_->Seek(key); }
    void next() override { it_->Next(); }
    bool valid() const override { return it_->Valid(); }
    String key() const override { return it_->key().ToString(); }
    String value() const override { return it_->value().ToString(); }

private:
    std::unique_ptr<rocksdb::Iterator> it_;
};

class ManifestStorageRocksDB : public IManifestStorage
{
public:
    static ManifestStoragePtr create(const String & dir);

    ManifestStatus put(const String & key, const String & value) override;
    ManifestStatus get(const String & key, String & value) const override;
    ManifestStatus del(const String & key) override;
    ManifestStatus delRange(const String & start_key, const String & end_key) override;
    ManifestStatus batchWrite(
        const std::vector<String> & keys_to_delete,
        const std::vector<std::pair<String, String>> & keys_to_put) override;
    void getByPrefix(
        const String & prefix,
        std::vector<String> & keys,
        std::vector<String> & values) const override;
    void getValuesByPrefix(
        const String & prefix,
        std::vector<String> & values) const override;
    void getValuesByRange(
        const String & begin_key,
        const String & end_key,
        std::vector<String> & values) const override;
    ManifestIteratorPtr createIterator() const override;
    uint64_t getEstimateNumKeys() const override;
    void flush() override;
    void shutdown() override;
    void drop() override;

private:
    explicit ManifestStorageRocksDB(
        std::unique_ptr<rocksdb::DB> db,
        rocksdb::ColumnFamilyHandle * default_cf_handle,
        const String & dir);

    static void setupRocksDBEnvThreads();
    static ManifestStatus convertStatus(const rocksdb::Status & status);

    std::unique_ptr<rocksdb::DB> rocksdb_;
    rocksdb::ColumnFamilyHandle * default_cf_handle_;
    std::atomic<bool> shutdown_called_{false};
    String db_dir_;
};

}

#endif
