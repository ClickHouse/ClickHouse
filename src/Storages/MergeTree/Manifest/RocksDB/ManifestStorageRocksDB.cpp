#include <Storages/MergeTree/Manifest/RocksDB/ManifestStorageRocksDB.h>

#include "config.h"

#if USE_ROCKSDB

#include <filesystem>
#include <thread>
#include <rocksdb/env.h>
#include <rocksdb/table.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ROCKSDB_ERROR;
}

void ManifestStorageRocksDB::setupRocksDBEnvThreads()
{
    static std::once_flag flag;
    std::call_once(
        flag,
        []
        {
            unsigned int cpu_cores = std::thread::hardware_concurrency();
            if (cpu_cores == 0)
                cpu_cores = 4;

            auto * env = rocksdb::Env::Default();
            env->SetBackgroundThreads(std::max(1u, cpu_cores - 1), rocksdb::Env::LOW);
            env->SetBackgroundThreads(4, rocksdb::Env::HIGH);
        });
}

ManifestStoragePtr ManifestStorageRocksDB::create(const String & dir)
{
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::DB * db;

    options.create_if_missing = true;
    // Avoid having too many WAL files (*.log) on the disk
    options.max_total_wal_size = options.write_buffer_size * 4;
    options.enable_thread_tracking = true;
    options.compression = rocksdb::CompressionType::kLZ4Compression;
    options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleUniversal;
    options.info_log_level = rocksdb::ERROR_LEVEL;

    table_options.no_block_cache = true;
    table_options.cache_index_and_filter_blocks = false;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    setupRocksDBEnvThreads();
    options.env = rocksdb::Env::Default();

    // Only use default ColumnFamily
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    rocksdb::Status status = rocksdb::DB::Open(options, dir, column_families, &cf_handles, &db);

    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {} status:{}", dir, status.ToString());

    if (cf_handles.empty())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "No column family handles returned from RocksDB");

    std::unique_ptr<rocksdb::DB> db_ptr(db);
    return std::shared_ptr<ManifestStorageRocksDB>(new ManifestStorageRocksDB(std::move(db_ptr), cf_handles[0], dir));
}

ManifestStorageRocksDB::ManifestStorageRocksDB(
    std::unique_ptr<rocksdb::DB> db,
    rocksdb::ColumnFamilyHandle * default_cf_handle,
    const String & dir)
    : rocksdb_(std::move(db))
    , default_cf_handle_(default_cf_handle)
    , db_dir_(dir)
{
}

ManifestStatus ManifestStorageRocksDB::convertStatus(const rocksdb::Status & status)
{
    if (status.ok())
        return ManifestStatus::OK;
    if (status.IsNotFound())
        return ManifestStatus::NotFound;
    if (status.IsCorruption())
        return ManifestStatus::Corruption;
    if (status.IsNotSupported())
        return ManifestStatus::NotSupported;
    if (status.IsInvalidArgument())
        return ManifestStatus::InvalidArgument;
    if (status.IsIOError())
        return ManifestStatus::IOError;
    if (status.IsBusy())
        return ManifestStatus::Busy;
    if (status.IsTimedOut())
        return ManifestStatus::TimedOut;
    return ManifestStatus::IOError; // default
}

ManifestStatus ManifestStorageRocksDB::put(const String & key, const String & value)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorageRocksDB is shutdown");

    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;

    auto status = rocksdb_->Put(options, default_cf_handle_, key, value);
    return convertStatus(status);
}

ManifestStatus ManifestStorageRocksDB::get(const String & key, String & value) const
{
    auto status = rocksdb_->Get(rocksdb::ReadOptions(), default_cf_handle_, key, &value);
    return convertStatus(status);
}

ManifestStatus ManifestStorageRocksDB::del(const String & key)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorageRocksDB is shutdown");

    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;

    auto status = rocksdb_->Delete(options, default_cf_handle_, key);
    return convertStatus(status);
}

ManifestStatus ManifestStorageRocksDB::delRange(const String & start_key, const String & end_key)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorageRocksDB is shutdown");

    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;

    auto status = rocksdb_->DeleteRange(options, default_cf_handle_, start_key, end_key);
    return convertStatus(status);
}

ManifestStatus ManifestStorageRocksDB::batchWrite(
    const std::vector<String> & keys_to_delete,
    const std::vector<std::pair<String, String>> & keys_to_put)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ManifestStorageRocksDB is shutdown");

    rocksdb::WriteBatch batch;

    for (const auto & key : keys_to_delete)
        batch.Delete(default_cf_handle_, key);

    for (const auto & [key, value] : keys_to_put)
        batch.Put(default_cf_handle_, key, value);

    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;

    auto status = rocksdb_->Write(options, &batch);
    return convertStatus(status);
}

void ManifestStorageRocksDB::getByPrefix(
    const String & prefix,
    std::vector<String> & keys,
    std::vector<String> & values) const
{
    keys.clear();
    values.clear();

    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), default_cf_handle_));

    rocksdb::Slice target(prefix);
    for (it->Seek(target); it->Valid(); it->Next())
    {
        const auto key = it->key();
        if (!key.starts_with(target))
            break;

        keys.emplace_back(key.data(), key.size());
        values.emplace_back(it->value().data(), it->value().size());
    }
}

void ManifestStorageRocksDB::getValuesByPrefix(
    const String & prefix,
    std::vector<String> & values) const
{
    values.clear();

    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), default_cf_handle_));

    rocksdb::Slice target(prefix);
    for (it->Seek(target); it->Valid(); it->Next())
    {
        const auto key = it->key();
        if (!key.starts_with(target))
            break;

        values.emplace_back(it->value().data(), it->value().size());
    }
}

void ManifestStorageRocksDB::getValuesByRange(
    const String & begin_key,
    const String & end_key,
    std::vector<String> & values) const
{
    values.clear();

    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), default_cf_handle_));

    rocksdb::Slice begin(begin_key);
    rocksdb::Slice end(end_key);

    for (it->Seek(begin); it->Valid(); it->Next())
    {
        const auto key = it->key();
        /// [begin_key, end_key)
        if (key.compare(end) >= 0)
            break;

        values.emplace_back(it->value().data(), it->value().size());
    }
}

ManifestIteratorPtr ManifestStorageRocksDB::createIterator() const
{
    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), default_cf_handle_));
    return std::make_unique<ManifestIteratorRocksDB>(std::move(it));
}

uint64_t ManifestStorageRocksDB::getEstimateNumKeys() const
{
    uint64_t keys = 0;
    rocksdb_->GetAggregatedIntProperty("rocksdb.estimate-num-keys", &keys);
    return keys;
}

void ManifestStorageRocksDB::flush()
{
    if (shutdown_called_.load())
        return;

    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    flush_options.allow_write_stall = true;

    auto status = rocksdb_->Flush(flush_options, default_cf_handle_);
    if (!status.ok())
        LOG_WARNING(&Poco::Logger::get("ManifestStorageRocksDB"),
                    "Failed to flush: {}", status.ToString());
}

void ManifestStorageRocksDB::shutdown()
{
    if (shutdown_called_.exchange(true))
        return;

    if (default_cf_handle_)
    {
        rocksdb_->DestroyColumnFamilyHandle(default_cf_handle_);
        default_cf_handle_ = nullptr;
    }

    rocksdb_->Close();
}

void ManifestStorageRocksDB::drop()
{
    shutdown();

    LOG_DEBUG(&Poco::Logger::get("ManifestStorageRocksDB"), "Dropping RocksDB directory: {}", db_dir_);

    if (!db_dir_.empty())
    {
        try
        {
            std::filesystem::path parent_dir = std::filesystem::path(db_dir_).parent_path();
            if (std::filesystem::exists(parent_dir))
            {
                std::filesystem::remove_all(parent_dir);
            }
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to remove RocksDB directory {}: {}", db_dir_, e.what());
        }
    }
}

}

#endif
