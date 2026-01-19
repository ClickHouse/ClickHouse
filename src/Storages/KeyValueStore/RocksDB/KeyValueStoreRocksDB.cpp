#include <Storages/KeyValueStore/RocksDB/KeyValueStoreRocksDB.h>

#if USE_ROCKSDB

#include <filesystem>
#include <thread>
#include <set>
#include <rocksdb/env.h>
#include <rocksdb/table.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
    extern const int LOGICAL_ERROR;
}

void KeyValueStoreRocksDB::setupRocksDBEnvThreads()
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

KVStatus KeyValueStoreRocksDB::convertStatus(const rocksdb::Status & status)
{
    if (status.ok())
        return KVStatus::OK;
    
    if (status.IsNotFound())
        return KVStatus::NotFound;
    
    if (status.IsCorruption())
        return KVStatus::Corruption;
    
    if (status.IsNotSupported())
        return KVStatus::NotSupported;
    
    if (status.IsInvalidArgument())
        return KVStatus::InvalidArgument;
    
    if (status.IsIOError())
        return KVStatus::IOError;
    
    if (status.IsMergeInProgress())
        return KVStatus::MergeInProgress;
    
    if (status.IsIncomplete())
        return KVStatus::Incomplete;
    
    if (status.IsShutdownInProgress())
        return KVStatus::ShutdownInProgress;
    
    if (status.IsTimedOut())
        return KVStatus::TimedOut;
    
    if (status.IsAborted())
        return KVStatus::Aborted;
    
    if (status.IsBusy())
        return KVStatus::Busy;
    
    if (status.IsExpired())
        return KVStatus::Expired;
    
    if (status.IsTryAgain())
        return KVStatus::TryAgain;
    
    return KVStatus::Unknown;
}

IKeyValueStorePtr KeyValueStoreRocksDB::create(
    const String & db_path,
    const std::vector<std::string> & expected_namespaces)
{
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::DB * db = nullptr;

    options.create_if_missing = true;
    options.max_total_wal_size = options.write_buffer_size * 4;
    options.enable_thread_tracking = true;
    options.create_missing_column_families = true;
    options.compression = rocksdb::CompressionType::kLZ4Compression;
    options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleUniversal;
    options.info_log_level = rocksdb::ERROR_LEVEL;
    
    table_options.no_block_cache = true;
    table_options.cache_index_and_filter_blocks = false;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    setupRocksDBEnvThreads();
    options.env = rocksdb::Env::Default();

    std::vector<std::string> exist_cf_names;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;

    if (std::filesystem::exists(db_path + "/CURRENT"))
    {
        rocksdb::Status list_cf = rocksdb::DB::ListColumnFamilies(options, db_path, &exist_cf_names);
        if (!list_cf.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "ListColumnFamilies failed: {}", list_cf.ToString());
        
        for (const auto & name : exist_cf_names)
            column_families.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }
    else
    {
        column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    }

    rocksdb::Status status = rocksdb::DB::Open(options, db_path, column_families, &cf_handles, &db);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {} status:{}", db_path, status.ToString());

    std::set<std::string> exist_set(exist_cf_names.begin(), exist_cf_names.end());

    for (const auto & name : expected_namespaces)
    {
        if (!exist_set.contains(name))
        {
            rocksdb::ColumnFamilyHandle * handle = nullptr;
            rocksdb::Status create_cf = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &handle);
            if (!create_cf.ok())
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "CreateColumnFamily failed: {}", create_cf.ToString());
            cf_handles.push_back(handle);
        }
    }

    std::unique_ptr<rocksdb::DB> db_ptr(db);
    auto store = std::shared_ptr<KeyValueStoreRocksDB>(new KeyValueStoreRocksDB(std::move(db_ptr), std::move(cf_handles)));
    
    // Initialize namespace cache
    for (auto * handle : store->cf_handles_)
    {
        auto ns = std::make_shared<KeyValueNamespaceRocksDB>(handle);
        ns->setStore(store.get());
        store->namespace_cache_[handle->GetName()] = ns;
    }

    return store;
}

KeyValueStoreRocksDB::KeyValueStoreRocksDB(
    std::unique_ptr<rocksdb::DB> db,
    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles)
    : rocksdb_(std::move(db))
    , cf_handles_(std::move(cf_handles))
{
}

KeyValueStoreRocksDB::~KeyValueStoreRocksDB()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

rocksdb::ColumnFamilyHandle * KeyValueStoreRocksDB::getColumnFamilyHandle(IKeyValueNamespace * ns) const
{
    if (!ns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Namespace cannot be null");

    auto * rocksdb_ns = dynamic_cast<KeyValueNamespaceRocksDB *>(ns);
    if (!rocksdb_ns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid namespace type for RocksDB");

    return rocksdb_ns->getHandle();
}

IKeyValueNamespacePtr KeyValueStoreRocksDB::getOrCreateNamespace(const String & name)
{
    std::lock_guard lock(namespace_cache_mutex_);
    
    auto it = namespace_cache_.find(name);
    if (it != namespace_cache_.end())
        return it->second;

    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyValueStoreRocksDB is shutdown");

    rocksdb::ColumnFamilyHandle * handle = nullptr;
    rocksdb::Status status = rocksdb_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &handle);
    
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "CreateColumnFamily failed: {}", status.ToString());

    cf_handles_.push_back(handle);
    auto ns = std::make_shared<KeyValueNamespaceRocksDB>(handle);
    ns->setStore(this);
    namespace_cache_[name] = ns;
    
    return ns;
}

IKeyValueNamespacePtr KeyValueStoreRocksDB::getNamespace(const String & name) const
{
    std::lock_guard lock(namespace_cache_mutex_);
    auto it = namespace_cache_.find(name);
    if (it != namespace_cache_.end())
        return it->second;
    return nullptr;
}

KVStatus KeyValueStoreRocksDB::dropNamespace(const String & name)
{
    // RocksDB doesn't support dropping column families directly
    // This would require complex logic to mark as deleted
    return KVStatus::NotSupported;
}

std::vector<String> KeyValueStoreRocksDB::listNamespaces() const
{
    std::lock_guard lock(namespace_cache_mutex_);
    std::vector<String> names;
    names.reserve(namespace_cache_.size());
    for (const auto & [name, _] : namespace_cache_)
        names.push_back(name);
    return names;
}

KVStatus KeyValueStoreRocksDB::put(IKeyValueNamespace * ns, const String & key, const String & value)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyValueStoreRocksDB is shutdown");

    auto * cf_handle = getColumnFamilyHandle(ns);
    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;
    
    auto status = rocksdb_->Put(options, cf_handle, key, value);
    return convertStatus(status);
}

KVStatus KeyValueStoreRocksDB::get(IKeyValueNamespace * ns, const String & key, String & value) const
{
    auto * cf_handle = getColumnFamilyHandle(ns);
    auto status = rocksdb_->Get(rocksdb::ReadOptions(), cf_handle, key, &value);
    return convertStatus(status);
}

KVStatus KeyValueStoreRocksDB::del(IKeyValueNamespace * ns, const String & key)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyValueStoreRocksDB is shutdown");

    auto * cf_handle = getColumnFamilyHandle(ns);
    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;
    
    auto status = rocksdb_->Delete(options, cf_handle, key);
    return convertStatus(status);
}

KVStatus KeyValueStoreRocksDB::delRange(IKeyValueNamespace * ns, const String & start_key, const String & end_key)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyValueStoreRocksDB is shutdown");

    auto * cf_handle = getColumnFamilyHandle(ns);
    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;
    
    auto status = rocksdb_->DeleteRange(options, cf_handle, start_key, end_key);
    return convertStatus(status);
}

void KeyValueStoreRocksDB::getByPrefix(
    IKeyValueNamespace * ns,
    const String & prefix,
    std::vector<String> & keys,
    std::vector<String> & values) const
{
    keys.clear();
    values.clear();

    auto * cf_handle = getColumnFamilyHandle(ns);
    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), cf_handle));

    if (!it)
    {
        LOG_ERROR(log, "Failed to create iterator for column family");
        return;
    }

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

void KeyValueStoreRocksDB::getValuesByPrefix(
    IKeyValueNamespace * ns,
    const String & prefix,
    std::vector<String> & values) const
{
    values.clear();

    auto * cf_handle = getColumnFamilyHandle(ns);
    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), cf_handle));

    if (!it)
    {
        LOG_ERROR(log, "Failed to create iterator for column family");
        return;
    }

    rocksdb::Slice target(prefix);
    for (it->Seek(target); it->Valid(); it->Next())
    {
        const auto key = it->key();
        if (!key.starts_with(target))
            break;

        values.emplace_back(it->value().data(), it->value().size());
    }
}

void KeyValueStoreRocksDB::getValuesByRange(
    IKeyValueNamespace * ns,
    const String & begin_key,
    const String & end_key,
    std::vector<String> & values) const
{
    values.clear();

    auto * cf_handle = getColumnFamilyHandle(ns);
    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), cf_handle));

    if (!it)
    {
        LOG_ERROR(log, "Failed to create iterator for column family when get values by range");
        return;
    }

    rocksdb::Slice begin(begin_key);
    rocksdb::Slice end(end_key);

    for (it->Seek(begin); it->Valid(); it->Next())
    {
        const auto key = it->key();
        // [begin_key, end_key)
        if (key.compare(end) >= 0)
            break;

        values.emplace_back(it->value().data(), it->value().size());
    }
}

IKeyValueIteratorPtr KeyValueStoreRocksDB::createIterator(IKeyValueNamespace * ns) const
{
    auto * cf_handle = getColumnFamilyHandle(ns);
    std::unique_ptr<rocksdb::Iterator> it(rocksdb_->NewIterator(rocksdb::ReadOptions(), cf_handle));
    return std::make_unique<KeyValueIteratorRocksDB>(std::move(it));
}

KVStatus KeyValueStoreRocksDB::batchWrite(const std::vector<BatchOperation> & operations)
{
    if (shutdown_called_.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeyValueStoreRocksDB is shutdown");

    if (operations.empty())
        return KVStatus::OK;

    rocksdb::WriteBatch batch;
    
    for (const auto & op : operations)
    {
        auto * cf_handle = getColumnFamilyHandle(op.namespace_ptr.get());
        
        if (op.type == BatchOperation::DELETE)
        {
            batch.Delete(cf_handle, op.key);
        }
        else if (op.type == BatchOperation::PUT)
        {
            batch.Put(cf_handle, op.key, op.value);
        }
    }

    rocksdb::WriteOptions options;
    options.sync = false;
    options.disableWAL = false;

    auto status = rocksdb_->Write(options, &batch);
    return convertStatus(status);
}

uint64_t KeyValueStoreRocksDB::getEstimateNumKeys(IKeyValueNamespace * ns) const
{
    auto * cf_handle = getColumnFamilyHandle(ns);
    uint64_t keys = 0;
    rocksdb_->GetIntProperty(cf_handle, "rocksdb.estimate-num-keys", &keys);
    return keys;
}

std::unordered_map<String, size_t> KeyValueStoreRocksDB::getProperty(IKeyValueNamespace * ns, const String & property) const
{
    std::unordered_map<String, size_t> results;
    
    auto * cf_handle = getColumnFamilyHandle(ns);
    uint64_t value = 0;
    
    if (rocksdb_->GetIntProperty(cf_handle, property, &value))
    {
        results.emplace(cf_handle->GetName(), static_cast<size_t>(value));
    }
    
    return results;
}

void KeyValueStoreRocksDB::flush()
{
    if (shutdown_called_.load())
    {
        LOG_WARNING(log, "Cannot flush after shutdown");
        return;
    }

    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    flush_options.allow_write_stall = true;

    for (auto * handle : cf_handles_)
    {
        auto status = rocksdb_->Flush(flush_options, handle);
        if (!status.ok())
            LOG_WARNING(log, "Failed to flush column family {}: {}", handle->GetName(), status.ToString());
    }
}

void KeyValueStoreRocksDB::shutdown()
{
    if (shutdown_called_.exchange(true))
    {
        LOG_WARNING(log, "The KeyValueStoreRocksDB has been shutdown and cannot be shutdown repeatedly");
        return;
    }

    {
        std::lock_guard lock(namespace_cache_mutex_);
        namespace_cache_.clear();
    }

    for (auto * handle : cf_handles_)
    {
        rocksdb_->DestroyColumnFamilyHandle(handle);
    }
    cf_handles_.clear();

    rocksdb_->Close();
}

} // namespace DB

#endif // USE_ROCKSDB

