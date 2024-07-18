#include "MergeTreeMetadataCache.h"

#if USE_ROCKSDB
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event MergeTreeMetadataCachePut;
    extern const Event MergeTreeMetadataCacheGet;
    extern const Event MergeTreeMetadataCacheDelete;
    extern const Event MergeTreeMetadataCacheSeek;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}


std::unique_ptr<MergeTreeMetadataCache> MergeTreeMetadataCache::create(const String & dir, size_t size)
{
    assert(size != 0);
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::DB * db;

    options.create_if_missing = true;
    auto cache = rocksdb::NewLRUCache(size);
    table_options.block_cache = cache;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    rocksdb::Status status = rocksdb::DB::Open(options, dir, &db);
    if (status != rocksdb::Status::OK())
        throw Exception(
            ErrorCodes::SYSTEM_ERROR,
            "Fail to open rocksdb path at: {} status:{}. You can try to remove the cache (this will not affect any table data).",
            dir,
            status.ToString());
    return std::make_unique<MergeTreeMetadataCache>(db);
}

MergeTreeMetadataCache::Status MergeTreeMetadataCache::put(const String & key, const String & value)
{
    auto options = rocksdb::WriteOptions();
    options.sync = true;
    options.disableWAL = false;
    auto status = rocksdb->Put(options, key, value);
    ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCachePut);
    return status;
}

MergeTreeMetadataCache::Status MergeTreeMetadataCache::del(const String & key)
{
    auto options = rocksdb::WriteOptions();
    options.sync = true;
    options.disableWAL = false;
    auto status = rocksdb->Delete(options, key);
    ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheDelete);
    LOG_TRACE(log, "Delete key:{} from MergeTreeMetadataCache status:{}", key, status.ToString());
    return status;
}

MergeTreeMetadataCache::Status MergeTreeMetadataCache::get(const String & key, String & value)
{
    auto status = rocksdb->Get(rocksdb::ReadOptions(), key, &value);
    ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheGet);
    LOG_TRACE(log, "Get key:{} from MergeTreeMetadataCache status:{}", key, status.ToString());
    return status;
}

void MergeTreeMetadataCache::getByPrefix(const String & prefix, Strings & keys, Strings & values)
{
    auto * it = rocksdb->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice target(prefix);
    for (it->Seek(target); it->Valid(); it->Next())
    {
        const auto key = it->key();
        if (!key.starts_with(target))
            break;

        const auto value = it->value();
        keys.emplace_back(key.data(), key.size());
        values.emplace_back(value.data(), value.size());
    }
    LOG_TRACE(log, "Seek with prefix:{} from MergeTreeMetadataCache items:{}", prefix, keys.size());
    ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCacheSeek);
    delete it;
}

uint64_t MergeTreeMetadataCache::getEstimateNumKeys() const
{
    uint64_t keys = 0;
    rocksdb->GetAggregatedIntProperty("rocksdb.estimate-num-keys", &keys);
    return keys;
}

void MergeTreeMetadataCache::shutdown()
{
    rocksdb->Close();
    rocksdb.reset();
}

}

#endif
