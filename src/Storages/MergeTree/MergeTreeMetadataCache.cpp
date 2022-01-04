#include "MergeTreeMetadataCache.h"

#if USE_ROCKSDB
#include <Common/ProfileEvents.h>
#include <base/logger_useful.h>

namespace ProfileEvents
{
    extern const Event MergeTreeMetadataCachePut;
    extern const Event MergeTreeMetadataCacheGet;
    extern const Event MergeTreeMetadataCacheDelete;
    extern const Event MergeTreeMetadataCacheSeek;
}

namespace DB
{
MergeTreeMetadataCache::Status MergeTreeMetadataCache::put(const String & key, const String & value)
{
    auto options = rocksdb::WriteOptions();
    auto status = rocksdb->Put(options, key, value);
    ProfileEvents::increment(ProfileEvents::MergeTreeMetadataCachePut);
    return status;
}

MergeTreeMetadataCache::Status MergeTreeMetadataCache::del(const String & key)
{
    auto options = rocksdb::WriteOptions();
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
}

void MergeTreeMetadataCache::shutdown()
{
    rocksdb->Close();
}

}

#endif
