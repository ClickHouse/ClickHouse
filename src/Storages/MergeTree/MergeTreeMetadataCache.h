#pragma once

#include "config_core.h"

#if USE_ROCKSDB
#include <base/types.h>
#include <Core/Types.h>
#include <Poco/Logger.h>
#include <rocksdb/table.h>
#include <rocksdb/db.h>

namespace DB
{
class MergeTreeMetadataCache
{
public:
    using Status = rocksdb::Status;

    explicit MergeTreeMetadataCache(rocksdb::DB * rocksdb_) : rocksdb{rocksdb_} { }
    MergeTreeMetadataCache(const MergeTreeMetadataCache &) = delete;
    MergeTreeMetadataCache & operator=(const MergeTreeMetadataCache &) = delete;

    Status put(const String & key, const String & value);
    Status del(const String & key);
    Status get(const String & key, String & value);
    void getByPrefix(const String & prefix, Strings & keys, Strings & values);

    void shutdown();
private:
    std::unique_ptr<rocksdb::DB> rocksdb;
    Poco::Logger * log = &Poco::Logger::get("MergeTreeMetadataCache");
};

using MergeTreeMetadataCachePtr = std::shared_ptr<MergeTreeMetadataCache>;
}

#endif
