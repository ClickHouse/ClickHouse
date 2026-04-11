#pragma once

#include "config.h"

#if USE_ROCKSDB

#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileCacheOriginInfo.h>
#include <base/types.h>
#include <Common/Logger.h>

#include <memory>
#include <vector>

namespace rocksdb
{
class DB;
}

namespace DB
{

/// RocksDB-based index that stores file segment metadata for fast cache loading on startup.
///
/// Each entry is (FileCacheKey, offset) -> (size, origin), where size = -1 means the segment
/// was not fully downloaded (requires stat on startup).
///
/// Operation ordering:
/// - Segment created: put(key, offset, -1, origin) BEFORE writing file data.
/// - Segment fully downloaded: put(key, offset, actual_size, origin) AFTER file is fsynced.
/// - Segment removed: remove(key, offset) AFTER file is deleted from disk.
/// - Startup: iterate all entries. size >= 0 -> use as-is. size == -1 -> stat the file.
class FileCacheRocksDBIndex
{
public:
    explicit FileCacheRocksDBIndex(const std::string & cache_base_path);
    ~FileCacheRocksDBIndex();

    /// Store a segment entry. Use size = -1 for not-yet-downloaded segments.
    void put(const FileCacheKey & key, size_t offset, Int64 size, const FileCacheOriginInfo & origin);

    /// Remove a segment's entry. Called AFTER the file is deleted from disk.
    void remove(const FileCacheKey & key, size_t offset);

    struct Entry
    {
        FileCacheKey key;
        size_t offset;
        Int64 size; /// -1 means unknown (need stat)
        FileCacheOriginInfo origin;
    };

    /// Iterate the entire index and return all entries.
    /// Called once at startup before loading metadata.
    std::vector<Entry> loadAll() const;

private:
    std::unique_ptr<rocksdb::DB> db;
    LoggerPtr log;

    static std::string serializeKey(const FileCacheKey & key, size_t offset);
    static void deserializeKey(std::string_view slice, FileCacheKey & key, size_t & offset);
};

using FileCacheRocksDBIndexPtr = std::shared_ptr<FileCacheRocksDBIndex>;

}

#endif
