#pragma once

#include "config.h"

#if USE_ROCKSDB

#include <Interpreters/FileCache/FileCacheKey.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <base/types.h>
#include <Common/Logger.h>

#include <atomic>
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
/// Each entry is (FileCacheKey, offset, user_id) -> (size, origin), where size = -1 means the segment
/// was not fully downloaded (requires stat on startup). user_id is part of the key so the index
/// stays unambiguous in `write_cache_per_user_id_directory` mode, where two users may cache the same
/// remote file at the same offset under different physical paths.
///
/// Operation ordering:
/// - Segment created: put(key, offset, -1, origin) BEFORE writing file data.
/// - Segment fully downloaded: put(key, offset, actual_size, origin) AFTER file is fsynced.
/// - Segment removed: remove(key, offset, user_id) BEFORE file is deleted from disk (in detach).
/// - Startup: iterate all entries. size >= 0 -> use as-is. size == -1 -> stat the file.
class FileCacheRocksDBIndex
{
public:
    FileCacheRocksDBIndex(const std::string & cache_base_path, const std::string & cache_name);
    ~FileCacheRocksDBIndex();

    /// Store a segment entry. Use size = -1 for not-yet-downloaded segments.
    /// Set is_new_entry = true when inserting a brand-new segment (first reservation),
    /// false when updating an existing entry (e.g. download completed).
    void put(const FileCacheKey & key, size_t offset, Int64 size, const FileCacheOriginInfo & origin, bool is_new_entry);

    /// Remove a segment's entry. Called BEFORE the file is deleted from disk (in detach).
    void remove(const FileCacheKey & key, size_t offset, const std::string & user_id);

    /// Check if an entry exists in the index.
    bool exists(const FileCacheKey & key, size_t offset, const std::string & user_id) const;

    struct Entry
    {
        FileCacheKey key{};
        size_t offset = 0;
        Int64 size = -1; /// -1 means unknown (need stat)
        FileCacheOriginInfo origin;
    };

    /// Iterate the entire index and return all entries. Pure read, no side effects.
    std::vector<Entry> loadAll() const;

    /// Iterate the entire index via loadAll, initialize the CurrentMetric, and return all entries.
    /// Must be called once at startup before loading metadata; repeated calls throw.
    std::vector<Entry> initializeAndLoadAll();

private:
    std::unique_ptr<rocksdb::DB> db;
    LoggerPtr log;
    bool initialized = false;

    /// Number of entries this instance has added to FilesystemCacheRocksDBIndexElements.
    /// Subtracted on destruction so the metric is idempotent across cache re-initialization.
    std::atomic<Int64> accounted_elements{0};

    static std::string serializeKey(const FileCacheKey & key, size_t offset, const std::string & user_id);
    static void deserializeKey(std::string_view slice, FileCacheKey & key, size_t & offset);
};

using FileCacheRocksDBIndexPtr = std::shared_ptr<FileCacheRocksDBIndex>;

}

#endif
