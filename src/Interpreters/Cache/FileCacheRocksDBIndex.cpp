#include "config.h"

#if USE_ROCKSDB

#include <Interpreters/Cache/FileCacheRocksDBIndex.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

FileCacheRocksDBIndex::FileCacheRocksDBIndex(const std::string & cache_base_path)
    : log(getLogger("FileCacheRocksDBIndex"))
{
    const auto rocksdb_path = fs::path(cache_base_path) / ".metadata_index";
    fs::create_directories(rocksdb_path);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.info_log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;
    options.compression = rocksdb::kLZ4Compression;
    options.max_background_jobs = 2;
    options.write_buffer_size = 4 * 1024 * 1024;

    rocksdb::DB * raw_db = nullptr;
    auto status = rocksdb::DB::Open(options, rocksdb_path.string(), &raw_db);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open RocksDB at {}: {}", rocksdb_path.string(), status.ToString());

    db.reset(raw_db);
    LOG_INFO(log, "Opened RocksDB metadata index at {}", rocksdb_path.string());
}

FileCacheRocksDBIndex::~FileCacheRocksDBIndex()
{
    if (db)
    {
        auto status = db->Close();
        if (!status.ok())
            LOG_ERROR(log, "Failed to close RocksDB: {}", status.ToString());
    }
}

std::string FileCacheRocksDBIndex::serializeKey(const FileCacheKey & key, size_t offset)
{
    /// Key format: UInt128 key (16 bytes, native endian) + UInt64 offset (8 bytes, native endian).
    std::string result;
    result.resize(sizeof(key.key) + sizeof(UInt64));
    memcpy(result.data(), &key.key, sizeof(key.key));
    UInt64 offset_val = static_cast<UInt64>(offset);
    memcpy(result.data() + sizeof(key.key), &offset_val, sizeof(UInt64));
    return result;
}

void FileCacheRocksDBIndex::deserializeKey(std::string_view slice, FileCacheKey & key, size_t & offset)
{
    memcpy(&key.key, slice.data(), sizeof(key.key));
    UInt64 offset_val = 0;
    memcpy(&offset_val, slice.data() + sizeof(key.key), sizeof(UInt64));
    offset = static_cast<size_t>(offset_val);
}

static std::string serializeValue(Int64 size, FileSegmentKeyType key_type)
{
    /// Value format: Int64 size (8 bytes) + UInt8 key_type (1 byte).
    std::string result;
    result.resize(sizeof(Int64) + sizeof(UInt8));
    memcpy(result.data(), &size, sizeof(Int64));
    result.back() = static_cast<UInt8>(key_type);
    return result;
}

static void deserializeValue(const rocksdb::Slice & slice, Int64 & size, FileSegmentKeyType & key_type)
{
    memcpy(&size, slice.data(), sizeof(Int64));
    key_type = static_cast<FileSegmentKeyType>(static_cast<UInt8>(slice.data()[sizeof(Int64)]));
}

void FileCacheRocksDBIndex::put(const FileCacheKey & key, size_t offset, Int64 size, FileSegmentKeyType key_type)
{
    auto serialized_key = serializeKey(key, offset);
    auto serialized_value = serializeValue(size, key_type);

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = db->Put(write_options, serialized_key, serialized_value);
    if (!status.ok())
        LOG_WARNING(log, "Failed to write to RocksDB index: {}", status.ToString());
}

void FileCacheRocksDBIndex::remove(const FileCacheKey & key, size_t offset)
{
    auto serialized_key = serializeKey(key, offset);

    rocksdb::WriteOptions write_options;
    write_options.sync = false;

    auto status = db->Delete(write_options, serialized_key);
    if (!status.ok())
        LOG_WARNING(log, "Failed to delete from RocksDB index: {}", status.ToString());
}

std::vector<FileCacheRocksDBIndex::Entry> FileCacheRocksDBIndex::loadAll() const
{
    std::vector<Entry> entries;

    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;

    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options));
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        auto key_slice = it->key();
        auto value_slice = it->value();

        static constexpr size_t expected_key_size = 24;
        static constexpr size_t expected_value_size = sizeof(Int64) + sizeof(UInt8);

        if (key_slice.size() != expected_key_size || value_slice.size() != expected_value_size)
        {
            LOG_WARNING(log, "Skipping malformed RocksDB entry: key_size={}, value_size={}", key_slice.size(), value_slice.size());
            continue;
        }

        Entry entry;
        deserializeKey(std::string_view(key_slice.data(), key_slice.size()), entry.key, entry.offset);
        deserializeValue(value_slice, entry.size, entry.key_type);
        entries.push_back(std::move(entry));
    }

    if (!it->status().ok())
        LOG_ERROR(log, "RocksDB iteration error: {}", it->status().ToString());

    LOG_INFO(log, "Loaded {} entries from RocksDB metadata index", entries.size());
    return entries;
}

}

#endif
