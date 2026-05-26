#include "config.h"

#if USE_ROCKSDB

#include <Interpreters/FileCache/FileCacheRocksDBIndex.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <Common/CurrentMetrics.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheRocksDBIndexElements;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
    extern const int LOGICAL_ERROR;
}

FileCacheRocksDBIndex::FileCacheRocksDBIndex(const std::string & cache_base_path, const std::string & cache_name)
    : log(getLogger("FileCacheRocksDBIndex(" + cache_name + ")"))
{
    const auto rocksdb_path = fs::path(cache_base_path) / ".metadata_index";
    fs::create_directories(rocksdb_path);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.info_log_level = rocksdb::InfoLogLevel::ERROR_LEVEL;
    options.compression = rocksdb::kLZ4Compression;
    options.max_background_jobs = 2;

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
    static_assert(std::is_same_v<decltype(key.key), UInt128>, "FileCacheKey::key must be UInt128");
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

enum class IndexValueVersion : UInt8
{
    V0 = 0, /// Initial binary format: size, segment_type, has_weight, weight, user_id.
};

static constexpr auto INDEX_VALUE_CURRENT_VERSION = IndexValueVersion::V0;

static std::string serializeValue(Int64 size, const FileCacheOriginInfo & origin)
{
    WriteBufferFromOwnString out;
    writeBinaryLittleEndian(static_cast<UInt8>(INDEX_VALUE_CURRENT_VERSION), out);
    writeBinaryLittleEndian(size, out);
    writeBinaryLittleEndian(static_cast<UInt8>(origin.segment_type), out);
    writeBinaryLittleEndian(origin.weight.has_value(), out);
    writeBinaryLittleEndian(origin.weight.value_or(0), out);
    writeBinary(origin.user_id, out);
    return out.str();
}

static void deserializeValue(const rocksdb::Slice & slice, Int64 & size, FileCacheOriginInfo & origin)
{
    ReadBufferFromMemory in(slice.data(), slice.size());

    UInt8 version_byte = 0;
    readBinaryLittleEndian(version_byte, in);
    auto version = static_cast<IndexValueVersion>(version_byte);

    if (version == IndexValueVersion::V0)
    {
        readBinaryLittleEndian(size, in);

        UInt8 key_type = 0;
        readBinaryLittleEndian(key_type, in);
        origin.segment_type = static_cast<FileSegmentKeyType>(key_type);

        bool has_weight = false;
        readBinaryLittleEndian(has_weight, in);

        UInt64 weight = 0;
        readBinaryLittleEndian(weight, in);
        origin.weight = has_weight ? std::optional<UInt64>(weight) : std::nullopt;

        readBinary(origin.user_id, in);
    }
    else
    {
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Unsupported RocksDB index value version: {}", static_cast<UInt16>(version_byte));
    }
}

void FileCacheRocksDBIndex::put(const FileCacheKey & key, size_t offset, Int64 size, const FileCacheOriginInfo & origin, bool is_new_entry)
{
    auto serialized_key = serializeKey(key, offset);
    auto serialized_value = serializeValue(size, origin);

#ifdef DEBUG_OR_SANITIZER_BUILD
    {
        rocksdb::ReadOptions read_options;
        std::string existing_value;
        auto get_status = db->Get(read_options, serialized_key, &existing_value);
        if (is_new_entry)
            chassert(get_status.IsNotFound());
        else
            chassert(get_status.ok());
    }
#endif

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = db->Put(write_options, serialized_key, serialized_value);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to write to RocksDB index: {}", status.ToString());

    if (is_new_entry)
        CurrentMetrics::add(CurrentMetrics::FilesystemCacheRocksDBIndexElements);
}

void FileCacheRocksDBIndex::remove(const FileCacheKey & key, size_t offset)
{
    auto serialized_key = serializeKey(key, offset);

    rocksdb::WriteOptions write_options;
    write_options.sync = true;

    auto status = db->Delete(write_options, serialized_key);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to delete from RocksDB index: {}", status.ToString());

    CurrentMetrics::sub(CurrentMetrics::FilesystemCacheRocksDBIndexElements);
}

bool FileCacheRocksDBIndex::exists(const FileCacheKey & key, size_t offset) const
{
    auto serialized_key = serializeKey(key, offset);
    rocksdb::ReadOptions read_options;
    std::string value;
    auto status = db->Get(read_options, serialized_key, &value);
    return status.ok();
}

std::vector<FileCacheRocksDBIndex::Entry> FileCacheRocksDBIndex::initializeAndLoadAll()
{
    if (initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB metadata index is already initialized");
    initialized = true;

    std::vector<Entry> entries;

    rocksdb::ReadOptions read_options;
    read_options.fill_cache = false;

    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options));
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        auto key_slice = it->key();
        auto value_slice = it->value();

        static constexpr size_t expected_key_size = 24;

        if (key_slice.size() != expected_key_size)
        {
            LOG_WARNING(log, "Skipping malformed RocksDB entry: key_size={}", key_slice.size());
            chassert(false);
            continue;
        }

        try
        {
            Entry entry;
            deserializeKey(std::string_view(key_slice.data(), key_slice.size()), entry.key, entry.offset);
            deserializeValue(value_slice, entry.size, entry.origin);
            entries.push_back(std::move(entry));
        }
        catch (...)
        {
            LOG_WARNING(log, "Skipping malformed RocksDB value: {}", getCurrentExceptionMessage(false));
            chassert(false);
        }
    }

    if (!it->status().ok())
        LOG_ERROR(log, "RocksDB iteration error: {}", it->status().ToString());

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheRocksDBIndexElements, entries.size());

    LOG_INFO(log, "Loaded {} entries from RocksDB metadata index", entries.size());
    return entries;
}

}

#endif
