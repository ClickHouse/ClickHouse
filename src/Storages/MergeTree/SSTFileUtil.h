#pragma once

#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <base/types.h>
#include <memory>
#include <unordered_map>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/iterator.h>
#include <rocksdb/db.h>
namespace DB
{

struct MergeTreeDataPartChecksums;
struct WriteSettings;
class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

class SSTFileWriter;

/// SST data file extension for SST-based column types.
inline constexpr auto SST_DATA_FILE_EXTENSION = ".sst";

/// SST file write stream: manages FileBuffer + SSTFileWriter for a single column.
class SSTFileWriteStream
{
public:
    SSTFileWriteStream(
        const String & escaped_column_name_,
        const MutableDataPartStoragePtr & data_part_storage,
        size_t buf_size,
        const WriteSettings & query_write_settings);

    ~SSTFileWriteStream();

    SSTFileWriter & getSSTWriter();

    /// Finalize SST writer and record checksums
    void fillSSTChecksums(MergeTreeDataPartChecksums & checksums);
    void preFinalize();
    void finalize();
    void cancel() noexcept;
    void sync() const;
    void addToChecksums(MergeTreeDataPartChecksums & checksums);

    HashingWriteBuffer & getWriteBuffer() { return *hashing; }

private:
    String escaped_column_name;
    std::unique_ptr<WriteBufferFromFileBase> plain_file;
    std::unique_ptr<HashingWriteBuffer> hashing;
    std::unique_ptr<SSTFileWriter> sst_writer;
    bool pre_finalized = false;
};

using SSTFileWriteStreams = std::unordered_map<String, std::unique_ptr<SSTFileWriteStream>>;

using SSTReadBuffers = std::unordered_map<String, std::unique_ptr<ReadBufferFromFileBase>>;


/// RocksDB Env helpers
std::unique_ptr<rocksdb::Env> createWriteBufferFileSystemEnv(WriteBuffer * write_buffer);
std::unique_ptr<rocksdb::Env> createReadBufferFileSystemEnv(SeekableReadBuffer * read_buffer, uint64_t base_offset, uint64_t region_size);

class SSTFileReader
{
public:
    using IndexPropertiesPtr = std::shared_ptr<const rocksdb::TableProperties>;

    SSTFileReader(SeekableReadBuffer * read_buffer, uint64_t base_offset, uint64_t region_size);

    std::unique_ptr<rocksdb::Iterator> newIterator(const rocksdb::ReadOptions & options) const;
    bool mayContainKey(std::string_view key) const;
    void verifyChecksums() const;
    IndexPropertiesPtr getProperties() const;
    bool isEmpty() const;

private:
    using MinMax = std::pair<std::string, std::string>;
    std::unique_ptr<rocksdb::Env> sst_env;
    std::unique_ptr<rocksdb::SstFileReader> index_reader;
    MinMax key_range;
};

class SSTFileWriter
{
public:
    explicit SSTFileWriter(WriteBuffer * write_buffer);

    void put(const rocksdb::Slice & key, const rocksdb::Slice & value);
    void finish();
    uint64_t fileSize() const;

    UInt64 getWrittenRowCount() const { return written_row_counter; }
    void addWrittenRowCount(UInt64 count) { written_row_counter += count; }

private:
    UInt64 written_row_counter = 0;
    std::unique_ptr<rocksdb::Env> sst_env;
    std::unique_ptr<rocksdb::SstFileWriter> writer;
    bool finished = false;
    bool has_entries = false;
};

class SSTFileReadStream : public MergeTreeReaderStreamSingleColumnWholePart
{
public:
    template <typename... Args>
    explicit SSTFileReadStream(Args &&... args)
        : MergeTreeReaderStreamSingleColumnWholePart{std::forward<Args>(args)...}
    {
    }
};

}
