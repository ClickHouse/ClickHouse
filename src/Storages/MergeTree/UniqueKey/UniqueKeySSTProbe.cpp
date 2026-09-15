#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>

#include "config.h"

/// This whole translation unit is RocksDB-only (see the header): the SST backend
/// is unavailable without RocksDB, and its only caller is guarded.
#if USE_ROCKSDB

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>

#include <IO/ReadSettings.h>
#include <Common/Exception.h>

#include <algorithm>

#include <base/unaligned.h>

#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int ROCKSDB_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int NOT_IMPLEMENTED;
}

/// Declares a `rocksdb::FileSystem` method override that unconditionally
/// returns `NotSupported`. `ReadBufferFileSystem` implements only the read-only,
/// random-access operations the SST reader needs; everything else is
/// fail-closed. Scoped to this file: `#undef`-ed right after the class.
#define ROCKSDB_IO_NOT_SUPPORTED(name, ...) \
    rocksdb::IOStatus name(__VA_ARGS__) override { return rocksdb::IOStatus::NotSupported(); }

namespace
{
    /// Decode the 4-byte big-endian row number written by `SSTIndexWriter`
    /// (`encodeRowNumberBE`), widened to UInt64 (== `_part_offset`). The writer
    /// always emits exactly 4 bytes; any other size is a corrupt or
    /// incompatible sidecar, so fail closed rather than decode a prefix.
    UInt64 decodeRowNumberBE(const char * data, size_t size)
    {
        if (size != sizeof(UInt32))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "UNIQUE KEY SST value has {} bytes, expected exactly 4", size);
        return unalignedLoadBigEndian<UInt32>(data);
    }

    /// RocksDB's `MultiGetContext` caps a call at 32 keys (debug assert /
    /// release OOB read). `SstFileReader::MultiGet` does not chunk.
    constexpr size_t MULTI_GET_BATCH_SIZE = 32;

    /// `FSRandomAccessFile` backed by a `ReadBuffer`. `readBigAt` is positional
    /// and lock-free, so concurrent RocksDB reads need no serialization. The
    /// buffer must support positional reads; that is enforced at open time by
    /// `NewRandomAccessFile`, so the ctor can assume it here.
    class ReadBufferBasedRandomAccessFile : public rocksdb::FSRandomAccessFile
    {
    public:
        ReadBufferBasedRandomAccessFile(std::unique_ptr<SeekableReadBuffer> buffer_, uint64_t file_size_)
            : buffer(std::move(buffer_))
            , file_size(file_size_)
        {
        }

        /// No direct IO, so reads need no alignment. The `kDefaultPageSize`
        /// default would inflate every read to a 4096-aligned window.
        size_t GetRequiredBufferAlignment() const override { return 1; }

        rocksdb::IOStatus Read(
            uint64_t offset,
            size_t n,
            const rocksdb::IOOptions &,
            rocksdb::Slice * result,
            char * scratch,
            rocksdb::IODebugContext *) const override
        {
            try
            {
                if (offset >= file_size)
                {
                    *result = rocksdb::Slice(scratch, 0);
                    return rocksdb::IOStatus::OK();
                }
                n = std::min(n, static_cast<size_t>(file_size - offset));

                size_t bytes_read = buffer->readBigAt(scratch, n, offset, {});
                *result = rocksdb::Slice(scratch, bytes_read);
                return rocksdb::IOStatus::OK();
            }
            catch (...)
            {
                return rocksdb::IOStatus::IOError(getCurrentExceptionMessage(/*with_stacktrace=*/false));
            }
        }

    private:
        std::unique_ptr<SeekableReadBuffer> buffer;
        uint64_t file_size;
    };

    /// Read-only `FileSystem` that opens files through `IDataPartStorage`, so
    /// remote SSTs are read in place. Only random-access is
    /// supported; everything else returns `NotSupported`.
    class ReadBufferFileSystem : public rocksdb::FileSystem
    {
    public:
        ReadBufferFileSystem(DataPartStoragePtr storage_, ReadSettings read_settings_)
            : storage(std::move(storage_))
            , read_settings(std::move(read_settings_))
        {
            /// `readBigAt` is missing in async buffers, and O_DIRECT rejects unaligned reads;
            /// force plain `pread`, no direct IO, no reader executor.
            /// TODO(unique-key): adapt to those paths and drop this.
            read_settings.local_fs_settings.method = LocalFSReadMethod::pread;
            read_settings.local_fs_settings.direct_io_threshold = 0;
            read_settings.reader_executor.enabled = false;
        }

        const char * Name() const override { return "ReadBufferFileSystem"; }

        rocksdb::IOStatus NewRandomAccessFile(
            const std::string & f,
            const rocksdb::FileOptions &,
            std::unique_ptr<rocksdb::FSRandomAccessFile> * r,
            rocksdb::IODebugContext *) override
        {
            try
            {
                auto buffer = storage->readFile(f, read_settings, /*read_hint=*/std::nullopt);
                /// Fail closed: without positional reads (`readBigAt`) the only
                /// alternative is a `seek`+`read` fallback under a mutex, which
                /// serializes every SST read. Reject such storage instead of
                /// silently degrading concurrency.
                if (!buffer->supportsReadAt())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "UNIQUE KEY SST cannot be read from `{}`: the storage does not support "
                        "positional reads (readBigAt), required for concurrent index probing",
                        f);
                const uint64_t file_size = storage->getFileSize(f);
                *r = std::make_unique<ReadBufferBasedRandomAccessFile>(std::move(buffer), file_size);
                return rocksdb::IOStatus::OK();
            }
            catch (...)
            {
                return rocksdb::IOStatus::IOError(getCurrentExceptionMessage(/*with_stacktrace=*/false));
            }
        }

        rocksdb::IOStatus FileExists(
            const std::string & f,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override
        {
            try
            {
                return storage->existsFile(f) ? rocksdb::IOStatus::OK() : rocksdb::IOStatus::NotFound();
            }
            catch (...)
            {
                return rocksdb::IOStatus::IOError(getCurrentExceptionMessage(/*with_stacktrace=*/false));
            }
        }

        rocksdb::IOStatus GetFileSize(
            const std::string & f,
            const rocksdb::IOOptions &,
            uint64_t * res,
            rocksdb::IODebugContext *) override
        {
            try
            {
                *res = storage->getFileSize(f);
                return rocksdb::IOStatus::OK();
            }
            catch (...)
            {
                return rocksdb::IOStatus::IOError(getCurrentExceptionMessage(/*with_stacktrace=*/false));
            }
        }

        /// Unsupported: this file system is read-only and random-access only.
        ROCKSDB_IO_NOT_SUPPORTED(NewSequentialFile,
            const std::string &, const rocksdb::FileOptions &,
            std::unique_ptr<rocksdb::FSSequentialFile> *, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(NewWritableFile,
            const std::string &, const rocksdb::FileOptions &,
            std::unique_ptr<rocksdb::FSWritableFile> *, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(NewDirectory,
            const std::string &, const rocksdb::IOOptions &,
            std::unique_ptr<rocksdb::FSDirectory> *, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(GetChildren,
            const std::string &, const rocksdb::IOOptions &,
            std::vector<std::string> *, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(DeleteFile,
            const std::string &, const rocksdb::IOOptions &, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(CreateDir,
            const std::string &, const rocksdb::IOOptions &, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(CreateDirIfMissing,
            const std::string &, const rocksdb::IOOptions &, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(DeleteDir,
            const std::string &, const rocksdb::IOOptions &, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(GetFileModificationTime,
            const std::string &, const rocksdb::IOOptions &, uint64_t *, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(GetAbsolutePath,
            const std::string &, const rocksdb::IOOptions &, std::string *, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(RenameFile,
            const std::string &, const std::string &, const rocksdb::IOOptions &, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(LockFile,
            const std::string &, const rocksdb::IOOptions &, rocksdb::FileLock **, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(UnlockFile,
            rocksdb::FileLock *, const rocksdb::IOOptions &, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(GetTestDirectory,
            const rocksdb::IOOptions &, std::string *, rocksdb::IODebugContext *)
        ROCKSDB_IO_NOT_SUPPORTED(IsDirectory,
            const std::string &, const rocksdb::IOOptions &, bool *, rocksdb::IODebugContext *)

    private:
        DataPartStoragePtr storage;
        ReadSettings read_settings;
    };
}

#undef ROCKSDB_IO_NOT_SUPPORTED

SSTFileReader::SSTFileReader(const DataPartStoragePtr & storage, const String & sst_file_name, const ReadSettings & read_settings)
{
    sst_env = rocksdb::NewCompositeEnv(std::make_shared<ReadBufferFileSystem>(storage, read_settings));

    /// Bloom policy must match `SSTIndexWriter`.
    rocksdb::Options options;
    options.env = sst_env.get();
    rocksdb::BlockBasedTableOptions block_based;
    block_based.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(SSTIndexWriter::BLOOM_BITS_PER_KEY));
    block_based.no_block_cache = true;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based));

    auto reader = std::make_unique<rocksdb::SstFileReader>(options);
    auto status = reader->Open(sst_file_name);
    if (!status.ok())
        /// A corruption means the file itself is damaged, so a caller may rebuild
        /// it; any other failure may be transient and the file must be kept.
        throw Exception(status.IsCorruption() ? ErrorCodes::CORRUPTED_DATA : ErrorCodes::CANNOT_OPEN_FILE,
            "Failed to open UNIQUE KEY SST `{}`: {}", sst_file_name, status.ToString());
    index_reader = std::move(reader);

    /// Capture the min/max key once at open, so probes can skip out-of-range keys.
    /// A read error leaves an endpoint empty, disabling that side only.
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it(index_reader->NewIterator(read_opts));
    it->SeekToFirst();
    if (it->Valid())
        key_range.first = it->key().ToString();
    it->SeekToLast();
    if (it->Valid())
        key_range.second = it->key().ToString();
}

std::unique_ptr<rocksdb::Iterator> SSTFileReader::newIterator(const rocksdb::ReadOptions & options) const
{
    return std::unique_ptr<rocksdb::Iterator>(index_reader->NewIterator(options));
}

std::vector<rocksdb::Status> SSTFileReader::multiGet(
    const std::vector<rocksdb::Slice> & keys, std::vector<std::string> * values_out) const
{
    if (keys.empty())
        return {};

    std::vector<rocksdb::Status> statuses;
    statuses.reserve(keys.size());
    std::vector<std::string> values(keys.size());

    /// Reused across chunks; `MultiGet` resizes `chunk_values` itself.
    std::vector<rocksdb::Slice> chunk_keys;
    chunk_keys.reserve(MULTI_GET_BATCH_SIZE);
    std::vector<std::string> chunk_values;

    for (size_t begin = 0; begin < keys.size(); begin += MULTI_GET_BATCH_SIZE)
    {
        const size_t end = std::min(begin + MULTI_GET_BATCH_SIZE, keys.size());
        chunk_keys.assign(keys.begin() + begin, keys.begin() + end);
        chunk_values.clear();
        auto chunk_statuses = index_reader->MultiGet(rocksdb::ReadOptions(), chunk_keys, &chunk_values);

        if (chunk_statuses.size() != chunk_keys.size() || chunk_values.size() != chunk_keys.size())
            throw Exception(ErrorCodes::ROCKSDB_ERROR,
                "UNIQUE KEY SST MultiGet returned {} statuses / {} values for {} keys",
                chunk_statuses.size(), chunk_values.size(), chunk_keys.size());

        /// A miss is `NotFound`; any other error is a read error - fail closed.
        for (size_t c = 0; c < chunk_keys.size(); ++c)
        {
            if (!chunk_statuses[c].ok() && !chunk_statuses[c].IsNotFound())
                throw Exception(ErrorCodes::ROCKSDB_ERROR,
                    "Failed to MultiGet from UNIQUE KEY SST: {}", chunk_statuses[c].ToString());
            statuses.push_back(std::move(chunk_statuses[c]));
            values[begin + c] = std::move(chunk_values[c]);
        }
    }

    if (values_out)
        *values_out = std::move(values);

    return statuses;
}

std::shared_ptr<const rocksdb::TableProperties> SSTFileReader::getProperties() const
{
    return index_reader->GetTableProperties();
}

rocksdb::Status SSTFileReader::verifyChecksum() const
{
    return index_reader->VerifyChecksum();
}

bool SSTFileReader::keyRangeIntersects(const MinMax & other) const
{
    if (key_range.first.empty() || key_range.second.empty())
        return true;

    /// No intersection: this.max < other.min OR this.min > other.max.
    if (key_range.second < other.first)
        return false;
    if (!other.second.empty() && key_range.first > other.second)
        return false;

    return true;
}

SSTFileReaderPtr openSSTReaderFromStorage(
    const DataPartStoragePtr & storage,
    const String & sst_file_name,
    const ReadSettings & read_settings)
{
    return std::make_shared<SSTFileReader>(storage, sst_file_name, read_settings);
}

SSTProbeTargetPart::SSTProbeTargetPart(
    const IMergeTreeDataPart * part_,
    std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
    SSTFileReaderPtr reader_)
    : part(part_)
    , pinned_bitmap(std::move(pinned_bitmap_))
    , reader(std::move(reader_))
{
}

void SSTProbeTargetPart::findRowIndexBatch(
    const std::vector<std::string_view> & encoded_keys,
    std::vector<std::optional<UInt64>> & out) const
{
    out.assign(encoded_keys.size(), std::nullopt);

    /// Fail closed: `NOT_FOUND` must mean "no active part holds the key", never
    /// "could not read this part" - a silent miss could let a duplicate through.
    if (!reader)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "UNIQUE KEY SST probe target has no readable index (invalid reader)");

    /// Keys outside the SST's [min, max] range cannot be present (bytewise
    /// comparator == lexicographic string order) - skip the bloom probe.
    const auto & key_range = reader->getKeyRange();

    std::vector<size_t> candidate_indices;
    candidate_indices.reserve(encoded_keys.size());
    for (size_t i = 0; i < encoded_keys.size(); ++i)
    {
        const std::string_view key = encoded_keys[i];
        if (!key_range.first.empty() && key < key_range.first)
            continue;
        if (!key_range.second.empty() && key > key_range.second)
            continue;
        candidate_indices.push_back(i);
    }

    if (candidate_indices.empty())
        return;

    /// Input is sorted by encoded key (interface contract), so each `MultiGet`
    /// chunk covers a contiguous key range.
    chassert(std::is_sorted(encoded_keys.begin(), encoded_keys.end()));

    std::vector<rocksdb::Slice> candidate_keys;
    candidate_keys.reserve(candidate_indices.size());
    for (size_t i : candidate_indices)
        candidate_keys.emplace_back(encoded_keys[i].data(), encoded_keys[i].size());

    /// `MultiGet` state is call-local, so concurrent probes need no iterator.
    std::vector<std::string> values;
    const auto statuses = reader->multiGet(candidate_keys, &values);

    for (size_t c = 0; c < candidate_keys.size(); ++c)
    {
        /// `multiGet` throws on read errors; a miss is `NotFound`.
        if (!statuses[c].ok())
            continue;

        const UInt64 row_number = decodeRowNumberBE(values[c].data(), values[c].size());
        /// Out-of-range row means a corrupt SST - fail closed.
        /// TODO(unique-key): enforce this once the probe factory passes a non-null part.
        if (part && row_number >= part->rows_count)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "UNIQUE KEY SST points at row {} but part '{}' has only {} rows",
                row_number, part->name, part->rows_count);
        out[candidate_indices[c]] = row_number;
    }
}

bool SSTProbeTargetPart::isRowDead(UInt64 row_number) const
{
    return pinned_bitmap && pinned_bitmap->contains(row_number);
}

}

#endif
