#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>

#include "config.h"

/// This whole translation unit is RocksDB-only (see the header): the SST backend
/// is unavailable without RocksDB, and its only caller is guarded.
#if USE_ROCKSDB

#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyIndexCache.h>

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>

#include <Common/Exception.h>

#include <base/unaligned.h>

#include <mutex>

#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/table_properties.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int ROCKSDB_ERROR;
    extern const int CORRUPTED_DATA;
}

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

    /// `rocksdb::FSRandomAccessFile` backed by a ClickHouse `ReadBuffer`. RocksDB
    /// only ever needs random-access reads for an SST (open, index/filter/data
    /// blocks). One file is shared across threads: the fast path
    /// `readBigAt` is positional (like `pread`, lock-free); the `seek`+`read`
    /// fallback needs the mutex. `supportsReadAt` is probed once (it may be slow
    /// and must not run concurrently with `readBigAt`). Two ownership modes: owned
    /// (holds the buffer) and non-owning (the caller keeps it alive).
    class ReadBufferBasedRandomAccessFile : public rocksdb::FSRandomAccessFile
    {
    public:
        explicit ReadBufferBasedRandomAccessFile(std::unique_ptr<ReadBufferFromFileBase> buffer_)
            : owned_buffer(std::move(buffer_))
            , buffer(owned_buffer.get())
            , supports_read_at(buffer->supportsReadAt())
        {
        }

        explicit ReadBufferBasedRandomAccessFile(SeekableReadBuffer & buffer_)
            : buffer(&buffer_)
            , supports_read_at(buffer->supportsReadAt())
        {
        }

        rocksdb::IOStatus Read(
            uint64_t offset,
            size_t n,
            const rocksdb::IOOptions &,
            rocksdb::Slice * result,
            char * scratch,
            rocksdb::IODebugContext *) const override
        {
            size_t bytes_read = 0;
            if (supports_read_at)
            {
                bytes_read = buffer->readBigAt(scratch, n, offset, {});
            }
            else
            {
                std::scoped_lock lock(mutex);
                buffer->seek(static_cast<off_t>(offset), SEEK_SET);
                bytes_read = buffer->read(scratch, n);
            }
            *result = rocksdb::Slice(scratch, bytes_read);
            return rocksdb::IOStatus::OK();
        }

    private:
        /// Set in owned mode only; `buffer` points into it. Empty in non-owning mode.
        std::unique_ptr<ReadBufferFromFileBase> owned_buffer;
        SeekableReadBuffer * buffer;
        const bool supports_read_at;
        mutable std::mutex mutex;
    };

    /// Read-only `rocksdb::FileSystem` over a ClickHouse `ReadBuffer`. Two modes:
    /// storage mode opens files through `IDataPartStorage::readFile` (a remote SST
    /// sidecar is read in place); injected mode wraps one externally managed
    /// `SeekableReadBuffer` (any source, caller keeps it alive). Only random-access
    /// reads are supported; everything else returns `NotSupported` (fail closed).
    class ReadBufferFileSystem : public rocksdb::FileSystem
    {
    public:
        explicit ReadBufferFileSystem(DataPartStoragePtr storage_)
            : storage(std::move(storage_))
        {
        }

        ReadBufferFileSystem(SeekableReadBuffer & injected_buffer_, uint64_t injected_file_size_)
            : injected_buffer(&injected_buffer_)
            , injected_file_size(injected_file_size_)
        {
        }

        const char * Name() const override { return "ReadBufferFileSystem"; }

        rocksdb::IOStatus NewRandomAccessFile(
            const std::string & f,
            const rocksdb::FileOptions &,
            std::unique_ptr<rocksdb::FSRandomAccessFile> * r,
            rocksdb::IODebugContext *) override
        {
            if (storage)
            {
                auto buffer = storage->readFile(f, ReadSettings(), /*read_hint=*/std::nullopt);
                *r = std::make_unique<ReadBufferBasedRandomAccessFile>(std::move(buffer));
            }
            else
            {
                *r = std::make_unique<ReadBufferBasedRandomAccessFile>(*injected_buffer);
            }
            return rocksdb::IOStatus::OK();
        }

        rocksdb::IOStatus FileExists(
            const std::string & f,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override
        {
            if (!storage)
                return rocksdb::IOStatus::OK();
            return storage->existsFile(f) ? rocksdb::IOStatus::OK() : rocksdb::IOStatus::NotFound();
        }

        rocksdb::IOStatus GetFileSize(
            const std::string & f,
            const rocksdb::IOOptions &,
            uint64_t * res,
            rocksdb::IODebugContext *) override
        {
            *res = storage ? storage->getFileSize(f) : injected_file_size;
            return rocksdb::IOStatus::OK();
        }

        /// Unsupported: this file system is read-only and random-access only.
        rocksdb::IOStatus NewSequentialFile(
            const std::string &,
            const rocksdb::FileOptions &,
            std::unique_ptr<rocksdb::FSSequentialFile> *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus NewWritableFile(
            const std::string &,
            const rocksdb::FileOptions &,
            std::unique_ptr<rocksdb::FSWritableFile> *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus NewDirectory(
            const std::string &,
            const rocksdb::IOOptions &,
            std::unique_ptr<rocksdb::FSDirectory> *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus GetChildren(
            const std::string &,
            const rocksdb::IOOptions &,
            std::vector<std::string> *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus DeleteFile(
            const std::string &,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus CreateDir(
            const std::string &,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus CreateDirIfMissing(
            const std::string &,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus DeleteDir(
            const std::string &,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus GetFileModificationTime(
            const std::string &,
            const rocksdb::IOOptions &,
            uint64_t *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus GetAbsolutePath(
            const std::string &,
            const rocksdb::IOOptions &,
            std::string *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus RenameFile(
            const std::string &,
            const std::string &,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus LockFile(
            const std::string &,
            const rocksdb::IOOptions &,
            rocksdb::FileLock **,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus UnlockFile(
            rocksdb::FileLock *,
            const rocksdb::IOOptions &,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus GetTestDirectory(
            const rocksdb::IOOptions &,
            std::string *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
        rocksdb::IOStatus IsDirectory(
            const std::string &,
            const rocksdb::IOOptions &,
            bool *,
            rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }

    private:
        /// Storage mode: files opened by name. Injected mode: one external buffer
        /// (caller-owned) of a known size.
        DataPartStoragePtr storage;
        SeekableReadBuffer * injected_buffer = nullptr;
        uint64_t injected_file_size = 0;
    };

    std::unique_ptr<rocksdb::Env> createReadBufferFileSystemEnv(const DataPartStoragePtr & storage)
    {
        return rocksdb::NewCompositeEnv(std::make_shared<ReadBufferFileSystem>(storage));
    }

    std::unique_ptr<rocksdb::Env> createReadBufferFileSystemEnv(SeekableReadBuffer & buffer, uint64_t file_size)
    {
        return rocksdb::NewCompositeEnv(std::make_shared<ReadBufferFileSystem>(buffer, file_size));
    }
}

SSTFileReader::SSTFileReader(const DataPartStoragePtr & storage, const String & sst_file_name, const UniqueKeyIndexCachePtr & block_cache)
{
    sst_env = createReadBufferFileSystemEnv(storage);
    init(sst_file_name, block_cache);
}

SSTFileReader::SSTFileReader(SeekableReadBuffer & buffer, uint64_t file_size, const UniqueKeyIndexCachePtr & block_cache)
{
    sst_env = createReadBufferFileSystemEnv(buffer, file_size);
    init(/*file_name=*/"", block_cache);
}

std::unique_ptr<rocksdb::Iterator> SSTFileReader::newIterator(const rocksdb::ReadOptions & options) const
{
    return std::unique_ptr<rocksdb::Iterator>(index_reader->NewIterator(options));
}

std::shared_ptr<const rocksdb::TableProperties> SSTFileReader::getProperties() const
{
    return index_reader->GetTableProperties();
}

void SSTFileReader::init(const String & file_name, const UniqueKeyIndexCachePtr & block_cache)
{
    /// Bloom policy must match `SSTIndexWriter` to honour the embedded filter.
    /// The shared `UniqueKeyIndexCache` caches blocks; null means no cache.
    rocksdb::Options options;
    options.env = sst_env.get();

    rocksdb::BlockBasedTableOptions block_based;
    block_based.filter_policy.reset(
        rocksdb::NewBloomFilterPolicy(SSTIndexWriter::BLOOM_BITS_PER_KEY));
    if (block_cache)
        block_based.block_cache = block_cache;
    else
        block_based.no_block_cache = true;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based));

    auto reader = std::make_unique<rocksdb::SstFileReader>(options);
    auto status = reader->Open(file_name);
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "Failed to open UNIQUE KEY SST `{}`: {}", file_name, status.ToString());
    index_reader = std::move(reader);
}

SSTFileReaderPtr openSSTReaderFromStorage(
    const DataPartStoragePtr & storage,
    const String & sst_file_name,
    const UniqueKeyIndexCachePtr & block_cache)
{
    return std::make_shared<SSTFileReader>(storage, sst_file_name, block_cache);
}

SSTFileReaderPtr openSSTReaderFromBuffer(
    ReadBufferFromFileBase & buffer,
    const UniqueKeyIndexCachePtr & block_cache)
{
    /// RocksDB needs the file size up front (to read the footer). Fail closed if
    /// the buffer cannot report it rather than guessing.
    auto file_size = buffer.tryGetFileSize();
    if (!file_size)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "Cannot open UNIQUE KEY SST from injected ReadBuffer: file size is unknown");

    return std::make_shared<SSTFileReader>(buffer, *file_size, block_cache);
}

SSTProbeTargetPart::SSTProbeTargetPart(
    const IMergeTreeDataPart * part_,
    std::shared_ptr<const DeleteBitmap> pinned_bitmap_,
    SSTReaderHandle handle_)
    : part(part_)
    , pinned_bitmap(std::move(pinned_bitmap_))
    , handle(std::move(handle_))
{
}

void SSTProbeTargetPart::findRowIndexBatch(
    const std::vector<std::string_view> & encoded_keys,
    std::vector<std::optional<UInt64>> & out) const
{
    out.assign(encoded_keys.size(), std::nullopt);

    /// Fail closed: `NOT_FOUND` must mean "no active part holds the key", never
    /// "could not read this part" — a silent miss could let a duplicate through.
    if (!handle.reader)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE,
            "UNIQUE KEY SST probe target has no readable index (invalid reader handle)");

    /// Fresh iterator per call: the target is shared as `shared_ptr<const>`, so a
    /// cached iterator would race across concurrent probes. `Seek` lands at >= key,
    /// so the exact-equality compare is required.
    rocksdb::ReadOptions read_opts;
    std::unique_ptr<rocksdb::Iterator> it = handle.reader->newIterator(read_opts);

    for (size_t i = 0; i < encoded_keys.size(); ++i)
    {
        rocksdb::Slice key_slice(encoded_keys[i].data(), encoded_keys[i].size());
        it->Seek(key_slice);
        if (it->Valid() && it->key().compare(key_slice) == 0)
        {
            auto value_slice = it->value();
            const UInt64 row_number = decodeRowNumberBE(value_slice.data(), value_slice.size());
            /// `row_number` is used directly as `_part_offset`; an out-of-range
            /// offset (corrupt/incompatible SST) would read past the part, so fail
            /// closed. `part` is null only in isolation unit tests.
            /// TODO(unique-key): the production probe-target factory must pass a
            /// non-null part so this bound is enforced once INSERT probe is wired.
            if (part && row_number >= part->rows_count)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "UNIQUE KEY SST points at row {} but part '{}' has only {} rows",
                    row_number, part->name, part->rows_count);
            out[i] = row_number;
        }
        else if (!it->status().ok())
        {
            /// A genuine miss leaves the iterator OK; a non-OK status is an SST read
            /// error and must not be reported as "not found".
            throw Exception(ErrorCodes::ROCKSDB_ERROR,
                "SSTProbeTargetPart: error seeking UNIQUE KEY SST: {}", it->status().ToString());
        }
    }
}

bool SSTProbeTargetPart::isRowDead(UInt64 row_number) const
{
    return pinned_bitmap && pinned_bitmap->contains(row_number);
}

}

#endif
