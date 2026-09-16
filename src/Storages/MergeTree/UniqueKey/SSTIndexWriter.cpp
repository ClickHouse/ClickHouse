#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>

#include <Columns/IColumn.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/sortBlock.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>

namespace ProfileEvents
{
    extern const Event UniqueKeySSTWriteMicroseconds;
}

#include "config.h"

#if USE_ROCKSDB
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#endif

#include <cstring>
#include <limits>
#include <string>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LIMIT_EXCEEDED;
}

const char * const SSTIndexWriter::FILE_NAME = "unique_key_index.sst";

#if USE_ROCKSDB

namespace
{

void encodeRowNumberBE(UInt32 row_number, char out[4])
{
    out[0] = static_cast<char>((row_number >> 24) & 0xFF);
    out[1] = static_cast<char>((row_number >> 16) & 0xFF);
    out[2] = static_cast<char>((row_number >> 8) & 0xFF);
    out[3] = static_cast<char>(row_number & 0xFF);
}

rocksdb::Options makeSSTOptions(rocksdb::Env * env)
{
    /// LZ4 compression; 32 KiB block size; embedded bloom filter (~1% FPR).
    rocksdb::Options options;
    options.env = env;
    options.compression = rocksdb::kLZ4Compression;
    rocksdb::BlockBasedTableOptions block_based;
    block_based.filter_policy.reset(rocksdb::NewBloomFilterPolicy(SSTIndexWriter::BLOOM_BITS_PER_KEY));
    block_based.block_size = 32 * 1024;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based));
    return options;
}

LoggerPtr getWriterLogger()
{
    return getLogger("SSTIndexWriter");
}

/// RocksDB SST needs strictly-ascending keys; when UK is a non-Nullable
/// ascending prefix of ORDER BY the block is already sorted by UK, so the
/// sorted writer can take it as-is and skip the re-sort.
bool isBlockSortedByUniqueKey(
    const Names & uk_names, const Names & sort_names,
    const std::vector<bool> & sort_reverse_flags, const Block & block)
{
    if (uk_names.size() > sort_names.size())
        return false;
    for (size_t i = 0; i < uk_names.size(); ++i)
        if (uk_names[i] != sort_names[i])
            return false;
    /// A descending ORDER BY column on the UK prefix would feed RocksDB
    /// decreasing keys, so the block is not sorted by UK in that case.
    for (size_t i = 0; i < uk_names.size(); ++i)
        if (i < sort_reverse_flags.size() && sort_reverse_flags[i])
            return false;
    for (const auto & name : uk_names)
        if (block.getByName(name).type->isNullable())
            return false;
    return true;
}

class WriteBufferWritableFile : public rocksdb::FSWritableFile
{
public:
    explicit WriteBufferWritableFile(WriteBuffer & write_buffer_)
        : write_buffer(write_buffer_)
        , file_size(0)
    {
    }

    rocksdb::IOStatus Append(
        const rocksdb::Slice & data,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override
    {
        try
        {
            write_buffer.write(data.data(), data.size());
            file_size += data.size();
            return rocksdb::IOStatus::OK();
        }
        catch (...)
        {
            auto error_msg = getCurrentExceptionMessage(true);
            return rocksdb::IOStatus::IOError("Failed to write data: " + error_msg);
        }
    }

    rocksdb::IOStatus Close(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Flush(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    rocksdb::IOStatus Sync(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return rocksdb::IOStatus::OK();
    }

    uint64_t GetFileSize(const rocksdb::IOOptions &, rocksdb::IODebugContext *) override
    {
        return file_size;
    }

private:
    WriteBuffer & write_buffer;
    uint64_t file_size;
};

/// A minimal RocksDB `FileSystem` that only knows how to hand out a
/// single writable file backed by a ClickHouse `WriteBuffer`. Everything
/// else is `NotSupported` — `SstFileWriter` needs nothing more.
class WriteBufferFileSystem : public rocksdb::FileSystem
{
public:
    explicit WriteBufferFileSystem(WriteBuffer * write_buffer_)
        : write_buffer(write_buffer_)
    {
    }

    const char * Name() const override { return "UniqueKeySSTFileSystem"; }

    rocksdb::IOStatus NewWritableFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSWritableFile> * r,
        rocksdb::IODebugContext *) override
    {
        if (!write_buffer)
            return rocksdb::IOStatus::InvalidArgument("WriteBuffer not set");
        *r = std::make_unique<WriteBufferWritableFile>(*write_buffer);
        return rocksdb::IOStatus::OK();
    }

    /// Unsupported methods — SstFileWriter never calls them.
    rocksdb::IOStatus NewSequentialFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSSequentialFile> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus NewRandomAccessFile(
        const std::string &,
        const rocksdb::FileOptions &,
        std::unique_ptr<rocksdb::FSRandomAccessFile> *,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus FileExists(
        const std::string &,
        const rocksdb::IOOptions &,
        rocksdb::IODebugContext *) override { return rocksdb::IOStatus::NotSupported(); }
    rocksdb::IOStatus GetFileSize(
        const std::string &,
        const rocksdb::IOOptions &,
        uint64_t *,
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
    WriteBuffer * write_buffer = nullptr;
};

}

struct SSTIndexWriter::Impl
{
    WriteSettings write_settings;
    /// Target file in the part storage. Opened lazily on the first
    /// `addEncoded` so an empty input produces no `.sst` file.
    std::unique_ptr<WriteBufferFromFileBase> out_file;
    /// Custom RocksDB env that redirects SST writes into `out_file`.
    std::unique_ptr<rocksdb::Env> sst_env;
    std::unique_ptr<rocksdb::SstFileWriter> writer;
    bool opened = false;
    Stopwatch lifetime_watch;
    /// Previous key fed to `addEncoded`, for the adjacent-duplicate check
    /// (callers feed keys in ascending encoded order, so equal keys are adjacent).
    std::string last_key;
    UInt32 last_row_number = 0;
};

#else // !USE_ROCKSDB

struct SSTIndexWriter::Impl
{
};

#endif


SSTIndexWriter::SSTIndexWriter(IDataPartStorage & part_storage_, ContextPtr context)
    : impl(std::make_unique<Impl>())
    , part_storage(part_storage_)
{
#if USE_ROCKSDB
    impl->write_settings = context->getWriteSettings();
#else
    (void)part_storage; (void)context;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter requires RocksDB support (USE_ROCKSDB=1)");
#endif
}

#if USE_ROCKSDB
void SSTIndexWriter::openOutputStreamOnFirstEntry()
{
    if (impl->opened)
        return;

    part_storage.createDirectories();
    impl->out_file = part_storage.writeFile(FILE_NAME, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, impl->write_settings);

    impl->sst_env = rocksdb::NewCompositeEnv(std::make_shared<WriteBufferFileSystem>(impl->out_file.get()));
    impl->writer = std::make_unique<rocksdb::SstFileWriter>(rocksdb::EnvOptions{}, makeSSTOptions(impl->sst_env.get()));

    /// Empty path: the custom filesystem ignores the name and always hands
    /// back the `WriteBuffer`-backed writable file.
    auto status = impl->writer->Open("");
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
            "SSTIndexWriter: failed to open SST writer for {}: {}",
            FILE_NAME, status.ToString());

    impl->opened = true;
    LOG_DEBUG(getWriterLogger(), "Opened SST writer streaming into part file {}", FILE_NAME);
}
#endif

void SSTIndexWriter::finish()
{
#if USE_ROCKSDB
    if (!impl || !impl->opened)
        return;
    auto status = impl->writer->Finish();
    impl->opened = false;
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
            "SSTIndexWriter::finish: SstFileWriter::Finish failed for {}: {}",
            FILE_NAME, status.ToString());
#endif
}

SSTIndexWriter::~SSTIndexWriter()
{
#if USE_ROCKSDB
    /// Not finalized → abandoned on some error path; cancel the live
    /// `WriteBuffer` so an object-storage writer aborts its multipart
    /// upload instead of leaking it. On local disk `cancel()` is a no-op
    /// and the file created by `writeFile` remains, so remove it too.
    if (impl && impl->out_file && !finalized)
    {
        impl->out_file->cancel();
        try
        {
            part_storage.removeFileIfExists(FILE_NAME);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("SSTIndexWriter"), "Failed to remove abandoned SST index file");
        }
    }
#endif
}

void SSTIndexWriter::addEncoded(const std::string_view & encoded_key, UInt32 row_number)
{
#if USE_ROCKSDB
    if (finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SSTIndexWriter::addEncoded called on finalized writer");

    openOutputStreamOnFirstEntry();

    /// Detect a duplicate UNIQUE KEY within the block here, before RocksDB
    /// rejects the non-increasing `Put` with a raw low-level error. Interim
    /// fail-closed stance: no INSERT-time dedup yet, so a clear defined error.
    if (entries_added > 0 && encoded_key == impl->last_key)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "INSERT block contains duplicate UNIQUE KEY values (part rows {} and {}). "
            "INSERT-time UNIQUE KEY deduplication is not yet implemented; "
            "deduplicate the block before inserting.",
            impl->last_row_number, row_number);

    char value_buf[4];
    encodeRowNumberBE(row_number, value_buf);

    auto status = impl->writer->Put(
        rocksdb::Slice(encoded_key.data(), encoded_key.size()),
        rocksdb::Slice(value_buf, sizeof(value_buf)));

    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
            "SSTIndexWriter::addEncoded failed (row_number={}): {}",
            row_number, status.ToString());

    impl->last_key.assign(encoded_key.data(), encoded_key.size());
    impl->last_row_number = row_number;
    ++entries_added;
#else
    (void)encoded_key; (void)row_number;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter requires RocksDB support (USE_ROCKSDB=1)");
#endif
}

UInt64 SSTIndexWriter::finalizeToStorage()
{
#if USE_ROCKSDB
    if (finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SSTIndexWriter::finalizeToStorage called twice");

    /// Emitted once per writer, on every exit path (success or throw).
    SCOPE_EXIT({
        ProfileEvents::increment(ProfileEvents::UniqueKeySSTWriteMicroseconds,
            impl->lifetime_watch.elapsedMicroseconds());
    });

    /// Empty input: the output stream was never opened, so no `.sst` is produced.
    if (!impl->out_file)
    {
        finalized = true;
        LOG_DEBUG(getWriterLogger(), "Finalized empty SST (no .sst produced)");
        return 0;
    }

    /// Close the RocksDB writer first — this flushes the SST footer through
    /// our `WriteBuffer`-backed file. Then finalize the outer `WriteBuffer`
    /// so the part storage commits the bytes.
    finish();
    impl->out_file->finalize();

    finalized = true;
    LOG_DEBUG(getWriterLogger(), "Finalized SST {}: {} entries", FILE_NAME, entries_added);
    return entries_added;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter requires RocksDB support (USE_ROCKSDB=1)");
#endif
}


UInt64 SSTIndexWriter::write(
    IDataPartStorage & part_storage,
    const Block & block,
    const Names & uk_names,
    const Names & sort_names,
    const std::vector<bool> & sort_reverse_flags,
    const IColumn::Permutation * permutation,
    UInt64 max_encoded_size,
    ContextPtr context)
{
    if (uk_names.empty())
        return 0;

#if USE_ROCKSDB
    if (isBlockSortedByUniqueKey(uk_names, sort_names, sort_reverse_flags, block))
        return writeFromBlock(part_storage, block, uk_names, permutation, max_encoded_size, context);
    return writeFromBlockUnsorted(part_storage, block, uk_names, permutation, max_encoded_size, context);
#else
    (void)sort_names; (void)sort_reverse_flags;
    return writeFromBlock(part_storage, block, uk_names, permutation, max_encoded_size, context);
#endif
}


UInt64 SSTIndexWriter::writeFromBlock(
    IDataPartStorage & part_storage,
    const Block & block,
    const Names & unique_key_column_names,
    const IColumn::Permutation * permutation,
    size_t max_encoded_size,
    ContextPtr context)
{
#if USE_ROCKSDB
    if (unique_key_column_names.empty())
        return 0;

    /// Reject malformed blocks before the empty-block fast path —
    /// `block.rows()` reports only the first column's size.
    block.checkNumberOfRows();
    if (block.rows() == 0)
        return 0;

    Columns uk_columns;
    uk_columns.reserve(unique_key_column_names.size());
    for (const auto & name : unique_key_column_names)
        uk_columns.push_back(block.getByName(name).column);

    const size_t num_rows = block.rows();
    if (permutation && permutation->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SSTIndexWriter::writeFromBlock: permutation size {} != block rows {}",
            permutation->size(), num_rows);
    if (num_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "SSTIndexWriter::writeFromBlock: part has {} rows, exceeds UInt32 row-number capacity",
            num_rows);

    VectorWithMemoryTracking<String> encoded;
    UniqueKeyEncoding::encodeBlock(uk_columns, permutation, max_encoded_size, encoded);

    SSTIndexWriter writer(part_storage, context);
    for (size_t i = 0; i < num_rows; ++i)
        writer.addEncoded(encoded[i], static_cast<UInt32>(i));
    return writer.finalizeToStorage();
#else
    (void)part_storage; (void)block; (void)unique_key_column_names;
    (void)permutation; (void)max_encoded_size; (void)context;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter::writeFromBlock requires RocksDB support (USE_ROCKSDB=1)");
#endif
}


UInt64 SSTIndexWriter::writeFromBlockUnsorted(
    IDataPartStorage & part_storage,
    const Block & block,
    const Names & unique_key_column_names,
    const IColumn::Permutation * permutation,
    size_t max_encoded_size,
    ContextPtr context)
{
#if USE_ROCKSDB
    if (unique_key_column_names.empty())
        return 0;

    /// See writeFromBlock — same ordering rationale.
    block.checkNumberOfRows();
    if (block.rows() == 0)
        return 0;

    const size_t num_rows = block.rows();
    if (permutation && permutation->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SSTIndexWriter::writeFromBlockUnsorted: permutation size {} != block rows {}",
            permutation->size(), num_rows);
    if (num_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "SSTIndexWriter::writeFromBlockUnsorted: part has {} rows, exceeds UInt32 row-number capacity",
            num_rows);

    Columns uk_columns;
    uk_columns.reserve(unique_key_column_names.size());
    for (const auto & name : unique_key_column_names)
        uk_columns.push_back(block.getByName(name).column);

    /// nulls_direction=1 matches the encoder: NULL flag 0x01 sorts after
    /// non-NULL 0x00, and Float NaN-as-0xFF sorts after non-NaN.
    SortDescription uk_sort_desc;
    uk_sort_desc.reserve(unique_key_column_names.size());
    for (const auto & name : unique_key_column_names)
        uk_sort_desc.emplace_back(name, /*direction=*/1, /*nulls_direction=*/1);

    IColumn::Permutation uk_perm;
    stableGetPermutation(block, uk_sort_desc, uk_perm);
    /// `stableGetPermutation` leaves `uk_perm` empty when every sort
    /// column is `ColumnConst` (rows are already "sorted" by definition).
    /// `encodeBlock` would then index a zero-length permutation; expand
    /// to identity so downstream code stays on the same shape.
    if (uk_perm.empty())
    {
        uk_perm.resize(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
            uk_perm[i] = i;
    }

    VectorWithMemoryTracking<String> encoded;
    UniqueKeyEncoding::encodeBlock(uk_columns, &uk_perm, max_encoded_size, encoded);

    /// Caller's `permutation` maps part_offset → source_row; invert once
    /// so we can look up part_offset by source row.
    std::vector<UInt32> source_to_part_offset(num_rows);
    if (permutation)
        for (size_t i = 0; i < num_rows; ++i)
            source_to_part_offset[(*permutation)[i]] = static_cast<UInt32>(i);
    else
        for (size_t i = 0; i < num_rows; ++i)
            source_to_part_offset[i] = static_cast<UInt32>(i);

    SSTIndexWriter writer(part_storage, context);
    for (size_t i = 0; i < num_rows; ++i)
        writer.addEncoded(encoded[i], source_to_part_offset[uk_perm[i]]);
    return writer.finalizeToStorage();
#else
    (void)part_storage; (void)block; (void)unique_key_column_names;
    (void)permutation; (void)max_encoded_size; (void)context;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter::writeFromBlockUnsorted requires RocksDB support (USE_ROCKSDB=1)");
#endif
}

}
