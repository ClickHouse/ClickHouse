#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>

#include <Columns/IColumn.h>
#include <Core/SortDescription.h>
#include <Interpreters/sortBlock.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event UniqueKeySSTWriteMicroseconds;
}

#include "config.h"

#if USE_ROCKSDB
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#endif

#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int LIMIT_EXCEEDED;
}

const char * const SSTIndexWriter::FILE_NAME = "unique_key_index.sst";
const char * const SSTIndexWriter::TMP_FILE_NAME = "unique_key_index.sst.tmp";

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

rocksdb::Options makeSSTOptions()
{
    /// LZ4 compression; 32 KiB block size; embedded bloom filter (~1% FPR).
    rocksdb::Options options;
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

}

struct SSTIndexWriter::Impl
{
    rocksdb::SstFileWriter writer;
    std::string tmp_full_path;
    bool opened = false;
    Stopwatch lifetime_watch;

    Impl()
        : writer(rocksdb::EnvOptions{}, makeSSTOptions())
    {
    }
};

#else // !USE_ROCKSDB

struct SSTIndexWriter::Impl
{
};

#endif


SSTIndexWriter::SSTIndexWriter(IDataPartStorage & part_storage_)
    : impl(std::make_unique<Impl>())
    , part_storage(part_storage_)
{
#if USE_ROCKSDB
    /// Merge path creates the part directory lazily — make sure it exists
    /// before SstFileWriter::Open hits ENOENT.
    part_storage.createDirectories();
    part_storage.removeFileIfExists(TMP_FILE_NAME);

    impl->tmp_full_path = (std::filesystem::path(part_storage.getFullPath()) / TMP_FILE_NAME).string();
    auto status = impl->writer.Open(impl->tmp_full_path);
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
            "SSTIndexWriter: failed to open SST tmp file '{}': {}",
            impl->tmp_full_path, status.ToString());
    impl->opened = true;
    LOG_DEBUG(getWriterLogger(), "Opened SST tmp file {}", impl->tmp_full_path);
#else
    (void)part_storage;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter requires RocksDB support (USE_ROCKSDB=1)");
#endif
}

void SSTIndexWriter::finish()
{
#if USE_ROCKSDB
    if (!impl || !impl->opened)
        return;
    rocksdb::ExternalSstFileInfo info;
    auto status = impl->writer.Finish(&info);
    impl->opened = false;
    if (status.ok())
        return;
    /// Zero-`Put` → RocksDB returns InvalidArgument; benign cleanup.
    /// After any successful Put, the same code is a real Finish failure.
    if (status.IsInvalidArgument() && entries_added == 0)
        return;
    throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
        "SSTIndexWriter::finish: SstFileWriter::Finish failed at {}: {}",
        impl->tmp_full_path, status.ToString());
#endif
}

SSTIndexWriter::~SSTIndexWriter()
{
#if USE_ROCKSDB
    /// Covers all non-finalized exits, including manual `finish()` without
    /// the matching `finalizeToStorage` rename and `replaceFile` throwing
    /// mid-`finalizeToStorage`.
    if (impl && !finalized)
    {
        try { part_storage.removeFileIfExists(TMP_FILE_NAME); }
        catch (...) { tryLogCurrentException(__PRETTY_FUNCTION__); }
    }
#endif
}

void SSTIndexWriter::addEncoded(const std::string_view & encoded_key, UInt32 row_number)
{
#if USE_ROCKSDB
    if (!impl->opened)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SSTIndexWriter::addEncoded called on closed writer");

    char value_buf[4];
    encodeRowNumberBE(row_number, value_buf);

    auto status = impl->writer.Put(
        rocksdb::Slice(encoded_key.data(), encoded_key.size()),
        rocksdb::Slice(value_buf, sizeof(value_buf)));

    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
            "SSTIndexWriter::addEncoded failed (row_number={}): {}",
            row_number, status.ToString());

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
    if (!impl->opened)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SSTIndexWriter::finalizeToStorage on closed writer");

    try
    {
        finish();
    }
    catch (...)
    {
        part_storage.removeFileIfExists(TMP_FILE_NAME);
        ProfileEvents::increment(ProfileEvents::UniqueKeySSTWriteMicroseconds,
            impl->lifetime_watch.elapsedMicroseconds());
        throw;
    }

    if (entries_added == 0)
    {
        finalized = true;
        part_storage.removeFileIfExists(TMP_FILE_NAME);
        ProfileEvents::increment(ProfileEvents::UniqueKeySSTWriteMicroseconds,
            impl->lifetime_watch.elapsedMicroseconds());
        LOG_DEBUG(getWriterLogger(), "Finalized empty SST (no .sst produced) at {}", impl->tmp_full_path);
        return 0;
    }

    part_storage.replaceFile(TMP_FILE_NAME, FILE_NAME);
    ProfileEvents::increment(ProfileEvents::UniqueKeySSTWriteMicroseconds,
        impl->lifetime_watch.elapsedMicroseconds());

    finalized = true;
    LOG_DEBUG(getWriterLogger(), "Finalized SST {}: {} entries", FILE_NAME, entries_added);
    return entries_added;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter requires RocksDB support (USE_ROCKSDB=1)");
#endif
}


UInt64 SSTIndexWriter::writeFromBlock(
    IDataPartStorage & part_storage,
    const Block & block,
    const Names & unique_key_column_names,
    const IColumn::Permutation * permutation,
    size_t max_encoded_size)
{
#if USE_ROCKSDB
    if (unique_key_column_names.empty())
        return 0;

    /// Reject malformed blocks before the empty-block fast path —
    /// `block.rows()` reports only the first column's size.
    block.checkNumberOfRows();
    if (block.rows() == 0)
        return 0;

    /// ColumnConst lies about dynamic type; materialize before encoder dispatch.
    Columns uk_columns;
    uk_columns.reserve(unique_key_column_names.size());
    for (const auto & name : unique_key_column_names)
        uk_columns.push_back(block.getByName(name).column->convertToFullColumnIfConst());

    const size_t num_rows = block.rows();
    if (permutation && permutation->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SSTIndexWriter::writeFromBlock: permutation size {} != block rows {}",
            permutation->size(), num_rows);
    if (num_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "SSTIndexWriter::writeFromBlock: part has {} rows, exceeds UInt32 row-number capacity",
            num_rows);

    std::vector<String> encoded;
    UniqueKeyEncoding::encodeBlock(uk_columns, permutation, max_encoded_size, encoded);

    SSTIndexWriter writer(part_storage);
    for (size_t i = 0; i < num_rows; ++i)
        writer.addEncoded(encoded[i], static_cast<UInt32>(i));
    return writer.finalizeToStorage();
#else
    (void)part_storage; (void)block; (void)unique_key_column_names; (void)permutation; (void)max_encoded_size;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter::writeFromBlock requires RocksDB support (USE_ROCKSDB=1)");
#endif
}


UInt64 SSTIndexWriter::writeFromBlockUnsorted(
    IDataPartStorage & part_storage,
    const Block & block,
    const Names & unique_key_column_names,
    const IColumn::Permutation * permutation,
    size_t max_encoded_size)
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
        uk_columns.push_back(block.getByName(name).column->convertToFullColumnIfConst());

    /// nulls_direction=1 matches the encoder: NULL flag 0x01 sorts after
    /// non-NULL 0x00, and Float NaN-as-0xFF sorts after non-NaN.
    SortDescription uk_sort_desc;
    uk_sort_desc.reserve(unique_key_column_names.size());
    for (const auto & name : unique_key_column_names)
        uk_sort_desc.emplace_back(name, /*direction=*/1, /*nulls_direction=*/1);

    IColumn::Permutation uk_perm;
    stableGetPermutation(block, uk_sort_desc, uk_perm);

    std::vector<String> encoded;
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

    SSTIndexWriter writer(part_storage);
    for (size_t i = 0; i < num_rows; ++i)
        writer.addEncoded(encoded[i], source_to_part_offset[uk_perm[i]]);
    return writer.finalizeToStorage();
#else
    (void)part_storage; (void)block; (void)unique_key_column_names;
    (void)permutation; (void)max_encoded_size;
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "SSTIndexWriter::writeFromBlockUnsorted requires RocksDB support (USE_ROCKSDB=1)");
#endif
}

}
