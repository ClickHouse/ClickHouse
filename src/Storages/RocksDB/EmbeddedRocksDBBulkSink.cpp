#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <stdatomic.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/RocksDB/EmbeddedRocksDBBulkSink.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>

#include <Columns/ColumnString.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/db_ttl.h>
#include <Common/SipHash.h>
#include <Common/getRandomASCIIString.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ROCKSDB_ERROR;
}

static const IColumn::Permutation & getAscendingPermutation(const IColumn & column, IColumn::Permutation & perm)
{
    column.getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Stable, 0, 1, perm);
    return perm;
}

static rocksdb::Status buildSSTFile(const String & path, const ColumnString & keys, const ColumnString & values, const std::optional<IColumn::Permutation> & perm_ = {})
{
    /// rocksdb::SstFileWriter requires keys to be sorted in ascending order
    IColumn::Permutation calculated_perm;
    const IColumn::Permutation & perm = perm_ ? *perm_ : getAscendingPermutation(keys, calculated_perm);

    rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions{}, rocksdb::Options{});
    auto status = sst_file_writer.Open(path);
    if (!status.ok())
        return status;

    auto rows = perm.size();
    WriteBufferFromOwnString wb_value;
    for (size_t i = 0; i < rows; ++i)
    {
        auto row = perm[i];

        status = sst_file_writer.Put(keys.getDataAt(row).toView(), values.getDataAt(row).toView());

        /// There could be duplicated keys in chunk, thus Put may give IsInvalidArgument. This is ok, as we're certain that
        /// keys are sorted in ascending order.
        if (!status.ok() && !status.IsInvalidArgument())
            return status;
    }

    sst_file_writer.Finish();
    return rocksdb::Status::OK();
}

EmbeddedRocksDBBulkSink::EmbeddedRocksDBBulkSink(
    ContextPtr context_, StorageEmbeddedRocksDB & storage_, const StorageMetadataPtr & metadata_snapshot_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock()), WithContext(context_), storage(storage_), metadata_snapshot(metadata_snapshot_)
{
    for (const auto & elem : getHeader())
    {
        if (elem.name == storage.primary_key)
            break;
        ++primary_key_pos;
    }
    serializations = getHeader().getSerializations();
    /// If max_insert_threads > 1 we may have multiple EmbeddedRocksDBBulkSink and getContext()->getCurrentQueryId() is not guarantee to
    /// to have a distinct path. Also we cannot use query id as directory name here, because it could be defined by user and not suitable
    /// for directory name
    auto base_directory_name = sipHash128String(getContext()->getCurrentQueryId());
    insert_directory_queue = fs::path(storage.getDataPaths()[0]) / (base_directory_name + "-" + getRandomASCIIString(8));
    fs::create_directory(insert_directory_queue);
}

EmbeddedRocksDBBulkSink::~EmbeddedRocksDBBulkSink()
{
    if (fs::exists(insert_directory_queue))
        fs::remove_all(insert_directory_queue);
}

void EmbeddedRocksDBBulkSink::consume(Chunk chunk)
{
    auto rows = chunk.getNumRows();

    if (rows == 0) /// TODO: squashing if rows are too small
        return;

    const auto columns = chunk.detachColumns();

    /// Convert chunk to rocksdb key-value pairs
    auto serialized_key_column = ColumnString::create();
    auto serialized_value_column = ColumnString::create();

    {
        auto & serialized_key_data = serialized_key_column->getChars();
        auto & serialized_key_offsets = serialized_key_column->getOffsets();
        auto & serialized_value_data = serialized_value_column->getChars();
        auto & serialized_value_offsets = serialized_value_column->getOffsets();

        serialized_key_offsets.reserve(rows);
        serialized_value_offsets.reserve(rows);
        WriteBufferFromVector<ColumnString::Chars> writer_key(serialized_key_data);
        WriteBufferFromVector<ColumnString::Chars> writer_value(serialized_value_data);

        for (size_t i = 0; i < rows; ++i)
        {
            for (size_t idx = 0; idx < columns.size(); ++idx)
                serializations[idx]->serializeBinary(*columns[idx], i, idx == primary_key_pos ? writer_key : writer_value, {});
            writeChar('\0', writer_key);
            writeChar('\0', writer_value);
            serialized_key_offsets.emplace_back(writer_key.count());
            serialized_value_offsets.emplace_back(writer_value.count());
        }

        writer_key.finalize();
        writer_value.finalize();
    }

    /// Build SST file from key-value pairs
    auto path = getTemporarySSTFilePath();
    if (auto status = buildSSTFile(path, *serialized_key_column, *serialized_value_column); !status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());

    /// Ingest the SST file
    rocksdb::IngestExternalFileOptions ingest_options;
    ingest_options.move_files = true; /// The temporary file is on the same disk, so move (or hardlink) file will be faster than copy
    if (auto status = storage.rocksdb_ptr->IngestExternalFile({path}, rocksdb::IngestExternalFileOptions()); !status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());

    if (fs::exists(path))
        fs::remove(path);
}

String EmbeddedRocksDBBulkSink::getTemporarySSTFilePath()
{
    return fs::path(insert_directory_queue) / (toString(file_counter++) + ".sst");
}

}
