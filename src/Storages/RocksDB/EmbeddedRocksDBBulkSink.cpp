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
#include <rocksdb/slice.h>
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

/// Build SST file from key-value pairs
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
    for (size_t idx = 0; idx < rows;)
    {
        /// We will write the last row of the same key
        size_t next_idx = idx + 1;
        while (next_idx < rows && keys.compareAt(perm[idx], perm[next_idx], keys, 1) == 0)
            ++next_idx;

        auto row = perm[next_idx - 1];
        status = sst_file_writer.Put(keys.getDataAt(row).toView(), values.getDataAt(row).toView());
        if (!status.ok())
            return status;

        idx = next_idx;
    }

    return sst_file_writer.Finish();
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
    min_block_size_rows = std::max(storage.getSettings().bulk_insert_block_size, getContext()->getSettingsRef().min_insert_block_size_rows);

    /// If max_insert_threads > 1 we may have multiple EmbeddedRocksDBBulkSink and getContext()->getCurrentQueryId() is not guarantee to
    /// to have a distinct path. Also we cannot use query id as directory name here, because it could be defined by user and not suitable
    /// for directory name
    auto base_directory_name = TMP_INSERT_PREFIX + sipHash128String(getContext()->getCurrentQueryId());
    insert_directory_queue = fs::path(storage.getDataPaths()[0]) / (base_directory_name + "-" + getRandomASCIIString(8));
    fs::create_directory(insert_directory_queue);
}

EmbeddedRocksDBBulkSink::~EmbeddedRocksDBBulkSink()
{
    try
    {
        if (fs::exists(insert_directory_queue))
            fs::remove_all(insert_directory_queue);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Error while removing temporary directory {}:", insert_directory_queue));
    }
}

std::vector<Chunk> EmbeddedRocksDBBulkSink::squash(Chunk chunk)
{
    /// End of input stream
    if (chunk.getNumRows() == 0)
    {
        return std::move(chunks);
    }

    /// Just read block is already enough.
    if (isEnoughSize(chunk))
    {
        /// If no accumulated data, return just read block.
        if (chunks.empty())
        {
            chunks.emplace_back(std::move(chunk));
            return {};
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        std::vector<Chunk> to_return;
        std::swap(to_return, chunks);
        chunks.emplace_back(std::move(chunk));
        return to_return;
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(chunks))
    {
        /// Return accumulated data and place new block to accumulated data.
        std::vector<Chunk> to_return;
        std::swap(to_return, chunks);
        chunks.emplace_back(std::move(chunk));
        return to_return;
    }

    chunks.emplace_back(std::move(chunk));
    if (isEnoughSize(chunks))
    {
        std::vector<Chunk> to_return;
        std::swap(to_return, chunks);
        return to_return;
    }

    /// Squashed block is not ready.
    return {};
}

std::pair<ColumnString::Ptr, ColumnString::Ptr> EmbeddedRocksDBBulkSink::serializeChunks(const std::vector<Chunk> & input_chunks) const
{
    auto serialized_key_column = ColumnString::create();
    auto serialized_value_column = ColumnString::create();

    {
        auto & serialized_key_data = serialized_key_column->getChars();
        auto & serialized_key_offsets = serialized_key_column->getOffsets();
        auto & serialized_value_data = serialized_value_column->getChars();
        auto & serialized_value_offsets = serialized_value_column->getOffsets();
        WriteBufferFromVector<ColumnString::Chars> writer_key(serialized_key_data);
        WriteBufferFromVector<ColumnString::Chars> writer_value(serialized_value_data);

        for (const auto & chunk : input_chunks)
        {
            const auto & columns = chunk.getColumns();
            auto rows = chunk.getNumRows();
            for (size_t i = 0; i < rows; ++i)
            {
                for (size_t idx = 0; idx < columns.size(); ++idx)
                    serializations[idx]->serializeBinary(*columns[idx], i, idx == primary_key_pos ? writer_key : writer_value, {});
                /// String in ColumnString must be null-terminated
                writeChar('\0', writer_key);
                writeChar('\0', writer_value);
                serialized_key_offsets.emplace_back(writer_key.count());
                serialized_value_offsets.emplace_back(writer_value.count());
            }
        }

        writer_key.finalize();
        writer_value.finalize();
    }

    return {std::move(serialized_key_column), std::move(serialized_value_column)};
}

void EmbeddedRocksDBBulkSink::consume(Chunk chunk_)
{
    std::vector<Chunk> to_written = squash(std::move(chunk_));

    if (to_written.empty())
        return;

    auto [serialized_key_column, serialized_value_column] = serializeChunks(to_written);
    auto sst_file_path = getTemporarySSTFilePath();
    if (auto status = buildSSTFile(sst_file_path, *serialized_key_column, *serialized_value_column); !status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());

    /// Ingest the SST file
    rocksdb::IngestExternalFileOptions ingest_options;
    ingest_options.move_files = true; /// The temporary file is on the same disk, so move (or hardlink) file will be faster than copy
    if (auto status = storage.rocksdb_ptr->IngestExternalFile({sst_file_path}, ingest_options); !status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());

    if (fs::exists(sst_file_path))
        fs::remove(sst_file_path);
}

void EmbeddedRocksDBBulkSink::onFinish()
{
    /// If there is any data left, write it.
    if (!chunks.empty())
        consume({});
}

String EmbeddedRocksDBBulkSink::getTemporarySSTFilePath()
{
    return fs::path(insert_directory_queue) / (toString(file_counter++) + ".sst");
}

bool EmbeddedRocksDBBulkSink::isEnoughSize(const std::vector<Chunk> & input_chunks) const
{
    size_t total_rows = 0;
    for (const auto & chunk : input_chunks)
        total_rows += chunk.getNumRows();
    return total_rows >= min_block_size_rows;
}

bool EmbeddedRocksDBBulkSink::isEnoughSize(const Chunk & chunk) const
{
    return chunk.getNumRows() >= min_block_size_rows;
}
}
