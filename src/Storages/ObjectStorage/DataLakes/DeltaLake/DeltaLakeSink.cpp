#include <Core/Settings.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DeltaLakeSink.h>

#if USE_DELTA_KERNEL_RS
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/WriteTransaction.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Interpreters/castColumn.h>


namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_rows_in_data_file;
    extern const SettingsNonZeroUInt64 delta_lake_insert_max_bytes_in_data_file;
}

namespace FailPoints
{
    extern const char delta_lake_write_commit_pause[];
}

namespace
{

/// `sample` with each column's type replaced by the Delta write-schema type (matched by name), so the
/// data files are written to match the Delta log (e.g. a declared `UInt8` column is stored as `short`).
SharedHeader makeWriteHeader(const Block & sample, const DB::NamesAndTypesList & write_schema)
{
    Block header = sample;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        auto & col = header.getByPosition(i);
        auto schema_col = write_schema.tryGetByName(col.name);
        if (schema_col && !schema_col->type->equals(*col.type))
        {
            col.type = schema_col->type;
            col.column = col.type->createColumn();
        }
    }
    return std::make_shared<const Block>(std::move(header));
}

/// Cast each column of `chunk` (typed by `in_header`) to the corresponding `out_header` type.
Chunk castChunkToWriteSchema(const Chunk & chunk, const Block & in_header, const Block & out_header)
{
    const auto & in_columns = chunk.getColumns();
    Columns out_columns;
    out_columns.reserve(in_columns.size());
    for (size_t i = 0; i < in_columns.size(); ++i)
    {
        const auto & from = in_header.getByPosition(i);
        const auto & to_type = out_header.getByPosition(i).type;
        if (from.type->equals(*to_type))
            out_columns.push_back(in_columns[i]);
        else
            out_columns.push_back(castColumn({in_columns[i], from.type, from.name}, to_type));
    }
    return Chunk(std::move(out_columns), chunk.getNumRows());
}

}

DeltaLakeSink::DeltaLakeSink(
    DeltaLake::WriteTransactionPtr delta_transaction_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    SharedHeader sample_block_,
    const std::optional<FormatSettings> & format_settings_,
    const String & format,
    const String & compression_method)
    : SinkToStorage(sample_block_)
    , WithContext(context_)
    , delta_transaction(delta_transaction_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , write_header(makeWriteHeader(*sample_block_, delta_transaction_->getWriteSchema()))
    , data_file_max_rows(context_->getSettingsRef()[Setting::delta_lake_insert_max_rows_in_data_file])
    , data_file_max_bytes(context_->getSettingsRef()[Setting::delta_lake_insert_max_bytes_in_data_file])
    , write_format(format)
    , write_compression_method(compression_method)
{
    delta_transaction->validateSchema(getHeader());
}

DeltaLakeSink::~DeltaLakeSink()
{
    if (isCancelled())
        cancelBuffers();
}

void DeltaLakeSink::cancelBuffers()
{
    /// The inner sinks are plain members, not pipeline processors, so the
    /// pipeline-wide cancel does not reach them. Cancel each one explicitly:
    /// this flips its isCancelled(), so its destructor finalizes/cancels its
    /// WriteBuffer instead of tripping the "neither finalized nor canceled" assert.
    /// WriteBuffer::cancel does not unlink an already written data file, so also
    /// remove each uncommitted object (mirrors the commit-failure cleanup in
    /// onFinish); otherwise a failed insert leaves an orphan parquet file behind.
    for (auto & data_file : data_files)
    {
        data_file.sink->cancel();
        try
        {
            object_storage->removeObjectIfExists(StoredObject(data_file.sink->getPath()));
        }
        catch (...)
        {
            tryLogCurrentException("DeltaLakeSink", "Failed to remove uncommitted data file on cancel");
        }
    }
}

void DeltaLakeSink::onException(std::exception_ptr)
{
    cancelBuffers();
}

DeltaLakeSink::StorageSinkPtr DeltaLakeSink::createStorageSink() const
{
    return std::make_unique<StorageObjectStorageSink>(
        DeltaLake::generateWritePath(delta_transaction->getDataPath(), write_format),
        object_storage,
        format_settings,
        write_header,
        getContext(),
        write_format,
        write_compression_method);
}

void DeltaLakeSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    /// Cast to the Delta write schema so the data files match the Delta log (e.g. `UInt8` -> `short`).
    Chunk write_chunk = castChunkToWriteSchema(chunk, *sample_block, *write_header);

    if (data_files.empty()
        || data_files.back().written_bytes >= data_file_max_bytes
        || data_files.back().written_rows >= data_file_max_rows)
    {
        data_files.emplace_back(createStorageSink());
    }

    auto & data_file = data_files.back();
    data_file.written_bytes += write_chunk.bytes();
    data_file.written_rows += write_chunk.getNumRows();
    data_file.sink->consume(write_chunk);
}

void DeltaLakeSink::onFinish()
{
    if (isCancelled())
        return;

    std::vector<DeltaLake::WriteTransaction::CommitFile> files;
    files.reserve(data_files.size());
    for (const auto & [sink, written_bytes, written_rows] : data_files)
    {
        sink->onFinish();
        auto file_location = sink->getPath().substr(delta_transaction->getDataPath().size());
        /// We use file size from sink to count all file, not just actual data.
        auto file_size = sink->getFileSize();
        files.emplace_back(std::move(file_location), file_size, written_rows, Map{});
    }

    /// Test-only hook: pause inside the commit window (after the data files are
    /// finalized, before commit) so a test can inject an external cancel and
    /// check that a late cancel does not delete committed files.
    FailPointInjection::pauseFailPoint(FailPoints::delta_lake_write_commit_pause);

    try
    {
        delta_transaction->commit(files);
    }
    catch (...)
    {
        for (const auto & [sink, written_bytes, written_rows] : data_files)
        {
            /// FIXME: this should be just removeObject,
            /// but IObjectStorage does not have such method.
           object_storage->removeObjectIfExists(StoredObject(sink->getPath()));

        }
        throw;
    }

    /// The commit succeeded: the data files are now referenced by the Delta log.
    /// Drop the tracked sinks so a cancel that arrives after this point (the
    /// pipeline executor flips isCancelled() asynchronously, so it can race with
    /// this commit) does not make ~DeltaLakeSink -> cancelBuffers() unlink the
    /// just-committed files and leave the Delta log pointing at missing data.
    data_files.clear();
}

}

#endif
