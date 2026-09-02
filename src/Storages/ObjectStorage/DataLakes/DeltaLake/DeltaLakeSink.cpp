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
        sample_block,
        getContext(),
        write_format,
        write_compression_method);
}

void DeltaLakeSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    if (data_files.empty()
        || data_files.back().written_bytes >= data_file_max_bytes
        || data_files.back().written_rows >= data_file_max_rows)
    {
        data_files.emplace_back(createStorageSink());
    }

    auto & data_file = data_files.back();
    data_file.written_bytes += chunk.bytes();
    data_file.written_rows += chunk.getNumRows();
    data_file.sink->consume(chunk);
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
