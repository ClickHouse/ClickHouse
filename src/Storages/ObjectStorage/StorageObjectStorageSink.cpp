#include "StorageObjectStorageSink.h"
#include <Formats/FormatFactory.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/isValidUTF8.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
}

StorageObjectStorageSink::StorageObjectStorageSink(
    ObjectStoragePtr object_storage,
    ConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings_,
    const Block & sample_block_,
    ContextPtr context,
    const std::string & blob_path)
    : SinkToStorage(sample_block_)
    , sample_block(sample_block_)
{
    const auto & settings = context->getSettingsRef();
    const auto path = blob_path.empty() ? configuration->getPaths().back() : blob_path;
    const auto chosen_compression_method = chooseCompressionMethod(path, configuration->compression_method);

    auto buffer = object_storage->writeObject(
        StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    write_buf = wrapWriteBufferWithCompressionMethod(
                    std::move(buffer),
                    chosen_compression_method,
                    static_cast<int>(settings.output_format_compression_level),
                    static_cast<int>(settings.output_format_compression_zstd_window_log));

    writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
        configuration->format, *write_buf, sample_block, context, format_settings_);
}

void StorageObjectStorageSink::consume(Chunk chunk)
{
    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;
    writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));
}

void StorageObjectStorageSink::onCancel()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
    cancelled = true;
}

void StorageObjectStorageSink::onException(std::exception_ptr exception)
{
    std::lock_guard lock(cancel_mutex);
    try
    {
        std::rethrow_exception(exception);
    }
    catch (...)
    {
        /// An exception context is needed to proper delete write buffers without finalization.
        release();
    }
}

void StorageObjectStorageSink::onFinish()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
}

void StorageObjectStorageSink::finalize()
{
    if (!writer)
        return;

    try
    {
        writer->finalize();
        writer->flush();
    }
    catch (...)
    {
        /// Stop ParallelFormattingOutputFormat correctly.
        release();
        throw;
    }

    write_buf->finalize();
}

void StorageObjectStorageSink::release()
{
    writer.reset();
    write_buf.reset();
}

PartitionedStorageObjectStorageSink::PartitionedStorageObjectStorageSink(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    std::optional<FormatSettings> format_settings_,
    const Block & sample_block_,
    ContextPtr context_,
    const ASTPtr & partition_by)
    : PartitionedSink(partition_by, context_, sample_block_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , query_settings(configuration_->getQuerySettings(context_))
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(context_)
{
}

SinkPtr PartitionedStorageObjectStorageSink::createSinkForPartition(const String & partition_id)
{
    auto partition_bucket = replaceWildcards(configuration->getNamespace(), partition_id);
    validateNamespace(partition_bucket);

    auto partition_key = replaceWildcards(configuration->getPath(), partition_id);
    validateKey(partition_key);

    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, query_settings, partition_key, /* sequence_number */1))
    {
        partition_key = *new_key;
    }

    return std::make_shared<StorageObjectStorageSink>(
        object_storage,
        configuration,
        format_settings,
        sample_block,
        context,
        partition_key
    );
}

void PartitionedStorageObjectStorageSink::validateKey(const String & str)
{
    /// See:
    /// - https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    /// - https://cloud.ibm.com/apidocs/cos/cos-compatibility#putobject

    if (str.empty() || str.size() > 1024)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect key length (not empty, max 1023 characters), got: {}", str.size());

    if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in key");

    validatePartitionKey(str, true);
}

void PartitionedStorageObjectStorageSink::validateNamespace(const String & str)
{
    configuration->validateNamespace(str);

    if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in bucket name");

    validatePartitionKey(str, false);
}

}
