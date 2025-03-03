#include "StorageObjectStorageSink.h"
#include <Formats/FormatFactory.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/isValidUTF8.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 output_format_compression_zstd_window_log;
}

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    void validateKey(const String & str)
    {
        /// See:
        /// - https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
        /// - https://cloud.ibm.com/apidocs/cos/cos-compatibility#putobject

        if (str.empty() || str.size() > 1024)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect key length (not empty, max 1023 characters), got: {}", str.size());

        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in key");

        PartitionedSink::validatePartitionKey(str, true);
    }

    void validateNamespace(const String & str, PartitionedStorageObjectStorageSink::ConfigurationPtr configuration)
    {
        configuration->validateNamespace(str);

        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in bucket name");

        PartitionedSink::validatePartitionKey(str, false);
    }

    std::string getFilePath(const String & partition_id, PartitionedStorageObjectStorageSink::ConfigurationPtr configuration)
    {
        if (!configuration->withPartitionWildcard())
        {
            return configuration->getPath() + "/" + partition_id;
        }

        auto partition_bucket = PartitionedSink::replaceWildcards(configuration->getNamespace(), partition_id);
        validateNamespace(partition_bucket, configuration);

        auto partition_key = PartitionedSink::replaceWildcards(configuration->getPath(), partition_id);
        validateKey(partition_key);

        return partition_key;
    }
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
        static_cast<int>(settings[Setting::output_format_compression_level]),
        static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]));

    writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
        configuration->format, *write_buf, sample_block, context, format_settings_);
}

void StorageObjectStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;
    writer->write(getHeader().cloneWithColumns(chunk.getColumns()));
}

void StorageObjectStorageSink::onFinish()
{
    chassert(!isCancelled());
    finalizeBuffers();
    releaseBuffers();
}

void StorageObjectStorageSink::finalizeBuffers()
{
    if (!writer)
        return;

    try
    {
        writer->flush();
        writer->finalize();
    }
    catch (...)
    {
        /// Stop ParallelFormattingOutputFormat correctly.
        cancelBuffers();
        releaseBuffers();
        throw;
    }

    write_buf->finalize();
}

void StorageObjectStorageSink::releaseBuffers()
{
    writer.reset();
    write_buf.reset();
}

void StorageObjectStorageSink::cancelBuffers()
{
    if (writer)
        writer->cancel();
    if (write_buf)
        write_buf->cancel();
}

PartitionedStorageObjectStorageSink::PartitionedStorageObjectStorageSink(
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    std::optional<FormatSettings> format_settings_,
    const Block & sample_block_,
    ContextPtr context_,
    const ASTPtr & partition_by)
    : PartitionedSink(partition_by, context_, sample_block_, configuration_->format)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , query_settings(configuration_->getQuerySettings(context_))
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(context_)
{
}

StorageObjectStorageSink::~StorageObjectStorageSink()
{
    if (isCancelled())
        cancelBuffers();
}

SinkPtr PartitionedStorageObjectStorageSink::createSinkForPartition(const String & partition_id)
{
    auto file_path = getFilePath(partition_id, configuration);

    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, query_settings, file_path, /* sequence_number */1))
    {
        file_path = *new_key;
    }

    return std::make_shared<StorageObjectStorageSink>(
        object_storage,
        configuration,
        format_settings,
        sample_block,
        context,
        file_path
    );
}

}
