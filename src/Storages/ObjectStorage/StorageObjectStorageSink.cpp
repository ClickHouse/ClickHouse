#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Formats/FormatFactory.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/isValidUTF8.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>
#include <Interpreters/Context.h>

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

StorageObjectStorageSink::StorageObjectStorageSink(
    const std::string & path_,
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context)
    : SinkToStorage(sample_block_)
    , path(path_)
    , sample_block(sample_block_)
{
    const auto & settings = context->getSettingsRef();
    const auto chosen_compression_method = chooseCompressionMethod(path, configuration->compression_method);

    auto buffer = object_storage->writeObject(
        StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    write_buf = wrapWriteBufferWithCompressionMethod(
        std::move(buffer),
        chosen_compression_method,
        static_cast<int>(settings[Setting::output_format_compression_level]),
        static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]));

    writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
        configuration->format, *write_buf, *sample_block, context, format_settings_);
}

void StorageObjectStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;
    writer->write(getHeader().cloneWithColumns(chunk.getColumns()));
}

void StorageObjectStorageSink::onFinish()
{
    if (isCancelled())
        return;

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
    StorageObjectStorageConfigurationPtr configuration_,
    std::optional<FormatSettings> format_settings_,
    SharedHeader sample_block_,
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

StorageObjectStorageSink::~StorageObjectStorageSink()
{
    if (isCancelled())
        cancelBuffers();
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
        partition_key,
        object_storage,
        configuration,
        format_settings,
        sample_block,
        context
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
