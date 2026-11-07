#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/FullyQualifiedObjectPath.h>
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
    extern const SettingsSnappyMode snappy_mode;
}

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
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

    void validateNamespace(const String & str, StorageObjectStorageConfigurationPtr configuration)
    {
        configuration->validateNamespace(str);

        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(str.data()), str.size()))
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Incorrect non-UTF8 sequence in bucket name");

        PartitionedSink::validatePartitionKey(str, false);
    }
}

StorageObjectStorageSink::StorageObjectStorageSink(
    const std::string & path_,
    ObjectStoragePtr object_storage,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context,
    const String & format,
    const String & compression_method,
    FormatFilterInfoPtr format_filter_info)
    : SinkToStorage(sample_block_)
    , path(path_)
    , sample_block(sample_block_)
{
    const auto & settings = context->getSettingsRef();
    const auto chosen_compression_method = chooseCompressionMethod(path, compression_method);

    auto buffer = object_storage->writeObject(
        StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    write_buf = wrapWriteBufferWithCompressionMethod(
        std::move(buffer),
        chosen_compression_method,
        static_cast<int>(settings[Setting::output_format_compression_level]),
        static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]),
        settings[Setting::snappy_mode]);

    writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
        format, *write_buf, *sample_block, context, format_settings_, std::move(format_filter_info));
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
    result_file_size = write_buf->count();
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

size_t StorageObjectStorageSink::getFileSize() const
{
    if (!result_file_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sink must be finalized before requesting result file size");
    return *result_file_size;
}

PartitionedStorageObjectStorageSink::PartitionedStorageObjectStorageSink(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    std::optional<FormatSettings> format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    PartitionedWriteAccessCheck write_access_check_)
    : PartitionedSink(configuration_->partition_strategy, context_, sample_block_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , query_settings(configuration_->getQuerySettings(context_))
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(context_)
    , write_access_check(std::move(write_access_check_))
{
}

StorageObjectStorageSink::~StorageObjectStorageSink()
{
    if (isCancelled())
        cancelBuffers();
}

SinkPtr PartitionedStorageObjectStorageSink::createSinkForPartition(const String & partition_id)
{
    auto file_path = configuration->getPathForWrite(partition_id).path;

    validateNamespace(configuration->getNamespace(), configuration);
    validateKey(file_path);

    /// A partition id can make the key fully qualified, e.g. `s3://other-bucket/x.tsv` for the template `{_partition_id}.tsv`,
    /// and the S3 object storage would then write to that bucket instead of the one whose URI is checked below.
    /// Other object storages write such a key as a literal name.
    if (object_storage->getType() == ObjectStorageType::S3 && !configuration->supportsFullyQualifiedPaths()
        && trySplitFullyQualifiedObjectPath(file_path))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The key {} of a partition refers to another bucket, which is not allowed",
            file_path);

    /// The partition id comes from the data, so its destination is checked before anything is written to it.
    /// The key is checked before it is looked up too, so that an existing object outside the grant is not disclosed.
    if (write_access_check)
        write_access_check(context, configuration->getURIForAccessCheck(file_path));

    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, query_settings, file_path, /* sequence_number */1))
    {
        file_path = *new_key;

        /// The new key can be outside the grant, e.g. `team.1.v1/data.tsv` for `team.v1/data.tsv`.
        if (write_access_check)
            write_access_check(context, configuration->getURIForAccessCheck(file_path));
    }

    last_written_object_path = file_path;

    return std::make_shared<StorageObjectStorageSink>(
        file_path,
        object_storage,
        format_settings,
        std::make_shared<Block>(partition_strategy->getFormatHeader()),
        context,
        configuration->format,
        configuration->compression_method);
}

}
