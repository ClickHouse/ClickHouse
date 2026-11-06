#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/ObjectStorage/StorageObjectStorageSink.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Common/isValidUTF8.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Storages/IPartitionStrategy.h>
#include <Storages/NumberedFileName.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>
#include <Interpreters/Context.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#include <Disks/IO/WriteBufferFromAzureDataLakeStorage.h>
#endif

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
    ObjectStoragePtr object_storage_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    const String & format_,
    const String & compression_method_,
    FormatFilterInfoPtr format_filter_info_,
    size_t split_on_write_by_size_bytes_,
    GetNextPathCallback get_next_path_,
    PublishPathCallback publish_path_,
    bool path_is_published_,
    RemovedPathCallback on_path_removed_)
    : SinkToStorage(sample_block_)
    , path(path_)
    , object_storage(object_storage_)
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(context_)
    , format(format_)
    , compression_method(compression_method_)
    , format_filter_info(std::move(format_filter_info_))
    , split_on_write_by_size_bytes(split_on_write_by_size_bytes_)
    , get_next_path(std::move(get_next_path_))
    , publish_path(std::move(publish_path_))
    , path_is_published(path_is_published_)
    , on_path_removed(std::move(on_path_removed_))
{
    if (split_on_write_by_size_bytes && !get_next_path)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Splitting the data by size is requested without a way to get the name of the next object");

    initialize();
}

void StorageObjectStorageSink::initialize()
{
    const auto & settings = context->getSettingsRef();
    const auto chosen_compression_method = chooseCompressionMethod(path, compression_method);

    /// The sink keeps the ownership of the buffer that writes into the object storage, so that the amount
    /// of the data written into the object can be checked for splitting. The compressing wrapper, if any,
    /// is created as a non-owning one on top of it.
    destination_buf = object_storage->writeObject(
        StoredObject(path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    if (chosen_compression_method != CompressionMethod::None)
        write_buf = wrapWriteBufferWithCompressionMethod(
            destination_buf.get(),
            chosen_compression_method,
            static_cast<int>(settings[Setting::output_format_compression_level]),
            static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]),
            settings[Setting::snappy_mode]);

    /// With the parallel formatting, the data is written into the buffer by a background thread,
    /// and the amount of the written data cannot be checked after every block without a data race.
    writer = split_on_write_by_size_bytes
        ? FormatFactory::instance().getOutputFormat(format, getWriteBuffer(), *sample_block, context, format_settings, format_filter_info)
        : FormatFactory::instance().getOutputFormatParallelIfPossible(
            format, getWriteBuffer(), *sample_block, context, format_settings, format_filter_info);
}

WriteBuffer & StorageObjectStorageSink::getWriteBuffer()
{
    return write_buf ? *write_buf : *destination_buf;
}

void StorageObjectStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    /// The previous object is already finished. Start the next one only now, when there is data for it.
    if (split_on_write_by_size_bytes && !writer)
    {
        path = get_next_path();
        initialize();
        path_is_published = false;
    }

    writer->write(getHeader().cloneWithColumns(chunk.getColumns()));

    /// Continue writing into a new object as soon as the current one became large enough.
    /// The current block is always written in full, so the object can be larger than the requested size.
    if (split_on_write_by_size_bytes && destination_buf->count() >= split_on_write_by_size_bytes)
    {
        finalizeBuffers();
        releaseBuffers();
    }
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

    if (write_buf)
        write_buf->finalize();
    destination_buf->finalize();
    result_file_size = getWriteBuffer().count();

    /// The object is committed - only now it becomes a part of the table. If the insert fails while
    /// writing it, the table keeps reading the objects of the previous shards, and not a key whose
    /// object does not exist or is incomplete.
    if (!path_is_published)
    {
        if (publish_path)
            publish_path(path);
        path_is_published = true;
    }
}

void StorageObjectStorageSink::releaseBuffers()
{
    writer.reset();
    write_buf.reset();
    destination_buf.reset();
}

void StorageObjectStorageSink::cancelBuffers()
{
    /// A cancelled write into HDFS deletes the file, see `WriteBufferFromHDFS::cancelImpl`, and so does a cancelled
    /// write into an ADLS Gen2 endpoint (e.g. Microsoft Fabric / OneLake), see `WriteBufferFromAzureDataLakeStorage::cancelImpl`.
    /// Blob storages create the object only when it is committed, so the cancellation leaves the previous one.
    bool cancellation_removes_object = false;
    if (destination_buf && !path_is_published)
    {
        cancellation_removes_object = object_storage->getType() == ObjectStorageType::HDFS;
#if USE_AZURE_BLOB_STORAGE
        cancellation_removes_object = cancellation_removes_object
            || dynamic_cast<const WriteBufferFromAzureDataLakeStorage *>(destination_buf.get()) != nullptr;
#endif
    }

    if (writer)
        writer->cancel();
    if (write_buf)
        write_buf->cancel();
    if (destination_buf)
        destination_buf->cancel();

    if (cancellation_removes_object && on_path_removed)
        on_path_removed(path);
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
    ContextPtr context_)
    : PartitionedSink(configuration_->partition_strategy, context_, sample_block_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , query_settings(configuration_->getQuerySettings(context_))
    , format_settings(format_settings_)
    , sample_block(sample_block_)
    , context(context_)
    , reservations(std::make_shared<WrittenPathReservations>(configuration_))
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

    /// The objects written after the first one are numbered: `data.1.tsv`, `data.2.tsv`, ... The number is placed
    /// into the path pattern rather than into the key of the partition, so that a partition id with a dot in it
    /// cannot shift it - see `IPartitionStrategy::getNumberedPathsForWrite`. An insert that has to step aside from
    /// an existing object into a numbered key continues the numbering from there.
    const NumberedFileNames numbered_keys = configuration->getNumberedPathsForWrite(partition_id, file_path);
    size_t sequence_number = 1;

    /// An insert split by size writes the numbered objects of the partition, and a truncating one deletes them (see below),
    /// but with this path pattern a numbered key of the partition may be the first object of another partition written by
    /// the same pattern. Nothing tells them apart: the split would write an object that the next insert into the other
    /// partition overwrites, or overwrite or delete the object of the other partition itself. So the insert is refused
    /// before anything is written or deleted, whatever the other settings are.
    if (query_settings.split_on_write_by_size_bytes && numbered_keys.may_be_names_of_other_partitions)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot split the partition {} with {}_split_on_write_by_size_bytes: "
            "the numbered objects of the partition, such as {}, may be the objects of other partitions written by the path pattern {}. "
            "Place a character that is not a digit between {} and the extension of the file, or {} into a directory of the path, "
            "or use a partition key of an integer, date or UUID type",
            partition_id, configuration->getTypeName(), numbered_keys.getName(1),
            configuration->getRawPath().path, PartitionedSink::PARTITION_ID_WILDCARD, PartitionedSink::PARTITION_ID_WILDCARD);

    /// See the same reservation in `StorageObjectStorage::write`: a generated key is invisible both for the
    /// readers and in the object storage until the object is committed, so it is held against the concurrent
    /// inserts into this table for as long as this insert lasts. The reservations of every partition belong to
    /// the partitioned sink, which outlives the sinks of the partitions it has created.
    if (auto new_key = checkAndGetNewFileOnInsertIfNeeded(
            *object_storage, *configuration, query_settings, file_path, numbered_keys, sequence_number, *reservations))
    {
        file_path = *new_key;
    }

    last_written_object_path = file_path;

    /// The `hive` strategy generates a fresh name for the first object of every insert, so an insert never
    /// meets an object of a previous insert, and a truncating insert does not overwrite or delete anything,
    /// with or without splitting: the numbered sequence of the new name cannot exist yet, and probing it
    /// would only cost requests.
    const bool names_are_generated = configuration->partition_strategy_type == PartitionStrategyFactory::StrategyType::HIVE;

    /// The reservations of this insert are released when it is over, and from then on only the object storage would
    /// tell the next insert into the partition that the keys are taken - and not every S3 implementation reports an
    /// object right after it has been written. So every committed key stays taken in the configuration of the table
    /// for a while, see `StorageObjectStorageConfiguration::commitPathWrittenByInsert`. It is done after the object is
    /// committed, before the reservation is released, so the key is never free in between. A generated name cannot
    /// be met again, so it is not kept.
    /// The callback also holds the reservations, whatever the settings are: the partitioned sink is destroyed
    /// before the sinks of its partitions, and a sink that is cancelled may still be cleaning up its object
    /// (HDFS deletes the file of a cancelled write) - if the key were free by then, another insert could start
    /// writing it, and the cleanup would delete the object of that insert.
    StorageObjectStorageSink::PublishPathCallback publish_path
        = [config = configuration, names_are_generated, path_reservations = reservations](const String & key)
    {
        if (!names_are_generated)
            config->commitPathWrittenByInsert(key);
    };

    StorageObjectStorageSink::GetNextPathCallback get_next_path;
    if (query_settings.split_on_write_by_size_bytes)
    {
        get_next_path = [storage = object_storage, config = configuration, settings = query_settings, numbered_keys, sequence_number, path_reservations = reservations]() mutable -> String
        {
            return getNextKeyForSplittingBySize(*storage, *config, settings, numbered_keys, sequence_number, *path_reservations);
        };
    }

    auto sink = std::make_shared<StorageObjectStorageSink>(
        file_path,
        object_storage,
        format_settings,
        std::make_shared<Block>(partition_strategy->getFormatHeader()),
        context,
        configuration->format,
        configuration->compression_method,
        /* format_filter_info = */ nullptr,
        query_settings.split_on_write_by_size_bytes,
        std::move(get_next_path),
        std::move(publish_path),
        /* path_is_published = */ false,
        /// See the same callback in `StorageObjectStorage::write`.
        [config = configuration](const String & removed_key) { config->forgetPathCommittedByInsert(removed_key); });

    /// A partitioned sink keeps no list of the objects it has written, so there is nothing to attribute
    /// the numbered keys of a previous insert to, and the removal is done only for a truncating insert
    /// that is split by size and therefore claims the whole sequence. It is done only after the sink has
    /// started writing the first object of the partition, so that an insert that cannot even start it
    /// fails without deleting a part of the old data. In HDFS, starting the first file of a rewrite already
    /// truncates it, see `StorageObjectStorage::createSink`.
    if (query_settings.split_on_write_by_size_bytes && query_settings.truncate_on_insert && !names_are_generated)
        removeStaleSplitObjectsByNumber(
            *object_storage,
            *configuration,
            numbered_keys,
            query_settings.create_new_file_on_insert,
            getLogger("PartitionedStorageObjectStorageSink"));

    return sink;
}

}
