#include <Disks/ObjectStorages/S3/copyS3FileToDisk.h>

#if USE_AWS_S3

#include <IO/S3/getObjectInfo.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>
#include <IO/S3/copyS3File.h>


namespace DB
{

void copyS3FileToDisk(
    const std::shared_ptr<const S3::Client> & src_s3_client,
    const String & src_bucket,
    const String & src_key,
    const std::optional<String> & version_id,
    std::optional<size_t> src_offset,
    std::optional<size_t> src_size,
    DiskPtr destination_disk,
    const String & destination_path,
    WriteMode write_mode,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const S3Settings::RequestSettings & request_settings,
    ThreadPoolCallbackRunner<void> scheduler)
{
    if (!src_offset)
        src_offset = 0;

    if (!src_size)
        src_size = S3::getObjectSize(*src_s3_client, src_bucket, src_key, version_id.value_or(""), request_settings) - *src_offset;

    auto destination_data_source_description = destination_disk->getDataSourceDescription();
    if (destination_data_source_description == DataSourceDescription{DataSourceType::S3, src_s3_client->getInitialEndpoint(), false, false})
    {
        /// Use native copy, the more optimal way.
        LOG_TRACE(&Poco::Logger::get("copyS3FileToDisk"), "Copying {} to disk {} using native copy", src_key, destination_disk->getName());
        auto write_blob_function = [&](const std::pair<String, String> & blob_path_, WriteMode write_mode_, const std::optional<ObjectAttributes> & object_attributes_) -> size_t
        {
            /// Object storage always uses mode `Rewrite` because it simulates append using metadata and different files.
            chassert(write_mode_ == WriteMode::Rewrite);

            copyS3File(
                src_s3_client,
                src_bucket,
                src_key,
                *src_offset,
                *src_size,
                /* dest_bucket= */ blob_path_.first,
                /* dest_key= */ blob_path_.second,
                request_settings,
                object_attributes_,
                scheduler,
                /* for_disk_s3= */ true);

            return *src_size;
        };

        destination_disk->writeFileUsingBlobWritingFunction(destination_path, write_mode, write_blob_function);
        return;
    }

    /// Fallback to copy through buffers.
    LOG_TRACE(&Poco::Logger::get("copyS3FileToDisk"), "Copying {} to disk {} through buffers", src_key, destination_disk->getName());
    ReadBufferFromS3 read_buffer{src_s3_client, src_bucket, src_key, {}, request_settings, read_settings};
    if (*src_offset)
        read_buffer.seek(*src_offset, SEEK_SET);
    auto write_buffer = destination_disk->writeFile(destination_path, std::min<size_t>(*src_size, DBMS_DEFAULT_BUFFER_SIZE), write_mode, write_settings);
    copyData(read_buffer, *write_buffer, *src_size);
    write_buffer->finalize();
}

void copyS3FileFromDisk(
    DiskPtr src_disk,
    const String & src_path,
    std::optional<size_t> src_offset,
    std::optional<size_t> src_size,
    const std::shared_ptr<const S3::Client> & dest_s3_client,
    const String & dest_bucket,
    const String & dest_key,
    const ReadSettings & read_settings,
    const S3Settings::RequestSettings & request_settings,
    ThreadPoolCallbackRunner<void> scheduler)
{
    if (!src_offset)
        src_offset = 0;

    if (!src_size)
        src_size = src_disk->getFileSize(src_path) - *src_offset;

    auto source_data_source_description = src_disk->getDataSourceDescription();
    if (source_data_source_description == DataSourceDescription{DataSourceType::S3, dest_s3_client->getInitialEndpoint(), false, false})
    {
        /// getBlobPath() can return std::nullopt if the file is stored as multiple objects in S3 bucket.
        /// In this case we can't use native copy.
        if (auto blob_path = src_disk->getBlobPath(src_path))
        {
            /// Use native copy, the more optimal way.
            LOG_TRACE(&Poco::Logger::get("copyS3FileFromDisk"), "Copying file {} to S3 using native copy", src_path);
            const auto & [src_bucket, src_key] = *blob_path;
            copyS3File(dest_s3_client, src_bucket, src_key, *src_offset, *src_size, dest_bucket, dest_key, request_settings, {}, scheduler);
            return;
        }
    }

    /// Fallback to copy through buffers.
    LOG_TRACE(&Poco::Logger::get("copyS3FileFromDisk"), "Copying {} to S3 through buffers", src_path);
    auto create_read_buffer = [src_disk, &src_path, &read_settings] { return src_disk->readFile(src_path, read_settings); };
    copyDataToS3File(create_read_buffer, *src_offset, *src_size, dest_s3_client, dest_bucket, dest_key, request_settings, {}, scheduler);
}

}

#endif
