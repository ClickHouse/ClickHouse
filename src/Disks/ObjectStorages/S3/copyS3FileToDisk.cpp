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
    const std::shared_ptr<const S3::Client> & s3_client,
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
        src_size = S3::getObjectSize(*s3_client, src_bucket, src_key, version_id.value_or(""), request_settings) - *src_offset;

    auto destination_data_source_description = destination_disk->getDataSourceDescription();
    if (destination_data_source_description != DataSourceDescription{DataSourceType::S3, s3_client->getInitialEndpoint(), false, false})
    {
        LOG_TRACE(&Poco::Logger::get("copyS3FileToDisk"), "Copying {} to disk {} through buffers", src_key, destination_disk->getName());
        ReadBufferFromS3 read_buffer{s3_client, src_bucket, src_key, {}, request_settings, read_settings};
        if (*src_offset)
            read_buffer.seek(*src_offset, SEEK_SET);
        auto write_buffer = destination_disk->writeFile(destination_path, std::min<size_t>(*src_size, DBMS_DEFAULT_BUFFER_SIZE), write_mode, write_settings);
        copyData(read_buffer, *write_buffer, *src_size);
        write_buffer->finalize();
        return;
    }

    LOG_TRACE(&Poco::Logger::get("copyS3FileToDisk"), "Copying {} to disk {} using native copy", src_key, destination_disk->getName());

    String dest_bucket = destination_disk->getObjectStorage()->getObjectsNamespace();

    auto custom_write_object = [&](const StoredObject & object_, WriteMode write_mode_, const std::optional<ObjectAttributes> & object_attributes_) -> size_t
    {
        /// Object storage always uses mode `Rewrite` because it simulates append using metadata and different files.
        chassert(write_mode_ == WriteMode::Rewrite);

        copyS3File(s3_client, src_bucket, src_key, *src_offset, *src_size, dest_bucket, /* dest_key= */ object_.absolute_path,
                   request_settings, object_attributes_, scheduler, /* for_disk_s3= */ true);

        return *src_size;
    };

    destination_disk->writeFileUsingCustomWriteObject(destination_path, write_mode, custom_write_object);
}

}

#endif
