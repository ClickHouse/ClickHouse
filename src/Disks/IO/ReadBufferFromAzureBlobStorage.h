#pragma once

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <IO/HTTPCommon.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WithFileName.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>

namespace DB
{

class ReadBufferFromAzureBlobStorage : public ReadBufferFromFileBase
{
public:
    using ContainerClientPtr = std::shared_ptr<const AzureBlobStorage::ContainerClient>;
    using BlobClientPtr = std::unique_ptr<const AzureBlobStorage::BlobClient>;

    ReadBufferFromAzureBlobStorage(
        ContainerClientPtr blob_container_client_,
        const String & path_,
        const ReadSettings & read_settings_,
        size_t max_single_read_retries_,
        size_t max_single_download_retries_,
        bool use_external_buffer_ = false,
        bool restricted_seek_ = false,
        size_t read_until_position_ = 0);

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    bool nextImpl() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    String getFileName() const override { return path; }

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    bool supportsRightBoundedReads() const override { return true; }

    std::optional<size_t> tryGetFileSize() override;

    size_t readBigAt(char * to, size_t n, size_t range_begin, const std::function<bool(size_t)> & progress_callback) const override;

    bool supportsReadAt() override { return true; }

private:
    void initialize();

    std::unique_ptr<Azure::Core::IO::BodyStream> data_stream;
    ContainerClientPtr blob_container_client;
    BlobClientPtr blob_client;

    const String path;
    size_t max_single_read_retries;
    size_t max_single_download_retries;
    ReadSettings read_settings;
    std::vector<char> tmp_buffer;
    size_t tmp_buffer_size;
    bool use_external_buffer;

    /// There is different seek policy for disk seek and for non-disk seek
    /// (non-disk seek is applied for seekable input formats: orc, arrow, parquet).
    bool restricted_seek;

    off_t read_until_position = 0;

    off_t offset = 0;
    size_t total_size;
    bool initialized = false;
    char * data_ptr;
    size_t data_capacity;

    LoggerPtr log = getLogger("ReadBufferFromAzureBlobStorage");
};

}

#endif
