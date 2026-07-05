#pragma once

#include "config.h"

#if USE_GOOGLE_CLOUD

#include <memory>
#include <base/types.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <Common/logger_useful.h>

#include <google/cloud/storage/client.h>

namespace DB
{

/// Reads a GCS object through the native google-cloud-cpp storage client.
///
/// Backed by a `google::cloud::storage::ObjectReadStream` (a std::istream). Ranged reads are used
/// for seeks and for a bounded right edge (`read_until_position`), mirroring ReadBufferFromS3 /
/// ReadBufferFromAzureBlobStorage semantics.
class ReadBufferFromGCS : public ReadBufferFromFileBase
{
public:
    ReadBufferFromGCS(
        std::shared_ptr<google::cloud::storage::Client> client_,
        const String & bucket_,
        const String & key_,
        const ReadSettings & read_settings_,
        bool use_external_buffer_ = false,
        size_t offset_ = 0,
        size_t read_until_position_ = 0,
        bool restricted_seek_ = false,
        std::optional<size_t> file_size_ = std::nullopt);

    ~ReadBufferFromGCS() override = default;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override;

    std::optional<size_t> tryGetFileSize() override;

    void setReadUntilPosition(size_t position) override;
    void setReadUntilEnd() override;

    size_t getFileOffsetOfBufferEnd() const override { return offset; }

    bool supportsRightBoundedReads() const override { return true; }

    String getFileName() const override { return bucket + "/" + key; }

private:
    void initialize();

    std::shared_ptr<google::cloud::storage::Client> client;
    const String bucket;
    const String key;

    ReadSettings read_settings;
    bool use_external_buffer;
    bool restricted_seek;

    /// File offset of the byte just past the current working buffer.
    off_t offset = 0;
    /// Right boundary (exclusive); 0 means "read to the end of the object".
    off_t read_until_position = 0;

    bool initialized = false;
    std::unique_ptr<google::cloud::storage::ObjectReadStream> read_stream;

    /// Own buffer used when not reading into an externally provided buffer.
    std::vector<char> tmp_buffer;
    size_t tmp_buffer_size;
    char * data_ptr = nullptr;
    size_t data_capacity = 0;

    LoggerPtr log = getLogger("ReadBufferFromGCS");
};

}

#endif
