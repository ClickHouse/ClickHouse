#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#    include <memory>
#    include <vector>
#    include <base/logger_useful.h>
#    include <base/types.h>

#    include <IO/BufferWithOwnMemory.h>
#    include <IO/WriteBuffer.h>

#    include <aws/core/utils/memory/stl/AWSStringStream.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{

/**
 * Buffer to write a data to a S3 object with specified bucket and key.
 * If data size written to the buffer is less than 'max_single_part_upload_size' write is performed using singlepart upload.
 * In another case multipart upload is used:
 * Data is divided on chunks with size greater than 'minimum_upload_part_size'. Last chunk can be less than this threshold.
 * Each chunk is written as a part to S3.
 */
class WriteBufferFromS3 : public BufferWithOwnMemory<WriteBuffer>
{
private:
    String bucket;
    String key;
    std::optional<std::map<String, String>> object_metadata;
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    size_t minimum_upload_part_size;
    size_t max_single_part_upload_size;
    /// Buffer to accumulate data.
    std::shared_ptr<Aws::StringStream> temporary_buffer;
    size_t last_part_size;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finish upload with listing all our parts.
    String multipart_upload_id;
    std::vector<String> part_tags;

    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromS3");

public:
    explicit WriteBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t minimum_upload_part_size_,
        size_t max_single_part_upload_size_,
        std::optional<std::map<String, String>> object_metadata_ = std::nullopt,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    /// Receives response from the server after sending all data.
    void finalize() override;

private:
    bool finalized = false;

    void allocateBuffer();

    void createMultipartUpload();
    void writePart();
    void completeMultipartUpload();

    void makeSinglepartUpload();

    void finalizeImpl();
};

}

#endif
