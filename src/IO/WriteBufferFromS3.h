#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#    include <memory>
#    include <vector>
#    include <common/types.h>
#    include <IO/BufferWithOwnMemory.h>
#    include <IO/HTTPCommon.h>
#    include <IO/WriteBuffer.h>
#    include <IO/WriteBufferFromString.h>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
/* Perform S3 HTTP PUT request.
 */
class WriteBufferFromS3 : public BufferWithOwnMemory<WriteBuffer>
{
private:
    bool is_multipart;

    String bucket;
    String key;
    std::shared_ptr<Aws::S3::S3Client> client_ptr;
    size_t minimum_upload_part_size;
    std::unique_ptr<WriteBufferFromOwnString> temporary_buffer;
    size_t last_part_size;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finish upload with listing all our parts.
    String upload_id;
    std::vector<String> part_tags;

    Poco::Logger * log = &Poco::Logger::get("WriteBufferFromS3");

public:
    explicit WriteBufferFromS3(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t minimum_upload_part_size_,
        bool is_multipart,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    /// Receives response from the server after sending all data.
    void finalize() override;

    ~WriteBufferFromS3() override;

private:
    bool finalized = false;

    void initiate();
    void writePart(const String & data);
    void complete();

    void finalizeImpl();
};

}

#endif
