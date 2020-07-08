#pragma once

#include <IO/WriteBufferFromS3.h>
#include <Common/config.h>

#if USE_AWS_S3

#include <memory>

namespace Aws::S3
{
class S3Client;
}

namespace DB
{
/* Perform COS HTTP PUT request.
 */
class WriteBufferFromCOS : public WriteBufferFromS3 {
    static constexpr auto log_name = "WriteBufferFromCOS";
public: 
    explicit WriteBufferFromCOS(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t minimum_upload_part_size_,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
    : WriteBufferFromS3(client_ptr_, bucket_, key_, minimum_upload_part_size_, buffer_size_, log_name)
    {
    }
};

}
#endif
