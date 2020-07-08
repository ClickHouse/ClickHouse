#pragma once

#include <Common/config.h>
#include <IO/ReadBufferFromS3.h>

#if USE_AWS_S3

#include <memory>


namespace Aws::S3
{
class S3Client;
}

namespace DB
{
class ReadBufferFromCOS : public ReadBufferFromS3 {
    static constexpr auto log_name = "ReadBufferFromCOS";
public:
    explicit ReadBufferFromCOS(
        std::shared_ptr<Aws::S3::S3Client> client_ptr_,
        const String & bucket_,
        const String & key_,
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE)
    : ReadBufferFromS3(client_ptr_, bucket_, key_, buffer_size_, log_name)
    {
    }
};
}

#endif
