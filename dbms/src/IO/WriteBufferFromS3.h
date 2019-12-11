#pragma once

#include <memory>
#include <vector>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>


namespace DB
{
/* Perform S3 HTTP PUT request.
 */
class WriteBufferFromS3 : public BufferWithOwnMemory<WriteBuffer>
{
private:
    Poco::URI uri;
    String access_key_id;
    String secret_access_key;
    size_t minimum_upload_part_size;
    ConnectionTimeouts timeouts;
    String buffer_string;
    std::unique_ptr<WriteBufferFromString> temporary_buffer;
    size_t last_part_size;
    RemoteHostFilter remote_host_filter;

    /// Upload in S3 is made in parts.
    /// We initiate upload, then upload each part and get ETag as a response, and then finish upload with listing all our parts.
    String upload_id;
    std::vector<String> part_tags;

public:
    explicit WriteBufferFromS3(const Poco::URI & uri,
        const String & access_key_id,
        const String & secret_access_key,
        size_t minimum_upload_part_size_,
        const ConnectionTimeouts & timeouts = {},
        const RemoteHostFilter & remote_host_filter_ = {});

    void nextImpl() override;

    /// Receives response from the server after sending all data.
    void finalize() override;

    ~WriteBufferFromS3() override;

private:
    void initiate();
    void writePart(const String & data);
    void complete();
};

}
