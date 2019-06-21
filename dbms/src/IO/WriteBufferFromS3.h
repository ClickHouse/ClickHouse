#pragma once

#include <functional>
#include <memory>
#include <vector>
#include <Core/Types.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Version.h>
#include <Common/DNSResolver.h>
#include <Common/config.h>
#include <common/logger_useful.h>


namespace DB
{
/* Perform S3 HTTP PUT request.
 */
class WriteBufferFromS3 : public BufferWithOwnMemory<WriteBuffer>
{
private:
    Poco::URI uri;
    size_t minimum_upload_part_size;
    ConnectionTimeouts timeouts;
    Poco::Net::HTTPRequest auth_request;
    String buffer_string;
    std::unique_ptr<WriteBufferFromString> temporary_buffer;
    size_t part_number;
    size_t last_part_size;
    String upload_id;
    std::vector<String> part_tags;

public:
    explicit WriteBufferFromS3(const Poco::URI & uri,
        size_t minimum_upload_part_size_,
        const ConnectionTimeouts & timeouts = {},
        const Poco::Net::HTTPBasicCredentials & credentials = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    void nextImpl() override;

    /// Receives response from the server after sending all data.
    void finalize();

    ~WriteBufferFromS3() override;

private:
    void initiate();
    void writePart(const String & data);
    void complete();
};

}
