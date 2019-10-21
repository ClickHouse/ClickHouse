#pragma once

#include <memory>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadBuffer.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>


namespace DB
{
/** Perform S3 HTTP GET request and provide response to read.
  */
class ReadBufferFromS3 : public ReadBuffer
{
protected:
    Poco::URI uri;
    std::string method;

    HTTPSessionPtr session;
    std::istream * istr; /// owned by session
    std::unique_ptr<ReadBuffer> impl;

public:
    explicit ReadBufferFromS3(Poco::URI uri_,
        const ConnectionTimeouts & timeouts = {},
        const Poco::Net::HTTPBasicCredentials & credentials = {},
        size_t buffer_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    bool nextImpl() override;
};

}
