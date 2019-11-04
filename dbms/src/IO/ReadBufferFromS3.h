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
    String access_key_id;
    String secret_access_key;

    HTTPSessionPtr session;
    std::istream * istr; /// owned by session
    std::unique_ptr<ReadBuffer> impl;

public:
    explicit ReadBufferFromS3(const Poco::URI & uri_,
        const String & access_key_id_,
        const String & secret_access_key_,
        const ConnectionTimeouts & timeouts = {});

    bool nextImpl() override;
};

}
