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
    HTTPSessionPtr session;
    std::istream * istr; /// owned by session
    std::unique_ptr<ReadBuffer> impl;

    RemoteHostFilter remote_host_filter;

public:
    explicit ReadBufferFromS3(const Poco::URI & uri_,
        const String & access_key_id_,
        const String & secret_access_key_,
        const ConnectionTimeouts & timeouts = {},
        const RemoteHostFilter & remote_host_filter_ = {});

    bool nextImpl() override;
};

}
