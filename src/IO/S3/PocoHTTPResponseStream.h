#pragma once

#include <aws/core/utils/stream/ResponseStream.h>
#include <Poco/Net/HTTPClientSession.h>

namespace DB::S3
{
/**
 * Wrapper of IStream to store response stream and corresponding HTTP session.
 */
class PocoHTTPResponseStream : public Aws::IStream
{
public:
    PocoHTTPResponseStream(std::shared_ptr<Poco::Net::HTTPClientSession> session_, std::istream & response_stream_);

private:
    /// Poco HTTP session is holder of response stream.
    std::shared_ptr<Poco::Net::HTTPClientSession> session;
};

}
