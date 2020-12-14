#pragma once

#include <IO/HTTPCommon.h>

#include <aws/core/utils/stream/ResponseStream.h>


namespace DB::S3
{
/**
 * Wrapper of IOStream to store response stream and corresponding HTTP session.
 */
template <typename Session>
class SessionAwareAwsStream : public Aws::IStream
{
public:
    SessionAwareAwsStream(Session session_, std::istream & response_stream_)
        : Aws::IStream(response_stream_.rdbuf()), session(std::move(session_))
    {
    }

private:
    /// Poco HTTP session is holder of response stream.
    Session session;
};

}
