#include <Common/config.h>

#if USE_AWS_S3


#include "PocoHTTPResponseStream.h"

#include <utility>

namespace DB::S3
{
PocoHTTPResponseStream::PocoHTTPResponseStream(std::shared_ptr<Poco::Net::HTTPClientSession> session_, std::istream & response_stream_)
    : Aws::IOStream(response_stream_.rdbuf()), session(std::move(session_))
{
}

}

#endif
