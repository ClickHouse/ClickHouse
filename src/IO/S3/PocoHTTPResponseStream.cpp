#include "PocoHTTPResponseStream.h"

#include <utility>

namespace DB::S3
{
PocoHTTPResponseStream::PocoHTTPResponseStream(std::shared_ptr<Poco::Net::HTTPClientSession> session_, std::istream & response_stream_)
    : Aws::IStream(response_stream_.rdbuf()), session(std::move(session_))
{
}

}
