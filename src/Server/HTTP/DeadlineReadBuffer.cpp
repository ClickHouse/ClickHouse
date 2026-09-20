#include <Server/HTTP/DeadlineReadBuffer.h>

#include <Poco/Net/NetException.h>

namespace DB
{

bool DeadlineReadBuffer::nextImpl()
{
    /// Sync our position to the inner buffer before asking for more data.
    in.position() = position();

    bool has_data = in.next();

    /// Check after the potentially blocking read so that
    /// `http_headers_read_timeout` remains a strict wall-clock limit.
    if (std::chrono::steady_clock::now() > deadline)
        throw Poco::Net::MessageException("Timeout exceeded while reading HTTP headers");

    if (has_data)
        BufferBase::set(in.position(), in.available(), 0);
    else
        BufferBase::set(in.position(), 0, 0);

    return has_data;
}

}
