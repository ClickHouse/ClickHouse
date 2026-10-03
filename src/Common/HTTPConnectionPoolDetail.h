#pragma once

#include <Poco/Net/HTTPFixedLengthStream.h>

#include <algorithm>

namespace DB::HTTPConnectionPoolDetail
{

template <typename GetBufferedSize>
bool tryCompleteBufferedFixedLengthResponse(
    Poco::Net::HTTPFixedLengthInputStream & response_stream, GetBufferedSize && get_buffered_size)
{
    /// Attempts to complete the response using only data already buffered by its HTTP session.
    /// Returns whether the response is complete on return, including when it was already complete.
    /// `get_buffered_size` must report the current session-buffer size for this response, not data
    /// available from the socket or TLS layer. The stream, its session, and both buffers must have
    /// no concurrent consumer.
    ///
    /// There are two buffering layers. Bytes in the fixed-length stream buffer have already left
    /// the session and are included in `isComplete`, even if the application has not consumed them.
    /// Bytes still in the session may be discarded here because `HTTPSession::read` does not
    /// continue to the socket while its buffer is nonempty. The fixed-length stream caps reads at
    /// `Content-Length`, leaving any trailing bytes for the pool's existing session-buffer check.
    ///
    /// The scratch array controls copy granularity; it is not a drain limit. Work is bounded by
    /// Poco's buffers. Cleanup may consume a partial remainder when the full body is unavailable.
    /// This pool-level cleanup intentionally also runs during cancellation and exception unwinding
    /// and is independent of reader-specific settings and byte accounting.
    char scratch[1024];
    auto * buffer = response_stream.rdbuf();
    while (!response_stream.isComplete())
    {
        const auto buffered_size = get_buffered_size();
        if (buffered_size <= 0)
            break;

        const auto stream_buffered_size = buffer->in_avail();
        /// Either the stream buffer satisfies the read itself, or the session buffer can satisfy
        /// the remainder, so using the larger available size cannot cause a socket read.
        const auto available_size = std::max<std::streamsize>(stream_buffered_size, buffered_size);
        const auto count = std::min<std::streamsize>(sizeof(scratch), available_size);
        if (buffer->sgetn(scratch, count) <= 0)
            break;
    }

    return response_stream.isComplete();
}

}
