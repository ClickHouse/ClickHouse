#pragma once

#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <Common/HTMLForm.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>

namespace DB
{

using HTTPServerRequest = Poco::Net::HTTPServerRequest;
using HTTPServerResponse = Poco::Net::HTTPServerResponse;
using HTTPResponseBufferPtr = std::shared_ptr<WriteBufferFromHTTPServerResponse>;

/* Raw data
 * ↓
 * CascadeWriteBuffer out_maybe_delayed_and_compressed (optional)
 * ↓ (forwards data if an overflow is occur or explicitly via pushDelayedResults)
 * CompressedWriteBuffer out_maybe_compressed (optional)
 * ↓
 * WriteBufferFromHTTPServerResponse out
 */
struct HTTPOutputStreams
{
    HTTPResponseBufferPtr out;
    /// Points to 'out' or to CompressedWriteBuffer(*out), depending on settings.
    std::shared_ptr<WriteBuffer> out_maybe_compressed;
    /// Points to 'out' or to CompressedWriteBuffer(*out) or to CascadeWriteBuffer.
    std::shared_ptr<WriteBuffer> out_maybe_delayed_and_compressed;

    ~HTTPOutputStreams();

    void finalize() const;

    WriteBufferPtr createMaybeDelayedAndCompressionOut(Context &context, HTMLForm &form, WriteBufferPtr &out_);

    WriteBufferPtr createMaybeCompressionOut(bool compression, std::shared_ptr<WriteBufferFromHTTPServerResponse> & out_);

    HTTPResponseBufferPtr createResponseOut(HTTPServerRequest & request, HTTPServerResponse & response, size_t keep_alive);

    HTTPOutputStreams(HTTPServerRequest & request, HTTPServerResponse & response, bool internal_compress, size_t keep_alive_timeout);

    HTTPOutputStreams(Context & context, HTTPServerRequest & request, HTTPServerResponse & response, HTMLForm & form, size_t keep_alive_timeout);
};

using HTTPOutputStreamsPtr = std::unique_ptr<HTTPOutputStreams>;

}
