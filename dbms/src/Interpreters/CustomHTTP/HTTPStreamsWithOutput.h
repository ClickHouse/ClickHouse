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


/* Raw data
 * ↓
 * CascadeWriteBuffer out_maybe_delayed_and_compressed (optional)
 * ↓ (forwards data if an overflow is occur or explicitly via pushDelayedResults)
 * CompressedWriteBuffer out_maybe_compressed (optional)
 * ↓
 * WriteBufferFromHTTPServerResponse out
 */
struct HTTPStreamsWithOutput
{
    using HTTPResponseBufferPtr = std::shared_ptr<WriteBufferFromHTTPServerResponse>;

    HTTPResponseBufferPtr out;
    /// Points to 'out' or to CompressedWriteBuffer(*out), depending on settings.
    std::shared_ptr<WriteBuffer> out_maybe_compressed;
    /// Points to 'out' or to CompressedWriteBuffer(*out) or to CascadeWriteBuffer.
    std::shared_ptr<WriteBuffer> out_maybe_delayed_and_compressed;

    void finalize() const;

    WriteBufferPtr createMaybeDelayedAndCompressionEndpoint(Context & context, HTMLForm & form, WriteBufferPtr & endpoint);

    WriteBufferPtr createMaybeCompressionEndpoint(HTMLForm & form, std::shared_ptr<WriteBufferFromHTTPServerResponse> & endpoint);

    HTTPResponseBufferPtr createEndpoint(HTTPServerRequest & request, HTTPServerResponse & response, size_t keep_alive_timeout);

    void attachSettings(Context & context, Settings & settings, HTTPServerRequest & request);

    void attachRequestAndResponse(Context & context, HTTPServerRequest & request, HTTPServerResponse & response, HTMLForm & form, size_t keep_alive_timeout);
};

}
