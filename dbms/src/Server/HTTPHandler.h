#pragma once

#include <Common/CurrentMetrics.h>
#include "Server.h"


namespace CurrentMetrics
{
    extern const Metric HTTPConnection;
}

namespace DB
{

class WriteBufferFromHTTPServerResponse;
class CascadeWriteBuffer;


class HTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit HTTPHandler(Server & server_);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    struct Output
    {
        /* Raw data
         * ↓
         * CascadeWriteBuffer out_maybe_delayed_and_compressed (optional)
         * ↓ (forwards data if an overflow is occur or explicitly via pushDelayedResults)
         * CompressedWriteBuffer out_maybe_compressed (optional)
         * ↓
         * WriteBufferFromHTTPServerResponse out
         */

        std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
        /// Points to 'out' or to CompressedWriteBuffer(*out), depending on settings.
        std::shared_ptr<WriteBuffer> out_maybe_compressed;
        /// Points to 'out' or to CompressedWriteBuffer(*out) or to CascadeWriteBuffer.
        std::shared_ptr<WriteBuffer> out_maybe_delayed_and_compressed;

        inline bool hasDelayed() const
        {
            return out_maybe_delayed_and_compressed != out_maybe_compressed;
        }
    };

    Server & server;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

    Logger * log;

    /// Also initializes 'used_output'.
    void processQuery(
        Poco::Net::HTTPServerRequest & request,
        HTMLForm & params,
        Poco::Net::HTTPServerResponse & response,
        Output & used_output);

    void trySendExceptionToClient(const std::string & s, int exception_code,
        Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
        Output & used_output);

    void pushDelayedResults(Output & used_output);
};

}
