#pragma once
#include <Poco/Net/HTTPRequestHandler.h>

namespace DB
{
/** Simple ping handler, answers "Ok." to GET request
 */
class PingHandler : public Poco::Net::HTTPRequestHandler
{
public:
    PingHandler(size_t keep_alive_timeout_) : keep_alive_timeout(keep_alive_timeout_) {}
    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    size_t keep_alive_timeout;
};
}
