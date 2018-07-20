#include <IO/HTTPCommon.h>

#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/config.h>
#include <Poco/Version.h>
#if USE_POCO_NETSSL
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/InvalidCertificateHandler.h>
#include <Poco/Net/PrivateKeyPassphraseHandler.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#endif
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/Application.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
    extern const int RECEIVED_ERROR_TOO_MANY_REQUESTS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
}


void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response, unsigned keep_alive_timeout)
{
    if (!response.getKeepAlive())
        return;

    Poco::Timespan timeout(keep_alive_timeout, 0);
    if (timeout.totalSeconds())
        response.set("Keep-Alive", "timeout=" + std::to_string(timeout.totalSeconds()));
}


void initSSL()
{
    // http://stackoverflow.com/questions/18315472/https-request-in-c-using-poco
#if USE_POCO_NETSSL
    struct Initializer
    {
        Initializer()
        {
            Poco::Net::initializeSSL();
        }
    };

    static Initializer initializer;
#endif
}


std::unique_ptr<Poco::Net::HTTPClientSession> makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts)
{
    bool is_ssl = static_cast<bool>(uri.getScheme() == "https");
    std::unique_ptr<Poco::Net::HTTPClientSession> session;

    if (is_ssl)
#if USE_POCO_NETSSL
        session = std::make_unique<Poco::Net::HTTPSClientSession>();
#else
        throw Exception("ClickHouse was built without HTTPS support", ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME);
#endif
    else
        session = std::make_unique<Poco::Net::HTTPClientSession>();

    session->setHost(DNSResolver::instance().resolveHost(uri.getHost()).toString());
    session->setPort(uri.getPort());

#if POCO_CLICKHOUSE_PATCH || POCO_VERSION >= 0x02000000
    session->setTimeout(timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout);
#else
    session->setTimeout(std::max({timeouts.connection_timeout, timeouts.send_timeout, timeouts.receive_timeout}));
#endif

    return session;
}


std::istream * receiveResponse(
    Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response)
{
    auto istr = &session.receiveResponse(response);
    auto status = response.getStatus();

    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        std::stringstream error_message;
        error_message << "Received error from remote server " << request.getURI() << ". HTTP status code: " << status << " "
                      << response.getReason() << ", body: " << istr->rdbuf();

        throw Exception(error_message.str(),
            status == HTTP_TOO_MANY_REQUESTS ? ErrorCodes::RECEIVED_ERROR_TOO_MANY_REQUESTS
                                             : ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER);
    }
    return istr;
}

}
