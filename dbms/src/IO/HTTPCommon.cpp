#include <IO/HTTPCommon.h>

#include <Common/config.h>
#if Poco_NetSSL_FOUND
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/InvalidCertificateHandler.h>
#include <Poco/Net/PrivateKeyPassphraseHandler.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#endif
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/Application.h>


namespace DB
{
void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response, unsigned keep_alive_timeout)
{
    if (!response.getKeepAlive())
        return;

    Poco::Timespan timeout(keep_alive_timeout, 0);
    if (timeout.totalSeconds())
        response.set("Keep-Alive", "timeout=" + std::to_string(timeout.totalSeconds()));
}

std::once_flag ssl_init_once;

void SSLInit()
{
    // http://stackoverflow.com/questions/18315472/https-request-in-c-using-poco
#if Poco_NetSSL_FOUND
    Poco::Net::initializeSSL();
#endif
}
}
