#include <Interpreters/InterserverIOHandler.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <Common/Exception.h>

#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
}

void InterserverIOEndpoint::authenticate(const HTTPServerRequest & request) const
{
    /// Reject a `Bearer` credential by default; an endpoint that accepts one overrides this.
    /// Basic / no-credential requests were already validated by the shared check.
    if (!request.hasCredentials())
        return;

    String scheme;
    String info;
    request.getCredentials(scheme, info);
    if (Poco::icompare(scheme, "Bearer") == 0)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "This interserver endpoint does not accept bearer authentication");
}

}
