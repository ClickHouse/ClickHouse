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
    /// `InterserverIOHTTPHandler::checkAuthentication` validates the server-wide interserver
    /// credentials (HTTP Basic) but defers a `Bearer` credential to the target endpoint. An
    /// endpoint that does not implement bearer authentication must reject it here, otherwise
    /// presenting a bearer token would bypass the interserver credentials entirely. Basic and
    /// no-credential requests were already accepted by the shared check, so they pass.
    if (!request.hasCredentials())
        return;

    String scheme;
    String info;
    request.getCredentials(scheme, info);
    if (Poco::icompare(scheme, "Bearer") == 0)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "This interserver endpoint does not accept bearer authentication");
}

}
