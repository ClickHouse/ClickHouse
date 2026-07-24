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
    /// `InterserverIOHTTPHandler::checkAuthentication` defers a `Bearer` credential to the target
    /// endpoint only when the endpoint's `acceptsBearerAuth` returns true; an endpoint that does
    /// not accept bearer never reaches this default for a bearer request. This default is the
    /// safety net for that contract: reject a `Bearer` credential so an endpoint that neither
    /// overrides `acceptsBearerAuth` nor `authenticate` can never be entered with a bearer token.
    /// Basic and no-credential requests were already validated by the shared check, so they pass.
    if (!request.hasCredentials())
        return;

    String scheme;
    String info;
    request.getCredentials(scheme, info);
    if (Poco::icompare(scheme, "Bearer") == 0)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "This interserver endpoint does not accept bearer authentication");
}

}
