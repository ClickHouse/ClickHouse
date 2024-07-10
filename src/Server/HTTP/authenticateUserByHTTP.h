#pragma once

#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
class HTTPServerRequest;
class HTMLForm;
class HTTPServerResponse;
class Session;
class Credentials;

/// Authenticates a user via HTTP protocol and initializes a session.
/// Usually retrieves the name and the password for that user from either the request's headers or from the query parameters.
/// Returns true when the user successfully authenticated,
/// the session instance will be configured accordingly, and the request_credentials instance will be dropped.
/// Returns false when the user is not authenticated yet, and the HTTP_UNAUTHORIZED response is sent with the "WWW-Authenticate" header,
/// in this case the `request_credentials` instance must be preserved until the next request or until any exception.
/// Throws an exception if authentication failed.
bool authenticateUserByHTTP(
    const HTTPServerRequest & request,
    const HTMLForm & params,
    HTTPServerResponse & response,
    Session & session,
    std::unique_ptr<Credentials> & request_credentials,
    ContextPtr global_context,
    LoggerPtr log);

}
