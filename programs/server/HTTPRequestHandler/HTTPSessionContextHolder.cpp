#include "HTTPSessionContextHolder.h"
#include <IO/ReadBufferFromString.h>
#include <Poco/Net/HTTPBasicCredentials.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int REQUIRED_PASSWORD;
    extern const int INVALID_SESSION_TIMEOUT;
}

static std::chrono::steady_clock::duration parseSessionTimeout(
    const Poco::Util::AbstractConfiguration & config,
    const HTMLForm & params)
{
    unsigned session_timeout = config.getInt("default_session_timeout", 60);

    if (params.has("session_timeout"))
    {
        unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
        std::string session_timeout_str = params.get("session_timeout");

        ReadBufferFromString buf(session_timeout_str);
        if (!tryReadIntText(session_timeout, buf) || !buf.eof())
            throw Exception("Invalid session timeout: '" + session_timeout_str + "'", ErrorCodes::INVALID_SESSION_TIMEOUT);

        if (session_timeout > max_session_timeout)
            throw Exception("Session timeout '" + session_timeout_str + "' is larger than max_session_timeout: " + toString(max_session_timeout)
                            + ". Maximum session timeout could be modified in configuration file.",
                            ErrorCodes::INVALID_SESSION_TIMEOUT);
    }

    return std::chrono::seconds(session_timeout);
}

HTTPSessionContextHolder::~HTTPSessionContextHolder()
{
    if (session_context)
        session_context->releaseSession(session_id, session_timeout);
}

void HTTPSessionContextHolder::authentication(Poco::Net::HTTPServerRequest & request, HTMLForm & params)
{
    auto user = request.get("X-ClickHouse-User", "");
    auto password = request.get("X-ClickHouse-Key", "");
    auto quota_key = request.get("X-ClickHouse-Quota", "");

    if (user.empty() && password.empty() && quota_key.empty())
    {
        /// User name and password can be passed using query parameters
        /// or using HTTP Basic auth (both methods are insecure).
        if (request.hasCredentials())
        {
            Poco::Net::HTTPBasicCredentials credentials(request);

            user = credentials.getUsername();
            password = credentials.getPassword();
        }
        else
        {
            user = params.get("user", "default");
            password = params.get("password", "");
        }

        quota_key = params.get("quota_key", "");
    }
    else
    {
        /// It is prohibited to mix different authorization schemes.
        if (request.hasCredentials()
            || params.has("user")
            || params.has("password")
            || params.has("quota_key"))
        {
            throw Exception("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and other authentication methods simultaneously", ErrorCodes::REQUIRED_PASSWORD);
        }
    }

    std::string query_id = params.get("query_id", "");
    query_context.setUser(user, password, request.clientAddress(), quota_key);
    query_context.setCurrentQueryId(query_id);
}

HTTPSessionContextHolder::HTTPSessionContextHolder(Context & query_context_, Poco::Net::HTTPServerRequest & request, HTMLForm & params)
    : query_context(query_context_)
{
    authentication(request, params);

    {
        session_id = params.get("session_id", "");

        if (!session_id.empty())
        {
            session_timeout = parseSessionTimeout(query_context.getConfigRef(), params);
            session_context = query_context.acquireSession(session_id, session_timeout, params.check<String>("session_check", "1"));

            query_context = *session_context;
            query_context.setSessionContext(*session_context);
        }
    }
}

}
