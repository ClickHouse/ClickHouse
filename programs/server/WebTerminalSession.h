#pragma once

#include <Core/Types.h>
#include "WebTerminalSessionQuery.h"


namespace DB
{

class WebTerminalSession
{
public:
    WebTerminalSession(Context & context_);

    void applySettings(const HTMLForm & params);

    void login(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params);

    void output(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params);

    void cancelQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params);

    void executeQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params);

    void configuration(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, const HTMLForm & params);

private:
    Context context;

    std::mutex mutex;
    std::optional<String> authenticated_token;
    ExpireUnorderedMap<String, WebTerminalSessionQuery> session_queries;

    void verifyingAuthenticatedToken(const HTMLForm & params);
};

String generateRandomString(size_t max_size);

ExpireUnorderedMap<String, WebTerminalSession> & getWebTerminalSessions();

}
