#include <Client/ExternalIdpJwtProvider.h>
#include <Common/Exception.h>
#include <config.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/Dynamic/Var.h>

#include <thread>
#include <chrono>
#include <cstdlib>
#include <iostream>

namespace DB
{


ExternalIdpJwtProvider::ExternalIdpJwtProvider(
    std::string auth_url,
    std::string client_id,
    std::ostream & out,
    std::ostream & err)
    : JwtProvider(std::move(auth_url), std::move(client_id), out, err)
{
}

std::string ExternalIdpJwtProvider::getJWT()
{
    Poco::Timestamp now;
    Poco::Timestamp expiration_buffer = 5 * Poco::Timespan::SECONDS;

    if (!idp_access_token.empty() && now < idp_access_token_expires_at - expiration_buffer)
        return idp_access_token;

    if (!idp_refresh_token.empty())
    {
        if (refreshIdPAccessToken())
            return idp_access_token;
    }

    if (initialLogin())
        return idp_access_token;

    error_stream << "Failed to obtain a valid JWT from the external provider." << std::endl;
    return "";
}

}
