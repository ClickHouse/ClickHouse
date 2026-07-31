#pragma once

#include <config.h>
#include <Client/OAuthLogin.h>

#if USE_JWT_CPP && USE_SSL

#include <Poco/JSON/Object.h>

#include <string>

namespace DB
{

std::string urlEncodeOAuth(const std::string & value);
Poco::JSON::Object::Ptr postOAuthForm(const std::string & url, const std::string & body);
std::string runOAuthAuthCodeFlow(const OAuthCredentials & creds);
std::string runOAuthDeviceFlow(OAuthCredentials creds);

/// Defined in OAuthLogin.cpp, used by the flow runners to persist the refresh token.
void writeCachedRefreshToken(const std::string & client_id, const std::string & refresh_token);

}

#endif // USE_JWT_CPP && USE_SSL
