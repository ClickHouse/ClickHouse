#pragma once

#include <config.h>
#include <Client/OAuthLogin.h>

#if USE_JWT_CPP && USE_SSL

#include <Poco/URI.h>

#include <memory>
#include <string>

namespace DB
{

/// Provider-specific behavior for OAuth/OIDC flows.
/// To add a new provider: subclass, implement all virtuals, add matches() check,
/// and register in IOAuthProviderPolicy::create().
class IOAuthProviderPolicy
{
public:
    virtual ~IOAuthProviderPolicy() = default;

    virtual std::string getAuthCodeScope() const = 0;
    virtual bool useAccessTypeOfflineForAuthCode() const = 0;
    virtual std::string getDeviceScope() const = 0;
    virtual std::string resolveDeviceAuthorizationEndpoint(const OAuthCredentials & creds) const = 0;

    static std::unique_ptr<IOAuthProviderPolicy> create(const OAuthCredentials & creds);
};

class GoogleOAuthProviderPolicy final : public IOAuthProviderPolicy
{
public:
    static bool matches(const OAuthCredentials & creds)
    {
        const std::string & host = Poco::URI(creds.token_uri).getHost();
        return host == "oauth2.googleapis.com" || host == "accounts.google.com";
    }

    std::string getAuthCodeScope() const override { return "openid email profile"; }
    bool useAccessTypeOfflineForAuthCode() const override { return true; }
    std::string getDeviceScope() const override { return "openid email profile"; }
    std::string resolveDeviceAuthorizationEndpoint(const OAuthCredentials & creds) const override;
};

class GenericOAuthProviderPolicy final : public IOAuthProviderPolicy
{
public:
    std::string getAuthCodeScope() const override { return "openid email profile offline_access"; }
    bool useAccessTypeOfflineForAuthCode() const override { return false; }
    std::string getDeviceScope() const override { return "openid email profile offline_access"; }
    std::string resolveDeviceAuthorizationEndpoint(const OAuthCredentials & creds) const override;
};

}

#endif // USE_JWT_CPP && USE_SSL
