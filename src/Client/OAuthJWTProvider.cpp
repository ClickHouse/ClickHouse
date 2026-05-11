#include <config.h>

#if USE_JWT_CPP && USE_SSL

#include <Client/JWTProvider.h>
#include <Client/OAuthLogin.h>

#include <Poco/Timespan.h>
#include <Poco/Timestamp.h>

#include <iostream>
#include <memory>

namespace DB
{

/// JWTProvider subclass for the credentials-file OIDC path (--login=browser /
/// --login=device).  Extends JWTProvider so that Connection::sendQuery can call
/// getJWT() transparently to refresh the id_token before it expires, eliminating
/// the 1-hour session limit that arises when the token is obtained only once at
/// startup.
///
/// getJWT() delegates to obtainIDToken() which already handles the full lifecycle:
///   1. try cached refresh token from disk
///   2. run interactive flow (browser or device) if the refresh token is absent
///      or rejected
class OAuthJWTProvider : public JWTProvider
{
public:
    OAuthJWTProvider(OAuthCredentials creds, OAuthFlowMode mode)
        : JWTProvider("", creds.client_id, "", std::cerr, std::cerr)
        , creds_(std::move(creds))
        , mode_(mode)
    {}

    std::string getJWT() override
    {
        constexpr int EXPIRY_BUFFER_SECONDS = 30;

        if (!idp_access_token.empty()
            && Poco::Timestamp() < idp_access_token_expires_at - Poco::Timespan(EXPIRY_BUFFER_SECONDS, 0))
            return idp_access_token;

        // obtainIDToken tries the disk-cached refresh token first and falls back
        // to an interactive flow only when necessary.
        idp_access_token = obtainIDToken(creds_, mode_);
        idp_access_token_expires_at = getJwtExpiry(idp_access_token);
        return idp_access_token;
    }

private:
    OAuthCredentials creds_;
    OAuthFlowMode mode_;
};

std::shared_ptr<JWTProvider> createOAuthJWTProvider(
    const OAuthCredentials & creds, OAuthFlowMode mode)
{
    return std::make_shared<OAuthJWTProvider>(creds, mode);
}

} // namespace DB

#endif // USE_JWT_CPP && USE_SSL
