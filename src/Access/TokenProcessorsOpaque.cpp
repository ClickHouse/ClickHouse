#include "TokenProcessors.h"

#if USE_JWT_CPP
#include <Access/ParseJSON.h>
#include <Common/RemoteHostFilter.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <cmath>
#include <limits>

namespace DB {

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
    /// The JSON reply from provider has only a few key-value pairs, so no need for any advanced parsing.
    /// Reduce complexity by using picojson.
    picojson::object parseJSON(const String & json_string) {
        picojson::value jsonValue;
        std::string err = parseWholeJSON(jsonValue, json_string);

        if (!err.empty()) {
            throw std::runtime_error("JSON parsing error: " + err);
        }

        if (!jsonValue.is<picojson::object>()) {
            throw std::runtime_error("JSON is not an object");
        }

        return jsonValue.get<picojson::object>();
    }

    template<typename ValueType = std::string, bool throw_on_exception = true>
    std::optional<ValueType> getValueByKey(const picojson::object & jsonObject, const std::string & key) {
        auto it = jsonObject.find(key); // Find the key in the object
        if (it == jsonObject.end())
        {
            if constexpr (throw_on_exception)
                throw std::runtime_error("Key not found: " + key);
            else
                return std::nullopt;
        }

        const picojson::value & value = it->second;
        if (!value.is<ValueType>()) {
            if constexpr (throw_on_exception)
                throw std::runtime_error("Value for key '" + key + "' has incorrect type.");
            else
                return std::nullopt;
        }

        return value.get<ValueType>();
    }

    picojson::object getObjectFromURI(const Poco::URI & uri, const ConnectionTimeouts & timeouts, const String & token = "")
    {
        Poco::Net::HTTPResponse response;
        std::ostringstream responseString;

        Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery()};
        request.add("Accept", "application/json");
        if (!token.empty())
            request.add("Authorization", "Bearer " + token);

        if (uri.getScheme() == "https") {
            Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
            setTimeouts(session, timeouts);
            session.sendRequest(request);
            Poco::StreamCopier::copyStream(session.receiveResponse(response), responseString);
        }
        else
        {
            Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
            setTimeouts(session, timeouts);
            session.sendRequest(request);
            Poco::StreamCopier::copyStream(session.receiveResponse(response), responseString);
        }

        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Failed to get user info by access token, code: {}, reason: {}", response.getStatus(),
                            response.getReason());

        try
        {
            return parseJSON(responseString.str());
        }
        catch (const std::runtime_error & e)
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Failed to parse server response: {}", e.what());
        }
    }

    /// RFC 7662 form-POST with `client_secret_basic` auth. Returns parsed JSON;
    /// non-200 throws so callers can distinguish "inactive" (200+active:false)
    /// from "client auth or transport failure".
    picojson::object postFormToURI(const Poco::URI & uri,
                                   const std::vector<std::pair<String, String>> & form,
                                   const String & basic_user,
                                   const String & basic_password,
                                   const ConnectionTimeouts & timeouts)
    {
        Poco::Net::HTTPResponse response;
        std::ostringstream responseString;

        String body;
        for (const auto & [key, value] : form)
        {
            if (!body.empty())
                body += '&';
            String encoded_key;
            String encoded_value;
            Poco::URI::encode(key, "", encoded_key);
            Poco::URI::encode(value, "", encoded_value);
            body += encoded_key + "=" + encoded_value;
        }

        Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery(),
                                       Poco::Net::HTTPMessage::HTTP_1_1};
        request.setContentType("application/x-www-form-urlencoded");
        request.setContentLength(body.size());
        request.add("Accept", "application/json");
        if (!basic_user.empty())
        {
            Poco::Net::HTTPBasicCredentials creds(basic_user, basic_password);
            creds.authenticate(request);
        }

        auto send_and_receive = [&](Poco::Net::HTTPClientSession & session)
        {
            setTimeouts(session, timeouts);
            session.sendRequest(request) << body;
            Poco::StreamCopier::copyStream(session.receiveResponse(response), responseString);
        };

        if (uri.getScheme() == "https")
        {
            Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
            send_and_receive(session);
        }
        else
        {
            Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
            send_and_receive(session);
        }

        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "POST to '{}' returned HTTP {} ({})",
                            uri.toString(), static_cast<int>(response.getStatus()), response.getReason());

        try
        {
            return parseJSON(responseString.str());
        }
        catch (const std::runtime_error & e)
        {
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                            "Failed to parse JSON response from '{}': {}", uri.toString(), e.what());
        }
    }
}

GoogleTokenProcessor::GoogleTokenProcessor(const String & processor_name_,
                                           UInt64 token_cache_lifetime_,
                                           const String & username_claim_,
                                           const String & groups_claim_,
                                           const String & expected_audience_,
                                           const ConnectionTimeouts & timeouts_)
    : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_)
    , expected_audience(expected_audience_)
    , timeouts(timeouts_)
{
    /// Without an audience pin, this processor accepts any Google access token
    /// that authenticates the user against Google -- including tokens minted for
    /// completely unrelated OAuth clients (a classic confused-deputy scenario).
    /// Operators who actually want token-based auth almost always want it bound
    /// to their own client_id; surface this gap loudly at startup so it can't
    /// stay silently un-enforced.
    if (expected_audience.empty())
        LOG_WARNING(getLogger("TokenAuthentication"),
                    "{}: 'expected_audience' is not configured for Google token processor. "
                    "Any valid Google access token (regardless of issuing client) will be accepted; "
                    "set 'expected_audience' to the OAuth client_id this processor should accept.",
                    processor_name);
}

bool GoogleTokenProcessor::resolveAndValidate(TokenCredentials & credentials) const
{
    const String & token = credentials.getToken();

    std::unordered_map<String, String> user_info;
    picojson::object user_info_json = getObjectFromURI(Poco::URI("https://www.googleapis.com/oauth2/v3/userinfo"), timeouts, token);

    if (!user_info_json.contains("email"))
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "{}: Specified username_claim {} not found in token", processor_name, username_claim);

    user_info["email"] = getValueByKey<std::string, false>(user_info_json, "email").value_or("");

    user_info[username_claim] = getValueByKey(user_info_json, username_claim).value();

    String user_name = user_info[username_claim];

    auto token_info = getObjectFromURI(Poco::URI("https://www.googleapis.com/oauth2/v3/tokeninfo"), timeouts, token);

    /// Audience binding (H-10): the Google /tokeninfo endpoint authoritatively
    /// reports the OAuth client_id the access token was issued for in its 'aud'
    /// field. Without this check, a token minted for any other Google OAuth
    /// client (the user's mobile app, a third-party tool) would authenticate
    /// here too -- because Google /userinfo will happily honor any valid token.
    /// Refusing tokens whose 'aud' does not match the configured client pin is
    /// what makes the binding strict.
    if (!expected_audience.empty())
    {
        const auto aud = getValueByKey<std::string, false>(token_info, "aud").value_or("");
        if (aud != expected_audience)
        {
            LOG_TRACE(getLogger("TokenAuthentication"),
                      "{}: Google access token audience '{}' does not match configured 'expected_audience' '{}'; rejecting",
                      processor_name, aud, expected_audience);
            return false;
        }
    }

    /// Reject empty resolved username (M-27). `TokenCredentials::setUserName`
    /// leaves `is_ready=false` for empty input but the function would still
    /// return true; the cache would then accept an entry under user_name "",
    /// collapsing every empty-username token across all IdPs into the same
    /// dynamic ClickHouse user.
    if (user_name.empty())
    {
        LOG_TRACE(getLogger("TokenAuthentication"),
                  "{}: Resolved username from token is empty; rejecting", processor_name);
        return false;
    }

    credentials.setUserName(user_name);

    if (token_info.contains("exp"))
    {
        /// picojson stores all numerics as double; we need to validate the
        /// value is a finite, positive Unix timestamp that fits in time_t
        /// before casting.
        const double exp = getValueByKey<double>(token_info, "exp").value();
        if (!std::isfinite(exp) || exp <= 0.0
            || exp > static_cast<double>(std::numeric_limits<time_t>::max()))
            throw Exception(
                ErrorCodes::AUTHENTICATION_FAILED,
                "{}: tokeninfo response contains an out-of-range 'exp' value: {}",
                processor_name, exp);
        credentials.setExpiresAt(std::chrono::system_clock::from_time_t(static_cast<time_t>(exp)));
    }

    /// Groups info can only be retrieved if user email is known.
    /// If no email found in user info, we skip this step and there are no external roles for the user.
    if (!user_info["email"].empty())
    {
        std::set<String> external_groups_names;
        const Poco::URI get_groups_uri = Poco::URI("https://cloudidentity.googleapis.com/v1/groups/-/memberships:searchDirectGroups?query=member_key_id==" + user_info["email"] + "'");

        try
        {
            auto groups_response = getObjectFromURI(get_groups_uri, timeouts, token);

            if (!groups_response.contains("memberships") || !groups_response["memberships"].is<picojson::array>())
            {
                LOG_TRACE(getLogger("TokenAuthentication"),
                          "{}: Failed to get Google groups: invalid content in response from server", processor_name);
                return true;
            }

            for (const auto & group: groups_response["memberships"].get<picojson::array>())
            {
                if (!group.is<picojson::object>())
                {
                    LOG_TRACE(getLogger("TokenAuthentication"),
                              "{}: Failed to get Google groups: invalid content in response from server", processor_name);
                    continue;
                }

                auto group_data = group.get<picojson::object>();

                /// Guard against a missing or non-object `groupKey`. Without
                /// these checks `group_data["groupKey"].get<picojson::object>()`
                /// would auto-insert a null `picojson::value` (because picojson
                /// objects are `std::map<string, picojson::value>` and `[]`
                /// default-constructs on a missing key) and then throw
                /// `std::bad_cast` on the `.get<picojson::object>()` call --
                /// which the `catch (const Exception &)` below does NOT
                /// catch (`std::bad_cast` is `std::exception`-derived, not
                /// `DB::Exception`-derived). The uncaught exception used to
                /// propagate out of `resolveAndValidate` and abort auth.
                auto group_key_it = group_data.find("groupKey");
                if (group_key_it == group_data.end() || !group_key_it->second.is<picojson::object>())
                {
                    LOG_TRACE(getLogger("TokenAuthentication"),
                              "{}: Group entry without a 'groupKey' object; skipping", processor_name);
                    continue;
                }

                String group_name = getValueByKey<std::string, false>(group_key_it->second.get<picojson::object>(), "id").value_or("");
                if (!group_name.empty())
                {
                    external_groups_names.insert(group_name);
                    LOG_TRACE(getLogger("TokenAuthentication"),
                              "{}: User {}: new external group {}",
                              processor_name, quoteString(user_name), quoteString(group_name));
                }
            }

            credentials.setGroups(external_groups_names);
        }
        catch (const std::exception & e)
        {
            /// Defense in depth: catch `std::exception` (not just `DB::Exception`)
            /// so picojson's `std::bad_cast` and `std::runtime_error` -- and any
            /// other future deviation -- degrade to "no roles mapped" rather
            /// than aborting the whole authentication.
            LOG_TRACE(getLogger("TokenAuthentication"),
                      "{}: Failed to get Google groups, no external roles will be mapped. reason: {}", processor_name, e.what());
            return true;
        }
    }

    return true;
}

OpenIdTokenProcessor::OpenIdTokenProcessor(const String & processor_name_,
                                           UInt64 token_cache_lifetime_,
                                           const String & username_claim_,
                                           const String & groups_claim_,
                                           const String & expected_issuer_,
                                           const String & expected_audience_,
                                           const String & userinfo_endpoint_,
                                           const String & token_introspection_endpoint_,
                                           const String & introspection_client_id_,
                                           const String & introspection_client_secret_,
                                           const ConnectionTimeouts & timeouts_)
        : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_),
          userinfo_endpoint(userinfo_endpoint_),
          token_introspection_endpoint(token_introspection_endpoint_),
          expected_issuer(expected_issuer_),
          expected_audience(expected_audience_),
          introspection_client_id(introspection_client_id_),
          introspection_client_secret(introspection_client_secret_),
          timeouts(timeouts_)
{
}

OpenIdTokenProcessor::OpenIdTokenProcessor(const String & processor_name_,
                                           UInt64 token_cache_lifetime_,
                                           const String & username_claim_,
                                           const String & groups_claim_,
                                           const String & expected_issuer_,
                                           const String & expected_audience_,
                                           bool allow_no_expiration_,
                                           const String & openid_config_endpoint_,
                                           UInt64 verifier_leeway_,
                                           UInt64 jwks_cache_lifetime_,
                                           const String & introspection_client_id_,
                                           const String & introspection_client_secret_,
                                           const RemoteHostFilter & remote_host_filter_,
                                           bool allow_http_discovery_urls_,
                                           const ConnectionTimeouts & timeouts_)
    : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_),
      expected_issuer(expected_issuer_),
      expected_audience(expected_audience_),
      introspection_client_id(introspection_client_id_),
      introspection_client_secret(introspection_client_secret_),
      timeouts(timeouts_)
{
    /// Defense in depth: the discovery endpoint itself was already validated by
    /// the parser, but re-check here in case this constructor is reached via a
    /// future code path that bypasses parseTokenProcessor.
    try
    {
        remote_host_filter_.checkURL(Poco::URI(openid_config_endpoint_));
    }
    catch (const Exception & e)
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "{}: 'configuration_endpoint' URL '{}' is not in <remote_url_allow_hosts>: {}",
                        processor_name, openid_config_endpoint_, e.message());
    }

    const picojson::object openid_config = getObjectFromURI(Poco::URI(openid_config_endpoint_), timeouts);

    if (!openid_config.contains("userinfo_endpoint"))
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "{}: Cannot extract userinfo_endpoint from OIDC configuration at '{}'; consider manual configuration.",
                        processor_name, openid_config_endpoint_);

    /// The discovery document is untrusted: even with the issuer-anchor check
    /// below (H-08), a poisoned or misdirected response can still try to point
    /// trust-chain endpoints (jwks_uri, userinfo_endpoint, introspection_endpoint)
    /// at hosts the operator never approved. Refuse to load the processor when
    /// any returned URL is outside <remote_url_allow_hosts>; this prevents the
    /// server from reaching out to attacker-controlled endpoints during token
    /// validation.
    ///
    /// Additionally, refuse non-HTTPS schemes on discovery-returned URLs.
    /// Without this, an attacker who can MITM the discovery fetch (operator
    /// typed an `http://` configuration_endpoint, or any TLS interception path)
    /// can substitute a discovery doc whose `jwks_uri` is `http://169.254.169.254/...`
    /// (cloud metadata), `http://127.0.0.1:...` (local admin ports), or
    /// `http://kubernetes.default.svc:...` -- and the server issues a one-shot
    /// HTTP GET under its own process identity. `<remote_url_allow_hosts>` is
    /// the primary defense, but not every deployment configures it; an
    /// HTTPS-only rule on returned URLs is a cheap, orthogonal layer that
    /// blocks all three of those targets independently. Operators who run an
    /// IdP over plain HTTP intentionally can wire the trust chain manually
    /// (`userinfo_endpoint`/`token_introspection_endpoint`/`jwks_uri` directly)
    /// instead of relying on discovery, or opt out of this check by setting
    /// `<allow_http_discovery_urls>true</allow_http_discovery_urls>` on the
    /// processor (false by default; <remote_url_allow_hosts> still applies).
    if (allow_http_discovery_urls_)
        LOG_WARNING(getLogger("TokenAuthentication"),
                    "{}: 'allow_http_discovery_urls' is enabled; HTTPS check on URLs returned by OIDC discovery "
                    "is suppressed. Make sure <remote_url_allow_hosts> restricts which targets the server may "
                    "be redirected to via a poisoned discovery document.",
                    processor_name);
    auto require_allowed_discovery_url = [&](const std::string & url, const char * field)
    {
        Poco::URI parsed_uri(url);
        if (!allow_http_discovery_urls_ && parsed_uri.getScheme() != "https")
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "{}: OIDC discovery at '{}' returned non-HTTPS '{}' URL '{}' (scheme '{}'). "
                            "The trust-chain URLs from discovery must use HTTPS so a poisoned discovery "
                            "document cannot redirect token validation through internal endpoints "
                            "(cloud metadata, localhost, in-cluster service IPs). If the IdP genuinely "
                            "runs over plain HTTP, either configure the trust chain manually instead of "
                            "using 'configuration_endpoint', or set "
                            "'<allow_http_discovery_urls>true</allow_http_discovery_urls>' on this processor.",
                            processor_name, openid_config_endpoint_, field, url, parsed_uri.getScheme());

        try
        {
            remote_host_filter_.checkURL(parsed_uri);
        }
        catch (const Exception & e)
        {
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "{}: OIDC discovery at '{}' returned '{}' URL '{}' which is not in "
                            "<remote_url_allow_hosts>: {}",
                            processor_name, openid_config_endpoint_, field, url, e.message());
        }
    };

    require_allowed_discovery_url(getValueByKey(openid_config, "userinfo_endpoint").value(), "userinfo_endpoint");
    if (openid_config.contains("introspection_endpoint"))
        require_allowed_discovery_url(getValueByKey(openid_config, "introspection_endpoint").value(), "introspection_endpoint");
    if (openid_config.contains("jwks_uri"))
        require_allowed_discovery_url(getValueByKey(openid_config, "jwks_uri").value(), "jwks_uri");

    /// Anchor the discovery document to a known issuer when one is configured.
    ///
    /// OIDC Discovery 1.0 §4.3 / RFC 8414 §3.3 require the metadata's "issuer"
    /// to be tied to the URL used to fetch it. Without this anchor a poisoned
    /// or misdirected discovery response can redirect the entire trust chain
    /// (jwks_uri, userinfo_endpoint, introspection_endpoint) to URLs the
    /// operator never approved -- and because the embedded JWT verifier only
    /// enforces the `iss` claim when expected_issuer is non-empty, JWTs signed
    /// by the attacker's keys would be silently accepted at runtime.
    ///
    /// Policy:
    ///   - expected_issuer configured => discovery's "issuer" MUST match it
    ///                                   (refuse to construct on mismatch or
    ///                                   absence). Verifier is pinned to it.
    ///   - expected_issuer empty      => log a warning so the gap is visible
    ///                                   in operator logs, then proceed with
    ///                                   the historical (lax) behavior. The
    ///                                   verifier is left without an issuer
    ///                                   pin to preserve compatibility.
    const auto issuer_from_discovery = getValueByKey<std::string, false>(openid_config, "issuer").value_or("");

    if (!expected_issuer_.empty())
    {
        if (issuer_from_discovery.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "{}: OIDC discovery document at '{}' does not advertise an 'issuer'; "
                            "cannot verify it against the configured 'expected_issuer' '{}'.",
                            processor_name, openid_config_endpoint_, expected_issuer_);

        if (issuer_from_discovery != expected_issuer_)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "{}: OIDC discovery 'issuer' mismatch: configured 'expected_issuer' is '{}' "
                            "but discovery document at '{}' returned issuer '{}'. Refusing to load the "
                            "processor to avoid trusting metadata that belongs to a different issuer.",
                            processor_name, expected_issuer_, openid_config_endpoint_, issuer_from_discovery);
    }
    else
    {
        LOG_WARNING(getLogger("TokenAuthentication"),
                    "{}: 'expected_issuer' is not configured for OIDC discovery at '{}'. "
                    "The JWT 'iss' claim will NOT be enforced.", processor_name, openid_config_endpoint_);
    }

    userinfo_endpoint = Poco::URI(getValueByKey(openid_config, "userinfo_endpoint").value());
    if (openid_config.contains("introspection_endpoint"))
        token_introspection_endpoint = Poco::URI(getValueByKey(openid_config, "introspection_endpoint").value());

    const bool can_enforce_via_jwks = openid_config.contains("jwks_uri");
    const bool can_enforce_via_introspection =
        openid_config.contains("introspection_endpoint") && !introspection_client_id_.empty();

    /// Catch creds configured for a discovery doc that does not advertise an
    /// introspection endpoint -- otherwise the credentials would be silently
    /// ignored at runtime.
    if (!introspection_client_id_.empty() && !openid_config.contains("introspection_endpoint"))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "{}: 'introspection_client_id' / 'introspection_client_secret' are set but the OIDC "
                        "discovery at '{}' does not advertise an 'introspection_endpoint'.",
                        processor_name, openid_config_endpoint_);

    if (!can_enforce_via_jwks && !can_enforce_via_introspection
        && (!expected_issuer_.empty() || !expected_audience_.empty()))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "{}: 'expected_issuer' / 'expected_audience' need either a 'jwks_uri' or an "
                        "'introspection_endpoint' (with operator credentials) in the discovery doc at '{}'.",
                        processor_name, openid_config_endpoint_);

    if (openid_config.contains("jwks_uri"))
    {
        LOG_TRACE(getLogger("TokenAuthentication"), "{}: JWKS URI set, local JWT processing will be attempted", processor_name_);
        /// `expected_typ` empty for the same reason as the manual constructor.
        jwt_validator.emplace(processor_name_ + "jwks_val",
                              token_cache_lifetime_,
                              username_claim_,
                              groups_claim_,
                              expected_issuer_,
                              expected_audience_,
                              /*expected_typ=*/"",
                              allow_no_expiration_,
                              "",
                              verifier_leeway_,
                              getValueByKey(openid_config, "jwks_uri").value(),
                              jwks_cache_lifetime_,
                              timeouts);
    }
}

bool OpenIdTokenProcessor::runIntrospection(const String & token,
                                            std::chrono::system_clock::time_point & expires_at) const
{
    expires_at = {};

    picojson::object response;
    try
    {
        response = postFormToURI(token_introspection_endpoint,
                                 {{"token", token}, {"token_type_hint", "access_token"}},
                                 introspection_client_id,
                                 introspection_client_secret,
                                 timeouts);
    }
    catch (const Exception & e)
    {
        /// LOG_WARNING (not TRACE): a non-200 from the introspection endpoint
        /// almost always means the operator's `introspection_client_*` is
        /// wrong or the IdP is unreachable -- worth surfacing by default.
        LOG_WARNING(getLogger("TokenAuthentication"),
                    "{}: Token introspection request failed: {}", processor_name, e.message());
        return false;
    }

    /// active=true is authoritative per RFC 7662 §2.2.
    const auto active_opt = getValueByKey<bool, false>(response, "active");
    if (!active_opt.has_value() || !active_opt.value())
    {
        LOG_TRACE(getLogger("TokenAuthentication"),
                  "{}: Token introspection reported active=false (or missing); rejecting", processor_name);
        return false;
    }

    if (!expected_issuer.empty())
    {
        const auto iss = getValueByKey<std::string, false>(response, "iss").value_or("");
        if (iss != expected_issuer)
        {
            LOG_TRACE(getLogger("TokenAuthentication"),
                      "{}: Token introspection 'iss' '{}' does not match expected_issuer '{}'; rejecting",
                      processor_name, iss, expected_issuer);
            return false;
        }
    }

    /// `aud` may be a string or an array (RFC 7519 §4.1.3).
    if (!expected_audience.empty())
    {
        auto aud_it = response.find("aud");
        bool ok = false;
        if (aud_it != response.end())
        {
            const picojson::value & aud_val = aud_it->second;
            if (aud_val.is<std::string>())
                ok = (aud_val.get<std::string>() == expected_audience);
            else if (aud_val.is<picojson::array>())
                for (const auto & v : aud_val.get<picojson::array>())
                    if (v.is<std::string>() && v.get<std::string>() == expected_audience)
                        ok = true;
        }
        if (!ok)
        {
            LOG_TRACE(getLogger("TokenAuthentication"),
                      "{}: Token introspection 'aud' does not contain expected_audience '{}'; rejecting",
                      processor_name, expected_audience);
            return false;
        }
    }

    if (response.contains("exp"))
    {
        const auto exp_opt = getValueByKey<double, false>(response, "exp");
        const double exp = exp_opt.value_or(0.0);
        if (exp_opt.has_value() && std::isfinite(exp) && exp > 0.0
            && exp <= static_cast<double>(std::numeric_limits<time_t>::max()))
            expires_at = std::chrono::system_clock::from_time_t(static_cast<time_t>(exp));
        else
            /// IdP advertised an `exp` we cannot use. Authentication still
            /// succeeds (the token IS active), but the cache loses its tighter
            /// upper bound; surface so operators see IdP drift.
            LOG_WARNING(getLogger("TokenAuthentication"),
                        "{}: Token introspection returned malformed 'exp'; cache TTL falls back to token_cache_lifetime",
                        processor_name);
    }

    return true;
}

bool OpenIdTokenProcessor::resolveAndValidate(TokenCredentials & credentials) const
{
    const String & token = credentials.getToken();
    String username;
    picojson::object user_info_json;

    if (jwt_validator.has_value())
    {
        /// When a `jwt_validator` is configured, it owns the operator's
        /// `expected_issuer` / `expected_audience` / `allow_no_expiration`
        /// bindings. If it rejects the token we MUST NOT fall back to the
        /// userinfo endpoint: userinfo only confirms "the IdP describes this
        /// user", it has no notion of the operator-pinned audience or issuer
        /// and does not enforce the local expiration policy. Falling back here
        /// would silently bypass exactly the bindings the operator opted into,
        /// e.g. a JWT with the wrong `aud` would still authenticate because
        /// the IdP's own userinfo accepts it for itself.
        if (!jwt_validator.value().resolveAndValidate(credentials))
        {
            /// DEBUG, not TRACE: this is the binding-rejection path. Operators
            /// running with DEBUG enabled will see a clear signal that the
            /// JWT-fastpath (which enforces `expected_issuer` / `expected_audience`
            /// / `allow_no_expiration`) rejected a token. The auth failure itself
            /// is also visible to the client, but the log line tells the operator
            /// *why* it was rejected on the local side.
            LOG_DEBUG(getLogger("TokenAuthentication"),
                      "{}: Local JWT validation rejected the token. Refusing to fall back to "
                      "userinfo: the operator-configured bindings (expected_issuer / expected_audience / "
                      "allow_no_expiration) cannot be enforced by userinfo, and a fallback would silently "
                      "bypass them.",
                      processor_name);
            return false;
        }

        try
        {
            auto decoded_token = jwt::decode(token);
            user_info_json = decoded_token.get_payload_json();
            username = getValueByKey(user_info_json, username_claim).value();

            if (decoded_token.has_expires_at())
                credentials.setExpiresAt(decoded_token.get_expires_at());
        }
        catch (const std::exception & ex)
        {
            /// WARNING: validation passed but extracting the payload locally
            /// failed -- a genuinely rare condition (the same token was just
            /// successfully verified, so its bytes ARE a valid JWT). The
            /// processor is about to fall back to userinfo for username
            /// extraction. Bindings were already enforced by `jwt_validator`,
            /// so this fallback is safe -- but the underlying mismatch
            /// (decode failure on a verified token) usually means an IdP
            /// behavioral change, a clock skew, or a payload-format drift,
            /// and operators should know about it loudly.
            LOG_WARNING(getLogger("TokenAuthentication"),
                        "{}: JWT validation succeeded but payload extraction failed: {}. "
                        "Falling back to userinfo for username; the operator-configured "
                        "bindings have ALREADY been enforced by JWT validation, so this "
                        "fallback is safe -- but the decode failure indicates an unexpected "
                        "JWT shape from the IdP.",
                        processor_name, ex.what());
        }
    }

    /// Run introspection whenever the operator configured it -- the JWT
    /// fast-path validates signature/exp but cannot detect server-side
    /// revocation, which is the whole reason to add introspection.
    if (!token_introspection_endpoint.empty() && !introspection_client_id.empty())
    {
        std::chrono::system_clock::time_point introspection_expires_at;
        if (!runIntrospection(token, introspection_expires_at))
            return false;
        if (introspection_expires_at != std::chrono::system_clock::time_point{})
            credentials.setExpiresAt(introspection_expires_at);
    }

    if (username.empty() || user_info_json.empty())
    {
        try
        {
            user_info_json = getObjectFromURI(userinfo_endpoint, timeouts, token);
            username = getValueByKey(user_info_json, username_claim).value();
        }
        catch (...)
        {
            return false;
        }
    }

    if (user_info_json.empty())
    {
        LOG_TRACE(getLogger("TokenAuthentication"), "{}: Failed to obtain user info", processor_name);
        return false;
    }

    if (username.empty())
    {
        LOG_TRACE(getLogger("TokenAuthentication"), "{}: Failed to get username", processor_name);
        return false;
    }

    credentials.setUserName(username);

    /// For now, list of groups is expected in a claim with specified name either in token itself or in userinfo response (Keycloak works this way)
    /// TODO: add support for custom endpoints for retrieving groups. Keycloak lists groups in /userinfo and token itself, which is not always the case.
    if (!groups_claim.empty() && user_info_json.contains(groups_claim))
    {
        if (!user_info_json[groups_claim].is<picojson::array>())
        {
            LOG_TRACE(getLogger("TokenAuthentication"),
                      "{}: Failed to extract groups: invalid content in user data", processor_name);
            return true;
        }

        std::set<String> external_groups_names;

        picojson::array groups_array = user_info_json[groups_claim].get<picojson::array>();
        for (const auto & group: groups_array)
        {
            if (group.is<std::string>())
                external_groups_names.insert(group.get<std::string>());
        }
        credentials.setGroups(external_groups_names);
    }

    return true;
}

}
#endif
