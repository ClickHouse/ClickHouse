#include "TokenProcessors.h"

#if USE_JWT_CPP
#include <Common/logger_useful.h>
#include <Poco/StreamCopier.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

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
        std::string err = picojson::parse(jsonValue, json_string);

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

    picojson::object getObjectFromURI(const Poco::URI & uri, const String & token = "")
    {
        Poco::Net::HTTPResponse response;
        std::ostringstream responseString;

        Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery()};
        if (!token.empty())
            request.add("Authorization", "Bearer " + token);

        if (uri.getScheme() == "https") {
            Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
            session.sendRequest(request);
            Poco::StreamCopier::copyStream(session.receiveResponse(response), responseString);
        }
        else
        {
            Poco::Net::HTTPClientSession session(uri.getHost(), uri.getPort());
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
}

bool GoogleTokenProcessor::resolveAndValidate(TokenCredentials & credentials) const
{
    const String & token = credentials.getToken();

    std::unordered_map<String, String> user_info;
    picojson::object user_info_json = getObjectFromURI(Poco::URI("https://www.googleapis.com/oauth2/v3/userinfo"), token);

    if (!user_info_json.contains("email"))
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "{}: Specified username_claim {} not found in token", processor_name, username_claim);

    user_info["email"] = getValueByKey<std::string, false>(user_info_json, "email").value_or("");

    user_info[username_claim] = getValueByKey(user_info_json, username_claim).value();

    String user_name = user_info[username_claim];

    credentials.setUserName(user_name);

    auto token_info = getObjectFromURI(Poco::URI("https://www.googleapis.com/oauth2/v3/tokeninfo"), token);
    if (token_info.contains("exp"))
        credentials.setExpiresAt(std::chrono::system_clock::from_time_t((getValueByKey<time_t>(token_info, "exp").value())));

    /// Groups info can only be retrieved if user email is known.
    /// If no email found in user info, we skip this step and there are no external roles for the user.
    if (!user_info["email"].empty())
    {
        std::set<String> external_groups_names;
        const Poco::URI get_groups_uri = Poco::URI("https://cloudidentity.googleapis.com/v1/groups/-/memberships:searchDirectGroups?query=member_key_id==" + user_info["email"] + "'");

        try
        {
            auto groups_response = getObjectFromURI(get_groups_uri, token);

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
                String group_name = getValueByKey<std::string, false>(group_data["groupKey"].get<picojson::object>(), "id").value_or("");
                if (!group_name.empty())
                {
                    external_groups_names.insert(group_name);
                    LOG_TRACE(getLogger("TokenAuthentication"),
                              "{}: User {}: new external group {}", processor_name, user_name, group_name);
                }
            }

            credentials.setGroups(external_groups_names);
        }
        catch (const Exception & e)
        {
            /// Could not get groups info. Log it and skip it.
            LOG_TRACE(getLogger("TokenAuthentication"),
                      "{}: Failed to get Google groups, no external roles will be mapped. reason: {}", processor_name, e.what());
            return true;
        }
    }

    return true;
}

bool AzureTokenProcessor::resolveAndValidate(TokenCredentials & credentials) const
{
    /// Token is a JWT in this case, but we cannot directly verify it against Azure AD JWKS.
    /// We will not trust user data in this token except for 'exp' value to determine caching duration.
    /// Explanation here: https://stackoverflow.com/questions/60778634/failing-signature-validation-of-jwt-tokens-from-azure-ad
    /// Let Azure validate it: only valid tokens will be accepted.
    /// Use GET https://graph.microsoft.com/oidc/userinfo to verify token and get user info at the same time

    const String & token = credentials.getToken();

    try
    {
        picojson::object user_info_json = getObjectFromURI(Poco::URI("https://graph.microsoft.com/oidc/userinfo"), token);
        String username = getValueByKey(user_info_json, username_claim).value();

        if (!username.empty())
            credentials.setUserName(username);
        else
            LOG_TRACE(getLogger("TokenAuthentication"), "{}: Failed to get username with token", processor_name);

    }
    catch (...)
    {
        return false;
    }

    try
    {
        credentials.setExpiresAt(jwt::decode(token).get_expires_at());
    }
    catch (...) {
        LOG_TRACE(getLogger("TokenAuthentication"),
                  "{}: No expiration data found in a valid token, will use default cache lifetime", processor_name);
    }

    std::set<String> external_groups_names;
    const Poco::URI get_groups_uri = Poco::URI("https://graph.microsoft.com/v1.0/me/memberOf");

    try
    {
        auto groups_response = getObjectFromURI(get_groups_uri, token);

        if (!groups_response.contains("value") || !groups_response["value"].is<picojson::array>())
        {
            LOG_TRACE(getLogger("TokenAuthentication"),
                      "{}: Failed to get Azure groups: invalid content in response from server", processor_name);
            return true;
        }

        picojson::array groups_array = groups_response["value"].get<picojson::array>();

        for (const auto & group: groups_array)
        {
            /// Got some invalid response. Ignore this, log this.
            if (!group.is<picojson::object >())
            {
                LOG_TRACE(getLogger("TokenAuthentication"),
                          "{}: Failed to get Azure groups: invalid content in response from server", processor_name);
                continue;
            }

            auto group_data = group.get<picojson::object>();
            if (!group_data.contains("displayName"))
                continue;

            String group_name = getValueByKey<std::string, false>(group_data, "displayName").value_or("");
            if (!group_name.empty())
            {
                external_groups_names.insert(group_name);
                LOG_TRACE(getLogger("TokenAuthentication"), "{}: User {}: new external group {}", processor_name, credentials.getUserName(), group_name);
            }
        }
    }
    catch (const Exception & e)
    {
        /// Could not get groups info. Log it and skip it.
        LOG_TRACE(getLogger("TokenAuthentication"),
                  "{}: Failed to get Azure groups, no external roles will be mapped. reason: {}", processor_name, e.what());
        return true;
    }

    credentials.setGroups(external_groups_names);
    return true;
}

OpenIdTokenProcessor::OpenIdTokenProcessor(const String & processor_name_,
                                           UInt64 token_cache_lifetime_,
                                           const String & username_claim_,
                                           const String & groups_claim_,
                                           const String & userinfo_endpoint_,
                                           const String & token_introspection_endpoint_,
                                           UInt64 verifier_leeway_,
                                           const String & jwks_uri_,
                                           UInt64 jwks_cache_lifetime_)
        : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_),
          userinfo_endpoint(userinfo_endpoint_), token_introspection_endpoint(token_introspection_endpoint_)
{
    if (!jwks_uri_.empty())
    {
        LOG_TRACE(getLogger("TokenAuthentication"), "{}: JWKS URI set, local JWT processing will be attempted", processor_name_);
        jwt_validator.emplace(processor_name_ + "jwks_val",
                              token_cache_lifetime_,
                              username_claim_,
                              groups_claim_,
                              "",
                              verifier_leeway_,
                              jwks_uri_,
                              jwks_cache_lifetime_);
    }
}

OpenIdTokenProcessor::OpenIdTokenProcessor(const String & processor_name_,
                                           UInt64 token_cache_lifetime_,
                                           const String & username_claim_,
                                           const String & groups_claim_,
                                           const String & openid_config_endpoint_,
                                           UInt64 verifier_leeway_,
                                           UInt64 jwks_cache_lifetime_)
    : ITokenProcessor(processor_name_, token_cache_lifetime_, username_claim_, groups_claim_)
{
    const picojson::object openid_config = getObjectFromURI(Poco::URI(openid_config_endpoint_));

    if (!openid_config.contains("userinfo_endpoint") || !openid_config.contains("introspection_endpoint"))
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "{}: Cannot extract userinfo_endpoint or introspection_endpoint from OIDC configuration, consider manual configuration.", processor_name);

    if (openid_config.contains("jwks_uri"))
    {
        LOG_TRACE(getLogger("TokenAuthentication"), "{}: JWKS URI set, local JWT processing will be attempted", processor_name_);
        jwt_validator.emplace(processor_name_ + "jwks_val",
                              token_cache_lifetime_,
                              username_claim_,
                              groups_claim_,
                              "",
                              verifier_leeway_,
                              getValueByKey(openid_config, "jwks_uri").value(),
                              jwks_cache_lifetime_);
    }
}

bool OpenIdTokenProcessor::resolveAndValidate(TokenCredentials & credentials) const
{
    const String & token = credentials.getToken();
    String username;
    picojson::object user_info_json;

    if (jwt_validator.has_value() && jwt_validator.value().resolveAndValidate(credentials))
    {
        try
        {
            auto decoded_token = jwt::decode(token);
            user_info_json = decoded_token.get_payload_json();
            username = getValueByKey(user_info_json, username_claim).value();

            /// TODO: Now we work only with Keycloak -- and it provides expires_at in token itself. Need to add actual token introspection logic for other OIDC providers.
            if (decoded_token.has_expires_at())
                credentials.setExpiresAt(decoded_token.get_expires_at());
        }
        catch (const std::exception & ex)
        {
            LOG_TRACE(getLogger("TokenAuthentication"), "{}: Failed to process token as JWT: {}", processor_name, ex.what());
        }
    }

    /// If username or user info is empty -- local validation failed, trying introspection via provider
    if (username.empty() || user_info_json.empty())
    {
        try
        {
            user_info_json = getObjectFromURI(userinfo_endpoint, token);
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
