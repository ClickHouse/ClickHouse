#include <Access/HTTPAccessStorage.h>
#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/HTTPAuthClient.h>
#include <Access/Role.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/User.h>
#include <Access/resolveSetting.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <sstream>


namespace DB
{
namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// Returns the status and, for `200 OK`, the body as is: the body is parsed outside of `HTTPAuthClient`,
    /// so that a malformed body is not treated as a network error and is not retried.
    struct RawResponseParser
    {
        struct Result
        {
            Poco::Net::HTTPResponse::HTTPStatus status;
            String body;
        };

        Result parse(const Poco::Net::HTTPResponse & response, ReadBuffer * body) const
        {
            Result result{response.getStatus(), {}};
            if (result.status == Poco::Net::HTTPResponse::HTTP_OK && body)
                readStringUntilEOF(result.body, *body);
            return result;
        }
    };
}


HTTPAccessStorage::HTTPAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix)
    : IAccessStorage(storage_name_)
    , access_control(access_control_)
    , hash_key0(thread_local_rng())
    , hash_key1(thread_local_rng())
    , memory_storage(storage_name_, access_control.getChangesNotifier(), false)
{
    server_name = config.getString(prefix + ".server", "");
    if (server_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or empty 'server' field for HTTP user directory");

    if (config.has(prefix + ".allowed_roles"))
    {
        Poco::Util::AbstractConfiguration::Keys role_names;
        config.keys(prefix + ".allowed_roles", role_names);
        allowed_roles.insert(role_names.begin(), role_names.end());
    }

    allowed_role_prefix = config.getString(prefix + ".allowed_role_prefix", "");
    cache_ttl = std::chrono::seconds(config.getUInt64(prefix + ".authentication_cache_ttl", 0));
}


String HTTPAccessStorage::getStorageParamsJSON() const
{
    Poco::JSON::Object params_json;
    params_json.set("server", server_name);

    Poco::JSON::Array allowed_roles_json;
    for (const auto & role : allowed_roles)
        allowed_roles_json.add(role);
    params_json.set("allowed_roles", allowed_roles_json);

    params_json.set("allowed_role_prefix", allowed_role_prefix);
    params_json.set("authentication_cache_ttl", static_cast<UInt64>(cache_ttl.count()));

    std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(params_json, oss);
    return oss.str();
}


bool HTTPAccessStorage::exists(const UUID & id) const
{
    return memory_storage.exists(id);
}


std::optional<UUID> HTTPAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    return memory_storage.find(type, name);
}


std::vector<UUID> HTTPAccessStorage::findAllImpl(AccessEntityType type) const
{
    return memory_storage.findAll(type);
}


AccessEntityPtr HTTPAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.read(id, throw_if_not_exists);
}


std::optional<std::pair<String, AccessEntityType>> HTTPAccessStorage::readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const
{
    return memory_storage.readNameWithType(id, throw_if_not_exists);
}


std::vector<UUID> HTTPAccessStorage::resolveRoles(const Strings & role_names, const String & user_name) const
{
    std::vector<UUID> ids;
    for (const auto & role_name : role_names)
    {
        bool allowed = allowed_roles.contains(role_name) || (!allowed_role_prefix.empty() && role_name.starts_with(allowed_role_prefix));
        if (!allowed)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "Role '{}' returned by HTTP authentication server '{}' for user '{}' is not allowed", role_name, server_name, user_name);

        auto id = access_control.find<Role>(role_name);
        if (!id)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "Role '{}' returned by HTTP authentication server '{}' for user '{}' does not exist", role_name, server_name, user_name);
        if (std::find(ids.begin(), ids.end(), *id) == ids.end())
            ids.push_back(*id);
    }

    /// `Context::setUser` applies only the settings of the user's own roles, so fail closed on returned roles with settings:
    /// their own or of the roles granted to them, or of the profiles assigned to them with `TO`.
    if (!ids.empty())
    {
        auto roles_info = access_control.getEnabledRolesInfo(ids, {});
        if (!roles_info->settings_from_enabled_roles.empty()
            || *access_control.getEnabledSettingsInfo(UUID{}, {}, roles_info->enabled_roles, {}) != *access_control.getEnabledSettingsInfo(UUID{}, {}, {}, {}))
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "Role(s) returned by HTTP authentication server '{}' for user '{}' have settings or settings profiles, "
                "which are not supported for roles of the `http` user directory", server_name, user_name);
    }
    return ids;
}


UUID HTTPAccessStorage::materializeUser(const String & user_name) const
{
    if (auto id = memory_storage.find<User>(user_name))
        return *id;

    auto user = std::make_shared<User>();
    user->setName(user_name);
    user->authentication_methods.emplace_back(AuthenticationType::HTTP);
    user->authentication_methods.back().setHTTPAuthenticationServerName(server_name);

    /// A concurrent authentication of the same user may have inserted it already.
    UUID conflicting_id;
    if (auto id = memory_storage.insert(user, /* replace_if_exists = */ false, /* throw_if_exists = */ false, &conflicting_id))
        return *id;
    return conflicting_id;
}


std::optional<AuthResult> HTTPAccessStorage::authenticateImpl(
    const Credentials & credentials,
    const Poco::Net::IPAddress & /* address */,
    const ExternalAuthenticators & external_authenticators,
    const ClientInfo & client_info,
    bool throw_if_user_not_exists,
    bool /* allow_no_password */,
    bool /* allow_plaintext_password */) const
{
    const String & user_name = credentials.getUserName();

    auto not_found = [&]() -> std::optional<AuthResult>
    {
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, user_name, getStorageName());
        return std::nullopt;
    };

    auto make_result = [&](const UUID & user_id, SettingsChanges settings, std::vector<UUID> roles)
    {
        AuthenticationData authentication_data(AuthenticationType::HTTP);
        authentication_data.setHTTPAuthenticationServerName(server_name);
        return AuthResult{
            .user_id = user_id,
            .settings = std::move(settings),
            .authentication_data = std::move(authentication_data),
            .user_name = user_name,
            .external_roles = std::move(roles)};
    };

    if (!credentials.isReady())
        return not_found();

    /// HTTP Basic credentials cannot carry `:` in the user name: the service would see another user.
    if (user_name.contains(':'))
        return not_found();

    /// A user with this name belongs to another storage: neither shadow it nor send its password to the server.
    if (auto id = access_control.find<User>(user_name); id && !memory_storage.exists(*id))
        return not_found();

    if (typeid_cast<const AlwaysAllowCredentials *>(&credentials))
        return make_result(materializeUser(user_name), {}, {});

    const auto * basic_credentials = dynamic_cast<const BasicCredentials *>(&credentials);
    if (!basic_credentials)
        return not_found();

    const auto params = external_authenticators.getHTTPAuthenticationParams(server_name);

    /// The response depends on the URI, the password and the forwarded headers, so all of them are in the cache key.
    const std::set<String, HTTPFieldLess> forward_headers(params.forward_headers.begin(), params.forward_headers.end());
    SipHash hash(hash_key0, hash_key1);
    auto hash_string = [&hash](const String & s)
    {
        hash.update(s.size());
        hash.update(s);
    };
    hash_string(params.uri.toString());
    hash_string(basic_credentials->getPassword());
    for (const auto & [name, value] : client_info.http_headers)
    {
        if (forward_headers.contains(name))
        {
            hash_string(name);
            hash_string(value);
        }
    }
    const UInt128 key_hash = hash.get128();

    std::optional<Response> response;
    if (cache_ttl.count() > 0)
    {
        std::lock_guard lock(cache_mutex);
        auto it = cache.find(user_name);
        if (it != cache.end() && it->second.hash == key_hash && std::chrono::steady_clock::now() - it->second.time <= cache_ttl)
            response = it->second.response;
    }

    std::vector<UUID> role_ids;
    if (response)
    {
        role_ids = resolveRoles(response->roles, user_name);
    }
    else
    {
        auto raw = HTTPBasicAuthClient<RawResponseParser>(params).authenticate(user_name, basic_credentials->getPassword(), client_info.http_headers);
        if (raw.status == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
            return not_found();
        if (raw.status != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                "HTTP authentication server '{}' rejected user '{}' with status {}", server_name, user_name, static_cast<int>(raw.status));

        /// An empty body is the same as `{}`. Anything else must be an object with optional `roles` (array of strings)
        /// and `settings` (object). A malformed body is an authentication failure.
        response.emplace();
        if (!raw.body.empty())
        {
            Poco::JSON::Parser parser;
            auto json = parser.parse(raw.body);
            if (json.type() != typeid(Poco::JSON::Object::Ptr))
                throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "The response of HTTP authentication server '{}' is not a JSON object", server_name);
            const auto obj = json.extract<Poco::JSON::Object::Ptr>();

            if (obj->has("roles"))
            {
                const auto roles = obj->getArray("roles");
                if (!roles)
                    throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Field 'roles' in the response of HTTP authentication server '{}' is not an array", server_name);
                for (const auto & role : *roles)
                {
                    if (!role.isString())
                        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Field 'roles' in the response of HTTP authentication server '{}' contains a non-string value", server_name);
                    response->roles.push_back(role.extract<String>());
                }
            }

            if (obj->has("settings"))
            {
                const auto settings = obj->getObject("settings");
                if (!settings)
                    throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Field 'settings' in the response of HTTP authentication server '{}' is not an object", server_name);
                for (const auto & [key, value] : *settings)
                {
                    /// `settingStringToValueUtil` accepts an unknown name as a custom setting, so check the name first.
                    access_control.checkSettingNameIsAllowed(key);
                    response->settings.emplace_back(key, settingStringToValueUtil(key, value));
                }
            }
        }

        /// Only a validated response is cached.
        role_ids = resolveRoles(response->roles, user_name);

        if (cache_ttl.count() > 0)
        {
            std::lock_guard lock(cache_mutex);
            cache[user_name] = CacheEntry{.hash = key_hash, .time = std::chrono::steady_clock::now(), .response = *response};
        }
    }

    return make_result(materializeUser(user_name), std::move(response->settings), std::move(role_ids));
}

}
