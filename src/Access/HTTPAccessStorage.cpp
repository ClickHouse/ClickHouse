#include <Access/HTTPAccessStorage.h>

#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/Role.h>
#include <Access/resolveSetting.h>
#include <Access/SettingsProfile.h>
#include <Access/User.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <base/scope_guard.h>

namespace ProfileEvents
{
    extern const Event HTTPUserDirectoryAuthRequests;
    extern const Event HTTPUserDirectoryAuthFailures;
    extern const Event HTTPUserDirectoryAuthMicroseconds;
    extern const Event HTTPUserDirectoryUsersCreated;
    extern const Event HTTPUserDirectoryCacheLimitExceeded;
}

namespace CurrentMetrics
{
    extern const Metric HTTPUserDirectoryCachedUsers;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_ADDRESS_PATTERN_TYPE;
}

HTTPAccessStorage::HTTPAccessStorage(
    const String & storage_name_,
    AccessControl & access_control_,
    const Poco::Util::AbstractConfiguration & config,
    const String & prefix)
    : IAccessStorage(storage_name_)
    , access_control(access_control_)
    , memory_storage(storage_name_, access_control_.getChangesNotifier(), /* allow_backup_= */ false)
{
    setConfiguration(config, prefix);
}

HTTPAccessStorage::~HTTPAccessStorage()
{
    CurrentMetrics::sub(CurrentMetrics::HTTPUserDirectoryCachedUsers, cached_user_count);
}

AuthenticationData HTTPAccessStorage::makeAuthenticationData(time_t valid_until) const
{
    AuthenticationData auth_data(AuthenticationType::HTTP);
    auth_data.setHTTPAuthenticationServerName(http_auth_server_name);
    auth_data.setHTTPAuthenticationScheme(HTTPAuthenticationScheme::BASIC);
    auth_data.setValidUntil(valid_until);
    return auth_data;
}

void HTTPAccessStorage::setConfiguration(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    http_auth_server_name = config.getString(prefix + ".server", "");
    if (http_auth_server_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'server' field for HTTP user directory");

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(prefix, keys);
    for (const auto & key : keys)
    {
        if (key == "allowed_roles")
        {
            Poco::Util::AbstractConfiguration::Keys role_keys;
            config.keys(prefix + ".allowed_roles", role_keys);
            for (const auto & role_key : role_keys)
            {
                if (role_key != "role" && !role_key.starts_with("role["))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unknown key '{}' inside 'allowed_roles' of HTTP user directory", role_key);
                auto role_name = config.getString(prefix + ".allowed_roles." + role_key);
                if (role_name.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty role name in 'allowed_roles' of HTTP user directory");
                allowed_roles.insert(role_name);
            }
        }
        else if (key == "allowed_role_prefix" || key.starts_with("allowed_role_prefix["))
        {
            auto role_prefix = config.getString(prefix + "." + key);
            if (role_prefix.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'allowed_role_prefix' in HTTP user directory");
            allowed_role_prefixes.push_back(role_prefix);
        }
        else
        {
            /// Strict validation: this is a new, security-sensitive directory, and a typo like
            /// <default_profle> or <max_cached_user> would silently weaken policy if ignored.
            /// (LDAP is permissive about unknown directory keys; we deliberately are not.)
            static const std::unordered_set<String> known_keys{
                "server", "allowed_roles", "default_profile", "networks", "max_cached_users", "name"};
            /// Poco exposes a repeated element as `key[n]`. Only `allowed_role_prefix` (handled
            /// above) is repeatable; every other key is read once through its plain name, so a
            /// repeated copy would be silently ignored - reject it instead of accepting an
            /// ambiguous policy.
            if (key.contains('['))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Repeated key '{}' in HTTP user directory configuration: only 'allowed_role_prefix' may be repeated", key);
            if (!known_keys.contains(key))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Unknown key '{}' in HTTP user directory configuration", key);
        }
    }

    default_profile = config.getString(prefix + ".default_profile", "");

    const auto networks_config = prefix + ".networks";
    if (config.has(networks_config))
    {
        Poco::Util::AbstractConfiguration::Keys network_keys;
        config.keys(networks_config, network_keys);
        allowed_client_hosts.clear();
        for (const String & key : network_keys)
        {
            String value = config.getString(networks_config + "." + key);
            /// Exact names only (plus Poco's `[n]` suffix for repeated elements): a typo such as
            /// `ip_typo` or `hostname` is rejected, consistent with the top-level keys.
            if (key == "ip" || key.starts_with("ip["))
                allowed_client_hosts.addSubnet(value);
            else if (key == "host_regexp" || key.starts_with("host_regexp["))
                allowed_client_hosts.addNameRegexp(value);
            else if (key == "host" || key.starts_with("host["))
                allowed_client_hosts.addName(value);
            else
                throw Exception(ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE, "Unknown address pattern type: {}", key);
        }
    }

    /// 0 means unlimited.
    max_cached_users = config.getUInt64(prefix + ".max_cached_users", 10000);
}

void HTTPAccessStorage::checkRoleIsAllowed(const String & role_name) const
{
    if (allowed_roles.contains(role_name))
        return;
    for (const auto & role_prefix : allowed_role_prefixes)
    {
        if (role_name.starts_with(role_prefix))
            return;
    }
    throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
        "Role {} returned by the HTTP authentication server is not allowed by user directory {}",
        backQuote(role_name), getStorageName());
}

std::optional<UUID> HTTPAccessStorage::resolveDefaultProfile() const
{
    if (default_profile.empty())
        return {};

    auto profile_id = access_control.find<SettingsProfile>(default_profile);
    if (!profile_id)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "Settings profile {} configured as default_profile of user directory {} was not found",
            backQuote(default_profile), getStorageName());
    return profile_id;
}

void HTTPAccessStorage::reconcileCachedProfile(const UUID & id, const std::optional<UUID> & profile_id) const
{
    auto user = memory_storage.read<User>(id);
    std::optional<UUID> cached_profile_id = user->settings.empty() ? std::nullopt : user->settings.front().parent_profile;
    if (cached_profile_id == profile_id)
        return;

    /// Reachable when `default_profile` was dropped and recreated under the same name with
    /// a new UUID, or when a concurrent authentication materialized/reconciled this user
    /// against an older resolution instant. The directory configuration itself is
    /// immutable until restart in v1, so the `default_profile` directive can never be
    /// added or removed at runtime — only the entity it names can change identity.
    memory_storage.update(id, [&profile_id](const AccessEntityPtr & old_entity, const UUID &) -> AccessEntityPtr
    {
        auto new_user = typeid_cast<std::shared_ptr<User>>(old_entity->clone());
        new_user->settings.clear();
        if (profile_id)
        {
            SettingsProfileElement profile_element;
            profile_element.parent_profile = *profile_id;
            new_user->settings.push_back(std::move(profile_element));
        }
        return new_user;
    });
}

void HTTPAccessStorage::reload(ReloadMode reload_mode)
{
    /// Only an explicit `SYSTEM RELOAD USERS` clears the cache. The users.xml-only mode runs on
    /// configuration changes, which are not a request to log this directory's users out.
    if (reload_mode != ReloadMode::ALL)
        return;

    std::lock_guard lock{mutex};
    memory_storage.removeAllExcept({});
    const auto dropped = cached_user_count;
    cached_user_count = 0;
    CurrentMetrics::sub(CurrentMetrics::HTTPUserDirectoryCachedUsers, dropped);
    LOG_INFO(getLogger(), "Dropped {} materialized users on reload", dropped);
}

UUID HTTPAccessStorage::getOrCreateUser(const String & user_name) const
{
    /// One critical section for resolve, lookup and insert. No remote I/O happens here (the
    /// external authentication has already completed), so distinct usernames still authenticate
    /// concurrently, while concurrent first authentications of one username converge on a single
    /// entity, the published `default_profile` identity is never older than one a concurrent
    /// authentication has already resolved, and the cache bound is exact.
    std::lock_guard lock{mutex};

    /// Re-resolved on every authentication, cache hit included: a configured `default_profile` is
    /// baseline policy for every user of this directory, so it must fail closed while dropped, and
    /// it may be recreated with a new UUID at any time. The generic dependency cleanup cannot
    /// repair a stale reference here, because this storage is read-only.
    auto profile_id = resolveDefaultProfile();

    if (auto id = memory_storage.find<User>(user_name))
    {
        reconcileCachedProfile(*id, profile_id);
        return *id;
    }

    /// Only a new username is subject to the bound; a materialized user always authenticates.
    if (max_cached_users && cached_user_count >= max_cached_users)
    {
        ProfileEvents::increment(ProfileEvents::HTTPUserDirectoryCacheLimitExceeded);
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "User directory {} reached the configured max_cached_users bound ({})",
            getStorageName(), max_cached_users);
    }

    auto user = std::make_shared<User>();
    user->setName(user_name);
    user->authentication_methods.emplace_back(makeAuthenticationData());
    user->allowed_client_hosts = allowed_client_hosts;
    if (profile_id)
    {
        SettingsProfileElement profile_element;
        profile_element.parent_profile = *profile_id;
        user->settings.push_back(std::move(profile_element));
    }

    auto id = memory_storage.insert(user);
    ++cached_user_count;
    ProfileEvents::increment(ProfileEvents::HTTPUserDirectoryUsersCreated);
    CurrentMetrics::add(CurrentMetrics::HTTPUserDirectoryCachedUsers);
    return id;
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

std::optional<AuthResult> HTTPAccessStorage::authenticateImpl(
    const Credentials & credentials,
    const Poco::Net::IPAddress & address,
    const ExternalAuthenticators & external_authenticators,
    const ClientInfo & client_info,
    bool throw_if_user_not_exists,
    bool /* allow_no_password */,
    bool /* allow_plaintext_password */) const
{
    /// Classify applicability by credential TYPE first, before calling
    /// credentials.getUserName (which throws LOGICAL_ERROR on a not-ready object — never
    /// true for AlwaysAllowCredentials or BasicCredentials, both always-ready by
    /// construction, but the general Credentials contract allows a not-ready object, e.g.
    /// mid-handshake) and before running this directory's `networks` policy. A credential
    /// kind this directory cannot evaluate at all (SSL certificate, Kerberos, ...) is
    /// storage-non-applicable: it must fall through to a later storage untouched by a
    /// networks policy that has no bearing on it, exactly like the "does not support"
    /// row of the fail-closed contract. Checking applicability first is what makes that
    /// true; running the networks check first would fail-close an unrelated storage's user
    /// on THIS directory's address policy.
    const bool is_always_allow_credentials = typeid_cast<const AlwaysAllowCredentials *>(&credentials) != nullptr;
    const auto * basic_credentials = typeid_cast<const BasicCredentials *>(&credentials);
    if (!credentials.isReady() || (!is_always_allow_credentials && !basic_credentials))
    {
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, credentials.isReady() ? credentials.getUserName() : String(), getStorageName());
        return {};
    }

    const String & user_name = credentials.getUserName();
    if (user_name.empty())
    {
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, user_name, getStorageName());
        return {};
    }

    /// ClickHouse-side network policy of this directory: for an applicable attempt only
    /// (AlwaysAllowCredentials or Basic), before any remote I/O. On the interserver path
    /// the address is the ORIGINAL CLIENT's address (TCPHandler passes
    /// client_info.initial_address), so this re-validates the real client on the remote
    /// node, like users.xml <networks>.
    ///
    /// Checked directly against `allowed_client_hosts` rather than through
    /// `isAddressAllowed` on a temporary `User`: `IAccessStorage::isAddressAllowed` is a
    /// one-line forward to `user.allowed_client_hosts.contains(address)`, and `contains` is
    /// public, so building a whole `User` (which also carries access rights, granted roles
    /// and profile elements) just to make that call is unnecessary allocation and copying
    /// on every applicable authentication, especially with hostnames or regexps configured.
    if (!allowed_client_hosts.contains(address))
        throwAddressNotAllowed(address);

    /// Internal already-authenticated path (interserver queries and config-embedded
    /// credentials): must not call the external HTTP server. Return the cached user if
    /// present, else materialize only the baseline user; the connection's effective roles
    /// come from the existing interserver role propagation machinery, not from here.
    /// (At most one `http` directory exists per the startup validation; the generic
    /// cross-type ordering ambiguity with other dynamic directories is documented.)
    if (is_always_allow_credentials)
    {
        AuthResult result;
        result.user_id = getOrCreateUser(user_name);
        result.user_name = user_name;
        result.authentication_data = makeAuthenticationData();
        return result;
    }

    /// basic_credentials is guaranteed non-null here: the applicability check above only
    /// let AlwaysAllowCredentials (handled above) or BasicCredentials through.

    /// The try below covers exactly the external-auth + validation + materialization stage
    /// of this applicable Basic attempt: `HTTPUserDirectoryAuthFailures` counts
    /// what fails closed inside it, and nothing else — not the applicability
    /// classification, not the networks check, not the `AlwaysAllowCredentials` path above,
    /// all of which stay outside.
    ///
    /// A 404 (`UserNotFound`) is a fallthrough, not a failure, so it must never reach the
    /// catch as an exception: the flag below lets that branch finish the try normally (no
    /// throw, no return) and defers `throwNotFound` to after the try, where it is no longer
    /// subject to the catch and therefore never counted.
    bool user_not_found = false;
    try
    {
        ProfileEvents::increment(ProfileEvents::HTTPUserDirectoryAuthRequests);

        /// Remote HTTP authentication. Performed without holding any storage-wide lock,
        /// so different usernames (and concurrent attempts for the same username)
        /// authenticate concurrently. Infrastructure failures propagate (fail-closed).
        HTTPUserDirectoryResponseParser::Result response;
        {
            /// Narrow scope: `HTTPUserDirectoryAuthMicroseconds` measures only the external
            /// call itself, not the validation/materialization below. Retries inside
            /// `HTTPAuthClient` are counted (they happen inside the call); a throw from the
            /// call itself is counted via `SCOPE_EXIT` before propagating to the catch below.
            Stopwatch watch;
            SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::HTTPUserDirectoryAuthMicroseconds, watch.elapsedMicroseconds()); });
            response = external_authenticators.checkHTTPUserDirectoryCredentials(http_auth_server_name, *basic_credentials, client_info);
        }

        if (response.status == HTTPUserDirectoryResponseParser::Result::Status::UserNotFound)
        {
            /// 404 fallthrough — NOT a failure. `throwNotFound` (if any) happens after the
            /// try, uncounted.
            user_not_found = true;
        }
        else
        {
            /// Validate all security metadata before touching any state.
            std::vector<UUID> external_role_ids;
            external_role_ids.reserve(response.role_names.size());
            for (const auto & role_name : response.role_names)
            {
                if (role_name.empty())
                    throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "The HTTP authentication server returned an empty role name");
                checkRoleIsAllowed(role_name);
                auto role_id = access_control.find<Role>(role_name);
                if (!role_id)
                    throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "Role {} returned by the HTTP authentication server does not exist", backQuote(role_name));
                external_role_ids.push_back(*role_id);
            }

            if (response.valid_until)
            {
                const time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                if (response.valid_until <= now)
                    throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "The HTTP authentication server returned an already expired valid_until");
            }

            /// Returned settings follow the normal ClickHouse setting-name policy: a built-in
            /// setting, or a custom setting matching `custom_settings_prefixes`; anything else
            /// (a typo, an arbitrary name) fails the attempt. Built-in values are cast to the
            /// setting's type here so a malformed value fails now, not at session creation;
            /// custom values keep the JSON scalar type the helper sent.
            SettingsChanges settings;
            settings.reserve(response.settings.size());
            for (const auto & change : response.settings)
            {
                try
                {
                    access_control.checkSettingNameIsAllowed(change.name);
                    if (settingIsBuiltin(change.name))
                        settings.emplace_back(change.name, settingCastValueUtil(change.name, change.value));
                    else
                        settings.emplace_back(change.name, change.value);
                }
                catch (...)
                {
                    throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
                        "Invalid setting {} in the HTTP authentication server response: {}",
                        backQuote(change.name), getCurrentExceptionMessage(false));
                }
            }

            AuthResult result;
            result.user_id = getOrCreateUser(user_name);
            result.user_name = user_name;
            result.settings = std::move(settings);
            result.external_roles = std::move(external_role_ids);

            /// Rides the existing per-authentication expiry machinery

            /// (Session::checkIfUserIsStillValid enforces it per query).

            result.authentication_data = makeAuthenticationData(response.valid_until);

            return result;
        }
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::HTTPUserDirectoryAuthFailures);
        throw;
    }

    /// Reached only for the `UserNotFound` fallthrough, decided above but thrown here —
    /// outside the try — so it is never caught by the catch above and never counted as an
    /// `HTTPUserDirectoryAuthFailures`.
    chassert(user_not_found);
    if (throw_if_user_not_exists)
        throwNotFound(AccessEntityType::USER, user_name, getStorageName());
    return {};
}

}
