#include <Access/HTTPAccessStorage.h>

#include <Access/AccessControl.h>
#include <Access/Credentials.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <Access/User.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>

#include <Poco/Util/AbstractConfiguration.h>

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
                "server", "allowed_roles", "allowed_role_prefix",
                "default_profile", "networks", "max_cached_users", "name"};
            String base_key = key;
            if (size_t bracket = base_key.find('['); bracket != String::npos)
                base_key.resize(bracket);
            if (!known_keys.contains(base_key))
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
            if (key.starts_with("ip"))
                allowed_client_hosts.addSubnet(value);
            else if (key.starts_with("host_regexp"))
                allowed_client_hosts.addNameRegexp(value);
            else if (key.starts_with("host"))
                allowed_client_hosts.addName(value);
            else
                throw Exception(ErrorCodes::UNKNOWN_ADDRESS_PATTERN_TYPE, "Unknown address pattern type: {}", key);
        }
    }

    /// A soft bound: concurrent successful authentications may exceed it by the
    /// number of in-flight materializations. 0 means unlimited.
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

UUID HTTPAccessStorage::getOrCreateUser(const String & user_name) const
{
    /// Deliberately narrow fast path. With no `default_profile` configured there is
    /// NOTHING an already-materialized user needs reconciled, so a cache hit is served
    /// without touching this directory's mutex — and that is the common shape of the hot
    /// path, because ordinary HTTP requests authenticate individually.
    ///
    /// Its safety rests entirely on v1 invariants, each of them stated elsewhere in this
    /// design, and it MUST be revisited if any one of them changes:
    ///   * the directory configuration is immutable until restart, so `default_profile`
    ///     cannot become non-empty later, and neither can `http_auth_server_name` or
    ///     `networks` — the only other configuration the cached `User` embeds;
    ///   * no authentication-derived state is stored on the cached `User`: helper-returned
    ///     roles, `settings` and `valid_until` all ride on `AuthResult`, and the roles are
    ///     session-scoped by design and never added to `granted_roles`;
    ///   * this storage is read-only, so nothing updates a cached user out from under us,
    ///     and the cache is append-only, so a found id stays valid;
    ///   * with no `default_profile` the `User` holds no `parent_profile` at all, so even
    ///     the generic `removeReferencesToRemovedIDs` cascade finds no dependency to strip.
    /// So the cached entity is a pure function of immutable configuration and the
    /// username, and there is no resolution instant for it to be compared against. If a
    /// later version adds reloadable baseline state, or stores any per-authentication
    /// state on the cached user, DELETE this fast path — do not try to patch it.
    ///
    /// Note what this is NOT: the generic "resolve the profile unlocked, compare it
    /// unlocked, return when equal" shortcut. That one is also sound today, but it
    /// re-creates the unlocked resolve-then-compare shape that two successive review
    /// rounds each found a bug in, with correctness resting on an invariant a later edit
    /// can silently break. It is deliberately not taken; see the implementer notes.
    if (default_profile.empty())
    {
        if (auto id = memory_storage.find<User>(user_name))
            return *id;
    }

    /// Soft concurrent bound: the capacity OBSERVATION stays outside the mutex, which is
    /// exactly what keeps the bound soft — simultaneous materializations of DIFFERENT new
    /// usernames each observe capacity independently and may overshoot the configured
    /// value by the number of in-flight authentications, with no reservation protocol and
    /// no capacity serialization (per the ADR).
    ///
    /// The observation must NOT decide the outcome by itself, though. Whether this
    /// username is still new is decided by the authoritative lookup under the mutex
    /// below, and only then may a full observation reject it. Otherwise the rejection
    /// would rest on two observations taken at different instants and describe no
    /// coherent cache state: `find` says "absent", a concurrent authentication of exactly
    /// this username then materializes it and takes the last slot, the count observation
    /// then says "full" — and this authentication would be rejected on the cache bound
    /// even though its user is, by that point, materialized. That contradicts the
    /// unconditional rule that an already-materialized user keeps authenticating, and it
    /// would make concurrent first authentications of one username fail instead of
    /// converging on the single entity.
    ///
    /// The unlocked `find` below is only a short-circuit sparing known users the count
    /// check; it decides nothing. Deferring the decision also loses no precision in the
    /// restrictive direction, because this cache is append-only for the lifetime of the
    /// process — nothing ever removes a user from it (this directory never removes, and
    /// the generic dependency cascade cannot write to it) — so an observation of "full"
    /// can never have become false by the time the decision is made.
    ///
    /// `cached_user_count` (relaxed load) replaces `memory_storage.findAll<User>().size()`
    /// here: `MemoryAccessStorage::findAllImpl` walks every entry under its own mutex to
    /// build a vector, so with a large cache that call would do O(cache size) work on every
    /// first-time authentication merely to learn a count. The counter is maintained
    /// alongside the single `memory_storage.insert` call below and is purely a capacity
    /// statistic — `memory_order_relaxed` is enough on both ends because it does not
    /// synchronize access to the cached entity itself, only this soft bound decision.
    bool observed_cache_full = false;
    if (max_cached_users && !memory_storage.find<User>(user_name))
        observed_cache_full = cached_user_count.load(std::memory_order_relaxed) >= max_cached_users;

    std::lock_guard lock{mutex};

    /// Resolve default_profile INSIDE the critical section, and use this resolution — and
    /// only this one — for both the reconciliation and the insertion below.
    ///
    /// Why on every call, cache hit included: a configured default_profile is baseline
    /// policy (it is how this directory expresses "every user of mine gets at least this
    /// settings profile"), so dropping it must fail authentication even for a user
    /// materialized before the drop, matching the unresolvable-default_profile row of the
    /// fail-closed contract. It cannot be resolved once at construction either, because
    /// the referenced profile may live in an access storage configured later than this
    /// directory, and it can be dropped and recreated (with a new UUID) at any later time.
    /// This also closes a gap in the generic dependency-cleanup machinery
    /// (`IAccessStorage::removeReferencesToRemovedIDs`): it treats an
    /// `ACCESS_STORAGE_READONLY` update failure as "harmless, reconciled on the next
    /// config reload" — true for `users.xml`, false for this directory, which never
    /// reloads. Left alone, a cached user's stale `parent_profile` UUID would sit
    /// unreconciled forever, and `SettingsProfilesCache::substituteProfiles` silently
    /// drops a `parent_profile` it can't find (see the source comment there) — so
    /// authentication would keep succeeding with the configured baseline policy silently
    /// gone. Re-resolving here, fail-closed, is what prevents that.
    ///
    /// Why under the mutex rather than before it: reconciliation is a compare-and-set
    /// against a resolution instant, so resolving outside and writing inside does NOT
    /// converge, for a cache hit any more than for a materialization. With the profile
    /// dropped and recreated under the same name, an authentication that resolved the old
    /// identity `A` can otherwise land its write after one that resolved the new identity
    /// `B` and put the cached user back onto `A` — which no longer exists, and which
    /// `substituteProfiles` then silently skips, so every later authentication served from
    /// that cache entry runs with the baseline policy gone. Holding the mutex across
    /// resolution and use makes the sequence atomic against the only other writer of
    /// `memory_storage`, which is another `getOrCreateUser` call.
    ///
    /// This is cheap and does not weaken the concurrency contract: everything inside the
    /// critical section is an in-memory `AccessControl::find` / `MemoryAccessStorage`
    /// lookup, the external HTTP authentication has already completed before
    /// getOrCreateUser is called, and no remote I/O is ever performed under this mutex —
    /// so distinct usernames still authenticate fully concurrently.
    ///
    /// Scope, stated honestly: this bounds staleness, it does not make concurrent
    /// settings-profile DDL atomic with respect to authentication. An authentication that
    /// resolves a profile which is dropped microseconds later still succeeds against the
    /// identity it observed; the next authentication of that user fails closed or
    /// reconciles. What is guaranteed is that no authentication publishes or is served an
    /// identity older than one a concurrent authentication has already resolved.
    auto profile_id = resolveDefaultProfile();

    /// Authoritative lookup. A cache hit converges on the existing entity, reconciled
    /// against the resolution above, and returns BEFORE the cache bound is consulted at
    /// all — the bound never applies to an already-materialized user, including one
    /// materialized by a concurrent authentication while we waited for the mutex. This
    /// branch therefore doubles as the recheck the miss path needs.
    if (auto id = memory_storage.find<User>(user_name))
    {
        reconcileCachedProfile(*id, profile_id);
        return *id;
    }

    /// Authoritatively a new username, and the cache was observed full: reject. Task 11
    /// increments `HTTPUserDirectoryCacheLimitExceeded` right here — an atomic
    /// `ProfileEvents` add, so holding the mutex across it costs nothing. The capacity is
    /// NOT re-observed under the mutex: that would serialize capacity and make the bound
    /// strict, which the ADR explicitly does not want.
    if (observed_cache_full)
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "User directory {} reached the configured max_cached_users bound ({})",
            getStorageName(), max_cached_users);

    auto user = std::make_shared<User>();
    user->setName(user_name);
    AuthenticationData auth_data(AuthenticationType::HTTP);
    auth_data.setHTTPAuthenticationServerName(http_auth_server_name);
    auth_data.setHTTPAuthenticationScheme(HTTPAuthenticationScheme::BASIC);
    user->authentication_methods.emplace_back(std::move(auth_data));
    user->allowed_client_hosts = allowed_client_hosts;
    if (profile_id)
    {
        SettingsProfileElement profile_element;
        profile_element.parent_profile = *profile_id;
        user->settings.push_back(std::move(profile_element));
    }

    auto id = memory_storage.insert(user);
    /// Relaxed: a capacity statistic feeding the soft bound above, not synchronization for
    /// the cached entity (the mutex held across this whole function already provides that).
    cached_user_count.fetch_add(1, std::memory_order_relaxed);
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
    /// credentials.getUserName() (which throws LOGICAL_ERROR on a not-ready object — never
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
        AuthenticationData auth_data(AuthenticationType::HTTP);
        auth_data.setHTTPAuthenticationServerName(http_auth_server_name);
        auth_data.setHTTPAuthenticationScheme(HTTPAuthenticationScheme::BASIC);
        result.authentication_data = std::move(auth_data);
        return result;
    }

    /// basic_credentials is guaranteed non-null here: the applicability check above only
    /// let AlwaysAllowCredentials (handled above) or BasicCredentials through.

    /// Remote HTTP authentication. Performed without holding any storage-wide lock,
    /// so different usernames (and concurrent attempts for the same username)
    /// authenticate concurrently. Infrastructure failures propagate (fail-closed).
    auto response = external_authenticators.checkHTTPUserDirectoryCredentials(http_auth_server_name, *basic_credentials, client_info);

    if (response.status == HTTPUserDirectoryResponseParser::Result::Status::UserNotFound)
    {
        if (throw_if_user_not_exists)
            throwNotFound(AccessEntityType::USER, user_name, getStorageName());
        return {};
    }

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

    AuthResult result;
    result.user_id = getOrCreateUser(user_name);
    result.user_name = user_name;
    result.settings = std::move(response.settings);
    result.external_roles = std::move(external_role_ids);

    AuthenticationData auth_data(AuthenticationType::HTTP);
    auth_data.setHTTPAuthenticationServerName(http_auth_server_name);
    auth_data.setHTTPAuthenticationScheme(HTTPAuthenticationScheme::BASIC);
    /// Rides the existing per-authentication expiry machinery
    /// (Session::checkIfUserIsStillValid enforces it per query).
    auth_data.setValidUntil(response.valid_until);
    result.authentication_data = std::move(auth_data);

    return result;
}

}
