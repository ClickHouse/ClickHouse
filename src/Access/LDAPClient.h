#pragma once

#include "config.h"

#include <base/types.h>

#if USE_LDAP
#   include <ldap.h>
#   define MAYBE_NORETURN
#else
#   define MAYBE_NORETURN [[noreturn]]
#endif

#include <chrono>
#include <optional>
#include <set>
#include <vector>

class SipHash;

namespace DB
{

class LDAPClient
{
public:
    struct SearchParams
    {
        enum class Scope : uint8_t
        {
            BASE,
            ONE_LEVEL,
            SUBTREE,
            CHILDREN
        };

        String base_dn;
        Scope scope = Scope::SUBTREE;
        String search_filter;
        String attribute = "cn";

        void updateHash(SipHash & hash) const;
    };

    struct RoleSearchParams
        : public SearchParams
    {
        String prefix;

        void updateHash(SipHash & hash) const;
    };

    using RoleSearchParamsList = std::vector<RoleSearchParams>;

    using SearchResults = std::set<String>;
    using SearchResultsList = std::vector<SearchResults>;

    struct Params
    {
        enum class ProtocolVersion : uint8_t
        {
            V2,
            V3
        };

        enum class TLSEnable : uint8_t
        {
            NO,
            YES_STARTTLS,
            YES
        };

        enum class TLSProtocolVersion : uint8_t
        {
            SSL2,
            SSL3,
            TLS1_0,
            TLS1_1,
            TLS1_2
        };

        enum class TLSRequireCert : uint8_t
        {
            NEVER,
            ALLOW,
            TRY,
            DEMAND
        };

        enum class SASLMechanism : uint8_t
        {
            UNKNOWN,
            SIMPLE
        };

        /// The value of `bind_dn` that selects search-and-bind: the user is located with
        /// `user_dn_detection` under the lookup identity first, and the password is then
        /// verified by binding as the DN that was found.
        static constexpr auto DETECTED_USER_DN_PLACEHOLDER = "{user_dn}";

        ProtocolVersion protocol_version = ProtocolVersion::V3;

        /// Name of the `ldap_servers` entry these parameters were parsed from. Used in
        /// messages only, so that a log line can tell apart several servers sharing the
        /// same service account; deliberately not part of `updateHash`.
        String name;

        String host;
        UInt16 port = 636;

        TLSEnable enable_tls = TLSEnable::YES;
        TLSProtocolVersion tls_minimum_protocol_version = TLSProtocolVersion::TLS1_2;
        TLSRequireCert tls_require_cert = TLSRequireCert::DEMAND;
        String tls_cert_file;
        String tls_key_file;
        String tls_ca_cert_file;
        String tls_ca_cert_dir;
        String tls_cipher_suite;

        SASLMechanism sasl_mechanism = SASLMechanism::SIMPLE;

        /// Template of the DN the user's password is verified against (`{user_name}` is
        /// substituted), or exactly `{user_dn}` to bind as the DN found by `user_dn_detection`.
        String bind_dn;
        String user;
        String password;

        /// Optional service-account ("lookup") credentials. Once configured, EVERY search
        /// (`user_dn_detection` and role mappings) runs under this identity, never under the
        /// user's, and `IAccessStorage::find(..., force_external_lookup=true)` can resolve a
        /// user without their password. When empty, the legacy behaviour applies: searches
        /// run on the connection bound as the user.
        String lookup_bind_dn;
        String lookup_password;

        std::optional<SearchParams> user_dn_detection;

        std::chrono::seconds verification_cooldown{0};

        std::chrono::seconds operation_timeout{40};
        std::chrono::seconds network_timeout{30};
        std::chrono::seconds search_timeout{20};
        UInt32 search_limit = 256; /// An arbitrary number, no particular motivation for this value.

        bool follow_referrals = false; /// Whether to follow LDAP referrals for server.

        /// True when a service account is configured and therefore all searches must run under it.
        bool hasLookupIdentity() const { return !lookup_bind_dn.empty(); }

        /// True for search-and-bind (`bind_dn` is exactly `{user_dn}`).
        bool bindsAsDetectedUserDN() const { return bind_dn == DETECTED_USER_DN_PLACEHOLDER; }

        /// True when substituting the placeholders into `search_template` (a `base_dn` or
        /// `search_filter` of `user_dn_detection`) makes the result depend on the login:
        /// directly through `{user_name}`, or through `{bind_dn}`/`{user_dn}` while `bind_dn`
        /// is a template that carries `{user_name}`. In search-and-bind `bind_dn` is `{user_dn}`
        /// itself, so only the literal `{user_name}` counts there (and `parseLDAPServer` rejects
        /// the DN placeholders in the detection, because no DN is known before it has run).
        /// This is the single definition of "target-specific" shared by the configuration check
        /// in `parseLDAPServer` and by `detectUserDN`, which treats `LDAP_NO_SUCH_OBJECT` as
        /// "user not found" only for such a `base_dn`.
        bool templateDependsOnUserName(const String & search_template) const;

        void updateHash(SipHash & hash) const;
    };

    explicit LDAPClient(const Params & params_);
    ~LDAPClient();

    LDAPClient(const LDAPClient &) = delete;
    LDAPClient(LDAPClient &&) = delete;
    LDAPClient & operator= (const LDAPClient &) = delete;
    LDAPClient & operator= (LDAPClient &&) = delete;

    /// The identity a connection is currently bound as.
    enum class BindMode : uint8_t
    {
        /// Connected, no successful bind yet (or the last bind failed).
        None,
        /// Bound as the user being authenticated (`Placeholders::bind_dn` with `params.password`).
        User,
        /// Bound as the service account (`params.lookup_bind_dn`, `params.lookup_password`).
        Service,
    };

protected:
    /// Values substituted into `bind_dn`, `base_dn` and `search_filter` templates.
    /// `user_name` is the RAW login; it is escaped exactly once at substitution time
    /// (`escapeForDN` in DN contexts, `escapeForFilter` in filters).
    struct Placeholders
    {
        String user_name;
        String bind_dn;
        String user_dn;
    };

    MAYBE_NORETURN void handleError(int result_code, String text = "");

    /// Initializes the handle, applies the options and StartTLS; does not bind.
    MAYBE_NORETURN void connect();

    /// Performs a simple bind as the given identity on the open connection.
    /// `BindMode::User`: returns false on `LDAP_INVALID_CREDENTIALS` (after logging the
    /// Active Directory sub-code, if any, at DEBUG), throws on every other error.
    /// `BindMode::Service`: throws `LDAP_ERROR` on `LDAP_INVALID_CREDENTIALS` because the
    /// service credentials come from the configuration, so a failure is an operator error
    /// and not a "user not found" signal.
    MAYBE_NORETURN bool bind(BindMode mode);

    /// Runs `params.user_dn_detection` and returns the single DN it yields. Throws
    /// `LDAP_ERROR` when more than one entry matches. When `tolerate_missing_user` is set an
    /// empty result yields nullopt, otherwise it throws. `LDAP_NO_SUCH_OBJECT` counts as an
    /// empty result only when `base_dn` depends on the login as defined by
    /// `Params::templateDependsOnUserName` (the base then legitimately does not exist for an
    /// unknown user); for a static `base_dn` it is a misconfiguration and always throws.
    MAYBE_NORETURN std::optional<String> detectUserDN(bool tolerate_missing_user);

    void closeConnection() noexcept;

    /// Runs a search on the open connection. Asserts (as `LOGICAL_ERROR`) that the connection
    /// is bound as the service account when `lookup_bind_dn` is configured, or as the user
    /// otherwise, so role searches can never accidentally run under the wrong identity.
    /// When `tolerate_no_such_object` is set, an `LDAP_NO_SUCH_OBJECT` (rc=32) reply from the
    /// directory is converted into an empty `SearchResults` instead of an `LDAP_ERROR`.
    MAYBE_NORETURN SearchResults search(const SearchParams & search_params, bool tolerate_no_such_object = false);

    const Params params;
#if USE_LDAP
    LDAP * handle = nullptr;
#endif
    BindMode bound_as = BindMode::None;
    Placeholders placeholders;
};

class LDAPSimpleAuthClient
    : private LDAPClient
{
public:
    using LDAPClient::LDAPClient;

    /// Verifies `params.user`/`params.password` and optionally runs the role searches.
    /// Returns false on wrong password (and, in search-and-bind mode, on unknown user),
    /// throws on every other error. One connection with up to three binds:
    ///   - `bind_dn` template, no `lookup_bind_dn`: bind as the user, search as the user;
    ///   - `bind_dn` template + `lookup_bind_dn`: bind as the user, re-bind as the service
    ///     account, detect the user DN and search;
    ///   - `bind_dn` = `{user_dn}` + `lookup_bind_dn`: bind as the service account, detect the
    ///     user DN, bind as it with the user's password, re-bind as the service account, search.
    bool authenticate(const RoleSearchParamsList * role_search_params, SearchResultsList * role_search_results);

    /// Looks up a user in LDAP using the service-account credentials configured in
    /// `params.lookup_bind_dn` / `params.lookup_password`. The user's existence is
    /// verified via `params.user_dn_detection`, which must also be configured.
    /// Returns true if the user was found. Optionally fills `role_search_results`.
    /// Returns false (without throwing) if the user does not exist, if the service-bind
    /// credentials are not configured, or if `user_dn_detection` is not configured.
    bool find(const RoleSearchParamsList * role_search_params, SearchResultsList * role_search_results);
};

}

#undef MAYBE_NORETURN
