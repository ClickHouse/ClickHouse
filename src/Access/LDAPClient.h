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

        /// Ordered from oldest to newest so that versions can be compared with the relational operators.
        enum class TLSProtocolVersion : uint8_t
        {
            SSL2,
            SSL3,
            TLS1_0,
            TLS1_1,
            TLS1_2,
            TLS1_3
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

        ProtocolVersion protocol_version = ProtocolVersion::V3;

        String host;
        UInt16 port = 636;

        TLSEnable enable_tls = TLSEnable::YES;
        /// Unset means `default_tls_minimum_protocol_version`; the value is kept optional so that a build of libldap without
        /// `LDAP_OPT_X_TLS_PROTOCOL_MIN` can reject an explicitly configured value instead of ignoring it, see `openConnection`.
        static constexpr TLSProtocolVersion default_tls_minimum_protocol_version = TLSProtocolVersion::TLS1_2;
        std::optional<TLSProtocolVersion> tls_minimum_protocol_version;
        /// Unset means "whatever the library negotiates"; when set, must not be lower than the effective minimum.
        std::optional<TLSProtocolVersion> tls_maximum_protocol_version;
        TLSRequireCert tls_require_cert = TLSRequireCert::DEMAND;
        String tls_cert_file;
        String tls_key_file;
        String tls_ca_cert_file;
        String tls_ca_cert_dir;
        String tls_cipher_suite;

        SASLMechanism sasl_mechanism = SASLMechanism::SIMPLE;

        String bind_dn;
        String user;
        String password;

        /// Optional service-account credentials used for lookups that do not have the user's
        /// password available (e.g. resolving an LDAP-backed name on `EXECUTE AS` before the
        /// user has authenticated). When `bind_dn` and `password` are empty the service-bind
        /// path is disabled and `IAccessStorage::find(..., force_external_lookup=true)` is a
        /// no-op for this server.
        String lookup_bind_dn;
        String lookup_password;

        std::optional<SearchParams> user_dn_detection;

        std::chrono::seconds verification_cooldown{0};

        /// How long to wait for the result of a bind or of a StartTLS negotiation on an established connection (`LDAP_OPT_TIMEOUT`).
        /// Searches are bounded by `search_timeout` instead. Unset means `default_operation_timeout`; the value is kept optional
        /// so that a build of libldap without the option can reject an explicitly configured value instead of ignoring it.
        static constexpr std::chrono::seconds default_operation_timeout{40};
        std::optional<std::chrono::seconds> operation_timeout;
        /// How long to wait for the TCP connection to the server to be established, including the TLS handshake
        /// (`LDAP_OPT_NETWORK_TIMEOUT`). Unset means `default_network_timeout`; optional for the same reason as `operation_timeout`.
        static constexpr std::chrono::seconds default_network_timeout{30};
        std::optional<std::chrono::seconds> network_timeout;
        /// Time limit passed with each search request (`ldap_search_ext_s`) and set as `LDAP_OPT_TIMELIMIT`: requested from the
        /// server and enforced on the client. Not optional because both are part of the base LDAP API, unlike the two options above.
        std::chrono::seconds search_timeout{20};
        UInt32 search_limit = 256; /// An arbitrary number, no particular motivation for this value.

        bool follow_referrals = false; /// Whether to follow LDAP referrals for server.

        /// Feeds every field that influences the outcome of a bind or a search into `hash`. `ExternalAuthenticators` uses
        /// the result as the key of the `verification_cooldown` cache and to detect a configuration reload that raced with
        /// an authentication in flight, so a field left out here lets a result obtained under the old policy survive a change.
        /// `verification_cooldown` itself is deliberately not hashed: it bounds the lifetime of a result, not its meaning.
        void updateHash(SipHash & hash) const;
    };

    explicit LDAPClient(const Params & params_);
    ~LDAPClient();

    LDAPClient(const LDAPClient &) = delete;
    LDAPClient(LDAPClient &&) = delete;
    LDAPClient & operator= (const LDAPClient &) = delete;
    LDAPClient & operator= (LDAPClient &&) = delete;

    enum class BindMode : uint8_t
    {
        /// Bind as the user being authenticated (the existing behavior).
        User,
        /// Bind with the service account (`params.lookup_bind_dn`, `params.lookup_password`)
        /// and use `user_dn_detection` to confirm the target user exists. Used when looking
        /// up a user without their password.
        Service,
    };

protected:
    MAYBE_NORETURN void handleError(int result_code, String text = "");
    MAYBE_NORETURN bool openConnection(BindMode mode = BindMode::User);
    void closeConnection() noexcept;
    /// When `tolerate_no_such_object` is set, an `LDAP_NO_SUCH_OBJECT` (rc=32) reply from the
    /// directory is converted into an empty `SearchResults` instead of an `LDAP_ERROR`. Used
    /// by the service-bind `user_dn_detection` lookup so a missing user (whose substituted
    /// `base_dn` does not exist in the directory) collapses to the canonical `UNKNOWN_USER`
    /// path rather than surfacing a low-level LDAP error to the caller.
    SearchResults search(const SearchParams & search_params, bool tolerate_no_such_object = false);

    const Params params;
#if USE_LDAP
    LDAP * handle = nullptr;
#endif
    String final_user_name;
    String final_bind_dn;
    String final_user_dn;
};

class LDAPSimpleAuthClient
    : private LDAPClient
{
public:
    using LDAPClient::LDAPClient;
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
