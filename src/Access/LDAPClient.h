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
#include <map>
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

        /// True when the search reads an attribute of the user's own entry: `base_dn` is exactly
        /// `{user_dn}` (or `{bind_dn}` when `bind_dn_is_user_dn`, i.e. the login binds as the detected
        /// DN), the scope is `base` and the filter is `(objectClass=*)`, e.g. an Active Directory
        /// `memberOf` lookup. `LDAPSyncClient::enumerate` answers such a search from the attributes of
        /// the enumerated entry instead of issuing one search per user.
        bool isSelfLookup(bool bind_dn_is_user_dn) const;

        void updateHash(SipHash & hash) const;
    };

    struct RoleSearchParams
        : public SearchParams
    {
        String prefix;

        /// If set, every value returned by the search is treated as a DN (for example an Active Directory
        /// `memberOf` value) and replaced by the value of its first RDN whose attribute type equals
        /// `rdn_attribute` case-insensitively. Values that are not DNs or have no such RDN are ignored.
        String rdn_attribute;

        /// Optional allow-list of groups, as configured. An entry containing `=` is a group DN and is
        /// compared (normalized by `normalizeDN`) against the raw search result before `rdn_attribute`
        /// extraction; the role name is then derived from the `rdn_attribute` value of the configured DN.
        /// Any other entry is a plain group name compared ASCII-case-insensitively against the value after
        /// extraction; the configured spelling wins. When the list is non-empty, values matching no entry
        /// are ignored. `prefix` is stripped afterwards in both cases, so every entry must start with it.
        /// Kept as configured for `updateHash` and `system.user_directories`; the lookups use the maps below.
        std::vector<String> groups;

        /// Lookup maps derived from `groups` by `parseLDAPRoleSearchParams`, the only producer of this struct.
        /// ASCII-lower-cased plain group name -> the name as configured.
        std::map<String, String> plain_groups;
        /// Normalized group DN (`LDAPClient::normalizeDN`) -> the `rdn_attribute` value as spelled in the configured DN.
        std::map<String, String> dn_groups;

        static bool isGroupDN(const String & group) { return group.contains('='); }

        void updateHash(SipHash & hash) const;
    };

    using RoleSearchParamsList = std::vector<RoleSearchParams>;

    using SearchResults = std::set<String>;
    using SearchResultsList = std::vector<SearchResults>;

    /// One directory entry as returned by `searchEntries`: its DN and the values of the requested
    /// attributes. Attribute names are ASCII-lower-cased (`memberOf` and `memberof` are the same
    /// key) and stripped of their options (`userCertificate;binary` is keyed as `usercertificate`);
    /// an attribute returned with a range option (`memberOf;range=0-1499`, Active Directory's
    /// `MaxValRange` truncation) fails the search instead of yielding a partial value set. Attributes
    /// the entry has no value for are absent. Empty values are dropped.
    struct Entry
    {
        String dn;
        std::map<String, std::set<String>> attributes;
    };

    /// Parameters of a user enumeration (the `<sync>` search of an `ldap` user directory): a plain
    /// search whose `attribute` yields the ClickHouse user name, plus the RFC 2696 page size and a
    /// client-side cap on the number of entries. Parsed by `parseLDAPSearchParams` plus the extra keys.
    struct UserEnumerationParams
        : public SearchParams
    {
        /// Entries requested per page (`pagedResultsControl`). Active Directory caps a page at 1000.
        UInt32 page_size = 500;
        /// The enumeration fails with `LDAP_ERROR` as soon as the directory returns more entries; 0 = unlimited.
        size_t max_entries = 0;
    };

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
        /// Unset means `default_tls_minimum_protocol_version`; the value is kept optional so that a build of libldap without
        /// `LDAP_OPT_X_TLS_PROTOCOL_MIN` can reject an explicitly configured value instead of ignoring it, see `openConnection`.
        static constexpr TLSProtocolVersion default_tls_minimum_protocol_version = TLSProtocolVersion::TLS1_2;
        std::optional<TLSProtocolVersion> tls_minimum_protocol_version;
        /// Unset means "whatever the library negotiates"; when set, must not be lower than the effective minimum.
        std::optional<TLSProtocolVersion> tls_maximum_protocol_version;
        /// Unset means `default_tls_require_cert`; optional for the same reason as the minimum protocol version.
        static constexpr TLSRequireCert default_tls_require_cert = TLSRequireCert::DEMAND;
        std::optional<TLSRequireCert> tls_require_cert;
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

        /// Feeds every field that influences the outcome of a bind or a search into `hash`. `ExternalAuthenticators` uses
        /// the result as the key of the `verification_cooldown` cache and to detect a configuration reload that raced with
        /// an authentication in flight, so a field left out here lets a result obtained under the old policy survive a change.
        /// `verification_cooldown` itself is deliberately not hashed: it bounds the lifetime of a result, not its meaning.
        void updateHash(SipHash & hash) const;
    };

    explicit LDAPClient(const Params & params_);
    ~LDAPClient();

    /// Parses `dn` as an LDAPv3 string representation of a distinguished name (RFC 4514) and returns the
    /// unescaped value of the first RDN (the most specific one) whose attribute type equals `rdn_attribute`
    /// case-insensitively. Returns `std::nullopt` if `dn` is not a valid non-empty DN or has no such RDN.
    static std::optional<String> extractRDNValue(const String & dn, const String & rdn_attribute);

    /// Returns a canonical, ASCII-lower-cased LDAPv3 string representation of `dn` intended for equality
    /// comparison only (differences in whitespace, escaping and letter case disappear), or `std::nullopt`
    /// if `dn` is not a valid non-empty DN.
    static std::optional<String> normalizeDN(const String & dn);

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
    /// Logs the protocol version and cipher of a protected connection, once per connection, on its first bind.
    void logNegotiatedTLS();

    /// Runs `params.user_dn_detection` and returns the single DN it yields. Throws
    /// `LDAP_ERROR` when more than one entry matches. When `tolerate_missing_user` is set an
    /// empty result yields nullopt, otherwise it throws. `LDAP_NO_SUCH_OBJECT` counts as an
    /// empty result only when `base_dn` depends on the login as defined by
    /// `Params::templateDependsOnUserName` (the base then legitimately does not exist for an
    /// unknown user); for a static `base_dn` it is a misconfiguration and always throws.
    MAYBE_NORETURN std::optional<String> detectUserDN(bool tolerate_missing_user);

    void closeConnection() noexcept;

    /// Throws `LOGICAL_ERROR` unless the connection is bound as the identity every search must
    /// run under: the service account when `lookup_bind_dn` is configured (users frequently
    /// cannot read group containers or their own `memberOf`, and a search as the user would
    /// silently return fewer roles), the user otherwise (the legacy model).
    MAYBE_NORETURN void assertBoundForSearch() const;

    /// Substitutes `placeholders` into `search_params.base_dn` and `search_params.search_filter`
    /// and returns the pair (base DN, filter). `{user_name}` is escaped once for the context it
    /// lands in; the DN placeholders are already DNs and are filter-escaped only in the filter;
    /// `{base_dn}` in the filter stands for the substituted base DN.
    MAYBE_NORETURN std::pair<String, String> resolveSearchTemplates(const SearchParams & search_params) const;

    /// Runs a search on the open connection, see `assertBoundForSearch` for the identity it
    /// runs under. Returns the values of `search_params.attribute` of every matching entry
    /// (the entry DNs when the attribute is `dn`), capped by `params.search_limit`.
    /// When `tolerate_no_such_object` is set, an `LDAP_NO_SUCH_OBJECT` (rc=32) reply from the
    /// directory is converted into an empty `SearchResults` instead of an `LDAP_ERROR`.
    MAYBE_NORETURN SearchResults search(const SearchParams & search_params, bool tolerate_no_such_object = false);

    /// Runs a paged search (RFC 2696 `pagedResultsControl`, `page_size` entries per page, marked
    /// critical so a directory that cannot page rejects the request instead of silently truncating
    /// it) on the open connection under the identity `assertBoundForSearch` demands, and returns
    /// every matching entry with its DN and the values of `attributes` (an empty list requests all
    /// user attributes; `dn` is not an attribute and is never in the map). No client-side size
    /// limit is requested, so `params.search_limit` does not apply. `ldap_global_mutex` is held for
    /// one page at a time, so other LDAP clients get to run between pages.
    /// Throws `LDAP_ERROR` naming the bound identity when the directory answers with
    /// `sizeLimitExceeded` or `adminLimitExceeded` (the lookup account needs a higher server-side
    /// limit), and as soon as more than `max_entries` entries were received when `max_entries` > 0.
    /// `search_params.attribute` is ignored: the attributes to fetch are `attributes`.
    MAYBE_NORETURN std::vector<Entry> searchEntries(const SearchParams & search_params, const std::vector<String> & attributes, UInt32 page_size, size_t max_entries);

    const Params params;
#if USE_LDAP
    LDAP * handle = nullptr;
#endif
    BindMode bound_as = BindMode::None;
    bool tls_logged = false;
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

/// Enumerates the users of a directory under the lookup identity, for the proactive
/// synchronisation of an `ldap` user directory. Never binds as a user.
class LDAPSyncClient
    : private LDAPClient
{
public:
    using LDAPClient::LDAPClient;

    struct UserEntry
    {
        /// The single value of `UserEnumerationParams::attribute`, i.e. the ClickHouse user name.
        String name;
        String dn;
        /// One `SearchResults` per element of the `RoleSearchParamsList` passed to `enumerate`, in
        /// the same order, exactly as `LDAPSimpleAuthClient::authenticate` would return them for
        /// this user (raw values; the mapping to role names is `LDAPAccessStorage::mapExternalRolesNoLock`).
        SearchResultsList external_roles;
    };

    /// Opens one connection, binds as `params.lookup_bind_dn` and runs the paged enumeration, then
    /// resolves the role mappings of every entry on the same connection: a self-lookup mapping
    /// (`SearchParams::isSelfLookup`) is read from the entry's own attributes, which were fetched
    /// along with the user name, every other mapping is one `search` per user with `{user_name}` the
    /// name, `{user_dn}` the entry's DN and `{bind_dn}` what a login of that user binds as: the entry's
    /// DN in search-and-bind mode, the `bind_dn` template with the name substituted otherwise. The
    /// result is in directory order and not deduplicated; the caller decides what a duplicate name means.
    /// Throws `BAD_ARGUMENTS` without a lookup identity, when `attribute` is empty or `dn`, or when
    /// `base_dn`/`search_filter` contain a per-user placeholder (nothing could substitute it);
    /// `LDAP_ERROR` for every directory-side failure (see `searchEntries`) and for an entry with zero
    /// or several values of `enumeration_params.attribute` or without a DN (a skipped entry would be
    /// a user removed by the next run). Never returns partial results: an error in the middle of the
    /// enumeration propagates.
    std::vector<UserEntry> enumerate(const UserEnumerationParams & enumeration_params, const RoleSearchParamsList & role_search_params);
};

}

#undef MAYBE_NORETURN
