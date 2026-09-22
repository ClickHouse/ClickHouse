#pragma once

#include <Access/Credentials.h>
#include <Access/GSSAcceptor.h>
#include <Access/HTTPAuthClient.h>
#include <Access/LDAPClient.h>
#include <Interpreters/ClientInfo.h>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/types.h>

#include <Poco/URI.h>

#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>


namespace Poco
{
    class Logger;

    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{

class SettingsChanges;

class ExternalAuthenticators
{
public:
    void reset();
    void setConfiguration(const Poco::Util::AbstractConfiguration & config, LoggerPtr log);

    // The name and readiness of the credentials must be verified before calling these.
    bool checkLDAPCredentials(const String & server, const BasicCredentials & credentials,
        const LDAPClient::RoleSearchParamsList * role_search_params = nullptr, LDAPClient::SearchResultsList * role_search_results = nullptr) const;
    bool checkKerberosCredentials(const String & realm, const GSSAcceptorContext & credentials) const;
    bool checkHTTPBasicCredentials(const String & server, const BasicCredentials & credentials, const ClientInfo & client_info, SettingsChanges & settings) const;

    /// Resolves an LDAP user name to "exists in directory" + role mappings, using the
    /// service-bind credentials configured on the named LDAP server. Returns false (without
    /// throwing) when the service-bind credentials are not configured, or when the user
    /// does not exist in the directory. Throws `BAD_ARGUMENTS` when the server name is not
    /// configured at all (mirrors `checkLDAPCredentials`), so a typo in the directory's
    /// `<server>` value surfaces instead of degrading to `UNKNOWN_USER`. Used by
    /// `LDAPAccessStorage::findImpl(..., force_external_lookup=true)`.
    bool findLDAPUser(const String & server, const String & user_name,
        const LDAPClient::RoleSearchParamsList * role_search_params = nullptr, LDAPClient::SearchResultsList * role_search_results = nullptr) const;

    /// Enumerates the users the named LDAP server returns for `enumeration_params` together with
    /// their role mappings, under the server's lookup identity (`LDAPSyncClient::enumerate`), for
    /// the proactive synchronisation of an `ldap` user directory. The server parameters are
    /// copied under `mutex` and the directory is contacted without it. Throws `BAD_ARGUMENTS` when
    /// the server is not configured or failed to parse (with the original reason, like
    /// `checkLDAPCredentials`) and when it has no `lookup_bind_dn`; `LDAP_ERROR` for every
    /// directory-side failure. Never returns a partial list.
    std::vector<LDAPSyncClient::UserEntry> enumerateLDAPUsers(const String & server,
        const LDAPClient::UserEnumerationParams & enumeration_params, const LDAPClient::RoleSearchParamsList & role_search_params) const;

    /// Checks that LDAP server `server` is defined, parsed, and has the lookup identity the user enumeration
    /// of a synchronised directory binds with, against the configuration this instance holds: a section-level
    /// error (a duplicated name is recorded for the whole `ldap_servers` map) counts like at login time. Throws
    /// `BAD_ARGUMENTS` otherwise. `AccessControl` calls it on the applied configuration once the storages exist
    /// (startup) and on a scratch instance holding a candidate configuration before applying it (reload), so
    /// that a synchronised directory whose server cannot enumerate is refused rather than discovered by its
    /// first run.
    void checkLDAPServerCanEnumerate(const String & server) const;

    /// Whether LDAP server `server` is configured with a service account (`lookup_bind_dn`). False when the
    /// server is unknown or failed to parse. Lets a lazy `ldap` directory decide whether it can revalidate a
    /// cached user against the directory on `EXECUTE AS`.
    bool hasLDAPLookupIdentity(const String & server) const;

    GSSAcceptorContext::Params getKerberosParams() const;

private:
    HTTPAuthClientParams getHTTPAuthenticationParams(const String& server) const;

    /// Copies the parameters of the named LDAP server. Throws `BAD_ARGUMENTS` when the server
    /// is unknown or, with the original error attached, when its configuration failed to parse.
    /// `ldap_server_parse_errors` is consulted before the blueprint, so a name that is in both
    /// (two `ldap_servers` entries sharing it) fails closed.
    LDAPClient::Params getLDAPServerParams(const String & server) const TSA_REQUIRES(mutex);

    struct LDAPCacheEntry
    {
        UInt128 last_successful_params_hash = 0;
        std::chrono::steady_clock::time_point last_successful_authentication_timestamp;
        LDAPClient::SearchResultsList last_successful_role_search_results;
    };

    using LDAPCache = std::unordered_map<String, LDAPCacheEntry>; // user name   -> cache entry
    using LDAPCaches = std::map<String, LDAPCache>;               // server name -> cache
    using LDAPParams = std::map<String, LDAPClient::Params>;      // server name -> params
    using LDAPParseErrors = std::map<String, String>;             // server name -> error message captured at parse time

    mutable std::mutex mutex;
    LDAPParams ldap_client_params_blueprint TSA_GUARDED_BY(mutex) ;
    /// LDAP servers declared in config but rejected by `parseLDAPServer`, with the error.
    /// `checkLDAPCredentials` and `findLDAPUser` rethrow it so a misconfigured server fails
    /// loud at use instead of degrading to "no such user". A name recorded here is never in
    /// `ldap_client_params_blueprint`. Rebuilt on every `setConfiguration`.
    LDAPParseErrors ldap_server_parse_errors TSA_GUARDED_BY(mutex) ;
    mutable LDAPCaches ldap_caches TSA_GUARDED_BY(mutex) ;
    std::optional<GSSAcceptorContext::Params> kerberos_params TSA_GUARDED_BY(mutex) ;
    std::unordered_map<String, HTTPAuthClientParams> http_auth_servers TSA_GUARDED_BY(mutex) ;

    void resetImpl() TSA_REQUIRES(mutex);
};

void parseLDAPRoleSearchParams(LDAPClient::RoleSearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix);

/// Parses the search part of a `<sync>` section of an `ldap` user directory: `base_dn`, `scope`,
/// `search_filter`, `attribute` and `page_size`. Unlike a role mapping, `base_dn`, `search_filter`
/// and `attribute` are mandatory (there is no sensible default for the attribute that holds the
/// user name), `attribute` must not be `dn`, the templates must not contain a per-user placeholder
/// (`{user_name}`, `{bind_dn}`, `{user_dn}`: nothing could substitute it before the users are known)
/// and `page_size` must be between 1 and 1000 (Active Directory rejects larger pages). Throws
/// `BAD_ARGUMENTS` otherwise. `max_entries` is left for the caller to set.
void parseLDAPUserEnumerationParams(LDAPClient::UserEnumerationParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix);

}
