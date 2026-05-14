#pragma once

#include <Access/TokenProcessors.h>
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
#include <memory>
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
    void setConfiguration(const Poco::Util::AbstractConfiguration & config, LoggerPtr log, bool token_auth_enabled = true);

    bool isTokenAuthEnabled() const;

    /// Returns true if a token processor with the given name is currently
    /// configured. Used by `Session::checkIfUserIsStillValid` to terminate
    /// active sessions whose authenticating processor was removed by config
    /// reload (M-28). Empty `name` is treated as "no specific pin" and
    /// returns true (token auth must still be enabled, of course).
    bool hasTokenProcessor(const String & name) const;

    // The name and readiness of the credentials must be verified before calling these.
    bool checkLDAPCredentials(const String & server, const BasicCredentials & credentials,
        const LDAPClient::RoleSearchParamsList * role_search_params = nullptr, LDAPClient::SearchResultsList * role_search_results = nullptr) const;
    bool checkKerberosCredentials(const String & realm, const GSSAcceptorContext & credentials) const;
    bool checkHTTPBasicCredentials(const String & server, const BasicCredentials & credentials, const ClientInfo & client_info, SettingsChanges & settings) const;

    /// `prime_cache_on_success` controls whether a successful validation populates the
    /// token cache. Per-user authentication paths (the chain reached from
    /// `Session::authenticate`) leave this at the default `true` -- their result is
    /// gated by the user's pinned processor and per-user JWT claims, so the cache
    /// entry it produces is safe to consult on subsequent requests. The HTTP and TCP
    /// bearer entry points authenticate the token *before* the user is known
    /// (they need the username from the token to drive user lookup) and so call
    /// this with `false`: their decision is made under no processor pin and no
    /// claims constraint, and a cache entry written from that context would be
    /// trusted by a later per-user call whose `processor_name` is empty -- bypassing
    /// the per-user processor and claim selection that would otherwise occur.
    bool checkTokenCredentials(const TokenCredentials & credentials,
                               const String & processor_name = "",
                               const String & jwt_claims = "",
                               bool prime_cache_on_success = true) const;

    GSSAcceptorContext::Params getKerberosParams() const;

private:
    HTTPAuthClientParams getHTTPAuthenticationParams(const String& server) const;

    struct LDAPCacheEntry
    {
        UInt128 last_successful_params_hash = 0;
        std::chrono::steady_clock::time_point last_successful_authentication_timestamp;
        LDAPClient::SearchResultsList last_successful_role_search_results;
    };

    using LDAPCache = std::unordered_map<String, LDAPCacheEntry>; // user name   -> cache entry
    using LDAPCaches = std::map<String, LDAPCache>;               // server name -> cache
    using LDAPParams = std::map<String, LDAPClient::Params>;      // server name -> params

    mutable std::mutex mutex;
    LDAPParams ldap_client_params_blueprint TSA_GUARDED_BY(mutex) ;
    mutable LDAPCaches ldap_caches TSA_GUARDED_BY(mutex) ;
    std::optional<GSSAcceptorContext::Params> kerberos_params TSA_GUARDED_BY(mutex) ;
    std::unordered_map<String, HTTPAuthClientParams> http_auth_servers TSA_GUARDED_BY(mutex) ;
    /// Ordered (std::map, not unordered_map) so that the auto-discovery
    /// dispatch order in `checkTokenCredentials` is deterministic across
    /// process runs. Without an ordering, the iteration order of
    /// `unordered_map` is implementation-defined and may differ run-to-run
    /// or after rehashing -- which means the same unpinned token can be
    /// validated by processor A in one run and processor B in another,
    /// producing different cached identities, different role mappings (each
    /// processor has its own `groups_claim`), and surprising debugging
    /// outcomes. Alphabetical-by-name order makes "first to succeed wins"
    /// stable and predictable from configuration alone.
    ///
    /// `shared_ptr` so callers can snapshot the relevant processor pointer
    /// (or the whole map) under the mutex, RELEASE the mutex, and run the
    /// expensive crypto verify without serializing the entire auth
    /// subsystem behind a single attacker-driven RSA verify (M-20). Cheap:
    /// processor count is tiny, snapshot is shared_ptr copies.
    mutable std::map<String, std::shared_ptr<ITokenProcessor>> token_processors TSA_GUARDED_BY(mutex) ;

    struct TokenCacheEntry
    {
        std::chrono::system_clock::time_point expires_at;
        String user_name;
        std::set<String> external_roles;
        /// Name of the token processor that produced this cache entry.
        String processor_name;
    };

    /// Home-made simple bi-mapping, needed to effectively clean up cache from old tokens.
    using TokenToUsernameCache = std::unordered_map<String, TokenCacheEntry>;  // Access token -> cache entry
    using UsernameToTokenCache = std::unordered_map<String, String>;                 // User name -> access token

    mutable TokenToUsernameCache access_token_to_username_cache TSA_GUARDED_BY(mutex) ;
    mutable UsernameToTokenCache username_to_access_token_cache TSA_GUARDED_BY(mutex) ;

    bool token_auth_enabled TSA_GUARDED_BY(mutex) = true;

    /// Validates the credentials with the given processor. On success, mutates
    /// `credentials` (user name, groups, effective expires_at) and returns true.
    /// Does NOT write the token cache -- caching is the responsibility of the
    /// caller, after the per-user `jwt_claims` policy has been evaluated.
    ///
    /// MUST be called WITHOUT holding `mutex`: this is the expensive crypto
    /// path (M-20). The processor must be passed by `shared_ptr` so it
    /// outlives a concurrent config reload that resets `token_processors`.
    bool checkCredentialsAgainstProcessor(const ITokenProcessor & processor,
                                          TokenCredentials & credentials) const;

    /// Writes the per-token cache entry. Must be called only after both processor
    /// validation AND any per-user `jwt_claims` policy have accepted the token.
    void primeTokenCache(const ITokenProcessor & processor,
                         const TokenCredentials & credentials) const TSA_REQUIRES(mutex);

    void resetImpl() TSA_REQUIRES(mutex);
};

void parseLDAPRoleSearchParams(LDAPClient::RoleSearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix);

}
