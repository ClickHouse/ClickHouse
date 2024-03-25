#pragma once

#include <Access/Credentials.h>
#include <Access/GSSAcceptor.h>
#include <Access/HTTPAuthClient.h>
#include <Access/LDAPClient.h>
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
    bool checkHTTPBasicCredentials(const String & server, const BasicCredentials & credentials, SettingsChanges & settings) const;

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

    void resetImpl() TSA_REQUIRES(mutex);
};

void parseLDAPRoleSearchParams(LDAPClient::RoleSearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix);

}
