#pragma once

#include <Access/LDAPClient.h>
#include <Access/Credentials.h>
#include <Access/GSSAcceptor.h>
#include <common/types.h>

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

class ExternalAuthenticators
{
public:
    void reset();
    void setConfiguration(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log);

    // The name and readiness of the credentials must be verified before calling these.
    bool checkLDAPCredentials(const String & server, const BasicCredentials & credentials,
        const LDAPClient::RoleSearchParamsList * role_search_params = nullptr, LDAPClient::SearchResultsList * role_search_results = nullptr) const;
    bool checkKerberosCredentials(const String & realm, const GSSAcceptorContext & credentials) const;

    GSSAcceptorContext::Params getKerberosParams() const;

private:
    struct LDAPCacheEntry
    {
        std::size_t last_successful_params_hash = 0;
        std::chrono::steady_clock::time_point last_successful_authentication_timestamp;
        LDAPClient::SearchResultsList last_successful_role_search_results;
    };

    using LDAPCache = std::unordered_map<String, LDAPCacheEntry>; // user name   -> cache entry
    using LDAPCaches = std::map<String, LDAPCache>;               // server name -> cache
    using LDAPParams = std::map<String, LDAPClient::Params>;      // server name -> params

private:
    mutable std::recursive_mutex mutex;
    LDAPParams ldap_client_params_blueprint;
    mutable LDAPCaches ldap_caches;
    std::optional<GSSAcceptorContext::Params> kerberos_params;
};

void parseLDAPRoleSearchParams(LDAPClient::RoleSearchParams & params, const Poco::Util::AbstractConfiguration & config, const String & prefix);

}
