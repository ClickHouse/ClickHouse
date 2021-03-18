#pragma once

#include <Access/LDAPParams.h>
#include <common/types.h>

#include <chrono>
#include <map>
#include <mutex>
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
    bool checkLDAPCredentials(const String & server, const String & user_name, const String & password) const;

private:
    struct LDAPCacheEntry
    {
        std::size_t last_successful_params_hash = 0;
        std::chrono::steady_clock::time_point last_successful_authentication_timestamp;
    };

    using LDAPServerCache = std::unordered_map<String, LDAPCacheEntry>; // user name   -> cache entry
    using LDAPServerCaches = std::map<String, LDAPServerCache>;         // server name -> cache
    using LDAPServersParams = std::map<String, LDAPServerParams>;       // server name -> params

private:
    mutable std::recursive_mutex mutex;
    LDAPServersParams ldap_server_params;
    mutable LDAPServerCaches ldap_server_caches;
};

}
