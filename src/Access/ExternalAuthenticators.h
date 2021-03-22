#pragma once

#include <Access/LDAPParams.h>
#include <Core/Types.h>

#include <map>
#include <memory>
#include <mutex>


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
    void setConfig(const Poco::Util::AbstractConfiguration & config, Poco::Logger * log);

    void setLDAPServerParams(const String & server, const LDAPServerParams & params);
    LDAPServerParams getLDAPServerParams(const String & server) const;

private:
    mutable std::recursive_mutex mutex;
    std::map<String, LDAPServerParams> ldap_server_params;
};

}
