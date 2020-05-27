#pragma once

#include <Access/LDAPParams.h>
#include <Core/Types.h>

#include <map>
#include <mutex>


namespace DB
{

class ExternalAuthenticators
{
public:
    void setLDAPServerParams(const String & server, const LDAPServerParams & params);
    LDAPServerParams getLDAPServerParams(const String & server) const;

private:
    mutable std::mutex mutex;
    std::map<String, LDAPServerParams> ldap_server_params;
};

}
