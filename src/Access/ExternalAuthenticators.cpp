#include <Access/ExternalAuthenticators.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ExternalAuthenticators::setLDAPServerParams(const String & server, const LDAPServerParams & params)
{
    std::scoped_lock lock(mutex);
    ldap_server_params.erase(server);
    ldap_server_params[server] = params;
}

LDAPServerParams ExternalAuthenticators::getLDAPServerParams(const String & server) const
{
    std::scoped_lock lock(mutex);
    auto it = ldap_server_params.find(server);
    if (it == ldap_server_params.end())
        throw Exception("LDAP server '" + server + "' is not configured", ErrorCodes::BAD_ARGUMENTS);
    return it->second;
}

}
