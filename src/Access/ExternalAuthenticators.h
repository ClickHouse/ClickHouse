#pragma once

#include <Core/Types.h>
#include <Access/LDAPClient.h>
#include <Access/GSSAcceptor.h>

#include <map>
#include <mutex>
#include <optional>


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

    void setLDAPClientParamsBlueprint(const String & server_name, const LDAPClient::Params & params);
    LDAPClient::Params getLDAPClientParamsBlueprint(const String & server_name) const;

    void setKerberosParams(const GSSAcceptorContext::Params & params);
    GSSAcceptorContext::Params getKerberosParams() const;

private:
    mutable std::recursive_mutex mutex;
    std::map<String, LDAPClient::Params> ldap_client_params_blueprint;
    std::optional<GSSAcceptorContext::Params> kerberos_params;
};

}
