#pragma once

#include <Core/Types.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>
#include <Access/AccessRights.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{
/** User and ACL.
  */
struct User
{
    String name;

    /// Required password.
    Authentication authentication;

    String profile;

    AllowedClientHosts allowed_client_hosts;

    AccessRights access;

    User(const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};


}
