#pragma once

#include <Core/Types.h>
#include <ACL/AllowedHosts.h>
#include <ACL/EncodedPassword.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>


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

    /// Required password. Could be stored in plaintext or in SHA256.
    EncodedPassword encoded_password;

    String profile;
    String quota;

    AllowedHosts allowed_hosts;

    /// List of allowed databases.
    using DatabaseSet = std::unordered_set<std::string>;
    DatabaseSet databases;

    /// Table properties.
    using PropertyMap = std::unordered_map<std::string /* name */, std::string /* value */>;
    using TableMap = std::unordered_map<std::string /* table */, PropertyMap /* properties */>;
    using DatabaseMap = std::unordered_map<std::string /* database */, TableMap /* tables */>;
    DatabaseMap table_props;

    User(const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};


}
