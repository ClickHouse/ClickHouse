#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>

#include <memory>
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

    /// Required password.
    Authentication authentication;

    String profile;

    AllowedClientHosts allowed_client_hosts;

    /// List of allowed databases.
    using DatabaseSet = std::unordered_set<std::string>;
    std::optional<DatabaseSet> databases;

    /// List of allowed dictionaries.
    using DictionarySet = std::unordered_set<std::string>;
    std::optional<DictionarySet> dictionaries;

    bool is_quota_management_allowed = false;

    User(const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};


}
