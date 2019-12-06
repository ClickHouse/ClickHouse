#pragma once

#include <Core/Types.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>

#include <memory>
#include <unordered_map>
#include <unordered_set>


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

    /// Table properties.
    using PropertyMap = std::unordered_map<std::string /* name */, std::string /* value */>;
    using TableMap = std::unordered_map<std::string /* table */, PropertyMap /* properties */>;
    using DatabaseMap = std::unordered_map<std::string /* database */, TableMap /* tables */>;
    DatabaseMap table_props;

    bool is_quota_management_allowed = false;

    User(const String & name_, const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};


}
