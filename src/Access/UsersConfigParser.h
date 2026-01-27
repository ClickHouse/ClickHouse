#pragma once

#include <Access/IAccessEntity.h>
#include <Core/UUID.h>
#include <vector>
#include <memory>
#include <unordered_set>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class AccessControl;
struct IAccessEntity;


class UsersConfigParser
{
public:
    explicit UsersConfigParser(AccessControl & access_control_);

    std::vector<AccessEntityPtr> parseUsers(
        const Poco::Util::AbstractConfiguration & config,
        const std::unordered_set<UUID> & allowed_profile_ids,
        const std::unordered_set<UUID> & role_ids_from_users_config) const;

    std::vector<AccessEntityPtr> parseRoles(
        const Poco::Util::AbstractConfiguration & config,
        const std::unordered_set<UUID> & role_ids_from_users_config) const;

    std::vector<AccessEntityPtr> parseQuotas(const Poco::Util::AbstractConfiguration & config) const;
    std::vector<AccessEntityPtr> parseRowPolicies(const Poco::Util::AbstractConfiguration & config) const;

    std::vector<AccessEntityPtr> parseSettingsProfiles(
        const Poco::Util::AbstractConfiguration & config,
        const std::unordered_set<UUID> & allowed_parent_profile_ids) const;

    static UUID generateID(AccessEntityType type, const String & name);
    static UUID generateID(const IAccessEntity & entity);

    static std::unordered_set<UUID> getAllowedIDs(
        const Poco::Util::AbstractConfiguration & config,
        const String & configuration_key,
        AccessEntityType type);

private:
    AccessControl & access_control;
    LoggerPtr log;
};

}
