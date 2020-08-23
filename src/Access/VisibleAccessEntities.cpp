#include <Access/VisibleAccessEntities.h>
#include <Access/AccessControlManager.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Access/RowPolicy.h>
#include <Access/Quota.h>
#include <Access/QuotaUsage.h>
#include <Access/SettingsProfile.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace
{
    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;

    struct CheckedGrants
    {
        std::optional<bool> show_users;
        std::optional<bool> show_roles;
        std::optional<bool> show_quotas;
        std::optional<bool> show_settings_profiles;
    };


    bool isVisibleImpl(
        const ContextAccess & access,
        const UUID & id,
        AccessEntityPtr & entity,
        std::optional<IAccessEntity::Type> & entity_type,
        CheckedGrants & checked_grants)
    {
        const auto & access_control = access.getAccessControlManager();

        if (!entity_type)
        {
            if (!entity)
            {
                entity = access_control.tryRead(id);
                if (!entity)
                    return false;
            }
            entity_type = entity->getType();
        }

        switch (*entity_type)
        {
            case EntityType::USER:
            {
                if (!checked_grants.show_users)
                    checked_grants.show_users = access.isGranted(AccessType::SHOW_USERS);
                if (*checked_grants.show_users)
                    return true;
                return (id == access.getUserID());
            }

            case EntityType::ROLE:
            {
                if (!checked_grants.show_roles)
                    checked_grants.show_roles = access.isGranted(AccessType::SHOW_ROLES);
                if (*checked_grants.show_roles)
                    return true;
                auto roles_info = access.getRolesInfo();
                return roles_info && roles_info->enabled_roles.count(id);
            }

            case EntityType::ROW_POLICY:
            {
                if (!entity)
                {
                    entity = access_control.tryRead(id);
                    if (!entity)
                        return false;
                }
                auto policy = typeid_cast<RowPolicyPtr>(entity);
                return access.isGranted(AccessType::SHOW_TABLES, policy->getDatabase(), policy->getTableName());
            }

            case EntityType::QUOTA:
            {
                if (!checked_grants.show_quotas)
                    checked_grants.show_quotas = access.isGranted(AccessType::SHOW_QUOTAS);
                if (*checked_grants.show_quotas)
                    return true;
                auto quota_usage = access.getQuotaUsage();
                return quota_usage && (id == quota_usage->quota_id);
            }

            case EntityType::SETTINGS_PROFILE:
            {
                if (!checked_grants.show_settings_profiles)
                    checked_grants.show_settings_profiles = access.isGranted(AccessType::SHOW_SETTINGS_PROFILES);
                if (*checked_grants.show_settings_profiles)
                    return true;
                auto enabled_profile_ids = access.getEnabledProfileIDs();
                return enabled_profile_ids && enabled_profile_ids->count(id);
            }

            case EntityType::MAX: break;
        }
        __builtin_unreachable();
    }


    std::vector<UUID> findAllImpl(const ContextAccess & access, EntityType entity_type)
    {
        const auto & access_control = access.getAccessControlManager();

        switch (entity_type)
        {
            case EntityType::USER:
            {
                if (access.isGranted(AccessType::SHOW_USERS))
                    return access_control.findAll<User>();
                if (auto id = access.getUserID())
                    return {*id};
                return {};
            }

            case EntityType::ROLE:
            {
                if (access.isGranted(AccessType::SHOW_ROLES))
                    return access_control.findAll<Role>();
                if (auto roles_info = access.getRolesInfo())
                    return std::vector<UUID>{roles_info->enabled_roles.begin(), roles_info->enabled_roles.end()};
                return {};
            }

            case EntityType::ROW_POLICY:
            {
                auto ids = access_control.findAll<RowPolicy>();
                boost::range::remove_erase_if(
                    ids, [&](const UUID & id) -> bool
                {
                    RowPolicyPtr policy = access_control.tryRead<RowPolicy>(id);
                    return !policy || !access.isGranted(AccessType::SHOW_TABLES, policy->getDatabase(), policy->getTableName());
                });
                return ids;
            }

            case EntityType::QUOTA:
            {
                if (access.isGranted(AccessType::SHOW_QUOTAS))
                    return access_control.findAll<Quota>();
                if (auto quota_usage = access.getQuotaUsage())
                    return {quota_usage->quota_id};
                return {};
            }

            case EntityType::SETTINGS_PROFILE:
            {
                if (access.isGranted(AccessType::SHOW_SETTINGS_PROFILES))
                    return access_control.findAll<SettingsProfile>();
                if (auto enabled_profile_ids = access.getEnabledProfileIDs())
                    return std::vector<UUID>(enabled_profile_ids->begin(), enabled_profile_ids->end());
                return {};
            }

            case EntityType::MAX: break;
        }
        __builtin_unreachable();
    }


    [[noreturn]] void throwNotFound(EntityType type, const String & name)
    {
        const auto & type_info = EntityTypeInfo::get(type);
        int error_code = type_info.not_found_error_code;
        throw Exception("There is no " + type_info.outputWithEntityName(name) + " in user directories", error_code);
    }
}


VisibleAccessEntities::VisibleAccessEntities(const std::shared_ptr<const ContextAccess> & access_) : access(access_)
{
}

VisibleAccessEntities::~VisibleAccessEntities() = default;

std::shared_ptr<const ContextAccess> VisibleAccessEntities::getAccess() const
{
    return access;
}

const AccessControlManager & VisibleAccessEntities::getAccessControlManager() const
{
    return access->getAccessControlManager();
}


bool VisibleAccessEntities::exists(const UUID & id) const
{
    AccessEntityPtr entity;
    std::optional<EntityType> entity_type;
    CheckedGrants checked_grants;
    return isVisibleImpl(*access, id, entity, entity_type, checked_grants);
}


std::optional<UUID> VisibleAccessEntities::find(EntityType type, const String & name) const
{
    auto id = access->getAccessControlManager().find(type, name);
    if (!id)
        return {};
    AccessEntityPtr entity;
    std::optional<EntityType> entity_type = type;
    CheckedGrants checked_grants;
    if (!isVisibleImpl(*access, *id, entity, entity_type, checked_grants))
        return {};
    return id;
}

std::vector<UUID> VisibleAccessEntities::find(EntityType type, const Strings & names) const
{
    auto ids = access->getAccessControlManager().find(type, names);
    std::optional<EntityType> entity_type = type;
    CheckedGrants checked_grants;
    boost::range::remove_erase_if(
        ids, [&](const UUID & id) -> bool
    {
        AccessEntityPtr entity;
        return !isVisibleImpl(*access, id, entity, entity_type, checked_grants);
    });
    return ids;
}


std::vector<UUID> VisibleAccessEntities::findAll(EntityType type) const
{
    return findAllImpl(*access, type);
}


UUID VisibleAccessEntities::getID(EntityType type, const String & name) const
{
    auto id = access->getAccessControlManager().find(type, name);
    if (id)
    {
        AccessEntityPtr entity;
        std::optional<EntityType> entity_type = type;
        CheckedGrants checked_grants;
        if (isVisibleImpl(*access, *id, entity, entity_type, checked_grants))
            return *id;
    }
    throwNotFound(type, name);
}


std::vector<UUID> VisibleAccessEntities::getIDs(EntityType type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    std::optional<EntityType> entity_type = type;
    CheckedGrants checked_grants;
    for (const String & name : names)
    {
        auto id = access->getAccessControlManager().find(type, name);
        if (id)
        {
            AccessEntityPtr entity;
            if (isVisibleImpl(*access, *id, entity, entity_type, checked_grants))
            {
                ids.emplace_back(*id);
                continue;
            }
        }
        throwNotFound(type, name);
    }
    return ids;
}


AccessEntityPtr VisibleAccessEntities::readImpl(const UUID & id) const
{
    return access->getAccessControlManager().read(id);
}

AccessEntityPtr VisibleAccessEntities::tryReadImpl(const UUID & id) const
{
    return access->getAccessControlManager().tryRead(id);
}

void VisibleAccessEntities::throwBadCast(const UUID & id, EntityType type, const String & name, EntityType required_type)
{
    const auto & type_info = EntityTypeInfo::get(type);
    throw Exception(
        "ID(" + toString(id) + "): " + type_info.outputWithEntityName(name) + " expected to be of type " + toString(required_type),
        ErrorCodes::LOGICAL_ERROR);
}

}
