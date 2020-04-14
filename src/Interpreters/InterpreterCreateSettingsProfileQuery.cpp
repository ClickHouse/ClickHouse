#include <Interpreters/InterpreterCreateSettingsProfileQuery.h>
#include <Parsers/ASTCreateSettingsProfileQuery.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Access/AccessControlManager.h>
#include <Access/SettingsProfile.h>
#include <Access/AccessFlags.h>


namespace DB
{
namespace
{
    void updateSettingsProfileFromQueryImpl(
        SettingsProfile & profile,
        const ASTCreateSettingsProfileQuery & query,
        const std::optional<SettingsProfileElements> & settings_from_query = {},
        const std::optional<ExtendedRoleSet> & roles_from_query = {})
    {
        if (query.alter)
        {
            if (!query.new_name.empty())
                profile.setName(query.new_name);
        }
        else
            profile.setName(query.name);

        const SettingsProfileElements * settings = nullptr;
        std::optional<SettingsProfileElements> temp_settings;
        if (settings_from_query)
            settings = &*settings_from_query;
        else if (query.settings)
            settings = &temp_settings.emplace(*query.settings);

        if (settings)
            profile.elements = *settings;

        const ExtendedRoleSet * roles = nullptr;
        std::optional<ExtendedRoleSet> temp_role_set;
        if (roles_from_query)
            roles = &*roles_from_query;
        else if (query.to_roles)
            roles = &temp_role_set.emplace(*query.to_roles);

        if (roles)
            profile.to_roles = *roles;
    }
}


BlockIO InterpreterCreateSettingsProfileQuery::execute()
{
    auto & query = query_ptr->as<ASTCreateSettingsProfileQuery &>();
    auto & access_control = context.getAccessControlManager();
    if (query.alter)
        context.checkAccess(AccessType::ALTER_SETTINGS_PROFILE);
    else
        context.checkAccess(AccessType::CREATE_SETTINGS_PROFILE);

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTagWithName(context.getUserName());
        return executeDDLQueryOnCluster(query_ptr, context);
    }

    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
        settings_from_query = SettingsProfileElements{*query.settings, access_control};

    std::optional<ExtendedRoleSet> roles_from_query;
    if (query.to_roles)
        roles_from_query = ExtendedRoleSet{*query.to_roles, access_control, context.getUserID()};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_profile = typeid_cast<std::shared_ptr<SettingsProfile>>(entity->clone());
            updateSettingsProfileFromQueryImpl(*updated_profile, query, settings_from_query, roles_from_query);
            return updated_profile;
        };
        if (query.if_exists)
        {
            if (auto id = access_control.find<SettingsProfile>(query.name))
                access_control.tryUpdate(*id, update_func);
        }
        else
            access_control.update(access_control.getID<SettingsProfile>(query.name), update_func);
    }
    else
    {
        auto new_profile = std::make_shared<SettingsProfile>();
        updateSettingsProfileFromQueryImpl(*new_profile, query, settings_from_query, roles_from_query);

        if (query.if_not_exists)
            access_control.tryInsert(new_profile);
        else if (query.or_replace)
            access_control.insertOrReplace(new_profile);
        else
            access_control.insert(new_profile);
    }

    return {};
}


void InterpreterCreateSettingsProfileQuery::updateSettingsProfileFromQuery(SettingsProfile & SettingsProfile, const ASTCreateSettingsProfileQuery & query)
{
    updateSettingsProfileFromQueryImpl(SettingsProfile, query);
}
}
