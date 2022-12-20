#include <Interpreters/Access/InterpreterCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/SettingsProfile.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>


namespace DB
{
namespace
{
    void updateSettingsProfileFromQueryImpl(
        SettingsProfile & profile,
        const ASTCreateSettingsProfileQuery & query,
        const String & override_name,
        const std::optional<SettingsProfileElements> & override_settings,
        const std::optional<RolesOrUsersSet> & override_to_roles)
    {
        if (!override_name.empty())
            profile.setName(override_name);
        else if (!query.new_name.empty())
            profile.setName(query.new_name);
        else if (query.names.size() == 1)
            profile.setName(query.names.front());

        if (override_settings)
            profile.elements = *override_settings;
        else if (query.settings)
            profile.elements = *query.settings;

        if (override_to_roles)
            profile.to_roles = *override_to_roles;
        else if (query.to_roles)
            profile.to_roles = *query.to_roles;
    }
}


BlockIO InterpreterCreateSettingsProfileQuery::execute()
{
    auto & query = query_ptr->as<ASTCreateSettingsProfileQuery &>();
    auto & access_control = getContext()->getAccessControl();
    if (query.alter)
        getContext()->checkAccess(AccessType::ALTER_SETTINGS_PROFILE);
    else
        getContext()->checkAccess(AccessType::CREATE_SETTINGS_PROFILE);

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(query_ptr, getContext());
    }

    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
        settings_from_query = SettingsProfileElements{*query.settings, access_control};

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.to_roles)
        roles_from_query = RolesOrUsersSet{*query.to_roles, access_control, getContext()->getUserID()};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_profile = typeid_cast<std::shared_ptr<SettingsProfile>>(entity->clone());
            updateSettingsProfileFromQueryImpl(*updated_profile, query, {}, settings_from_query, roles_from_query);
            return updated_profile;
        };
        if (query.if_exists)
        {
            auto ids = access_control.find<SettingsProfile>(query.names);
            access_control.tryUpdate(ids, update_func);
        }
        else
            access_control.update(access_control.getIDs<SettingsProfile>(query.names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_profiles;
        for (const auto & name : query.names)
        {
            auto new_profile = std::make_shared<SettingsProfile>();
            updateSettingsProfileFromQueryImpl(*new_profile, query, name, settings_from_query, roles_from_query);
            new_profiles.emplace_back(std::move(new_profile));
        }

        if (query.if_not_exists)
            access_control.tryInsert(new_profiles);
        else if (query.or_replace)
            access_control.insertOrReplace(new_profiles);
        else
            access_control.insert(new_profiles);
    }

    return {};
}


void InterpreterCreateSettingsProfileQuery::updateSettingsProfileFromQuery(SettingsProfile & SettingsProfile, const ASTCreateSettingsProfileQuery & query)
{
    updateSettingsProfileFromQueryImpl(SettingsProfile, query, {}, {}, {});
}
}
