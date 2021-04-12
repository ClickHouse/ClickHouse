#include <Interpreters/InterpreterCreateRoleQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>


namespace DB
{
namespace
{
    void updateRoleFromQueryImpl(
        Role & role,
        const ASTCreateRoleQuery & query,
        const String & override_name,
        const std::optional<SettingsProfileElements> & override_settings)
    {
        if (!override_name.empty())
            role.setName(override_name);
        else if (!query.new_name.empty())
            role.setName(query.new_name);
        else if (query.names.size() == 1)
            role.setName(query.names.front());

        if (override_settings)
            role.settings = *override_settings;
        else if (query.settings)
            role.settings = *query.settings;
    }
}


BlockIO InterpreterCreateRoleQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateRoleQuery &>();
    auto & access_control = getContext()->getAccessControlManager();
    if (query.alter)
        getContext()->checkAccess(AccessType::ALTER_ROLE);
    else
        getContext()->checkAccess(AccessType::CREATE_ROLE);

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, getContext());

    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
        settings_from_query = SettingsProfileElements{*query.settings, access_control};

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
        {
            auto updated_role = typeid_cast<std::shared_ptr<Role>>(entity->clone());
            updateRoleFromQueryImpl(*updated_role, query, {}, settings_from_query);
            return updated_role;
        };
        if (query.if_exists)
        {
            auto ids = access_control.find<Role>(query.names);
            access_control.tryUpdate(ids, update_func);
        }
        else
            access_control.update(access_control.getIDs<Role>(query.names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_roles;
        for (const auto & name : query.names)
        {
            auto new_role = std::make_shared<Role>();
            updateRoleFromQueryImpl(*new_role, query, name, settings_from_query);
            new_roles.emplace_back(std::move(new_role));
        }

        if (query.if_not_exists)
            access_control.tryInsert(new_roles);
        else if (query.or_replace)
            access_control.insertOrReplace(new_roles);
        else
            access_control.insert(new_roles);
    }

    return {};
}


void InterpreterCreateRoleQuery::updateRoleFromQuery(Role & role, const ASTCreateRoleQuery & query)
{
    updateRoleFromQueryImpl(role, query, {}, {});
}
}
