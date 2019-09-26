#include <Interpreters/InterpreterGrantQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTGrantQuery.h>
#include <ACL/AccessControlManager.h>
#include <ACL/Role.h>


namespace DB
{
BlockIO InterpreterGrantQuery::execute()
{
    const auto & query = query_ptr->as<ASTGrantQuery &>();
    bool is_grant = (query.kind == ASTGrantQuery::Kind::GRANT);
    String database = query.use_current_database ? context.getCurrentDatabase() : query.database;
    AccessControlManager & manager = context.getAccessControlManager();

    AccessControlManager::UpdateFunctions<Role> update_functions;
    for (const auto & to_role_name : query.to_roles)
    {
        update_functions.emplace_back(
            to_role_name,
            [&](Role::Attributes & role_attributes)
            {
                auto grant_or_revoke_access = [&](auto && ... params)
                {
                    if (is_grant)
                    {
                        role_attributes.allowed_databases_by_grant_option[query.grant_option].grant(params...);
                    }
                    else
                    {
                        role_attributes.allowed_databases_by_grant_option[true].revoke(params...);
                        if (!query.grant_option)
                            role_attributes.allowed_databases_by_grant_option[false].revoke(params...);
                    }
                };

                auto grant_or_revoke_role = [&](const UUID & role_id)
                {
                    if (is_grant)
                    {
                        role_attributes.granted_roles_by_admin_option[query.grant_option].insert(role_id);
                    }
                    else
                    {
                        role_attributes.granted_roles_by_admin_option[true].erase(role_id);
                        if (!query.grant_option)
                            role_attributes.granted_roles_by_admin_option[false].erase(role_id);
                    }
                };

                if (query.access)
                {
                    if (database.empty())
                        grant_or_revoke_access(query.access);
                    else if (query.table.empty())
                        grant_or_revoke_access(query.access, database);
                    else
                        grant_or_revoke_access(query.access, database, query.table);
                }

                for (const auto & [column_name, column_access] : query.columns_access)
                    grant_or_revoke_access(column_access, database, query.table, column_name);

                for (const auto & role_name : query.roles)
                    grant_or_revoke_role(manager.get<Role>(role_name).getID());
            });
    }

    manager.update<Role>(update_functions);
    return {};
}
}
