#include <Interpreters/InterpreterGrantQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTGrantQuery.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>


namespace DB
{
BlockIO InterpreterGrantQuery::execute()
{
    const auto & query = query_ptr->as<ASTGrantQuery &>();
    bool is_grant = (query.kind == ASTGrantQuery::Kind::GRANT);
    String database = query.use_current_database ? context.getCurrentDatabase() : query.database;
    AccessControlManager & manager = context.getAccessControlManager();
    bool partial_revokes = context.getSettings().partial_revokes;

    std::vector<UUID> role_ids;
    role_ids.reserve(query.roles.size());
    for (const auto & role_name : query.roles)
        role_ids.emplace_back(manager.getID(role_name, Role::TYPE));

    manager.update<Role>(
        query.to_roles,
        [&](Role & role)
        {
            auto grant_or_revoke_access = [&](auto && ... params)
            {
                if (is_grant)
                {
                    role.privileges[query.grant_option].grant(params...);
                }
                else
                {
                    role.privileges[true].revoke(params..., partial_revokes);
                    if (!query.grant_option)
                        role.privileges[false].revoke(params..., partial_revokes);
                }
            };

            auto grant_or_revoke_role = [&](const UUID & role_id)
            {
                if (is_grant)
                {
                    role.granted_roles[query.grant_option].insert(role_id);
                }
                else
                {
                    role.granted_roles[true].erase(role_id);
                    if (!query.grant_option)
                        role.granted_roles[false].erase(role_id);
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

            for (const auto & role_id : role_ids)
                grant_or_revoke_role(role_id);
        });

    return {};
}
}
