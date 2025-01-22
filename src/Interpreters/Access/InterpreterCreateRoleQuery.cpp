#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateRoleQuery.h>

#include <Access/AccessControl.h>
#include <Access/Role.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
}

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
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query_ptr->as<const ASTCreateRoleQuery &>();

    auto & access_control = getContext()->getAccessControl();
    if (query.alter)
        getContext()->checkAccess(AccessType::ALTER_ROLE);
    else
        getContext()->checkAccess(AccessType::CREATE_ROLE);

    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
    {
        settings_from_query = SettingsProfileElements{*query.settings, access_control};

        if (!query.attach)
            getContext()->checkSettingsConstraints(*settings_from_query, SettingSource::ROLE);
    }

    if (!query.cluster.empty())
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());

    IAccessStorage * storage = &access_control;
    MultipleAccessStorage::StoragePtr storage_ptr;

    if (!query.storage_name.empty())
    {
        storage_ptr = access_control.getStorageByName(query.storage_name);
        storage = storage_ptr.get();
    }

    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
        {
            auto updated_role = typeid_cast<std::shared_ptr<Role>>(entity->clone());
            updateRoleFromQueryImpl(*updated_role, query, {}, settings_from_query);
            return updated_role;
        };
        if (query.if_exists)
        {
            auto ids = storage->find<Role>(query.names);
            storage->tryUpdate(ids, update_func);
        }
        else
            storage->update(storage->getIDs<Role>(query.names), update_func);
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

        if (!query.storage_name.empty())
        {
            for (const auto & name : query.names)
            {
                if (auto another_storage_ptr = access_control.findExcludingStorage(AccessEntityType::ROLE, name, storage_ptr))
                    throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "Role {} already exists in storage {}", name, another_storage_ptr->getStorageName());
            }
        }

        if (query.if_not_exists)
            storage->tryInsert(new_roles);
        else if (query.or_replace)
            storage->insertOrReplace(new_roles);
        else
            storage->insert(new_roles);
    }

    return {};
}


void InterpreterCreateRoleQuery::updateRoleFromQuery(Role & role, const ASTCreateRoleQuery & query)
{
    updateRoleFromQueryImpl(role, query, {}, {});
}

void registerInterpreterCreateRoleQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateRoleQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateRoleQuery", create_fn);
}

}
