#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateSettingsProfileQuery.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/SettingsProfile.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
}

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
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTCreateSettingsProfileQuery &>();

    auto & access_control = getContext()->getAccessControl();
    if (query.alter)
        getContext()->checkAccess(AccessType::ALTER_SETTINGS_PROFILE);
    else
        getContext()->checkAccess(AccessType::CREATE_SETTINGS_PROFILE);

    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
    {
        settings_from_query = SettingsProfileElements{*query.settings, access_control};

        if (!query.attach)
            getContext()->checkSettingsConstraints(*settings_from_query, SettingSource::PROFILE);
    }

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        return executeDDLQueryOnCluster(updated_query_ptr, getContext());
    }

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.to_roles)
        roles_from_query = RolesOrUsersSet{*query.to_roles, access_control, getContext()->getUserID()};


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
            auto updated_profile = typeid_cast<std::shared_ptr<SettingsProfile>>(entity->clone());
            updateSettingsProfileFromQueryImpl(*updated_profile, query, {}, settings_from_query, roles_from_query);
            return updated_profile;
        };
        if (query.if_exists)
        {
            auto ids = storage->find<SettingsProfile>(query.names);
            storage->tryUpdate(ids, update_func);
        }
        else
            storage->update(storage->getIDs<SettingsProfile>(query.names), update_func);
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

        if (!query.storage_name.empty())
        {
            for (const auto & name : query.names)
            {
                if (auto another_storage_ptr = access_control.findExcludingStorage(AccessEntityType::SETTINGS_PROFILE, name, storage_ptr))
                    throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "Settings profile {} already exists in storage {}", name, another_storage_ptr->getStorageName());
            }
        }

        if (query.if_not_exists)
            storage->tryInsert(new_profiles);
        else if (query.or_replace)
            storage->insertOrReplace(new_profiles);
        else
            storage->insert(new_profiles);
    }

    return {};
}


void InterpreterCreateSettingsProfileQuery::updateSettingsProfileFromQuery(SettingsProfile & SettingsProfile, const ASTCreateSettingsProfileQuery & query)
{
    updateSettingsProfileFromQueryImpl(SettingsProfile, query, {}, {}, {});
}

void registerInterpreterCreateSettingsProfileQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateSettingsProfileQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateSettingsProfileQuery", create_fn);
}

}
