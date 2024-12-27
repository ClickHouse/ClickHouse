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
    extern const int SETTING_CONSTRAINT_VIOLATION;
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

    std::vector<UUID> name_ids;
    if (query.alter)
    {
        if (query.if_exists)
        {
            name_ids = access_control.find<SettingsProfile>(query.names);
        }
        else
        {
            name_ids = access_control.getIDs<SettingsProfile>(query.names);
        }
    }
    bool has_parent_profile = false;
    std::optional<SettingsProfileElements> settings_from_query;
    if (query.settings)
    {
        settings_from_query = SettingsProfileElements{*query.settings, access_control};
        for (const auto & element : *settings_from_query)
        {
            if (element.parent_profile.has_value())
            {
                has_parent_profile = true;
                for (const auto & name_id: name_ids)
                {
                    if (access_control.isExpectedProfileOrDescendant(element.parent_profile.value(), name_id))
                    {
                        throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Inherited profiles may not have circular dependencies");
                    }
                }
            }
        }
        // This checks that each defined parent is either default or inheriting from it
        if (!query.attach)
            getContext()->checkSettingsConstraints(*settings_from_query, SettingSource::PROFILE);
    }

    // If the query is a create (= not an alter), then it must either be the default
    // or have a parent profile (which has been checked to be to inherit from default).
    // If the query is an alter and it has settings change, same thing applies.
    // A query can be an alter without settings change if it's a noop or just renaming the profile itself.
    // Instead of rejecting a query without a parent, we auto-inject the default profile as a parent.
    if (!query.alter || settings_from_query)
    {
        if (!has_parent_profile && !getContext()->getSettings().allow_non_default_profile)
        {
            bool has_default_profile = false;
            const auto & default_profile_id = access_control.getDefaultProfileId();
            if (!default_profile_id.has_value())
                throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Cannot alter or create profiles if default profile is not configured.");
            for (const auto & name_id : name_ids)
                if (name_id == default_profile_id.value())
                    has_default_profile = true;
            if (has_default_profile and query.names.size() > 1)
                throw Exception(ErrorCodes::SETTING_CONSTRAINT_VIOLATION, "Cannot alter the default and other profiles simultaneously.");
            SettingsProfileElement default_parent;
            default_parent.parent_profile = default_profile_id;
            default_parent.setting_name = "profile";
            if (!settings_from_query.has_value())
                settings_from_query = SettingsProfileElements();
            settings_from_query->insert(settings_from_query->begin(), default_parent);
        }
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
        auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
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
        auto check_func = [](const AccessEntityPtr &){};
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
            storage->insertOrReplace(new_profiles, check_func);
        else
            storage->insert(new_profiles, check_func);
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
