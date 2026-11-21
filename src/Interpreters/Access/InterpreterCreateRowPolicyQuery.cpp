#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateRowPolicyQuery.h>

#include <Access/AccessControl.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/RowPolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{

namespace ErrorCodes
{
}

namespace
{
    void updateRowPolicyFromQueryImpl(
        RowPolicy & policy,
        const ASTCreateRowPolicyQuery & query,
        const RowPolicyName & override_name,
        const std::optional<RolesOrUsersSet> & override_to_roles)
    {
        if (!override_name.empty())
            policy.setFullName(override_name);
        else if (!query.new_short_name.empty())
            policy.setShortName(query.new_short_name);
        else if (query.names->full_names.size() == 1)
            policy.setFullName(query.names->full_names.front());

        if (query.is_restrictive)
            policy.setRestrictive(*query.is_restrictive);

        for (const auto & [filter_type, filter] : query.filters)
            policy.filters[static_cast<size_t>(filter_type)] = filter ? filter->formatWithSecretsOneLine() : String{};

        if (override_to_roles)
            policy.to_roles = *override_to_roles;
        else if (query.roles)
            policy.to_roles = *query.roles;
    }
}


BlockIO InterpreterCreateRowPolicyQuery::execute()
{
    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTCreateRowPolicyQuery &>();
    auto required_access = getRequiredAccess();

    /// Reject invalid filters on user-facing CREATE/ALTER only. Deserialization of persisted
    /// policies (ATTACH/replicated/restored) must not fail here; the query-time guard in
    /// ContextAccess::getRowPolicyFilter rejects such policies when they are actually used.
    for (const auto & [filter_type, filter] : query.filters)
        checkRowPolicyFilterExpression(filter);

    /// Must be done before replication: a DDL worker resolves a bare table name in its own current database.
    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        required_access.replaceEmptyDatabase(getContext()->getCurrentDatabase());
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(updated_query_ptr, getContext(), params);
    }

    chassert(query.names->cluster.empty());
    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(required_access);

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.roles)
    {
        roles_from_query = RolesOrUsersSet{*query.roles, access_control, getContext()->getUserID()};
        if (access_control.doesForbidSQLAccessEntitiesTargetingUsersConfig())
            roles_from_query->throwIfTargetingUsersFromUsersConfig(access_control);
    }

    IAccessStorage * storage = &access_control;
    MultipleAccessStorage::StoragePtr storage_ptr;

    if (!query.storage_name.empty())
    {
        storage_ptr = access_control.getStorageByName(query.storage_name);
        storage = storage_ptr.get();
    }

    Strings names = query.names->toStrings();
    if (query.alter)
    {
        auto update_func = [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
        {
            auto updated_policy = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
            updateRowPolicyFromQueryImpl(*updated_policy, query, {}, roles_from_query);
            return updated_policy;
        };
        if (query.if_exists)
        {
            auto ids = storage->find<RowPolicy>(names);
            /// The IDs are resolved in the requested storage, but the update goes through `AccessControl`,
            /// so a rename is checked against the names in the other storages too.
            access_control.tryUpdate(ids, update_func);
        }
        else
            access_control.update(storage->getIDs<RowPolicy>(names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_policies;
        for (const auto & full_name : query.names->full_names)
        {
            auto new_policy = std::make_shared<RowPolicy>();
            updateRowPolicyFromQueryImpl(*new_policy, query, full_name, roles_from_query);
            new_policies.emplace_back(std::move(new_policy));
        }

        /// With a named storage, the check for a name collision in the other storages and the insertion
        /// are done under one lock in `AccessControl`, so that no concurrent statement takes the name in between.
        if (!query.storage_name.empty())
            access_control.insertInto(query.storage_name, new_policies, query.or_replace, !query.if_not_exists);
        else if (query.if_not_exists)
            access_control.tryInsert(new_policies);
        else if (query.or_replace)
            access_control.insertOrReplace(new_policies);
        else
            access_control.insert(new_policies);
    }

    return {};
}


void InterpreterCreateRowPolicyQuery::updateRowPolicyFromQuery(RowPolicy & policy, const ASTCreateRowPolicyQuery & query)
{
    updateRowPolicyFromQueryImpl(policy, query, {}, {});
}


AccessRightsElements InterpreterCreateRowPolicyQuery::getRequiredAccess() const
{
    const auto & query = query_ptr->as<const ASTCreateRowPolicyQuery &>();
    AccessRightsElements res;

    /// `CREATE ROW POLICY OR REPLACE` throws away an existing policy of the same name - including which
    /// roles it applies to - so it is a drop followed by a create and requires the privileges of both.
    /// `DROP ROW POLICY` is required whether or not the policy currently exists, mirroring `REPLACE
    /// TABLE`, so that the check does not reveal which policies exist either.
    AccessFlags access_type = query.alter ? AccessType::ALTER_ROW_POLICY : AccessType::CREATE_ROW_POLICY;
    if (query.or_replace)
        access_type |= AccessType::DROP_ROW_POLICY;

    for (const auto & row_policy_name : query.names->full_names)
        res.emplace_back(access_type, row_policy_name.database, row_policy_name.table_name);
    return res;
}

void registerInterpreterCreateRowPolicyQuery(InterpreterFactory & factory);
void registerInterpreterCreateRowPolicyQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateRowPolicyQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterCreateRowPolicyQuery", create_fn);
}

}
