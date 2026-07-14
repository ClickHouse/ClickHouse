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
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
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
    {
        /// fold on the original query so getRequiredAccess sees the stored names
        auto & original = query_ptr->as<ASTCreateRowPolicyQuery &>();

        /// Row policies have no namespace-scoped wildcard, so `ON *` under `USE db.namespace`
        /// would silently target the whole catalog instead of the selected namespace.
        const auto current_db_info = getContext()->getCurrentDatabaseInfo();
        if (!current_db_info.table_prefix.empty())
            for (const auto & full_name : original.names->full_names)
                if (full_name.database.empty() && full_name.table_name.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "CREATE ROW POLICY ON * is not supported while a namespace is selected "
                        "(the policy would apply to the whole database {}); specify a table "
                        "or use the database without a namespace", backQuoteIfNeed(current_db_info.database));

        original.replaceEmptyDatabase(current_db_info);
    }

    const auto updated_query_ptr = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    auto & query = updated_query_ptr->as<ASTCreateRowPolicyQuery &>();

    /// Reject invalid filters on user-facing CREATE/ALTER only. Deserialization of persisted
    /// policies (ATTACH/replicated/restored) must not fail here; the query-time guard in
    /// ContextAccess::getRowPolicyFilter rejects such policies when they are actually used.
    for (const auto & [filter_type, filter] : query.filters)
        checkRowPolicyFilterExpression(filter);

    if (!query.cluster.empty())
    {
        auto required_access = getRequiredAccess();
        query.replaceCurrentUserTag(getContext()->getUserName());
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(updated_query_ptr, getContext(), params);
    }

    chassert(query.names->cluster.empty());
    auto required_access = getRequiredAccess();
    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(required_access);

    std::optional<RolesOrUsersSet> roles_from_query;
    if (query.roles)
        roles_from_query = RolesOrUsersSet{*query.roles, access_control, getContext()->getUserID()};

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
            storage->tryUpdate(ids, update_func);
        }
        else
            storage->update(storage->getIDs<RowPolicy>(names), update_func);
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

        if (!query.storage_name.empty())
        {
            for (const auto & name : names)
            {
                if (auto another_storage_ptr = access_control.findExcludingStorage(AccessEntityType::ROW_POLICY, name, storage_ptr))
                    throw Exception(ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, "Row policy {} already exists in storage {}", name, another_storage_ptr->getStorageName());
            }
        }

        if (query.if_not_exists)
            storage->tryInsert(new_policies);
        else if (query.or_replace)
            storage->insertOrReplace(new_policies);
        else
            storage->insert(new_policies);
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
    auto access_type = (query.alter ? AccessType::ALTER_ROW_POLICY : AccessType::CREATE_ROW_POLICY);
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
