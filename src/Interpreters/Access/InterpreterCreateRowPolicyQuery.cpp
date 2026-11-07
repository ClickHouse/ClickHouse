#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/Access/InterpreterCreateRowPolicyQuery.h>

#include <Access/AccessControl.h>
#include <Access/AccessRights.h>
#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Access/RowPolicy.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/misc.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Planner/Utils.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageProxy.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

namespace
{
    /// Tables and dictionaries named in a filter, or in an ALIAS column it reads, are resolved against the reader's current database.
    bool readsOtherObjects(const IAST & ast, const ColumnsDescription & columns, const StorageID & table_id, NameSet & followed_aliases)
    {
        if (ast.as<ASTSubquery>() || ast.as<ASTSelectWithUnionQuery>() || ast.as<ASTSelectQuery>())
            return true;

        if (const auto * identifier = ast.as<ASTIdentifier>())
        {
            /// A column can be qualified with the table, or with the database and the table, and followed by a subcolumn.
            const auto & parts = identifier->name_parts;
            for (size_t qualifier = 0; qualifier < std::min<size_t>(parts.size(), 3); ++qualifier)
            {
                if ((qualifier == 1 && parts[0] != table_id.table_name)
                    || (qualifier == 2 && (parts[0] != table_id.database_name || parts[1] != table_id.table_name)))
                    continue;
                String name;
                for (size_t i = qualifier; i < parts.size(); ++i)
                {
                    name += (i == qualifier ? "" : ".") + parts[i];
                    const auto * column = columns.tryGet(name);
                    if (column && column->default_desc.kind == ColumnDefaultKind::Alias && column->default_desc.expression
                        && followed_aliases.insert(name).second
                        && readsOtherObjects(*column->default_desc.expression, columns, table_id, followed_aliases))
                        return true;
                }
            }
        }

        if (const auto * function = ast.as<ASTFunction>())
        {
            const auto & name = function->name;
            if (functionIsDictGet(name) || functionIsJoinGet(name) || name == "predictXGBoost" || name == "assignCentroid"
                || name == "hasColumnInTable")
                return true;

            if (functionIsInOrGlobalInOperator(name) && function->arguments && function->arguments->children.size() == 2)
            {
                const auto & rhs = function->arguments->children[1];
                if (const auto * rhs_function = rhs->as<ASTFunction>();
                    rhs_function && TableFunctionFactory::instance().isTableFunctionName(rhs_function->name))
                    return true;
                if (const auto * rhs_identifier = rhs->as<ASTIdentifier>(); rhs_identifier && !columns.has(rhs_identifier->name()))
                    return true;
            }
        }

        for (const auto & child : ast.children)
            if (child && readsOtherObjects(*child, columns, table_id, followed_aliases))
                return true;
        return false;
    }

    bool readsOtherObjects(const IAST & ast, const ColumnsDescription & columns, const StorageID & table_id)
    {
        NameSet followed_aliases;
        return readsOtherObjects(ast, columns, table_id, followed_aliases);
    }

    /// Nothing is disclosed about a table whose columns the user cannot see.
    bool canSeeColumnsByName(const AccessRights & rights, const String & database, const String & table)
    {
        return rights.isGranted(AccessType::SHOW_COLUMNS, database, table) || rights.isGranted(AccessType::SELECT, database, table);
    }

    bool canSeeAliasTarget(const StoragePtr & storage, const ContextPtr & context)
    {
        auto alias = castStorage<StorageAlias>(storage, DeferredTable::Skip);
        return !alias || alias->isTargetTableGranted(context, AccessType::SHOW_COLUMNS, "")
            || alias->isTargetTableGranted(context, AccessType::SELECT, "");
    }

    /// A filter over stored columns only is checked without creating a deferred storage.
    bool referencesOnlyDeclaredColumns(const IAST & ast, const ColumnsDescription & columns)
    {
        if (const auto * identifier = ast.as<ASTIdentifier>())
            return columns.hasPhysical(identifier->name());

        for (const auto & child : ast.children)
            if (child && !referencesOnlyDeclaredColumns(*child, columns))
                return false;
        return true;
    }

    void checkFilterResolves(
        const ASTPtr & filter, const ASTPtr & expanded, const StoragePtr & storage, const String & policy_name, const ContextPtr & context)
    {
        try
        {
            TableNodePtr table_node;
            const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get());
            if (proxy && !proxy->tryGetNested())
            {
                const auto metadata = storage->getInMemoryMetadataPtr(context, false);
                const auto & columns = metadata->getColumns();
                if (columns.empty() || readsOtherObjects(*expanded, columns, storage->getStorageID()))
                    return;
                if (referencesOnlyDeclaredColumns(*expanded, columns))
                    table_node = std::make_shared<TableNode>(std::make_shared<StorageDummy>(storage->getStorageID(), columns), context);
            }

            if (!table_node)
            {
                auto lock = storage->tryLockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
                if (!lock)
                    return;
                /// A storage without columns, such as an `Alias` whose table is gone, is not checked.
                if (const auto cached_metadata = storage->getInMemoryMetadataPtr(context, false); cached_metadata->getColumns().empty())
                    return;
                /// As a read does, refresh the columns of a storage whose schema lives outside the server.
                storage->updateExternalDynamicMetadataIfExists(context);
                auto metadata = storage->getInMemoryMetadataPtr(context, false);
                const auto & columns = metadata->getColumns();
                if (columns.empty() || readsOtherObjects(*expanded, columns, storage->getStorageID()))
                    return;
                auto snapshot = storage->getStorageSnapshot(metadata, context);
                table_node = std::make_shared<TableNode>(storage, std::move(lock), std::move(snapshot));
            }

            auto filter_query_tree = buildFilterQueryTree(filter->clone(), table_node, context);
            assertNoAggregateFunctionNodes(filter_query_tree, "in row policy filter");
            assertNoWindowFunctionNodes(filter_query_tree, "in row policy filter");
            assertNoGroupingFunctionNodes(filter_query_tree, "in row policy filter");

            const auto result_type = filter_query_tree->getResultType();
            if (!result_type->canBeUsedInBooleanContext())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "Invalid type for filter of row policy {}: {}", policy_name, result_type->getName());
        }
        catch (Exception & e)
        {
            e.addMessage("while checking the filter of row policy {} against table {}", policy_name, storage->getStorageID().getFullTableName());
            throw;
        }
    }

    void checkFiltersResolveForExistingTables(
        const ASTCreateRowPolicyQuery & query, const std::vector<RowPolicyName> & names, const ContextPtr & context)
    {
        if (!context->getUserID())
            return;

        std::vector<std::pair<ASTPtr, ASTPtr>> filters;
        for (const auto & [filter_type, filter] : query.filters)
        {
            if (!filter)
                continue;
            ASTPtr expanded = filter->clone();
            UserDefinedSQLFunctionVisitor::visit(expanded, context);
            filters.emplace_back(filter, std::move(expanded));
        }
        if (filters.empty())
            return;

        const auto access_rights = context->getAccess()->getAccessRightsWithImplicit();
        auto check_table = [&](const RowPolicyName & name, const StoragePtr & storage)
        {
            if (!canSeeAliasTarget(storage, context))
                return;
            for (const auto & [filter, expanded] : filters)
                checkFilterResolves(filter, expanded, storage, name.toString(), context);
        };

        for (const auto & name : names)
        {
            auto database = DatabaseCatalog::instance().tryGetDatabase(name.database);
            if (!database || database->isExternal())
                continue;

            if (name.table_name == RowPolicyName::ANY_TABLE_MARK)
            {
                auto can_see_by_name = [&](const String & table_name)
                { return canSeeColumnsByName(*access_rights, name.database, table_name); };
                for (auto iterator = database->getTablesIterator(context, can_see_by_name, /*skip_not_loaded=*/ false); iterator->isValid();
                     iterator->next())
                {
                    if (const auto & storage = iterator->table())
                        check_table(name, storage);
                }
            }
            else if (canSeeColumnsByName(*access_rights, name.database, name.table_name))
            {
                if (auto storage = DatabaseCatalog::instance().tryGetTable(StorageID(name.database, name.table_name), context))
                    check_table(name, storage);
            }
        }
    }

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

    if (!query.cluster.empty())
    {
        query.replaceCurrentUserTag(getContext()->getUserName());
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(updated_query_ptr, getContext(), params);
    }

    chassert(query.names->cluster.empty());
    auto & access_control = getContext()->getAccessControl();
    getContext()->checkAccess(required_access);

    query.replaceEmptyDatabase(getContext()->getCurrentDatabase());

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

    std::vector<RowPolicyName> installed_names;
    for (const auto & full_name : query.names->full_names)
    {
        const bool exists = storage->find<RowPolicy>(full_name.toString()).has_value();
        if ((query.alter && query.if_exists && !exists) || (!query.alter && query.if_not_exists && exists))
            continue;
        installed_names.push_back(full_name);
    }
    checkFiltersResolveForExistingTables(query, installed_names, getContext());

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
            Strings checked_names;
            checked_names.reserve(installed_names.size());
            for (const auto & full_name : installed_names)
                checked_names.push_back(full_name.toString());
            auto ids = storage->find<RowPolicy>(checked_names);
            storage->tryUpdate(ids, update_func);
        }
        else
            storage->update(storage->getIDs<RowPolicy>(names), update_func);
    }
    else
    {
        std::vector<AccessEntityPtr> new_policies;
        for (const auto & full_name : installed_names)
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
