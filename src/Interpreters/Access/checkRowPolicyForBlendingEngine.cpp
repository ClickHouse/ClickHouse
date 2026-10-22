#include <Interpreters/Access/checkRowPolicyForBlendingEngine.h>

#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/RowPolicy.h>
#include <Common/typeid_cast.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageLoop.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageTableFunction.h>
#include <Storages/StorageView.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_suspicious_row_policies_with_blending_engines;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// The table of a table expression or of the right operand of `IN`. `CREATE VIEW` marks a table name in `IN` as a table
/// identifier, but not an alias of an expression
const ASTTableIdentifier * getTableIdentifier(const IAST & ast)
{
    if (const auto * expr = ast.as<ASTTableExpression>(); expr && expr->database_and_table_name)
        return expr->database_and_table_name->as<ASTTableIdentifier>();
    if (const auto * function = ast.as<ASTFunction>();
        function && functionIsInOrGlobalInOperator(function->name) && function->arguments && function->arguments->children.size() == 2)
        return function->arguments->children[1]->as<ASTTableIdentifier>();
    return nullptr;
}

/// The tables which the query and its subqueries read. `CREATE VIEW` qualifies the names of the tables,
/// so a name without a database which a `WITH` element of an enclosing query declares is a common table expression
void collectTableIds(const IAST & ast, const ContextPtr & context, Names & with_names, std::vector<StorageID> & ids)
{
    size_t enclosing_with_names = with_names.size();
    if (const auto * select = ast.as<ASTSelectQuery>(); select && select->with())
    {
        for (const auto & child : select->with()->children)
            if (const auto * with_element = child->as<ASTWithElement>())
                with_names.push_back(with_element->name);
    }

    if (const auto * identifier = getTableIdentifier(ast))
    {
        auto id = identifier->getTableId();
        if (!id.database_name.empty())
            ids.push_back(std::move(id));
        else if (!std::ranges::contains(with_names, id.table_name))
            ids.emplace_back(context->getCurrentDatabase(), id.table_name);
    }
    for (const auto & child : ast.children)
        collectTableIds(*child, context, with_names, ids);

    with_names.resize(enclosing_with_names);
}

/// Throws unless the user can see all the tables which the definition of `storage` names for the read, whether they exist
/// or not. Otherwise the answer tells the user the engine of a table which the user cannot see, or that the table exists.
void checkCanSeeReadTables(bool granted, const IStorage & storage)
{
    if (!granted)
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "Not enough privileges to check the engine of the tables which {} reads: a row policy on a table which reads "
            "a table with an engine that merges rows does not hide the values of the rows it filters out. "
            "Grant SHOW TABLES on all the tables which it can read, or set allow_suspicious_row_policies_with_blending_engines = 1 "
            "to continue anyway",
            storage.getStorageID().getNameForLogs());
}

/// The storages which `storage` hands the row policy filter to
std::vector<StoragePtr> getReadTargets(const IStorage & storage, const ContextPtr & context)
{
    auto access = context->getAccess();
    auto can_see = [&](const StorageID & id) { return access->isGranted(AccessType::SHOW_TABLES, id.database_name, id.table_name); };
    auto check = [&](bool granted) { checkCanSeeReadTables(granted, storage); };

    /// A materialized view, a `Buffer`, a `Loop` and a `Merge` table hand the filter to the read of their targets, but do not list
    /// them as underlying storages, because the targets' own policies must not apply to them
    if (const auto * view = typeid_cast<const StorageMaterializedView *>(&storage))
    {
        /// The inner table of a materialized view is a part of the view, so the user sees its engine anyway
        check(view->hasInnerTable() || can_see(view->getTargetTableId()));
        return {view->tryGetTargetTable()};
    }
    if (const auto * buffer = typeid_cast<const StorageBuffer *>(&storage))
    {
        check(!buffer->getDestinationId() || can_see(buffer->getDestinationId()));
        return {buffer->getDestinationTable()};
    }
    if (const auto * loop = typeid_cast<const StorageLoop *>(&storage))
    {
        check(can_see(loop->getInnerStorage()->getStorageID()));
        return {loop->getInnerStorage()};
    }
    if (const auto * merge = typeid_cast<const StorageMerge *>(&storage))
    {
        /// A regular expression selects the sources, so the user must see all the tables which it can select
        auto source_database = merge->getSourceDatabase();
        check(source_database ? access->isGranted(AccessType::SHOW_TABLES, *source_database) : access->isGranted(AccessType::SHOW_TABLES));
        std::vector<StoragePtr> sources;
        merge->hasChildTable([&](const StoragePtr & source)
        {
            sources.push_back(source);
            return false;
        });
        return sources;
    }
    if (const auto * alias = typeid_cast<const StorageAlias *>(&storage))
    {
        check(alias->isDeclaredTargetGranted(context, AccessType::SHOW_TABLES, {}));
        return {alias->tryGetTargetTable()};
    }

    /// A policy on a view filters the rows of its query, which can be the rows of a blending table as they are.
    /// ponytail: a table function in the query of a view is not followed, it would have to create its storage
    if (typeid_cast<const StorageView *>(&storage))
    {
        auto metadata = storage.getInMemoryMetadataPtr(context, false);
        Names with_names;
        std::vector<StorageID> ids;
        collectTableIds(*metadata->getSelectQuery().inner_query, context, with_names, ids);
        for (const auto & id : ids)
            check(can_see(id));
        std::vector<StoragePtr> sources;
        for (const auto & id : ids)
            sources.push_back(DatabaseCatalog::instance().tryGetTable(id, context));
        return sources;
    }

    auto targets = storage.getUnderlyingStorages();
    for (const auto & target : targets)
        check(!target || can_see(target->getStorageID()));
    return targets;
}

/// The visited set terminates the walk if the chain of underlying storages loops
ConstStoragePtr findBlendingEngineImpl(const IStorage & storage, const ContextPtr & context, NameSet & visited)
{
    /// A table function creates its storage with the privileges of the server and can open any table of it.
    /// A table function which reads data from outside of the server does not read a blending table.
    if (const auto * table_function = typeid_cast<const StorageTableFunctionProxy *>(&storage))
    {
        if (table_function->readsExternalSource())
            return nullptr;
        checkCanSeeReadTables(context->getAccess()->isGranted(AccessType::SHOW_TABLES), storage);
    }

    if (storage.isBlendingEngine())
        return storage.shared_from_this();

    /// A table function or a lazily loaded table reads through the storage it creates, which has the same name
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(&storage))
        return findBlendingEngineImpl(*proxy->getNested(), context, visited);

    if (!visited.emplace(storage.getStorageID().getNameForLogs()).second)
        return nullptr;

    for (const auto & target : getReadTargets(storage, context))
    {
        if (!target)
            continue;
        if (auto blending = findBlendingEngineImpl(*target, context, visited))
            return blending;
    }
    return nullptr;
}

}

ConstStoragePtr findBlendingEngine(const IStorage & storage, const ContextPtr & context)
{
    NameSet visited;
    return findBlendingEngineImpl(storage, context, visited);
}

void checkNoRowPolicyForBlendingEngine(
    const IStorage & storage,
    const StorageID & table_id,
    bool include_database_policies,
    const ContextPtr & context,
    const IStorage * replaced)
{
    if (context->getSettingsRef()[Setting::allow_suspicious_row_policies_with_blending_engines])
        return;

    const auto & access_control = context->getAccessControl();
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy || policy->getDatabase() != table_id.database_name)
            continue;
        if (policy->isForDatabase() ? !include_database_policies : policy->getTableName() != table_id.table_name)
            continue;

        /// Resolving the storages can load a lazy table, so it happens only after a policy is found
        auto blending = findBlendingEngine(storage, context);
        if (!blending || (replaced && findBlendingEngine(*replaced, context)))
            return;

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "A row policy is used for table {}, which {} the {} engine: a merge takes the values of all the rows with "
            "the same sorting key into one row, so the policy does not hide the values of the rows it filters out. "
            "Drop the policy and define it on the table which stores the raw rows, or set "
            "allow_suspicious_row_policies_with_blending_engines = 1 to continue anyway",
            table_id.getNameForLogs(),
            blending.get() == &storage ? "has" : "reads a table with",
            blending->getName());
    }
}

}
