#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <base/sort.h>

#include <algorithm>


namespace DB
{

namespace Setting
{
    extern const SettingsBool check_named_collection_dependencies;
}

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_IS_USED;
}

namespace
{

/// A table detached with a plain `DETACH TABLE` keeps its metadata file, so it is attached again on
/// the next server start, but it is gone from `DatabaseCatalog`, so `isTableExist` says `false` for
/// it. Dropping a named collection that such a table references makes the `ATTACH` replayed at
/// startup throw `NAMED_COLLECTION_DOESNT_EXIST`, which aborts loading the metadata and the server
/// does not start, so a detached table still counts as a dependent.
/// `DETACH TABLE ... PERMANENTLY` is a different case: it renames the metadata file, so the table is
/// not loaded at startup, and it removes the dependency right away.
bool isTemporarilyDetached(const StorageID & table_id, const ContextPtr & context)
{
    auto database = DatabaseCatalog::instance().tryGetDatabase(table_id.database_name);

    /// A whole database can be detached as well, and then the dependent table is not reachable
    /// through the catalog at all. `DETACH DATABASE` keeps the database metadata file, so the
    /// database and all of its tables are loaded again on the next server start (there is no
    /// `DETACH DATABASE ... PERMANENTLY`). `DROP DATABASE` removes that file, and it also drops the
    /// dependencies of the tables it contains.
    if (!database)
        return context->getDatabaseDisk()->existsFile(DatabaseCatalog::getMetadataFilePath(table_id.database_name));

    /// Only a database that keeps table metadata on disk can bring a detached table back at startup.
    /// The others (`Memory`, `Filesystem`, `S3`, ...) return an empty path here and do not implement
    /// `getDetachedTablesIterator` either.
    if (database->getObjectMetadataPath(table_id.table_name).empty())
        return false;

    auto detached_tables = database->getDetachedTablesIterator(
        context, [&](const String & name) { return name == table_id.table_name; }, /* skip_not_loaded= */ false);

    for (; detached_tables->isValid(); detached_tables->next())
    {
        if (detached_tables->isPermanently())
            continue;

        /// In an `Atomic` database dependencies are tracked by UUID, so a table that merely reuses
        /// the name of the dependent is a different table and does not revive the dependency.
        if (!table_id.hasUUID() || detached_tables->uuid() == table_id.uuid)
            return true;
    }

    return false;
}

}

BlockIO InterpreterDropNamedCollectionQuery::execute()
{
    auto current_context = getContext();

    const auto updated_query = removeOnClusterClauseIfNeeded(query_ptr, getContext());
    const auto & query = updated_query->as<const ASTDropNamedCollectionQuery &>();

    current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION, query.collection_name);

    if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(updated_query, current_context, params);
    }

    if (current_context->getSettingsRef()[Setting::check_named_collection_dependencies])
    {
        auto dependents = NamedCollectionFactory::instance().getDependents(query.collection_name);
        if (!dependents.empty())
        {
            /// Filter out tables that no longer exist (e.g. from failed CREATE TABLE).
            /// Dependencies are registered during table configuration parsing, before the table
            /// is fully created. If CREATE TABLE fails after that point, a stale dependency remains.
            std::vector<String> dependent_names;
            dependent_names.reserve(dependents.size());
            for (const auto & dep : dependents)
            {
                if (DatabaseCatalog::instance().isTableExist(dep, current_context))
                    dependent_names.push_back(dep.getFullTableName());
                else if (isTemporarilyDetached(dep, current_context))
                    dependent_names.push_back(dep.getFullTableName() + " (detached)");
                else
                    NamedCollectionFactory::instance().removeDependencies(dep);
            }

            if (!dependent_names.empty())
            {
                /// A table can be registered as a dependent more than once (the `URL` engine, for
                /// example, resolves its arguments both to dispatch on the scheme and to create the
                /// delegate storage), and the order the dependencies come in is not defined.
                ::sort(dependent_names.begin(), dependent_names.end());
                dependent_names.erase(std::unique(dependent_names.begin(), dependent_names.end()), dependent_names.end());

                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_IS_USED,
                    "Named collection `{}` is used by tables: {}",
                    query.collection_name,
                    fmt::join(dependent_names, ", "));
            }
        }
    }

    NamedCollectionFactory::instance().removeFromSQL(query);

    return {};
}

void registerInterpreterDropNamedCollectionQuery(InterpreterFactory & factory);
void registerInterpreterDropNamedCollectionQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropNamedCollectionQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropNamedCollectionQuery", create_fn);
}

}
