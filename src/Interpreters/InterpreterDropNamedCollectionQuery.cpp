#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/removeOnClusterClauseIfNeeded.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Storages/IStorage.h>

#include <algorithm>


namespace DB
{

namespace
{

/// The table the dependency was registered for, looked up by its UUID. Only a table that is in the
/// catalog is returned: while a `CREATE` is in flight, the UUID is already reserved, but no table
/// belongs to it yet.
StoragePtr tryGetLiveTableByUUID(const StorageID & dependency)
{
    if (!dependency.hasUUID())
        return nullptr;

    auto [database, table] = DatabaseCatalog::instance().tryGetByUUID(dependency.uuid);
    if (!database || !table)
        return nullptr;

    /// The UUID identifies the table the entry was registered for, with one exception:
    /// `CREATE TABLE ... UUID` can reuse the UUID of a failed create under a different table name. The
    /// two cases are told apart by the name the live table's own dependencies are recorded under - the
    /// name is set at the registration and follows the table across `RENAME` (`renameDependencies`):
    /// - an entry that carries another name while the live table has entries under its current name
    ///   belongs to a create that failed - it is left to the stale-entry cleanup;
    /// - when the live table has no entry under its current name (its names went stale without being
    ///   re-keyed - an `EXCHANGE` or a `RENAME DATABASE` - or it registered no dependency at all), the
    ///   entries under the UUID are treated as its own, which can only refuse the drop, never allow it.
    const auto live_table_id = table->getStorageID();
    if (live_table_id.database_name != dependency.database_name || live_table_id.table_name != dependency.table_name)
    {
        if (NamedCollectionFactory::instance().hasDependencyRegisteredFor(live_table_id))
            return nullptr;
    }

    return table;
}

}


namespace Setting
{
    extern const SettingsBool check_named_collection_dependencies;
}

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_IS_USED;
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
                /// A dependency with an empty table name belongs to a database engine. Its metadata
                /// is replayed on every server start just like table metadata, so it must keep the
                /// collection alive while that database exists.
                if (dep.table_name.empty())
                {
                    if (DatabaseCatalog::instance().tryGetDatabase(dep.database_name))
                    {
                        dependent_names.push_back(fmt::format("database `{}`", dep.database_name));
                        continue;
                    }

                    /// Database creation is synchronized by the database-level DDL guard. Once it
                    /// is acquired, an absent database dependency can only be a failed create.
                    auto ddl_guard = DatabaseCatalog::instance().getExclusiveDDLGuardForDatabase(dep.database_name);
                    if (DatabaseCatalog::instance().tryGetDatabase(dep.database_name))
                    {
                        dependent_names.push_back(fmt::format("database `{}`", dep.database_name));
                        continue;
                    }

                    NamedCollectionFactory::instance().removeDependency(query.collection_name, dep);
                    continue;
                }

                /// A dictionary defined in the configuration files is not created through DDL and
                /// never appears in `DatabaseCatalog`: it lives in `ExternalDictionariesLoader` for
                /// as long as its definition stays in the configuration, and every (re)load of it
                /// resolves the collection again. Its dependency is recorded under the identifier
                /// from the definition (`StorageID::fromDictionaryConfig`), which is also how the
                /// loader keys it: `db.name` when the definition sets a `<database>` and the bare
                /// name otherwise. Consult the loader before the catalog, and, for an entry that can
                /// belong to nothing else (no table has an empty database name), prune it only when
                /// the definition is gone.
                const auto dictionary_name
                    = dep.database_name.empty() ? dep.table_name : dep.database_name + "." + dep.table_name;
                if (current_context->getExternalDictionariesLoader().has(dictionary_name))
                {
                    dependent_names.push_back(fmt::format("dictionary `{}`", dictionary_name));
                    continue;
                }

                if (dep.database_name.empty())
                {
                    NamedCollectionFactory::instance().removeDependency(query.collection_name, dep);
                    continue;
                }

                /// A table of an `Atomic` database is identified by its UUID: `RENAME` and
                /// `EXCHANGE` change its name but keep the UUID recorded in the dependency, so the
                /// name of the entry goes stale while the table is still there and still uses the
                /// collection. Resolve such an entry by its UUID, and fall back to the name only
                /// when no table has that UUID any more (a failed `CREATE`, or a `DROP` of a table
                /// whose name was taken by another one afterwards).
                if (const auto table_by_uuid = tryGetLiveTableByUUID(dep))
                {
                    dependent_names.push_back(table_by_uuid->getStorageID().getFullTableName());
                    continue;
                }

                const auto table = DatabaseCatalog::instance().tryGetTable(
                    StorageID{dep.database_name, dep.table_name}, current_context);
                if (table && (!dep.hasUUID() || table->getStorageID().uuid == dep.uuid))
                {
                    dependent_names.push_back(dep.getFullTableName());
                    continue;
                }

                /// The table is also absent while the `CREATE`/`ATTACH` that registered the dependency
                /// is still in flight: the registration happens while the engine arguments are resolved,
                /// before the table is committed to the catalog. Classifying such an entry as stale would
                /// let the drop proceed while the create later succeeds, leaving metadata that references
                /// a collection that no longer exists. The creating query holds the `DDLGuard` of the
                /// table name for the whole window between the registration and the commit, so re-check
                /// under that guard: once it is acquired, no create is in flight, and the table's absence
                /// proves the earlier create failed and left a stale entry.
                auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(dep.database_name, dep.table_name, nullptr);
                if (const auto table_by_uuid = tryGetLiveTableByUUID(dep))
                {
                    dependent_names.push_back(table_by_uuid->getStorageID().getFullTableName());
                    continue;
                }

                const auto updated_table = DatabaseCatalog::instance().tryGetTable(
                    StorageID{dep.database_name, dep.table_name}, current_context);
                if (updated_table && (!dep.hasUUID() || updated_table->getStorageID().uuid == dep.uuid))
                {
                    dependent_names.push_back(dep.getFullTableName());
                    continue;
                }

                /// A failed CREATE of an Atomic table can leave an entry with the old UUID. Before
                /// this guard was acquired, another CREATE of the same table name may have completed
                /// with a new UUID and a dependency on this collection. The old StorageID cannot find
                /// that table, so check the current dependencies by name before pruning the stale one.
                /// The DDLGuard keeps a new CREATE of this name from registering between this check and
                /// the removal below.
                const auto updated_dependents = NamedCollectionFactory::instance().getDependents(query.collection_name);
                const auto same_name_live_dependency = std::any_of(
                    updated_dependents.begin(),
                    updated_dependents.end(),
                    [&](const auto & candidate)
                    {
                        return candidate.database_name == dep.database_name
                            && candidate.table_name == dep.table_name
                            && candidate.uuid != dep.uuid
                            && DatabaseCatalog::instance().isTableExist(candidate, current_context);
                    });
                if (same_name_live_dependency)
                {
                    dependent_names.push_back(dep.getFullTableName());
                    continue;
                }

                /// Only this exact entry: the guard proves that no create is in flight for the recorded
                /// table name, but `CREATE TABLE ... UUID` can reuse the UUID of the failed create under
                /// a different name, and erasing everything under the UUID would remove the live
                /// dependency of such an in-flight create.
                NamedCollectionFactory::instance().removeDependency(query.collection_name, dep);
            }

            if (!dependent_names.empty())
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_IS_USED,
                    "Named collection `{}` is used by tables: {}",
                    query.collection_name,
                    fmt::join(dependent_names, ", "));
            }
        }

        /// A detached table is not in `DatabaseCatalog`, but the metadata it is attached from still
        /// references the collection: if the collection is dropped, the `ATTACH` replayed at the next
        /// server start throws `NAMED_COLLECTION_DOESNT_EXIST` and the server does not start.
        /// Every entry blocks the drop, even when a table with that name exists in `DatabaseCatalog`
        /// again. The entry cannot be pruned here based on the table's existence: a concurrent `ATTACH`
        /// registers its live dependency after the check above already ran, and a concurrent `DETACH`
        /// records the entry while the table is still in the catalog - in both windows the table exists,
        /// yet nothing in this query has validated the live dependency. Entries are removed only by the
        /// events that prove the metadata under the recorded name is gone or harmless: `DROP TABLE`,
        /// `DETACH TABLE ... PERMANENTLY`, `RENAME` of the table, and `DROP DATABASE`.
        auto detached_dependents = NamedCollectionFactory::instance().getDetachedDependents(query.collection_name);
        if (!detached_dependents.empty())
        {
            std::vector<String> detached_names;
            detached_names.reserve(detached_dependents.size());
            for (const auto & dep : detached_dependents)
            {
                /// An entry of a database engine has no table name.
                if (dep.table_name.empty())
                    detached_names.push_back(fmt::format("database `{}`", dep.database_name));
                else
                    detached_names.push_back(dep.getFullTableName());
            }

            throw Exception(
                ErrorCodes::NAMED_COLLECTION_IS_USED,
                "Named collection `{}` may still be used by detached tables: {}",
                query.collection_name,
                fmt::join(detached_names, ", "));
        }

        if (!NamedCollectionFactory::instance().removeFromSQLIfNoDependencies(query))
            throw Exception(ErrorCodes::NAMED_COLLECTION_IS_USED, "Named collection `{}` is used by a table", query.collection_name);
    }
    else
    {
        NamedCollectionFactory::instance().removeFromSQL(query);
    }

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
