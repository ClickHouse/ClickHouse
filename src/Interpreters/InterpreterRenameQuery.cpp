#include <Backups/BackupUtils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DDLRenamingVisitor.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/checkTimeSeriesVersion.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/QueryLog.h>
#include <Access/Common/AccessRightsElement.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>

#include <algorithm>
#include <map>
#include <set>


namespace DB
{
namespace Setting
{
    extern const SettingsBool check_table_dependencies;
    extern const SettingsBool check_referential_table_dependencies;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_TOO_LARGE;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

/// The attached tables that `table` created and owns, whose definitions are derived from its own.
std::vector<StoragePtr> getInnerTables(const IStorage & table, const ContextPtr & context)
{
    std::vector<StoragePtr> res;
    if (const auto * materialized_view = typeid_cast<const StorageMaterializedView *>(&table))
    {
        if (materialized_view->hasInnerTable())
            if (auto inner_table = materialized_view->tryGetTargetTable())
                res.push_back(inner_table);
    }
    else if (const auto * time_series = typeid_cast<const StorageTimeSeries *>(&table))
    {
        for (auto kind : StorageTimeSeries::getTargetKinds())
            if (time_series->hasTarget(kind) && time_series->isInnerTable(kind))
                if (auto inner_table = time_series->tryGetTargetTable(kind, context))
                    res.push_back(inner_table);
    }
    return res;
}

/// The set operations of a definition read from metadata are normalized before a table uses it, as on load.
void normalizeSetOperations(ASTPtr query, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    SelectIntersectExceptQueryVisitor::Data intersect_except_data{settings[Setting::intersect_default_mode], settings[Setting::except_default_mode]};
    SelectIntersectExceptQueryVisitor{intersect_except_data}.visit(query);
    NormalizeSelectWithUnionQueryVisitor::Data union_data{settings[Setting::union_default_mode]};
    NormalizeSelectWithUnionQueryVisitor{union_data}.visit(query);
}

/// Temporary views belong to their sessions and keep their definitions.
Databases getDatabasesWithViewsToRename()
{
    auto databases = DatabaseCatalog::instance().getDatabases({.with_datalake_catalogs = false, .with_remote_databases = false});
    databases.erase(DatabaseCatalog::TEMPORARY_DATABASE);
    return databases;
}

/// Rewrites the definitions of the views and TimeSeries tables of all databases that refer to tables of a renamed
/// database by its name. Every refusal happens in `prepare`, before anything is changed.
class ViewDefinitionsRenaming
{
public:
    ViewDefinitionsRenaming(String old_database_name_, String new_database_name_, ContextPtr context_)
        : old_database_name(std::move(old_database_name_)), new_database_name(std::move(new_database_name_)), context(std::move(context_))
    {
    }

    /// Takes the locks and stages the new definitions. Called before the database is renamed,
    /// with the old database name held by `getExclusiveDDLGuardForDatabase`.
    void prepare();

    /// Publishes the new definitions in memory, then persists them. Called after the database is renamed.
    void commit();

private:
    struct View
    {
        DatabasePtr owner;
        /// The name before the rename.
        QualifiedTableName name;
        StoragePtr table;
        ASTPtr definition;
    };

    /// Returns the renamed copy of `definition`, or null if the rename does not change it.
    ASTPtr rename(const ASTPtr & definition, const QualifiedTableName & name, std::set<QualifiedTableName> * tables_to_check) const;

    std::vector<View> scan(std::set<QualifiedTableName> & tables_to_check) const;
    void checkDetachedTables() const;
    void stage();

    const String old_database_name;
    const String new_database_name;
    const ContextPtr context;

    std::unique_lock<SharedMutex> new_database_guard;
    std::map<QualifiedTableName, DDLGuardPtr> table_guards;
    std::vector<IStorage::AlterLockHolder> alter_locks;
    std::vector<View> views;
};

ASTPtr ViewDefinitionsRenaming::rename(
    const ASTPtr & definition, const QualifiedTableName & name, std::set<QualifiedTableName> * tables_to_check) const
{
    auto renamed = definition->clone();
    DatabaseRenamingInViewDefinition renaming(renamed, old_database_name, new_database_name, name);
    renaming.apply(context->getGlobalContext());
    if (getObjectDefinitionFromCreateQuery(renamed) == getObjectDefinitionFromCreateQuery(definition))
        return nullptr;
    if (tables_to_check)
    {
        auto tables = renaming.getTablesToCheck();
        tables_to_check->insert(tables.begin(), tables.end());
    }
    return renamed;
}

std::vector<ViewDefinitionsRenaming::View> ViewDefinitionsRenaming::scan(std::set<QualifiedTableName> & tables_to_check) const
{
    std::vector<View> res;
    for (const auto & [database_name, database] : getDatabasesWithViewsToRename())
    {
        for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
        {
            const auto & table = it->table();
            if (!table || !(table->isView() || table->getName() == "TimeSeries"))
                continue;

            QualifiedTableName name{database_name, it->name()};
            auto definition = database->tryGetCreateTableQuery(name.table, context);
            if (!definition)
                continue;
            qualifyNamesFromLegacyMetadata(definition->as<ASTCreateQuery &>(), name.database, context);
            if (rename(definition, name, &tables_to_check))
                res.push_back({database, name, table, nullptr});
        }
    }
    return res;
}

void ViewDefinitionsRenaming::checkDetachedTables() const
{
    auto & catalog = DatabaseCatalog::instance();
    const auto timeout = std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds());
    for (const auto & [database_name, database] : getDatabasesWithViewsToRename())
    {
        DatabaseDetachedTablesSnapshotIteratorPtr it;
        try
        {
            it = database->getDetachedTablesIterator(context, {}, false);
        }
        catch (const Exception & e)
        {
            /// The engine has no detached tables.
            if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
                continue;
            throw;
        }
        for (; it->isValid(); it->next())
        {
            /// ATTACH rewrites the metadata of a detached table under its DDL guard.
            DDLGuardPtr guard;
            if (database_name != old_database_name && database_name != new_database_name)
            {
                guard = catalog.tryGetDDLGuard(database_name, it->table(), nullptr, timeout);
                /// The database is being dropped, detached or renamed.
                if (guard->databaseLockBusy())
                    break;
                if (!guard->ownsTableLock())
                    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                        "Cannot acquire the DDL guard for {}.{}, lock_acquire_timeout is {} ms",
                        backQuoteIfNeed(database_name), backQuoteIfNeed(it->table()), timeout.count());
            }

            ASTPtr definition;
            if (dynamic_cast<const DatabaseOnDisk *>(database.get()))
            {
                auto disk = database->getDisk();
                try
                {
                    definition = DatabaseOnDisk::parseQueryFromMetadata(
                        getLogger("InterpreterRenameQuery"), context, disk, it->metadataPath(), /*throw_on_error=*/ false);
                }
                catch (const Exception &)
                {
                    /// A concurrent RENAME or DROP of the table removes its metadata file.
                    if (disk->existsFile(it->metadataPath()))
                        throw;
                }
            }
            else
                definition = database->tryGetCreateTableQuery(it->table(), context);
            if (!definition)
                continue;

            const auto & create = definition->as<ASTCreateQuery &>();
            if ((create.isView() || create.is_time_series_table || BackupUtils::isInnerTable(database_name, it->table()))
                && rename(definition, {database_name, it->table()}, nullptr))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot rename database {} to {}, because the detached table {}.{} refers to it. Attach or drop it before renaming",
                    backQuoteIfNeed(old_database_name), backQuoteIfNeed(new_database_name), backQuoteIfNeed(database_name), backQuoteIfNeed(it->table()));
        }
    }
}

void ViewDefinitionsRenaming::prepare()
{
    auto & catalog = DatabaseCatalog::instance();
    const auto & settings = context->getSettingsRef();

    /// Guards are keyed by name, so the new name is guarded until the end too.
    new_database_guard = catalog.getExclusiveDDLGuardForDatabase(new_database_name);

    checkDetachedTables();

    /// The tables of other databases are guarded one by one, so scan again until no new table is found.
    while (true)
    {
        std::set<QualifiedTableName> tables_to_check;
        views = scan(tables_to_check);

        bool guarded_new_table = false;
        auto guard = [&](const StoragePtr & table)
        {
            auto name = table->getStorageID().getQualifiedName();
            if (name.database == old_database_name || table_guards.contains(name))
                return;
            table_guards.emplace(name, catalog.getDDLGuardForStorage(table, settings[Setting::lock_acquire_timeout]));
            guarded_new_table = true;
        };

        for (const auto & view : views)
        {
            guard(view.table);
            for (const auto & inner_table : getInnerTables(*view.table, context))
                guard(inner_table);
        }
        for (const auto & name : tables_to_check)
            if (auto table = catalog.tryGetTable(StorageID{name.database, name.table}, context))
                guard(table);

        if (!guarded_new_table)
            break;
    }

    checkDetachedTables();

    std::sort(views.begin(), views.end(), [](const View & lhs, const View & rhs) { return lhs.name < rhs.name; });
    for (const auto & view : views)
        alter_locks.push_back(view.table->lockForAlter(settings[Setting::lock_acquire_timeout]));

    stage();
}

void ViewDefinitionsRenaming::stage()
{
    std::vector<View> staged;
    for (auto & view : views)
    {
        /// Read again: the scan may have read it before it was guarded.
        auto definition = view.owner->getCreateTableQuery(view.name.table, context);
        /// As on load, the unqualified names of legacy metadata resolve against the database of the view.
        qualifyNamesFromLegacyMetadata(definition->as<ASTCreateQuery &>(), view.name.database, context);
        view.definition = rename(definition, view.name, nullptr);
        if (!view.definition)
            continue;

        if (const auto * time_series = typeid_cast<const StorageTimeSeries *>(view.table.get()))
            checkTimeSeriesVersionIsSupported(*time_series);

        auto metadata = view.table->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ true);
        DatabaseRenamingInViewDefinition(definition->clone(), old_database_name, new_database_name, view.name)
            .check(metadata->getColumns(), context);

        size_t max_query_size = context->getSettingsRef()[Setting::max_query_size];
        size_t statement_size = getObjectDefinitionFromCreateQuery(view.definition).size();
        if (max_query_size && statement_size > max_query_size)
            throw Exception(ErrorCodes::QUERY_IS_TOO_LARGE,
                "Cannot rename database {} to {}, because the definition of {} would exceed max_query_size ({} > {})",
                backQuoteIfNeed(old_database_name), backQuoteIfNeed(new_database_name), view.name.getFullName(), statement_size, max_query_size);

        if (!view.owner->canAlterTableDefinition())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot rename database {} to {}, because {} refers to it and database engine {} cannot rewrite its definition. "
                "Drop it or rewrite it before renaming",
                backQuoteIfNeed(old_database_name), backQuoteIfNeed(new_database_name), view.name.getFullName(), view.owner->getEngineName());

        for (const auto & inner_table : getInnerTables(*view.table, context))
        {
            auto inner_table_name = inner_table->getStorageID().table_name;
            auto inner_definition = view.owner->tryGetCreateTableQuery(inner_table_name, context);
            if (inner_definition && rename(inner_definition, {view.name.database, inner_table_name}, nullptr))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot rename database {} to {}, because the inner table {} of {} refers to it. Drop or rewrite it before renaming",
                    backQuoteIfNeed(old_database_name), backQuoteIfNeed(new_database_name), backQuoteIfNeed(inner_table_name),
                    view.name.getFullName());
        }

        normalizeSetOperations(view.definition, context->getGlobalContext());
        staged.push_back(view);
    }
    views = std::move(staged);
}

void ViewDefinitionsRenaming::commit()
{
    for (const auto & view : views)
        view.table->updateReferencedTables(view.definition->as<ASTCreateQuery &>(), context);

    for (const auto & view : views)
    {
        auto modifier = [&](const ASTPtr & create_query)
        {
            DatabaseRenamingInViewDefinition(create_query, old_database_name, new_database_name, view.name).apply(context->getGlobalContext());
        };
        /// The dependencies of the definition are resolved against the database of the view, as on load.
        auto view_context = Context::createCopy(context);
        view_context->setCurrentDatabase(view.owner->getDatabaseName());
        view.owner->alterTableDefinition(view_context, view.table->getStorageID(), modifier);
    }
}

}

InterpreterRenameQuery::InterpreterRenameQuery(const ASTPtr & query_ptr_, ContextPtr context_)
    : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterRenameQuery::execute()
{
    const auto & rename = query_ptr->as<const ASTRenameQuery &>();

    if (!rename.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccess(rename.database ? RenameType::RenameDatabase : RenameType::RenameTable);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (!skip_access_check)
        getContext()->checkAccess(getRequiredAccess(rename.database ? RenameType::RenameDatabase : RenameType::RenameTable));

    String current_database = getContext()->getCurrentDatabase();

    /** In case of error while renaming, it is possible that only part of tables was renamed
      *  or we will be in inconsistent state. (It is worth to be fixed.)
      */

    RenameDescriptions descriptions;
    descriptions.reserve(rename.getElements().size());

    /// Don't allow to drop tables (that we are renaming); don't allow to create tables in places where tables will be renamed.
    TableGuards table_guards;

    for (const auto & elem : rename.getElements())
    {
        descriptions.emplace_back(elem, current_database);
        const auto & description = descriptions.back();

        UniqueTableName from(description.from_database_name, description.from_table_name);
        UniqueTableName to(description.to_database_name, description.to_table_name);

        table_guards[from];
        table_guards[to];
    }

    auto & database_catalog = DatabaseCatalog::instance();

    /// Must do it in consistent order.
    for (auto & table_guard : table_guards)
        table_guard.second = database_catalog.getDDLGuard(table_guard.first.database_name, table_guard.first.table_name, nullptr);

    if (rename.database)
        return executeToDatabase(rename, descriptions);
    return executeToTables(rename, descriptions, table_guards);
}

BlockIO InterpreterRenameQuery::executeToTables(const ASTRenameQuery & rename, const RenameDescriptions & descriptions, TableGuards & ddl_guards)
{
    chassert(!rename.rename_if_cannot_exchange || descriptions.size() == 1);
    chassert(!(rename.rename_if_cannot_exchange && rename.exchange));
    auto & database_catalog = DatabaseCatalog::instance();

    for (const auto & elem : descriptions)
    {
        if (elem.if_exists)
        {
            chassert(!rename.exchange);
            if (!database_catalog.isTableExist(StorageID(elem.from_database_name, elem.from_table_name), getContext()))
                continue;
        }

        bool exchange_tables = false;
        if (rename.exchange)
        {
            exchange_tables = true;
        }
        else if (rename.rename_if_cannot_exchange)
        {
            exchange_tables = database_catalog.isTableExist(StorageID(elem.to_database_name, elem.to_table_name), getContext());
            renamed_instead_of_exchange = !exchange_tables;
        }
        else
        {
            exchange_tables = false;
            database_catalog.assertTableDoesntExist(StorageID(elem.to_database_name, elem.to_table_name), getContext());
        }

        /// Run the caller's pre-swap check while still holding `ddl_guards`. If it
        /// throws, the guards release via RAII, no rename happens, and the caller's
        /// catch path runs. Skip when the destination doesn't exist — there is no
        /// storage to check (this is a plain `RENAME TO new_name`, not an exchange).
        if (pre_swap_check && exchange_tables)
            pre_swap_check(StorageID(elem.to_database_name, elem.to_table_name));

        DatabasePtr database = database_catalog.getDatabase(elem.from_database_name);
        if (database->shouldReplicateQuery(getContext(), query_ptr))
        {
            if (1 < descriptions.size())
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Database {} is Replicated, "
                    "it does not support renaming of multiple tables in single query.",
                    elem.from_database_name);

            UniqueTableName from(elem.from_database_name, elem.from_table_name);
            UniqueTableName to(elem.to_database_name, elem.to_table_name);
            ddl_guards[from]->releaseTableLock();
            ddl_guards[to]->releaseTableLock();
            return database->tryEnqueueReplicatedDDL(query_ptr, getContext(), {}, std::move(ddl_guards[from]));
        }

        StorageID from_table_id{elem.from_database_name, elem.from_table_name};
        StorageID to_table_id{elem.to_database_name, elem.to_table_name};
        std::vector<StorageID> from_ref_dependencies;
        std::vector<StorageID> from_loading_dependencies;
        std::vector<StorageID> from_mv_dependencies;
        std::vector<StorageID> from_dependent_views;
        std::vector<StorageID> to_ref_dependencies;
        std::vector<StorageID> to_loading_dependencies;
        std::vector<StorageID> to_mv_dependencies;
        std::vector<StorageID> to_dependent_views;

        if (exchange_tables)
        {
            DatabaseCatalog::instance().checkTablesCanBeExchangedWithNoCyclicDependencies(from_table_id, to_table_id);
            std::tie(from_ref_dependencies, from_loading_dependencies, from_mv_dependencies) = database_catalog.removeDependencies(from_table_id, false, false, false, /*is_mv*/ true);
            std::tie(to_ref_dependencies, to_loading_dependencies, to_mv_dependencies) = database_catalog.removeDependencies(to_table_id, false, false, false, /*is_mv*/ true);
            from_dependent_views = database_catalog.takeSourceViewDependencies(from_table_id);
            to_dependent_views = database_catalog.takeSourceViewDependencies(to_table_id);
        }
        else
        {
            /// The limit is derived from the receiver's own name, so the destination database
            /// is the one that has to be able to hold the new name.
            DatabasePtr to_database = database_catalog.getDatabase(elem.to_database_name);
            to_database->checkTableNameLength(to_table_id.table_name);

            DatabaseCatalog::instance().checkTableCanBeRenamedWithNoCyclicDependencies(from_table_id, to_table_id);
            bool check_ref_deps = getContext()->getSettingsRef()[Setting::check_referential_table_dependencies];
            bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef()[Setting::check_table_dependencies];
            std::tie(from_ref_dependencies, from_loading_dependencies, from_mv_dependencies) = database_catalog.removeDependencies(from_table_id, check_ref_deps, check_loading_deps, false, /*is_mv*/ true);
            if (!keep_source_view_dependencies)
                from_dependent_views = database_catalog.takeSourceViewDependencies(from_table_id);
        }
        try
        {
            database->renameTable(
                getContext(),
                elem.from_table_name,
                *database_catalog.getDatabase(elem.to_database_name),
                elem.to_table_name,
                exchange_tables,
                rename.dictionary);

            DatabaseCatalog::instance().addDependencies(to_table_id, from_ref_dependencies, from_loading_dependencies, from_mv_dependencies);
            if (!to_ref_dependencies.empty() || !to_loading_dependencies.empty() || !to_mv_dependencies.empty())
                DatabaseCatalog::instance().addDependencies(from_table_id, to_ref_dependencies, to_loading_dependencies, to_mv_dependencies);

            if (exchange_tables)
            {
                /// `EXCHANGE TABLES` (and the synthetic exchange used by
                /// `CREATE OR REPLACE TABLE` / `REPLACE TABLE`): source-side
                /// view-dependency edges must follow the name, not the data.
                /// The `MV`'s stored `select_table_id` is not rewritten by the
                /// rename, so cross-swapping would orphan the `MV`. See #105021.
                DatabaseCatalog::instance().addSourceViewDependencies(from_table_id, from_dependent_views);
                DatabaseCatalog::instance().addSourceViewDependencies(to_table_id, to_dependent_views);
            }
            else
            {
                /// Plain `RENAME TABLE a TO c`: re-key source-view edges from
                /// the old name to the new one (needed when the table is moved
                /// across databases — see `01155_rename_move_materialized_view`).
                DatabaseCatalog::instance().addSourceViewDependencies(to_table_id, from_dependent_views);
            }

            NamedCollectionFactory::instance().renameDependencies(from_table_id, to_table_id);
            if (exchange_tables)
                NamedCollectionFactory::instance().renameDependencies(to_table_id, from_table_id);

            /// The name -> storage mapping just changed. Drop the affected names from this query's
            /// per-query storage cache so the query's own subsequent lookups resolve to the current
            /// tables rather than the version pinned before the swap. In particular this lets the
            /// internal DROP in `CREATE OR REPLACE ... POPULATE` target the old table by the
            /// temporary name after the internal EXCHANGE (see #108726). Concurrent queries keep
            /// their own per-query caches and remain isolated from this rename, so a running SELECT
            /// still reads the version it was planned against (see 03915_exchange_tables_race).
            if (getContext()->hasQueryContext())
            {
                auto query_context = getContext()->getQueryContext();
                query_context->dropStorageCacheEntry(from_table_id);
                query_context->dropStorageCacheEntry(to_table_id);
            }
        }
        catch (...)
        {
            /// Restore dependencies if RENAME fails
            DatabaseCatalog::instance().addDependencies(from_table_id, from_ref_dependencies, from_loading_dependencies, from_mv_dependencies);
            DatabaseCatalog::instance().addSourceViewDependencies(from_table_id, from_dependent_views);
            if (!to_ref_dependencies.empty() || !to_loading_dependencies.empty() || !to_mv_dependencies.empty())
                DatabaseCatalog::instance().addDependencies(to_table_id, to_ref_dependencies, to_loading_dependencies, to_mv_dependencies);
            DatabaseCatalog::instance().addSourceViewDependencies(to_table_id, to_dependent_views);
            throw;
        }
    }

    return {};
}

BlockIO InterpreterRenameQuery::executeToDatabase(const ASTRenameQuery &, const RenameDescriptions & descriptions)
{
    chassert(descriptions.size() == 1);
    chassert(descriptions.front().from_table_name.empty());
    chassert(descriptions.front().to_table_name.empty());

    const auto & old_name = descriptions.front().from_database_name;
    const auto & new_name = descriptions.back().to_database_name;
    auto & catalog = DatabaseCatalog::instance();

    auto db = descriptions.front().if_exists ? catalog.tryGetDatabase(old_name) : catalog.getDatabase(old_name);

    if (db)
    {
        /// The database-level `DDLGuard` taken above only serializes this query against other
        /// database-level DDL (`CREATE`, `DROP`, `RENAME DATABASE`). Table-level DDL takes the shared
        /// side of `database_ddl_mutex`, so the exclusive side is what excludes a concurrent
        /// `CREATE TABLE` / `ATTACH TABLE`: the rename re-keys every table of the database, and
        /// `renameDatabase` validates and rewrites the exact set of tables it sees under this lock.
        /// The same lock is held by `CREATE DATABASE` and `DROP DATABASE`.
        auto db_guard = catalog.getExclusiveDDLGuardForDatabase(old_name);

        catalog.assertDatabaseDoesntExist(new_name);
        ViewDefinitionsRenaming view_definitions(old_name, new_name, getContext());
        view_definitions.prepare();
        catalog.assertDatabaseDoesntExist(new_name);
        db->renameDatabase(getContext(), new_name);
        view_definitions.commit();
    }

    return {};
}

AccessRightsElements InterpreterRenameQuery::getRequiredAccess(InterpreterRenameQuery::RenameType type) const
{
    AccessRightsElements required_access;
    const auto & rename = query_ptr->as<const ASTRenameQuery &>();
    for (const auto & elem : rename.getElements())
    {
        if (type == RenameType::RenameTable)
        {
            required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, elem.from.getDatabase(), elem.from.getTable());
            required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, elem.to.getDatabase(), elem.to.getTable());
            if (rename.exchange)
            {
                required_access.emplace_back(AccessType::CREATE_TABLE | AccessType::INSERT, elem.from.getDatabase(), elem.from.getTable());
                required_access.emplace_back(AccessType::SELECT | AccessType::DROP_TABLE, elem.to.getDatabase(), elem.to.getTable());
            }
        }
        else if (type == RenameType::RenameDatabase)
        {
            required_access.emplace_back(AccessType::SELECT | AccessType::DROP_DATABASE, elem.from.getDatabase());
            required_access.emplace_back(AccessType::CREATE_DATABASE | AccessType::INSERT, elem.to.getDatabase());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown type of rename query");
        }
    }
    return required_access;
}

void InterpreterRenameQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const
{
    const auto & rename = ast->as<const ASTRenameQuery &>();
    for (const auto & element : rename.getElements())
    {
        {
            String database = backQuoteIfNeed(!element.from.database ? getContext()->getCurrentDatabase() : element.from.getDatabase());
            elem.query_databases.insert(database);
            elem.query_tables.insert(database + "." + backQuoteIfNeed(element.from.getTable()));
        }
        {
            String database = backQuoteIfNeed(!element.to.database ? getContext()->getCurrentDatabase() : element.to.getDatabase());
            elem.query_databases.insert(database);
            elem.query_tables.insert(database + "." + backQuoteIfNeed(element.to.getTable()));
        }
    }
}

void registerInterpreterRenameQuery(InterpreterFactory & factory);
void registerInterpreterRenameQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterRenameQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterRenameQuery", create_fn);
}

}
