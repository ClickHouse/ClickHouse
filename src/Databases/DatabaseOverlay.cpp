#include <Databases/DatabaseOverlay.h>

#include <Access/ContextAccess.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/AsyncLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <Storages/IStorage.h>
#include <Storages/StorageView.h>
#include <Core/UUID.h>

#include <Databases/DatabaseFactory.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/logger_useful.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_GET_CREATE_TABLE_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_IS_PERMANENTLY_READ_ONLY;
    extern const int ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
}

DatabaseOverlay::DatabaseOverlay(const String & name_, ContextPtr context_, bool readonly_)
    : IDatabase(name_)
    , WithContext(context_->getGlobalContext())
    , log(getLogger("DatabaseOverlay(" + name_ + ")"))
    , readonly(readonly_)
{
}

DatabaseOverlay & DatabaseOverlay::registerNextDatabase(DatabasePtr database)
{
    if (!database)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Overlay database {} received a null underlying database pointer",
            backQuote(getDatabaseName()));
    databases.push_back(std::move(database));
    return *this;
}

DatabaseOverlay & DatabaseOverlay::registerNextDatabaseByName(const String & source_name)
{
    if (source_name.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Overlay database {} received an empty underlying database name",
            backQuote(getDatabaseName()));
    source_names.push_back(source_name);
    return *this;
}

bool DatabaseOverlay::isReadonlyFacade(const IDatabase * database)
{
    return asReadonlyFacade(database) != nullptr;
}

const DatabaseOverlay * DatabaseOverlay::asReadonlyFacade(const IDatabase * database)
{
    const auto * overlay = typeid_cast<const DatabaseOverlay *>(database);
    return (overlay && overlay->readonly) ? overlay : nullptr;
}

std::shared_ptr<const DatabaseOverlay> DatabaseOverlay::tryGetReadonlyFacade(const String & written_database_name)
{
    /// A written name can be unqualified, and the current database of the context can be unset
    /// (a bare `Context` outside a session, as in the unit tests), so an empty name reaches here
    /// and must answer "not a facade" instead of tripping the catalog's non-empty assertion.
    if (written_database_name.empty())
        return nullptr;
    auto database = DatabaseCatalog::instance().tryGetDatabase(written_database_name);
    if (!asReadonlyFacade(database.get()))
        return nullptr;
    /// Keeps the database alive for as long as the caller holds the facade.
    return std::static_pointer_cast<const DatabaseOverlay>(std::move(database));
}

bool DatabaseOverlay::isSourceTableVisibleNoLoad(const String & table_name, ContextPtr context_, AccessType access_to_check) const
{
    for (const auto & db : resolveDatabases())
    {
        bool exists = false;
        try
        {
            exists = db->isTableExist(table_name, context_);
        }
        catch (...)
        {
            /// The existence probe itself can reach a remote catalog (`MySQL`, `PostgreSQL`,
            /// data-lake catalogs) and throw that source's own error. The caller has not yet
            /// proven the source-side grant, so surfacing the error would turn the facade into
            /// an oracle for hidden broken sources. If the caller is granted on the name in this
            /// source, the error is theirs to see — direct access to the source would surface
            /// the same; otherwise fail closed and answer as for a hidden or missing name.
            if (context_->getAccess()->isGranted(access_to_check, db->getDatabaseName(), table_name))
                throw;
            return false;
        }
        if (exists)
            return context_->getAccess()->isGranted(access_to_check, db->getDatabaseName(), table_name);
    }
    return false;
}

void DatabaseOverlay::checkSourceTableAccess(const String & table_name, ContextPtr context_, AccessType access_to_check) const
{
    /// The denial names the facade exactly as it was written in the query, never the resolved
    /// source id. `ContextAccess` formats `ACCESS_DENIED` from the real `db.table`, and its hint
    /// filter only strips column names, so letting `checkAccess` throw here would tell a caller
    /// that holds the facade-side grant alone both that the name exists behind the facade and
    /// which source database owns it - precisely what the source-side grant exists to hide.
    auto deny = [&]
    {
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "{}: Not enough privileges. To execute this query, it's necessary to have the grant {} ON {}.{} in the "
            "underlying source database of this Overlay facade",
            context_->getUserName(),
            toString(access_to_check),
            backQuote(getDatabaseName()),
            backQuote(table_name));
    };

    for (const auto & db : resolveDatabases())
    {
        bool exists = false;
        try
        {
            exists = db->isTableExist(table_name, context_);
        }
        catch (...)
        {
            /// Same fencing as in `isSourceTableVisibleNoLoad`: the probe can throw a remote
            /// source's own error before the source-side grant is proven. Deny exactly as if the
            /// name had resolved to this source and the grant check failed, keeping a broken
            /// hidden source and a denied healthy one indistinguishable; for a granted caller the
            /// source's own error is rethrown as theirs to see.
            if (!context_->getAccess()->isGranted(access_to_check, db->getDatabaseName(), table_name))
                deny();
            throw;
        }
        if (exists)
        {
            if (!context_->getAccess()->isGranted(access_to_check, db->getDatabaseName(), table_name))
                deny();
            return;
        }
    }
}

void DatabaseOverlay::checkSourceTableAccessIfFacade(const StorageID & table_id, ContextPtr context_, AccessType access_to_check)
{
    if (const auto facade = tryGetReadonlyFacade(table_id.database_name))
        facade->checkSourceTableAccess(table_id.table_name, context_, access_to_check);
}

bool DatabaseOverlay::areSourceDatabaseNamesVisible(const ContextPtr & context_) const
{
    /// The `clickhouse-local` variant is registered programmatically and its definition carries no
    /// engine clause at all, so there is nothing to disclose.
    if (!readonly)
        return true;

    const auto & access = context_->getAccess();
    for (const auto & name : source_names)
        if (!access->isGranted(AccessType::SHOW_DATABASES, name))
            return false;
    return true;
}

bool DatabaseOverlay::usesSourceDatabase(const String & source_database_name) const
{
    if (!readonly)
        return false;
    /// `source_names` is filled by the factory before the database is published in the catalog and
    /// is never modified afterwards, so it needs no synchronization here.
    return std::find(source_names.begin(), source_names.end(), source_database_name) != source_names.end();
}

void DatabaseOverlay::checkSourceDatabaseNamesVisible(const ContextPtr & context_) const
{
    if (areSourceDatabaseNamesVisible(context_))
        return;

    /// Deliberately names only the facade: naming the source that failed the check would leak
    /// exactly what the check protects.
    throw Exception(
        ErrorCodes::ACCESS_DENIED,
        "Not enough privileges to show the definition of database {}: it is an Overlay facade, and its definition "
        "lists its source databases, which requires SHOW DATABASES on every one of them",
        backQuote(getDatabaseName()));
}

std::optional<StorageID> DatabaseOverlay::getSourceTableIdForReadonlyFacade(const StorageID & written_id, const StoragePtr & storage)
{
    if (!storage || !written_id.hasDatabase())
        return {};

    /// A parameterized view reached through a facade is re-wrapped into a synthesized
    /// `StorageView` whose own id is the facade name as written in the query, so the two ids
    /// coincide and cannot reveal the source. The synthesized view carries the id of the
    /// underlying source view instead (recorded by `Context` at the only places that build it,
    /// after the name resolved through a read-only facade).
    if (const auto * view = typeid_cast<const StorageView *>(storage.get()))
        if (const auto & carried_source_id = view->getOverlaySourceTableId())
            return carried_source_id;

    auto source_id = storage->getStorageID();
    if (!source_id.hasDatabase()
        || (source_id.database_name == written_id.database_name && source_id.table_name == written_id.table_name))
        return {};
    const auto written_db = DatabaseCatalog::instance().tryGetDatabase(written_id.database_name);
    if (isReadonlyFacade(written_db.get()))
        return source_id;
    return {};
}

bool DatabaseOverlay::isSourceTableHiddenFromShow(
    const ContextAccessWrapper & access, const String & written_database_name, const String & written_table_name, const StoragePtr & storage)
{
    const auto source_id = getSourceTableIdForReadonlyFacade(StorageID{written_database_name, written_table_name}, storage);
    return source_id && !access.isGranted(AccessType::SHOW_TABLES, source_id->database_name, source_id->table_name);
}

std::vector<DatabasePtr> DatabaseOverlay::resolveDatabases() const
{
    if (!readonly)
        return databases;

    std::vector<DatabasePtr> resolved;
    resolved.reserve(source_names.size());
    for (const auto & name : source_names)
    {
        auto db = DatabaseCatalog::instance().tryGetDatabase(name);
        if (!db)
            continue;
        /// If the source is not (yet) registered, skip it silently: during
        /// `loadMetadata` the Overlay may be processed before one of its sources,
        /// and the missing source will simply become visible once it is loaded.

        /// A read-only `Overlay` must not use another read-only `Overlay` as a source. Such nesting
        /// would silently bypass the intermediate facade in every runtime check: reading `top.t`
        /// (with `top = Overlay('mid')` and `mid = Overlay('src')`) resolves the storage straight
        /// to `src.t`, so the access and row-policy code — which only sees the written id (`top.t`)
        /// and the resolved storage id (`src.t`) — would never require the grants or apply the row
        /// policies defined on `mid.t`.
        ///
        /// Every path that can configure such a pair rejects it up front with a clear error: both
        /// directions are checked when the `Overlay` is created or explicitly attached (see
        /// `registerDatabaseOverlay`) — the source being a facade already, and the new database
        /// being a source of an existing facade, which is how a cycle can form after creation
        /// (create `db_b` as `Overlay('db_a')`, drop `db_a`, re-create `db_a` as `Overlay('db_b')`).
        ///
        /// Here — the lazy path, reachable only for metadata that was not written by those checks,
        /// e.g. a pair persisted by an older server and replayed at startup — the nested source is
        /// skipped instead. Skipping is fail-closed (the nested source contributes no table to the
        /// union, so no grant or row policy can be bypassed through it) and, unlike an exception,
        /// it keeps one misconfigured facade from breaking unrelated queries: `resolveDatabases` is
        /// reached from whole-server scans (`system.mutations`, `system.rocksdb`, the asynchronous
        /// metrics, ...) that walk every database, and throwing here failed such a scan entirely.
        if (const auto * nested = typeid_cast<const DatabaseOverlay *>(db.get()); nested && nested->readonly)
        {
            /// Once per instance: the state is permanent, while this runs on every lookup.
            if (!nested_source_warning_logged.test_and_set())
                LOG_WARNING(
                    log,
                    "Overlay database {} uses another Overlay database as a source, which is not supported. "
                    "That source is excluded from the facade; re-create the database to fix its definition",
                    backQuote(getDatabaseName()));
            continue;
        }

        resolved.push_back(std::move(db));
    }
    return resolved;
}

bool DatabaseOverlay::isTableExist(const String & table_name, ContextPtr context_) const
{
    for (const auto & db : resolveDatabases())
    {
        if (db->isTableExist(table_name, context_))
            return true;
    }
    return false;
}

StoragePtr DatabaseOverlay::tryGetTable(const String & table_name, ContextPtr context_) const
{
    StoragePtr result = nullptr;
    for (const auto & db : resolveDatabases())
    {
        result = db->tryGetTable(table_name, context_);
        if (result)
            break;
    }
    return result;
}

void DatabaseOverlay::createTable(ContextPtr context_, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    for (auto & db : resolveDatabases())
    {
        if (!db->isReadOnly())
        {
            db->createTable(context_, table_name, table, query);
            return;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for CREATE TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::dropTable(ContextPtr context_, const String & table_name, bool sync)
{
    if (readonly)
        throw Exception(
            ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY,
            "Database {} is an Overlay facade (read-only). "
            "Run DROP TABLE in the underlying database that owns the table",
            backQuote(getDatabaseName()));
    for (auto & db : resolveDatabases())
    {
        if (db->isTableExist(table_name, context_))
        {
            db->dropTable(context_, table_name, sync);
            return;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for DROP TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::attachTable(
    ContextPtr context_, const String & table_name, const StoragePtr & table, const String & relative_table_path)
{
    if (readonly)
        throw Exception(
            ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY,
            "Database {} is an Overlay facade (read-only). "
            "Run ATTACH TABLE in an underlying database",
            backQuote(getDatabaseName()));

    for (auto & db : resolveDatabases())
    {
        try
        {
            db->attachTable(context_, table_name, table, relative_table_path);
            return;
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for ATTACH TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

StoragePtr DatabaseOverlay::detachTable(ContextPtr context_, const String & table_name)
{
    if (readonly)
        throw Exception(
            ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY,
            "Database {} is an Overlay facade (read-only). "
            "Run DETACH TABLE in the underlying database that owns the table",
            backQuote(getDatabaseName()));
    for (auto & db : resolveDatabases())
    {
        if (db->isTableExist(table_name, context_))
            return db->detachTable(context_, table_name);
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for DETACH TABLE `{}` query in database `{}` (engine {})",
        table_name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::renameTable(
    ContextPtr current_context,
    const String & name,
    IDatabase & to_database,
    const String & to_name,
    bool exchange,
    bool dictionary)
{
    if (readonly)
        throw Exception(
            ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY,
            "Database {} is an Overlay facade (read-only). "
            "Run RENAME TABLE in an underlying database",
            backQuote(getDatabaseName()));
    for (auto & db : resolveDatabases())
    {
        if (db->isTableExist(name, current_context))
        {
            if (DatabaseOverlay * to_overlay_database = typeid_cast<DatabaseOverlay *>(&to_database))
            {
                /// Renaming from Overlay database inside itself or into another Overlay database.
                /// Just use the first database in the overlay as a destination.
                auto target_databases = to_overlay_database->resolveDatabases();
                if (target_databases.empty())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The destination Overlay database {} does not have any members", to_database.getDatabaseName());

                db->renameTable(current_context, name, *target_databases[0], to_name, exchange, dictionary);
            }
            else
            {
                /// Renaming into a different type of database. E.g. from Overlay on top of Atomic database into just Atomic database.
                db->renameTable(current_context, name, to_database, to_name, exchange, dictionary);
            }

            return;
        }
    }
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist", backQuote(getDatabaseName()), backQuote(name));
}

ASTPtr DatabaseOverlay::getCreateTableQueryImpl(const String & name, ContextPtr context_, bool throw_on_error) const
{
    ASTPtr result = nullptr;
    for (const auto & db : resolveDatabases())
    {
        result = db->tryGetCreateTableQuery(name, context_);
        if (result)
            break;
    }
    if (!result && throw_on_error)
        throw Exception(
            ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY,
            "There is no metadata of table `{}` in database `{}` (engine {})",
            name,
            getDatabaseName(),
            getEngineName());
    return result;
}

ASTPtr DatabaseOverlay::getCreateDatabaseQueryImpl() const
{
    auto query = make_intrusive<ASTCreateQuery>();
    query->setDatabase(database_name);

    if (readonly)
    {
        auto storage = make_intrusive<ASTStorage>();

        auto engine_func = make_intrusive<ASTFunction>();
        engine_func->name = "Overlay";

        auto args = make_intrusive<ASTExpressionList>();
        args->children.reserve(source_names.size());
        /// Use the symbolic `source_names` directly: this works even if some of
        /// the sources have not been (re)loaded yet, and preserves what the user
        /// wrote, not what `DatabaseCatalog` happens to resolve right now.
        for (const auto & name : source_names)
            args->children.emplace_back(make_intrusive<ASTLiteral>(name));
        engine_func->arguments = args;

        storage->set(storage->engine, engine_func);
        query->set(query->storage, storage);
    }
    return query;
}

String DatabaseOverlay::getTableDataPath(const String & table_name) const
{
    String result;
    for (const auto & db : resolveDatabases())
    {
        result = db->getTableDataPath(table_name);
        if (!result.empty())
            break;
    }
    return result;
}

String DatabaseOverlay::getTableDataPath(const ASTCreateQuery & query) const
{
    String result;
    for (const auto & db : resolveDatabases())
    {
        result = db->getTableDataPath(query);
        if (!result.empty())
            break;
    }
    return result;
}

bool DatabaseOverlay::isReadOnly() const
{
    return readonly;
}

UUID DatabaseOverlay::getUUID() const
{
    // The table creation path delegates to the first available source database. Returning its
    // UUID ensures that an Atomic source generates table UUIDs before it receives the table.
    UUID result = UUIDHelpers::Nil;
    for (const auto & db : resolveDatabases())
    {
        result = db->getUUID();
        if (result != UUIDHelpers::Nil)
            break;
    }
    return result;
}

UUID DatabaseOverlay::tryGetTableUUID(const String & table_name) const
{
    UUID result = UUIDHelpers::Nil;
    for (const auto & db : resolveDatabases())
    {
        result = db->tryGetTableUUID(table_name);
        if (result != UUIDHelpers::Nil)
            break;
    }
    return result;
}

void DatabaseOverlay::drop(ContextPtr context_)
{
    if (readonly)
        return;
    /// `drop` is the non-readonly path only — `databases` is authoritative here.
    for (auto & db : databases)
        db->drop(context_);
}

void DatabaseOverlay::alterTable(ContextPtr local_context, const StorageID & table_id, const StorageInMemoryMetadata & metadata, const bool validate_new_create_query)
{
    if (readonly)
        throw Exception(
            ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY,
            "Database {} is an Overlay facade (read-only). "
            "Run ALTER TABLE in an underlying database",
            backQuote(getDatabaseName()));
    for (auto & db : resolveDatabases())
    {
        if (!db->isReadOnly() && db->isTableExist(table_id.table_name, local_context))
        {
            db->alterTable(local_context, table_id, metadata, validate_new_create_query);
            return;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases for ALTER TABLE `{}` query in database `{}` (engine {})",
        table_id.table_name,
        getDatabaseName(),
        getEngineName());
}

std::vector<std::pair<ASTPtr, StoragePtr>>
DatabaseOverlay::getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr & local_context) const
{
    /// The read-only facade owns no tables: backing them up here would rewrite their
    /// `CREATE TABLE` statements to the facade's name, producing a backup that cannot be
    /// restored (table creation through the facade is rejected). The tables are backed up
    /// with the source databases that own them; `BACKUP DATABASE` on the facade captures
    /// only the `CREATE DATABASE ... ENGINE = Overlay(...)` definition.
    if (readonly)
        return {};

    std::vector<std::pair<ASTPtr, StoragePtr>> result;
    for (const auto & db : resolveDatabases())
    {
        auto db_backup = db->getTablesForBackup(filter, local_context);
        result.insert(result.end(), std::make_move_iterator(db_backup.begin()), std::make_move_iterator(db_backup.end()));
    }
    return result;
}

void DatabaseOverlay::createTableRestoredFromBackup(
    const ASTPtr & create_table_query,
    ContextMutablePtr local_context,
    std::shared_ptr<IRestoreCoordination> /*restore_coordination*/,
    UInt64 /*timeout_ms*/)
{
    if (readonly)
        throw Exception(
            ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY,
            "Database {} is an Overlay facade (read-only). "
            "Run CREATE TABLE in an underlying database",
            backQuote(getDatabaseName()));
    /// Creates a tables by executing a "CREATE TABLE" query.
    InterpreterCreateQuery interpreter{create_table_query, local_context};
    interpreter.setInternal(true);
    interpreter.setIsRestoreFromBackup(true);
    interpreter.execute();
}

bool DatabaseOverlay::empty() const
{
    if (readonly)
        return true;
    for (const auto & db : databases)
        if (!db->empty())
            return false;
    return true;
}

void DatabaseOverlay::shutdown()
{
    /// In read-only (facade) mode the underlying databases are owned by `DatabaseCatalog`,
    /// not by this Overlay. Propagating `shutdown` would take down those real databases when the
    /// Overlay alone is dropped, so the facade's `shutdown` must be a no-op here.
    if (readonly)
        return;
    for (auto & db : databases)
        db->shutdown();
}

void DatabaseOverlay::collectFromSourceDatabases(ContextPtr context_, const std::function<void(const DatabasePtr &)> & collect) const
{
    for (const auto & db : resolveDatabases())
    {
        /// Fail-closed listing through the read-only facade: opening a source's iterator can reach
        /// a remote catalog (`MySQL`, `PostgreSQL`, data-lake catalogs) and throw that source's own
        /// error before any source-side grant is proven, which would let a caller granted only on
        /// the facade use listings (`SHOW TABLES`, `system.tables`, `system.columns`, ...) as an
        /// oracle for hidden broken sources — the same fencing as in `isSourceTableVisibleNoLoad`.
        /// A caller granted `SHOW TABLES` on the whole source database is entitled to the error
        /// (listing the source directly would surface the same), so it propagates as theirs to see;
        /// for anyone else a failing source contributes nothing and ends the walk, so the listing is
        /// indistinguishable from the one a hidden healthy source would produce. Per-table
        /// visibility of the collected names is still enforced by the
        /// metadata readers themselves against the owning source table.
        if (readonly && context_ && !context_->getAccess()->isGranted(AccessType::SHOW_TABLES, db->getDatabaseName()))
        {
            try
            {
                collect(db);
            }
            catch (...)
            {
                /// Ok to swallow: the caller has not proven the source-side `SHOW TABLES` grant,
                /// so the source's error must not surface through the facade (see above).
                tryLogCurrentException(log, fmt::format("Hidden from the caller: failed to list tables of the source database {}", backQuote(db->getDatabaseName())));
                /// Stop the walk. The failed source contributed an unknown set of names, and any of
                /// them could have shadowed a same-named table of a later source. Continuing would
                /// list the later source's table while the read path — which stops at the first
                /// source that owns the name — still refuses it, and it would make the listing of a
                /// hidden *broken* source differ from that of a hidden *healthy* one under the same
                /// grants, i.e. exactly the oracle this fencing exists to prevent.
                return;
            }
        }
        else
        {
            collect(db);
        }
    }
}

DatabaseTablesIteratorPtr DatabaseOverlay::getTablesIterator(ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    /// Note: the `Overlay` exposes the *union* of tables from its underlying
    /// databases. The same physical table may also be reachable directly via
    /// its owner database registered in `DatabaseCatalog`. Callers that
    /// aggregate across all databases (e.g. `ServerAsynchronousMetrics`) must
    /// deduplicate by `IStorage *` to avoid double-counting.
    Tables tables;
    std::unordered_map<String, String> table_sources;
    collectFromSourceDatabases(context_, [&](const DatabasePtr & db)
    {
        for (auto table_it = db->getTablesIterator(context_, filter_by_table_name, skip_not_loaded); table_it->isValid(); table_it->next())
            if (tables.insert({table_it->name(), table_it->table()}).second)
                table_sources.emplace(table_it->name(), db->getDatabaseName());
    });
    return std::make_unique<TablesSnapshotIterator>(std::move(tables), getDatabaseName(), std::move(table_sources));
}

DatabaseTablesIteratorPtr DatabaseOverlay::getTablesIteratorWithHint(
    ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded, const TablesFilter & tables_filter) const
{
    /// Same as `getTablesIterator`, but each source database gets the hint, so a source that pushes
    /// it down to an external catalog still does, and a source whose hint-aware iterator keeps
    /// unresolvable tables (a null storage) instead of aborting the listing still does. The facade
    /// must not degrade a source's own listing semantics.
    Tables tables;
    std::unordered_map<String, String> table_sources;
    collectFromSourceDatabases(context_, [&](const DatabasePtr & db)
    {
        for (auto table_it = db->getTablesIteratorWithHint(context_, filter_by_table_name, skip_not_loaded, tables_filter);
             table_it->isValid();
             table_it->next())
            if (tables.insert({table_it->name(), table_it->table()}).second)
                table_sources.emplace(table_it->name(), db->getDatabaseName());
    });
    return std::make_unique<TablesSnapshotIterator>(std::move(tables), getDatabaseName(), std::move(table_sources));
}

std::vector<LightWeightTableDetails> DatabaseOverlay::getLightweightTablesIterator(
    ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    return getLightweightTablesIteratorWithHint(context_, filter_by_table_name, skip_not_loaded, {});
}

std::vector<LightWeightTableDetails> DatabaseOverlay::getLightweightTablesIteratorWithHint(
    ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded, const TablesFilter & tables_filter) const
{
    /// The default implementation of `IDatabase::getLightweightTablesIterator` walks the heavyweight
    /// `getTablesIterator`, which behind a facade resolves the storage of every table of every
    /// source. Forward to the sources' own lightweight listing instead, so a names-only query
    /// (`SHOW TABLES`, `SELECT name FROM system.tables`) stays as cheap through the facade as it is
    /// against the source databases. Names are deduplicated because the same physical table can be
    /// exposed by several sources; the first listed source wins, as in `getTablesIterator`.
    std::vector<LightWeightTableDetails> result;
    std::unordered_set<String> seen_names;
    collectFromSourceDatabases(context_, [&](const DatabasePtr & db)
    {
        for (const auto & details : db->getLightweightTablesIteratorWithHint(context_, filter_by_table_name, skip_not_loaded, tables_filter))
            if (seen_names.insert(details.name).second)
                result.push_back(details);
    });
    return result;
}

DatabaseDetachedTablesSnapshotIteratorPtr DatabaseOverlay::getDetachedTablesIterator(
    ContextPtr context_, const FilterByNameFunction & filter_by_table_name, bool skip_not_loaded) const
{
    /// A read-only facade has no detached tables of its own: `ATTACH` and `DETACH` through it are
    /// rejected, so a name detached in a source database is not part of the facade's namespace.
    /// Reporting the sources' detached tables here would also expose them to a caller granted only
    /// on the facade, which is exactly what the read-only facade must not do.
    if (readonly)
        return std::make_unique<DatabaseDetachedTablesSnapshotIterator>(SnapshotDetachedTables{});

    /// In `clickhouse-local` the overlay owns its underlying databases and a `DETACH` goes to one of
    /// them, so report the union of their detached tables, as for the attached ones. The first
    /// listed source wins for a name detached in several of them. A source that does not implement
    /// detached tables at all (`DatabaseFilesystem`) is skipped, so that it does not hide the
    /// detached tables of the sources that do implement them.
    SnapshotDetachedTables detached_tables;
    for (const auto & db : resolveDatabases())
    {
        DatabaseDetachedTablesSnapshotIteratorPtr table_it;
        try
        {
            table_it = db->getDetachedTablesIterator(context_, filter_by_table_name, skip_not_loaded);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
                continue;
            throw;
        }
        for (; table_it->isValid(); table_it->next())
        {
            detached_tables.emplace(
                table_it->table(),
                SnapshotDetachedTable{
                    .database = getDatabaseName(),
                    .table = table_it->table(),
                    .uuid = table_it->uuid(),
                    .metadata_path = table_it->metadataPath(),
                    .is_permanently = table_it->isPermanently()});
        }
    }
    return std::make_unique<DatabaseDetachedTablesSnapshotIterator>(std::move(detached_tables));
}

bool DatabaseOverlay::isExternal() const
{
    for (const auto & db : resolveDatabases())
        if (!db->isExternal())
            return false;
    return true;
}

bool DatabaseOverlay::isRemoteDatabase() const
{
    /// A server-side (read-only) `Overlay` is reported as remote so that it follows
    /// `show_remote_databases_in_system_tables` exactly like the remote database engines it may
    /// wrap: because the facade is a local object that walks all of its underlying databases in
    /// `getTablesIterator`, an `Overlay` over a remote source (`MySQL`/`PostgreSQL`/`DataLake`)
    /// would otherwise let `system.tables`/`system.columns` reach the remote service and issue
    /// implicit calls (e.g. query `INFORMATION_SCHEMA`) even when the setting excludes remote
    /// databases, and would let the catalog enumeration `getDatabases({.with_remote_databases =
    /// false})` used by the asynchronous metrics do the same unconditionally. Explicit
    /// `SHOW TABLES` and direct queries through the facade work regardless of the setting, and
    /// `system.databases` always lists it.
    ///
    /// We answer from the `readonly` flag rather than by resolving sources: it is constant per
    /// instance (so it does not depend on the order in which databases are loaded at startup) and it
    /// touches no other database (so it is safe to call from `DatabaseCatalog::attachDatabase`, which
    /// holds `databases_mutex`). The `clickhouse-local` (non-read-only) `Overlay`, which owns its
    /// underlying databases and acts as the default database, stays non-remote so that its tables
    /// keep showing up in `system.tables`.
    return readonly;
}

void DatabaseOverlay::loadStoredObjects(ContextMutablePtr local_context, LoadingStrictnessLevel loading_mode)
{
    if (readonly)
        return;
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->loadStoredObjects(local_context, loading_mode);
}

bool DatabaseOverlay::supportsLoadingInTopologicalOrder() const
{
    if (readonly)
        return false;
    for (const auto & db : databases)
        if (db->supportsLoadingInTopologicalOrder())
            return true;
    return false;
}

void DatabaseOverlay::beforeLoadingMetadata(ContextMutablePtr local_context, LoadingStrictnessLevel loading_mode)
{
    if (readonly)
        return;
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->beforeLoadingMetadata(local_context, loading_mode);
}

void DatabaseOverlay::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    if (readonly)
        return;
    for (auto & db : databases)
        if (!db->isReadOnly())
            db->loadTablesMetadata(local_context, metadata, is_startup);
}

void DatabaseOverlay::loadTableFromMetadata(
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->loadTableFromMetadata(local_context, file_path, name, ast, loading_mode);
            return;
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of loading table `{}` from path `{}` in database `{}` (engine {})",
        name.table,
        file_path,
        getDatabaseName(),
        getEngineName());
}

LoadTaskPtr DatabaseOverlay::loadTableFromMetadataAsync(
    AsyncLoader & async_loader,
    LoadJobSet load_after,
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->loadTableFromMetadataAsync(async_loader, load_after, local_context, file_path, name, ast, loading_mode);
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of loading table `{}` from path `{}` in database `{}` (engine {})",
        name.table,
        file_path,
        getDatabaseName(),
        getEngineName());
}

LoadTaskPtr DatabaseOverlay::startupTableAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    const QualifiedTableName & name,
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->startupTableAsync(async_loader, startup_after, name, loading_mode);
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of starting up table `{}` in database `{}` (engine {})",
        name.table,
        getDatabaseName(),
        getEngineName());
}

LoadTaskPtr DatabaseOverlay::startupDatabaseAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    LoadingStrictnessLevel loading_mode)
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            return db->startupDatabaseAsync(async_loader, startup_after, loading_mode);
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of starting up asynchronously in database `{}` (engine {})",
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::waitTableStarted(const String & name) const
{
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->waitTableStarted(name);
            return;
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of waiting for table startup `{}` in database `{}` (engine {})",
        name,
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::waitDatabaseStarted() const
{
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->waitDatabaseStarted();
            return;
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of waiting for startup in database `{}` (engine {})",
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::stopLoading()
{
    for (auto & db : databases)
    {
        if (db->isReadOnly())
            continue;

        try
        {
            db->stopLoading();
            return;
        }
        catch (const std::exception &)
        {
            continue;
        }
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "There are no databases capable of stop loading in database `{}` (engine {})",
        getDatabaseName(),
        getEngineName());
}

void DatabaseOverlay::checkMetadataFilenameAvailability(const String & table_name) const
{
    if (readonly)
        throw Exception(
            ErrorCodes::TABLE_IS_PERMANENTLY_READ_ONLY,
            "Database {} is an Overlay facade (read-only). Path resolution is not supported here.",
            backQuote(getDatabaseName()));
    for (const auto & db : databases)
    {
        if (db->isReadOnly())
            continue;
        db->checkMetadataFilenameAvailability(table_name);
        return;
    }
}

void registerDatabaseOverlay(DatabaseFactory & factory);
void registerDatabaseOverlay(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        const auto * engine_def = args.create_query.storage;
        const auto * engine = engine_def->engine;
        const String & engine_name = engine->name;

        std::vector<String> sources;

        if (!engine->arguments || engine->arguments->children.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "{} database requires at least 1 argument: underlying database name(s)", engine_name);

        for (const auto & arg_ast : engine->arguments->children)
        {
            auto lit = evaluateConstantExpressionOrIdentifierAsLiteral(arg_ast, args.context);
            const auto & value = lit->as<ASTLiteral &>().value;
            sources.emplace_back(value.safeGet<String>());
        }

        if (sources.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} database requires at least 1 source database", engine_name);

        /// Preserve the user-supplied order of source databases (it is part of the lookup contract:
        /// for tables with the same name, the database listed earlier wins). Deduplicate in place,
        /// keeping the first occurrence.
        {
            std::unordered_set<String> seen;
            seen.reserve(sources.size());
            std::vector<String> deduped;
            deduped.reserve(sources.size());
            for (const auto & name : sources)
            {
                if (seen.insert(name).second)
                    deduped.push_back(name);
            }
            sources = std::move(deduped);
        }

        for (const auto & source_name : sources)
        {
            if (source_name == args.database_name)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} database cannot reference itself: {}", engine_name, source_name);
        }

        auto overlay = std::make_shared<DatabaseOverlay>(args.database_name, args.context, true /* readonly */);

        /// Source databases are stored symbolically and resolved lazily through
        /// `DatabaseCatalog` on each operation. Resolving eagerly here would make
        /// `loadMetadata` order-dependent: `loadMetadata` processes databases in
        /// alphabetical order, so an `Overlay` whose name sorts before one of its
        /// sources would fail to load on server startup.
        ///
        /// For a user-initiated `CREATE DATABASE ... ENGINE = Overlay(...)` (mode
        /// `CREATE`) we still validate that every source exists right now so the
        /// user gets an immediate error on typos. During `ATTACH`/`FORCE_ATTACH`
        /// (server startup) and `SECONDARY_CREATE` (`DatabaseReplicated` /
        /// `RESTORE`) we skip that check and rely on lazy resolution.
        const bool validate_sources_exist = (args.mode == LoadingStrictnessLevel::CREATE);

        /// An explicit `ATTACH DATABASE ... ENGINE = Overlay(...)` (mode `ATTACH`) is user-facing
        /// DDL just like `CREATE`, and it persists metadata: letting it attach a facade over another
        /// read-only facade would write a database that silently loses that source on every lookup
        /// (see `resolveDatabases`). Unlike `CREATE`, sources may legitimately be missing at this point
        /// (databases can be reattached in a different order than they were detached), so only the
        /// nested-facade rejection applies, and only when the source is currently resolvable.
        const bool validate_no_nested_facade = validate_sources_exist || (args.mode == LoadingStrictnessLevel::ATTACH);

        /// The same nesting can also be configured from the other side: an existing facade names a
        /// database that is only now (re-)created as a facade itself (`db_top = Overlay('db_hid')`,
        /// then `db_hid` dropped and re-created as `Overlay('db_src')`), which is also how a
        /// reference cycle forms after creation. Reject that too, so no user DDL can leave a facade
        /// with a source it has to skip. The message names only the database being created: the
        /// source names of the existing facade are protected metadata (see
        /// `areSourceDatabaseNamesVisible`), so naming it would disclose one of them.
        if (validate_no_nested_facade)
        {
            for (const auto & existing : DatabaseCatalog::instance().getDatabases(
                     GetDatabasesOptions{.with_datalake_catalogs = true, .with_remote_databases = true}))
            {
                const auto * facade = DatabaseOverlay::asReadonlyFacade(existing.second.get());
                if (facade && facade->usesSourceDatabase(args.database_name))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Database {} cannot be created with the {} engine because another Overlay database "
                        "already uses it as a source, and one Overlay database cannot be a source of another",
                        backQuote(args.database_name), engine_name);
            }
        }

        for (const auto & source_name : sources)
        {
            if (validate_sources_exist || validate_no_nested_facade)
            {
                const auto source_db = DatabaseCatalog::instance().tryGetDatabase(source_name);
                if (!source_db && validate_sources_exist)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "{} database requires existing underlying database '{}', but it was not found",
                        engine_name, source_name);

                /// Reject nesting one read-only `Overlay` inside another up front. Lazy resolution
                /// only skips such a source (see `resolveDatabases`), so this is where the user
                /// learns that the definition they wrote cannot work.
                if (source_db)
                    if (const auto * nested = typeid_cast<const DatabaseOverlay *>(source_db.get()); nested && nested->isReadOnly())
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "{} database cannot use another Overlay database '{}' as a source",
                            engine_name, source_name);
            }
            overlay->registerNextDatabaseByName(source_name);
        }

        return overlay;
    };

    factory.registerDatabase("Overlay", create_fn, { .supports_arguments = true }, Documentation{
        .description = R"DOCS_MD(
A facade that exposes **the union of the tables of several underlying databases**. The facade does not own data itself: each table name is resolved through the listed source databases in order, reads and `INSERT` queries pass through to the underlying table, and `CREATE TABLE` creates the table in the first source database.

`Overlay` is also used implicitly in `clickhouse-local` to represent local files from the filesystem in the default database.

## Creating a database {#creating-a-database}

```sql
CREATE DATABASE dboverlay ENGINE = Overlay('db_a', 'db_b');
```

**Engine Parameters**

- `'db1', 'db2', ...` — Names of the underlying source databases. At least one is required. Duplicate names are removed while preserving the first occurrence.

A user-initiated `CREATE DATABASE ... ENGINE = Overlay(...)` validates that every source database exists right now. After `ATTACH`, restore, or server startup, sources are resolved lazily by name, and a currently-missing source is simply omitted from the union until it is (re)created. An `Overlay` database cannot reference itself or use another `Overlay` database as a source. That is checked in both directions: a database that an existing `Overlay` database already uses as a source cannot be created (or attached) as an `Overlay` database either.

## Table discovery {#discovery}

- `SHOW TABLES FROM dboverlay` — returns the **union** of member database tables.
- `SELECT ... FROM dboverlay.table` — **reads** transparently hit the underlying table.

### Lookup order {#lookup-order}

Sources are searched in the order they were listed in `CREATE DATABASE ... ENGINE = Overlay(...)`. When two member databases contain tables with the same name, the table from the database listed **first** in the engine arguments wins.

## Mutating operations {#operations}

| Operation                  | Behavior                                                                                        |
| :------------------------- | :-----------------------------------------------------------------------------------------------|
| `CREATE TABLE dboverlay.*` | **Pass-through** — creates the table in the first underlying database.                              |
| `ATTACH TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Attach the table in an underlying database.     |
| `ALTER TABLE dboverlay.*`  | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`.                                                |
| `RENAME TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`.                                                |
| `DROP TABLE dboverlay.*`   | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Drop the table in the underlying database that owns it. |
| `DETACH TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Detach the table in the underlying database that owns it. |
| `TRUNCATE TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Truncate the table in the underlying database that owns it. |
| `OPTIMIZE TABLE dboverlay.*` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Optimize the table in the underlying database that owns it. |
| `DELETE FROM dboverlay.*`  | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Delete in the underlying database that owns the table.     |
| `UPDATE dboverlay.*`       | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Update in the underlying database that owns the table.     |
| `SYSTEM ... dboverlay.*` / `SYSTEM ... FROM DATABASE dboverlay` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Run the `SYSTEM` command (e.g. `STOP MERGES`, `RESTART REPLICA`, `DROP REPLICA`) against the underlying database that owns the table. This includes the commands that take a list of tables, such as `SYSTEM FLUSH ASYNC INSERT QUEUE dboverlay.t` and its unqualified form when `dboverlay` is the current database. Whole-server `SYSTEM` commands (with no table or database named) skip the facade and reach its source tables through their owner databases instead. |
| `TRUNCATE DATABASE dboverlay` / `TRUNCATE TABLES FROM dboverlay` | **Rejected** — `TABLE_IS_PERMANENTLY_READ_ONLY`. Truncate in the underlying databases that own the tables. |
| `INSERT INTO dboverlay.*`  | **Pass-through** — executes against the table in the corresponding underlying database.         |

The facade is a **view**: table creation is delegated to the first member database; other data definition and data mutation happen in the member databases.

`DROP DATABASE dboverlay` drops the facade only — the member databases and their tables are left untouched.

## Error codes and messages {#error-codes}

| Scenario                                   | Error                                                                                                                                              |
| :----------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- |
| Any mutating/management operation through the facade except `CREATE TABLE` — `ATTACH`/`ALTER`/`RENAME`/`DROP`/`DETACH`/`TRUNCATE`/`OPTIMIZE TABLE`, `DELETE FROM`, `UPDATE`, `SYSTEM`, `TRUNCATE DATABASE` | `TABLE_IS_PERMANENTLY_READ_ONLY` — "Database `<name>` is an Overlay facade (read-only). Run this operation in an underlying database." |
| Overlay references itself                  | `BAD_ARGUMENTS`                                                                                                                                    |
| Overlay references another Overlay, or a reference cycle (e.g. `db_a` → `db_b` → `db_a`, formed by re-creating a source) | `BAD_ARGUMENTS` on the `CREATE`/`ATTACH` that would form it — checked from both sides, so a persisted definition never becomes unusable |
| Overlay references missing database at `CREATE` | `BAD_ARGUMENTS` — a user-initiated `CREATE DATABASE ... ENGINE = Overlay(...)` validates that every source exists right now |
| Overlay references missing database after `ATTACH`/restore/startup | No error — sources are resolved lazily by name, and a currently-missing source is simply omitted from the union until it is (re)created |
| `DROP DATABASE` overlay while tables "exist" | Succeeds — the facade is always considered empty for the purposes of `DATABASE_NOT_EMPTY` |

## Notes {#notes}

- `DROP TABLE dboverlay.*` and `DETACH TABLE dboverlay.*` are rejected so that they cannot drop or detach the real table living in the underlying database. Drop or detach the table in the database that owns it.
- `DROP DATABASE dboverlay` drops the facade only; it does not call `shutdown`/`drop` on the member databases.
- `BACKUP DATABASE dboverlay` stores only the facade definition (`CREATE DATABASE ... ENGINE = Overlay(...)`). The tables are backed up with the underlying databases that own them, so back up those databases (or `BACKUP ALL`) to capture the data. An explicit `BACKUP TABLE dboverlay.t` is rejected with `CANNOT_BACKUP_TABLE` — back up the table from the underlying database that owns it.
- `RESTORE DATABASE dboverlay AS ...` rewrites the facade's source database names through the restore renaming map. If a source database is restored under a new name in the same `RESTORE` (e.g. `RESTORE DATABASE db_a AS db_a2, DATABASE dboverlay AS dboverlay2`), the restored facade points at the restored source (`db_a2`); a source that is not renamed is referenced under its original name.

## Examples of use {#examples-of-use}

```sql
-- Create/prepare underlying DBs and tables
CREATE DATABASE db_a ENGINE = Atomic;
CREATE DATABASE db_b ENGINE = Atomic;

CREATE TABLE db_a.t_a (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE db_b.t_b (id UInt32, s String) ENGINE = MergeTree ORDER BY id;

INSERT INTO db_a.t_a VALUES (1,'a1'), (2,'a2');
INSERT INTO db_b.t_b VALUES (10,'b10'), (20,'b20');

-- Create the overlay facade
CREATE DATABASE dboverlay ENGINE = Overlay('db_a', 'db_b');

-- Discover and read through overlay
SHOW TABLES FROM dboverlay;                -- t_a, t_b
SELECT * FROM dboverlay.t_a ORDER BY id;   -- rows from db_a.t_a
SELECT * FROM dboverlay.t_b ORDER BY id;   -- rows from db_b.t_b

-- Add a new table in an underlying database (overlay is read-only for DDL)
CREATE TABLE db_a.t_new (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO db_a.t_new VALUES (100,'x'), (200,'y');

-- Read the new table via overlay
SHOW TABLES FROM dboverlay;                -- now includes t_new
SELECT * FROM dboverlay.t_new ORDER BY k;  -- rows from db_a.t_new

-- Rename/drop in the underlying DB; overlay reflects changes
RENAME TABLE db_a.t_new TO db_a.t_new_renamed;
SELECT * FROM dboverlay.t_new_renamed ORDER BY k;

-- DDL must target the underlying database; DROP TABLE dboverlay.t_new_renamed would throw TABLE_IS_PERMANENTLY_READ_ONLY.
DROP TABLE db_a.t_new_renamed;
SHOW TABLES FROM dboverlay;                -- t_new_renamed disappears

-- Remove the overlay (does not touch member DBs)
DROP DATABASE dboverlay SYNC;
```

## Access control {#access-control}

Accessing a table through an `Overlay` database requires a grant on **both** the `Overlay` database (the name written in the query) and the underlying source database that owns the table. This applies to both reads and `INSERT`s:

- a `SELECT` grant on the `Overlay` alone is **not** enough to read through the facade — the user must also be granted `SELECT` on the underlying source database;
- a `SELECT` grant on an underlying database alone is **not** enough to read through the facade either, though it does allow reading that database directly (independently of the `Overlay`);
- `INSERT` through the facade likewise requires the `INSERT` privilege on both the `Overlay` and the underlying source database;
- the same dual-grant `SELECT` check covers the other read entrypoints that resolve the facade name to a source table, such as `WATCH`;
- management operations that resolve the facade name to a source table follow the same rule: `CHECK TABLE` requires the `CHECK` privilege on both the `Overlay` and the underlying source table, and `KILL MUTATION` / `KILL PART_MOVE TO SHARD` targeting a facade row of `system.mutations` / `system.part_moves_between_shards` require the corresponding `ALTER` privilege on both;
- create-time paths that read or write through the facade follow the same rule: `CREATE TABLE ... AS` (including `CLONE AS`) a facade name copies the schema of the underlying source table and requires `SHOW COLUMNS` on both the `Overlay` and the source table, and `CREATE MATERIALIZED VIEW ... TO` a facade target funnels writes into the source table and requires the `SELECT` and `INSERT` privileges on both;
- a parameterized view called through the facade (`SELECT ... FROM overlay_db.v(param = ...)`) runs the underlying source view and requires `SELECT` on both the `Overlay` and the source view, and `DESCRIBE` of such a call requires `SHOW COLUMNS` on both.

Row policies follow the same rule: reading a table through the facade applies the `SELECT` row policies of **both** the `Overlay` and the underlying source table (a row is returned only if it passes both). A row policy defined on the source table still applies to direct reads of that table, independently of the `Overlay`. This holds even when the source object is a view and the analyzer inlines it (`analyzer_inline_views = 1`): the facade's own row policies are still combined in, so inlining does not bypass them.

Metadata visibility follows the same dual-grant rule. `SHOW TABLES`, `SHOW CREATE TABLE`, `DESCRIBE`, `SHOW COLUMNS`, `EXISTS`, and the rows of `system.tables` / `system.columns` that belong to the facade expose a table only when the corresponding `SHOW` privilege is granted on **both** the `Overlay` and the underlying source table. Listings simply omit a table whose source-side privilege is missing, `EXISTS` reports it as nonexistent, and the point lookups (`SHOW CREATE TABLE`, `DESCRIBE`) are denied. The same rule covers the operational system tables that enumerate tables per database (`system.parts`, `system.parts_columns`, `system.mutations`, `system.replicas`, `system.replication_queue`, `system.distribution_queue`, `system.data_skipping_indices`, `system.projections`, `system.constraints`, and similar): a facade row is shown only when `SHOW TABLES` is also granted on the underlying source table. The `Maybe you meant ...?` hints attached to unknown-table errors follow the same rule: a misspelled facade name suggests a source table only to a user who holds the `SHOW` privilege on both the `Overlay` and that source table, so error messages do not reveal hidden names.

The facade's own definition — `Overlay('db_a', 'db_b', ...)` — names its source databases, so it is subject to the visibility of those databases too: `SHOW CREATE DATABASE` on the facade and `BACKUP DATABASE` of the facade require `SHOW DATABASES` on **every** source database and are denied otherwise, and the `engine_full` column of `system.databases` reports the bare `Overlay` engine, without the member list, to a user who is not granted on all of them. This keeps the facade from disclosing the names of databases the user is not allowed to see; the denial message names only the facade, never a source.

System views and metrics that aggregate or enumerate tables across all databases (`CHECK ALL TABLES`, `system.graphite_retentions`, `system.rocksdb`, `system.s3_queue_settings`, `system.azure_queue_settings`, and the asynchronous mutation / detached-part metrics) count each physical table exactly once. Because a read-only `Overlay` facade owns no tables of its own and merely re-exposes its sources, it is skipped by these whole-server scans so that an overlay-backed table is never listed or counted twice.

For the same reason a read-only `Overlay` reports no detached tables in `system.detached_tables`: `ATTACH` and `DETACH` through the facade are rejected, so a table detached in a source database is not part of the facade's namespace and is reported for the source database only. A facade being present never makes a whole-server scan of the detached tables fail.

The dual-grant checks are fail-closed even when a source database is broken or unreachable (for example a `PostgreSQL` or `MySQL` source whose server is down). The data entrypoints that resolve a facade name to a source table (`SELECT`, `INSERT`, `WATCH`, `CHECK TABLE`) prove source-side visibility **before** the source table is resolved and loaded — for every table of the query, including the tables of a `JOIN`, the right-hand side of an `IN` and the tables of the subqueries of a distributed query, and both with and without the analyzer — so a user without a grant on the source receives the same access-denied error for a hidden broken source as for a hidden healthy one: the facade never surfaces the hidden source's own error and cannot be used as an oracle for the state of sources the user is not allowed to see. Once the source-side grant is present, the source's own error propagates as usual. The listing-style readers (`SHOW TABLES` / `system.tables`, `system.columns`, `system.data_skipping_indices`, and the other per-database enumerations) follow the same rule when they walk the facade: a source database that fails while being listed contributes no rows and ends the walk, so the sources after it are not listed either, unless the caller is granted `SHOW TABLES` on that source database, in which case the source's own error propagates, the same as when listing the source directly. Ending the walk is what keeps the listing of a hidden broken source identical to the listing of a hidden healthy one: a table that a hidden source owns is not visible through the facade anyway, so continuing to a later source could only advertise a name that the read path, which stops at the first source owning it, still refuses.

Every diagnostic raised through a facade names only the facade, exactly as it was written in the query, and never the source database a name resolved to. A caller who holds the facade-side grant but not the source-side one is denied on the facade name, so the denial does not disclose which source database owns the name, and the runtime rejection of a facade that became nested through a late reconfiguration (a source database dropped and re-created as another read-only `Overlay`) names only the facade as well. The source names remain available through `SHOW CREATE DATABASE` and `system.databases.engine_full` to a caller holding `SHOW DATABASES` on every source.

Listing through the facade otherwise preserves each source database's own listing behaviour. The table-name filter of a `system.tables` query is forwarded to every source database, so a source that can push it down to an external catalog (a `DataLake` database) still does, and a source whose listing tolerates a table whose metadata cannot be fetched still lists the rest of its tables rather than failing the whole facade. A names-only listing (`SHOW TABLES`, `SELECT name FROM system.tables`) likewise stays a names-only listing and does not resolve the storage of every source table. A table name exposed by several sources is listed once, resolved to the first source that has it, as with any other lookup through the facade.

Creating an `Overlay` database requires a `SELECT` privilege on each underlying database it unions. A user who cannot read a source database therefore cannot expose it through a new `Overlay`. Creating an `Overlay` confers no privileges on the overlay database itself; as with any database engine, the creator must additionally be granted the relevant privileges on the `Overlay` before they can use it.

```sql
-- Both grants are required to read db_a tables through `dboverlay`:
GRANT SELECT ON dboverlay.* TO some_user;
GRANT SELECT ON db_a.* TO some_user;
```

## Compatibility notes {#compatibility}

- `Overlay` delegates `CREATE TABLE` to its first source database. `ATTACH`, `ALTER`, `RENAME`, `DROP`, `DETACH`, and `TRUNCATE` are rejected; reads and `INSERT` pass through to the underlying tables.
)DOCS_MD",
        .syntax = "ENGINE = Overlay('db1', 'db2', ...)",
        .examples = {{"Union of two databases", "CREATE DATABASE facade ENGINE = Overlay('db1', 'db2')", ""}},
        .related = {"Atomic"}});
}

void DatabaseOverlay::checkTableNameLength(const String & table_name) const
{
    /// The limit belongs to the member createTable writes to, which owns the metadata file.
    for (const auto & db : resolveDatabases())
    {
        if (db->isReadOnly())
            continue;
        db->checkTableNameLength(table_name);
        return;
    }
}

}
