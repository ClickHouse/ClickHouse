#include <Storages/IStorage.h>
#include <Storages/StorageAlias.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/UDT/ILifecycleAdapter.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/TableNameHints.h>
#include <Interpreters/UDTTableIntrospection.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Common/Exception.h>

#include <chrono>
#include <new>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool show_table_uuid_in_table_create_query_if_not_nil;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int SYNTAX_ERROR;
    extern const int THERE_IS_NO_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
    extern const int DEADLOCK_AVOIDED;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int CANNOT_ALLOCATE_MEMORY;
}

BlockIO InterpreterShowCreateQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


Block InterpreterShowCreateQuery::getSampleBlock()
{
    return Block{{
        ColumnString::create(),
        std::make_shared<DataTypeString>(),
        "statement"}};
}


QueryPipeline InterpreterShowCreateQuery::executeImpl()
{
    ASTPtr create_query;
    ASTQueryWithTableAndOutput * show_query = nullptr;
    if ((show_query = query_ptr->as<ASTShowCreateTableQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateViewQuery>()) ||
        (show_query = query_ptr->as<ASTShowCreateDictionaryQuery>()))
    {
        /// Only `SHOW CREATE TABLE` should resolve temporary tables for an unqualified name —
        /// `VIEW` and `DICTIONARY` cannot refer to a temporary table, so resolving to one would
        /// shadow a permanent view/dictionary with the same name and fail with `BAD_ARGUMENTS`.
        Context::StorageNamespace resolve_table_type = Context::ResolveOrdinary;
        if (show_query->isTemporary())
            resolve_table_type = Context::ResolveExternal;
        else if (query_ptr->as<ASTShowCreateTableQuery>())
            resolve_table_type = Context::ResolveAll;
        auto table_id = getContext()->resolveStorageID(*show_query, resolve_table_type);

        bool is_dictionary = static_cast<bool>(query_ptr->as<ASTShowCreateDictionaryQuery>());
        DatabasePtr udt_introspection_database;
        StoragePtr udt_introspection_table;
        TableLockHolder udt_introspection_table_lock;
        std::shared_ptr<void> udt_introspection_lease;

        /// Access is checked on the *requested* identifier, before any lookup or hinting. This is what
        /// bounds the "Maybe you meant ...?" hint: it appears only when the user's grant covers the
        /// requested name - a broad `db.*` / `*.*` grant, or the exact name itself. With an
        /// object-level `SHOW` grant scoped to one specific name, a misspelled name is not covered, so
        /// this check denies it first and no hint is produced (covered by
        /// `04611_show_create_hint_object_level_grant`).
        ///
        /// This narrower contract is deliberate. `SELECT` resolves the name during analysis, before its
        /// access check, so it does surface a hint (and, for an existing but hidden object, distinguishes
        /// it from a missing name) even under an object-level grant. `SHOW CREATE` must not behave as
        /// such an existence oracle: checking access on the requested name first means an existing but
        /// hidden object and a missing name are both reported as `ACCESS_DENIED` and stay
        /// indistinguishable to a user who is not granted on that name.
        if (is_dictionary)
            getContext()->checkAccess(AccessType::SHOW_DICTIONARIES, table_id);
        else
            getContext()->checkAccess(AccessType::SHOW_COLUMNS, table_id);

        /// `SHOW CREATE DICTIONARY` is authorized with `SHOW DICTIONARIES`, which does not imply
        /// `SHOW TABLES`/`SHOW COLUMNS`. A user with only `SHOW DICTIONARIES` must not be able to tell
        /// a hidden regular table apart from a name that does not exist - otherwise `SHOW CREATE
        /// DICTIONARY` becomes an existence oracle for tables they may not see (the "is not a
        /// DICTIONARY" error below would confirm the table exists). So, for such a user, this path is
        /// fail-closed: the create query is fetched and validated in a *single* lookup (no separate
        /// existence probe, hence no TOCTOU window in which the object could be dropped and recreated as
        /// a different kind between a probe and the fetch), and the object is shown only if that one
        /// fetch proves it to be a dictionary. Every other outcome is reported identically as a missing
        /// dictionary, carrying the same "Maybe you meant ...?" hint (which only ever suggests
        /// dictionaries to this user): a hidden regular table, a name that does not exist, and - crucially
        /// - any object/metadata failure of `getCreateTableQuery` itself. Query cancellation, its
        /// deadline, and allocation failure are control-flow/resource failures and are always rethrown.
        /// The visibility decision otherwise must not key on the failure's error code:
        /// `getCreateTableQuery` goes through `tryGetTable`, so a hidden regular table that
        /// is still starting up, has corrupted metadata, or otherwise throws would surface a distinguishing
        /// error and reopen the oracle. Unless the fetch positively proves the object is a dictionary, it
        /// is masked. The swallowed error is not lost: it resurfaces when a user who is allowed to see the
        /// object really accesses it. Users who can also observe the object as a table keep the precise
        /// diagnostics below. This mirrors `InterpreterExistsQuery`, which likewise answers a
        /// dictionary-only user from a single observation instead of a second visibility-changing lookup.
        bool remask_as_missing_dictionary = false;
        bool dictionary_only_visibility = false;
        if (is_dictionary)
        {
            const auto & access = getContext()->getAccess();
            const bool can_see_as_table
                = access->isGranted(AccessType::SHOW_TABLES, table_id.database_name, table_id.table_name)
                || access->isGranted(AccessType::SHOW_COLUMNS, table_id.database_name, table_id.table_name);
            dictionary_only_visibility = !can_see_as_table;
            if (!can_see_as_table)
            {
                try
                {
                    create_query = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getCreateTableQuery(table_id.table_name, getContext());
                }
                catch (const std::bad_alloc &)
                {
                    throw;
                }
                catch (const Exception & error)
                {
                    if (error.code() == ErrorCodes::QUERY_WAS_CANCELLED || error.code() == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT
                        || error.code() == ErrorCodes::TIMEOUT_EXCEEDED || error.code() == ErrorCodes::DEADLOCK_AVOIDED
                        || error.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || error.code() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
                        throw;
                    create_query = nullptr;
                }
                catch (...)
                {
                    /// Ok to swallow: fail closed for a dictionary-only user. Any non-control-flow failure to produce the
                    /// create query - a name that does not exist, a hidden regular table that fails to
                    /// load or start up, or metadata that cannot be read - is treated as "not a
                    /// dictionary", so all of these stay indistinguishable and the hidden table's
                    /// existence cannot be inferred from the error. The decision cannot key on the error
                    /// code: `getCreateTableQuery` calls `tryGetTable` first, which rethrows a hidden
                    /// table's own load/startup failure. The swallowed error is not lost - it resurfaces
                    /// when a user who is allowed to see the object really accesses it.
                    create_query = nullptr;
                }
                /// Show the object only if the single fetch positively proves it to be a dictionary;
                /// otherwise (a hidden table, a missing name, or any failure above) remask it.
                if (!create_query || !create_query->as<ASTCreateQuery &>().is_dictionary)
                    remask_as_missing_dictionary = true;
            }
        }

        const auto throw_missing_dictionary = [&]() -> void
        {
            auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
            TableNameHints hints(database, getContext());
            auto hint = hints.getHintForTable(table_id.table_name);
            if (hint.first.empty())
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "There is no dictionary {}.{}",
                    backQuoteIfNeed(table_id.database_name), backQuoteIfNeed(table_id.table_name));
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "There is no dictionary {}.{}. Maybe you meant {}.{}?",
                backQuoteIfNeed(table_id.database_name), backQuoteIfNeed(table_id.table_name),
                backQuoteIfNeed(hint.first), backQuoteIfNeed(hint.second));
        };

        if (remask_as_missing_dictionary)
        {
            /// Discard any create query fetched for a hidden table so its definition cannot leak.
            create_query = nullptr;
            throw_missing_dictionary();
        }

        if (!create_query && !is_dictionary)
        {
            udt_introspection_database
                = DatabaseCatalog::instance().getDatabase(table_id.database_name);
            udt_introspection_table = udt_introspection_database->tryGetTable(
                table_id.table_name, getContext());
            if (udt_introspection_table)
            {
                udt_introspection_table_lock = udt_introspection_table->lockForShare(
                    getContext()->getInitialQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
                /// Keep this unconditional for a live object: the lifecycle
                /// adapter chooses a schema-only physical lease or the full
                /// ALTER -> schema lease while closing a concurrent
                /// physical-to-mapped transition between those paths.
                udt_introspection_lease = udt_introspection_database->getUDTLifecycleAdapter().acquireTableIntrospectionLease(
                    udt_introspection_table,
                    std::chrono::milliseconds(getContext()->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()),
                    [context = getContext()]
                    {
                        if (const auto process_list_element = context->getProcessListElementSafe())
                            static_cast<void>(process_list_element->checkTimeLimit());
                    });
                /// Unsupported lifecycle adapters return no lease. Such
                /// databases cannot own durable UDT bindings and may expose a
                /// fresh transient proxy on every lookup (DatabaseRemote is
                /// one example), so pointer identity is meaningful only when
                /// an adapter actually pinned the introspection lifecycle.
                if (udt_introspection_lease
                    && udt_introspection_database->tryGetTable(table_id.table_name, getContext()) != udt_introspection_table)
                {
                    throw Exception(ErrorCodes::ABORTED, "Table changed while acquiring its user-defined type introspection lease");
                }
                /// Refetch under the share lock and schema snapshot so the
                /// live AST and outer metadata describe one object image.
                create_query = udt_introspection_database->getCreateTableQuery(
                    table_id.table_name, getContext());
            }
            else
            {
                /// DatabaseOnDisk deliberately serves persisted CREATE
                /// metadata for detached objects. Keep this AST-only fallback
                /// reachable when there is no live Storage to lock or lease.
                create_query = udt_introspection_database->getCreateTableQuery(
                    table_id.table_name, getContext());
            }
        }
        if (!create_query)
            create_query = DatabaseCatalog::instance().getDatabase(table_id.database_name)->getCreateTableQuery(table_id.table_name, getContext());

        /// A table is a `StorageAlias` exactly when its definition names the `Alias` engine, so the
        /// create query answers that without opening the storage object, which can throw on its own.
        const auto & initially_fetched_create_query = create_query->as<const ASTCreateQuery &>();
        if (!is_dictionary && initially_fetched_create_query.storage && initially_fetched_create_query.storage->engine
            && initially_fetched_create_query.storage->engine->name == "Alias")
        {
            auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
            if (const auto * alias = table ? table->as<StorageAlias>() : nullptr;
                alias && !alias->isTargetTableGranted(getContext(), AccessType::SHOW_COLUMNS, {}))
                throw Exception(ErrorCodes::ACCESS_DENIED, "Not enough privileges to show metadata exposed by {}", table_id.getNameForLogs());
        }

        const auto validate_requested_kind = [&](const ASTCreateQuery & create)
        {
            if (query_ptr->as<ASTShowCreateViewQuery>() && !create.isView())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a VIEW", backQuote(create.getDatabase()), backQuote(create.getTable()));
            if (is_dictionary && !create.is_dictionary)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "{}.{} is not a DICTIONARY", backQuote(create.getDatabase()), backQuote(create.getTable()));
        };
        validate_requested_kind(create_query->as<const ASTCreateQuery &>());

        /// Dictionary-only visibility was already decided from one positive
        /// CREATE fetch above. Acquire the mapped-metadata lease only after
        /// that proof so a hidden regular table can never become an oracle.
        /// If the object changes during acquisition, remask the race as the
        /// same missing-dictionary result for that restricted caller.
        if (is_dictionary)
        {
            const UUID initially_fetched_uuid = create_query->as<const ASTCreateQuery &>().uuid;
            const auto acquire_dictionary_introspection = [&]
            {
                udt_introspection_database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
                udt_introspection_table = udt_introspection_database->tryGetTable(table_id.table_name, getContext());
                if (!udt_introspection_table)
                    return;
                udt_introspection_table_lock = udt_introspection_table->lockForShare(
                    getContext()->getInitialQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
                auto metadata_snapshot = udt_introspection_table->IStorage::getInMemoryMetadataPtr(getContext(), true);
                if (metadata_snapshot->getBoundUDTReferences())
                {
                    udt_introspection_lease = udt_introspection_database->getUDTLifecycleAdapter().acquireTableIntrospectionLease(
                        udt_introspection_table,
                        std::chrono::milliseconds(getContext()->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds()),
                        [context = getContext()]
                        {
                            if (const auto process_list_element = context->getProcessListElementSafe())
                                static_cast<void>(process_list_element->checkTimeLimit());
                        });
                }
                if (udt_introspection_database->tryGetTable(table_id.table_name, getContext()) != udt_introspection_table)
                    throw Exception(ErrorCodes::ABORTED, "Dictionary changed while acquiring its user-defined type introspection lease");
                create_query = udt_introspection_database->getCreateTableQuery(table_id.table_name, getContext());
                validate_requested_kind(create_query->as<const ASTCreateQuery &>());
                const auto current_uuid = udt_introspection_table->getStorageID().uuid;
                const auto fetched_uuid = create_query->as<const ASTCreateQuery &>().uuid;
                if ((initially_fetched_uuid != UUIDHelpers::Nil && initially_fetched_uuid != current_uuid)
                    || (fetched_uuid != UUIDHelpers::Nil && fetched_uuid != current_uuid))
                    throw Exception(ErrorCodes::ABORTED, "Dictionary identity changed while acquiring its introspection snapshot");
            };

            if (dictionary_only_visibility)
            {
                try
                {
                    acquire_dictionary_introspection();
                }
                catch (const std::bad_alloc &)
                {
                    throw;
                }
                catch (const Exception & error)
                {
                    if (error.code() == ErrorCodes::QUERY_WAS_CANCELLED || error.code() == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT
                        || error.code() == ErrorCodes::TIMEOUT_EXCEEDED || error.code() == ErrorCodes::DEADLOCK_AVOIDED
                        || error.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED || error.code() == ErrorCodes::CANNOT_ALLOCATE_MEMORY)
                        throw;
                    create_query = nullptr;
                    throw_missing_dictionary();
                }
                catch (...)
                {
                    create_query = nullptr;
                    throw_missing_dictionary();
                }
            }
            else
                acquire_dictionary_introspection();
        }

        if (!udt_introspection_table)
        {
            const auto detached_uuid = create_query->as<const ASTCreateQuery &>().uuid;
            auto * atomic_database = udt_introspection_database
                ? typeid_cast<DatabaseAtomic *>(udt_introspection_database.get())
                : nullptr;
            if (atomic_database && detached_uuid != UUIDHelpers::Nil
                && atomic_database->hasDatabaseOwnedUDTObject(detached_uuid))
            {
                /// Persisted mapped CREATE metadata is physicalized. Without
                /// the detached storage's exact bound sidecar, returning that
                /// AST would silently erase logical identity from introspection.
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot show CREATE for detached mapped object {} without its exact live user-defined type binding",
                    table_id.getNameForLogs());
            }
        }

        auto & ast_create_query = create_query->as<ASTCreateQuery &>();

        if (udt_introspection_table && query_ptr->as<ASTShowCreateTableQuery>()
            && !ast_create_query.isView() && !ast_create_query.is_dictionary)
        {
            auto table = udt_introspection_table;
            auto metadata_snapshot = table->IStorage::getInMemoryMetadataPtr(getContext(), true);
            if (metadata_snapshot->getBoundUDTReferences())
            {
                if (!udt_introspection_database || !udt_introspection_lease)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped SHOW CREATE TABLE lacks its Atomic schema lease");
                auto declared_columns = UDT::projectCurrentDeclaredTableColumnTypes(
                    table->getStorageID(), *metadata_snapshot, *udt_introspection_database);
                UDT::applyCurrentDeclaredTableColumnTypes(
                    ast_create_query, table->getStorageID(), declared_columns);
            }
        }
        else if (udt_introspection_table && (ast_create_query.isView() || ast_create_query.is_dictionary))
        {
            auto table = udt_introspection_table;
            auto metadata_snapshot = table->IStorage::getInMemoryMetadataPtr(getContext(), true);
            if (metadata_snapshot->getBoundUDTReferences())
            {
                if (!udt_introspection_database || !udt_introspection_lease)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped SHOW CREATE stored object lacks its Atomic schema lease");
                UDT::applyCurrentDeclaredStoredObjectTypes(
                    ast_create_query, table->getStorageID(), *metadata_snapshot, *udt_introspection_database);
            }
        }
    }
    else if ((show_query = query_ptr->as<ASTShowCreateDatabaseQuery>()))
    {
        if (show_query->isTemporary())
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary databases are not possible.");
        show_query->setDatabase(getContext()->resolveDatabase(show_query->getDatabase()));
        getContext()->checkAccess(AccessType::SHOW_DATABASES, show_query->getDatabase());
        create_query = DatabaseCatalog::instance().getDatabase(show_query->getDatabase())->getCreateDatabaseQuery();
    }

    if (!create_query)
        throw Exception(ErrorCodes::THERE_IS_NO_QUERY,
                        "Unable to show the create query of {}. Maybe it was created by the system.",
                        show_query->getTable());

    if (!getContext()->getSettingsRef()[Setting::show_table_uuid_in_table_create_query_if_not_nil])
    {
        auto & create = create_query->as<ASTCreateQuery &>();
        create.uuid = UUIDHelpers::Nil;
        if (create.targets)
            create.targets->resetInnerUUIDs();
    }

    MutableColumnPtr column = ColumnString::create();
    column->insert(format(
    {
        .ctx = getContext(),
        .query = *create_query,
        .one_line = false
    }));

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(Block{{
        std::move(column),
        std::make_shared<DataTypeString>(),
        "statement"}})));
}

void registerInterpreterShowCreateQuery(InterpreterFactory & factory);
void registerInterpreterShowCreateQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowCreateQuery>(args.query, args.context);
    };

    factory.registerInterpreter("InterpreterShowCreateQuery", create_fn);
}
}
