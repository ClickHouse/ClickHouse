#include <Parsers/ASTRenameQuery.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/IStorage.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/QueryLog.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/RowPolicy.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Databases/DatabaseReplicated.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool check_table_dependencies;
    extern const SettingsBool check_referential_table_dependencies;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int ACCESS_ENTITY_ALREADY_EXISTS;
    extern const int ACCESS_STORAGE_READONLY;
}

namespace
{
    /// A single re-keying of a row policy: move the policy identified by `id` so that its
    /// `(database, table_name)` becomes (`new_database`, `new_table`), keeping its short name.
    struct RowPolicyRekey
    {
        UUID id;
        String new_database;
        String new_table; /// RowPolicyName::ANY_TABLE_MARK ("") means a database-wide policy.
    };

    /// The transient table name a policy is parked under during phase 1 of `applyRowPolicyRekeys`.
    /// Must be identical between the preflight (which checks it is free) and the apply (which uses
    /// it), so it lives in one place. It embeds the policy UUID and the index to be unique among
    /// the re-keys of a single rename.
    String tempRekeyTableName(const UUID & id, size_t index)
    {
        return ".tmp_rename_row_policy_" + toString(id) + "_" + std::to_string(index);
    }

    /// Returns the re-keyings needed so that row policies bound to (`from_db`, `from_table`)
    /// follow the table to (`to_db`, `to_table`).
    std::vector<RowPolicyRekey> collectRowPolicyRekeys(
        const AccessControl & access_control,
        const String & from_db, const String & from_table,
        const String & to_db, const String & to_table)
    {
        std::vector<RowPolicyRekey> result;
        for (const auto & id : access_control.findAll<RowPolicy>())
        {
            auto policy = access_control.tryRead<RowPolicy>(id);
            if (policy && (policy->getDatabase() == from_db) && (policy->getTableName() == from_table))
                result.emplace_back(RowPolicyRekey{id, to_db, to_table});
        }
        return result;
    }

    /// Returns the re-keyings needed when a whole database is renamed from `from_db` to `to_db`:
    /// every row policy bound to `from_db` (both database-wide `ON from_db.*` and per-table
    /// `ON from_db.tbl`) must move to `to_db`, keeping its table name.
    std::vector<RowPolicyRekey> collectRowPolicyRekeysForDatabase(
        const AccessControl & access_control, const String & from_db, const String & to_db)
    {
        std::vector<RowPolicyRekey> result;
        for (const auto & id : access_control.findAll<RowPolicy>())
        {
            auto policy = access_control.tryRead<RowPolicy>(id);
            if (policy && (policy->getDatabase() == from_db))
                result.emplace_back(RowPolicyRekey{id, to_db, policy->getTableName()});
        }
        return result;
    }

    /// True if a database-wide row policy (`ON db.*`) is defined on `db`. Such a policy filters
    /// every table in `db` via the ANY_TABLE_MARK fallback in EnabledRowPolicies::getFilter, but it
    /// is not bound to any single table name, so it cannot follow one table out of the database.
    bool hasDatabaseWideRowPolicy(const AccessControl & access_control, const String & db)
    {
        for (const auto & id : access_control.findAll<RowPolicy>())
        {
            auto policy = access_control.tryRead<RowPolicy>(id);
            if (policy && (policy->getDatabase() == db) && policy->isForDatabase())
                return true;
        }
        return false;
    }

    /// Verifies that every planned re-key can be applied, BEFORE the table/database rename is
    /// committed. The actual re-key (`applyRowPolicyRekeys`) runs after the rename, where a
    /// throw could no longer be rolled back (the metadata rename is already committed) and would
    /// leave the renamed object readable without its row filter -- i.e. reintroduce the very
    /// escape this fix closes. So we reject the rename up front when a policy cannot be moved:
    ///   - it lives in a read-only storage (e.g. loaded from users.xml),
    ///   - its destination name is already taken by a different policy that is not itself moving
    ///     out of the way (a real collision; an EXCHANGE that swaps two same-short-name policies
    ///     is not a collision because both are in `rekeys`), or
    ///   - the transient parking name used during the move is already taken by a non-moving policy
    ///     (deterministic, because the name is derived from the visible policy UUID).
    /// Throws (failing the rename with nothing changed) if any planned re-key is not applicable.
    void preflightRowPolicyRekeys(const AccessControl & access_control, const std::vector<RowPolicyRekey> & rekeys)
    {
        if (rekeys.empty())
            return;

        /// IDs that are moving (so their current name is about to be vacated and must not be
        /// treated as a collision with another moving policy's destination).
        std::unordered_set<UUID> moving_ids;
        moving_ids.reserve(rekeys.size());
        for (const auto & rekey : rekeys)
            moving_ids.insert(rekey.id);

        const auto reject_if_taken =
            [&](const RowPolicyName & name, const UUID & moving_id, const RowPolicyPtr & moving_policy, const char * what)
        {
            if (auto existing_id = access_control.find<RowPolicy>(name.toString());
                existing_id && (*existing_id != moving_id) && !moving_ids.contains(*existing_id))
            {
                throw Exception(
                    ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS,
                    "Cannot rename because {} would have to follow the table, "
                    "but row policy {} already exists at the {}",
                    moving_policy->formatTypeWithName(),
                    backQuoteIfNeed(name.toString()),
                    what);
            }
        };

        for (size_t i = 0; i < rekeys.size(); ++i)
        {
            const auto & rekey = rekeys[i];
            auto policy = access_control.tryRead<RowPolicy>(rekey.id);
            if (!policy)
                continue;

            /// (1) Read-only storage: AccessControl::update would throw after the commit point.
            if (access_control.isReadOnly(rekey.id))
                throw Exception(
                    ErrorCodes::ACCESS_STORAGE_READONLY,
                    "Cannot rename because {} is stored in a read-only access storage "
                    "and cannot follow the table to its new name",
                    policy->formatTypeWithName());

            /// (2) Transient parking name (phase 1 of the apply) is taken by a non-moving policy.
            RowPolicyName parking_name;
            parking_name.short_name = policy->getShortName();
            parking_name.database = policy->getDatabase();
            parking_name.table_name = tempRekeyTableName(rekey.id, i);
            reject_if_taken(parking_name, rekey.id, policy, "transient name used while renaming");

            /// (3) Final destination name is taken by a policy that is NOT itself moving.
            RowPolicyName dst_name;
            dst_name.short_name = policy->getShortName();
            dst_name.database = rekey.new_database;
            dst_name.table_name = rekey.new_table;
            reject_if_taken(dst_name, rekey.id, policy, "destination");
        }
    }

    /// Applies a set of row-policy re-keyings collision-free by routing every affected policy
    /// through a unique temporary table name first, then to its final destination. The two-phase
    /// move is needed for EXCHANGE TABLES, where two policies with the same short name would
    /// otherwise transiently collide while being swapped between the two table names.
    /// `preflightRowPolicyRekeys` must have proven every step applicable beforehand; the rollback
    /// here covers only residual errors.
    void applyRowPolicyRekeys(AccessControl & access_control, const std::vector<RowPolicyRekey> & rekeys)
    {
        if (rekeys.empty())
            return;

        /// Remember the original binding of each policy so we can restore it if something throws.
        std::vector<std::pair<UUID, RowPolicyName>> original_names;
        original_names.reserve(rekeys.size());
        for (const auto & rekey : rekeys)
        {
            auto policy = access_control.tryRead<RowPolicy>(rekey.id);
            if (policy)
                original_names.emplace_back(rekey.id, policy->getFullName());
        }

        const auto restore = [&]
        {
            for (const auto & [id, name] : original_names)
            {
                try
                {
                    access_control.tryUpdate(id, [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
                    {
                        auto updated = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
                        updated->setFullName(name);
                        return updated;
                    });
                }
                catch (...)
                {
                    /// Best-effort rollback; keep restoring the rest.
                    tryLogCurrentException(getLogger("InterpreterRenameQuery"), "Failed to restore row policy binding during rename rollback");
                }
            }
        };

        try
        {
            /// Phase 1: park every policy under a unique temporary table name (preflighted as free)
            /// to avoid transient name collisions during the swap.
            for (size_t i = 0; i < rekeys.size(); ++i)
            {
                const String tmp_table = tempRekeyTableName(rekeys[i].id, i);
                access_control.update(rekeys[i].id, [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
                {
                    auto updated = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
                    updated->setTableName(tmp_table);
                    return updated;
                });
            }

            /// Phase 2: move every policy from its temporary name to the final destination.
            for (const auto & rekey : rekeys)
            {
                access_control.update(rekey.id, [&](const AccessEntityPtr & entity, const UUID &) -> AccessEntityPtr
                {
                    auto updated = typeid_cast<std::shared_ptr<RowPolicy>>(entity->clone());
                    updated->setDatabase(rekey.new_database);
                    updated->setTableName(rekey.new_table);
                    return updated;
                });
            }
        }
        catch (...)
        {
            restore();
            throw;
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

    /// `getContext()` is const, but updating row policies (a process-global access entity)
    /// requires a mutable AccessControl. A copied context shares the same AccessControl
    /// singleton, so updates through it persist and replicate as usual.
    auto mutable_context = Context::createCopy(getContext());
    auto & access_control = mutable_context->getAccessControl();

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

        /// Row policies are keyed by (database, table). They must follow the table on rename,
        /// otherwise after the rename the policy stays orphaned on the old name and the table
        /// becomes readable with no filtering under its new name (a row-policy escape).
        std::vector<RowPolicyRekey> row_policy_rekeys;

        /// CREATE OR REPLACE TABLE / REPLACE TABLE reach this code through a synthetic rename/exchange
        /// that swaps a freshly built table (temporary name) into the target name. Only the target's
        /// storage is replaced; the target keeps its name, so the row policies bound to that name must
        /// stay there (they then filter the replacement data). Re-keying would move the target's policy
        /// onto the transient name, which is dropped immediately after, leaving the replaced table
        /// unfiltered -- the very row-policy escape this fix closes. So skip the re-key for this case
        /// entirely; the target's existing policies already protect the new storage.
        if (!rename.create_or_replace)
        {
            /// A database-wide policy (`ON db.*`) is not bound to any single table name, so it cannot
            /// follow a table that moves to a different database: the destination lookup is `new_db.tbl`
            /// then `new_db.*`, which never sees the old `db.*`, so the moved data would be readable
            /// unfiltered (or under an unrelated destination `db.*`). Reject such cross-database moves
            /// rather than silently dropping the filter. A same-database rename is unaffected: the
            /// `db.*` policy keeps covering the table through the ANY_TABLE_MARK fallback.
            if (elem.from_database_name != elem.to_database_name)
            {
                if (hasDatabaseWideRowPolicy(access_control, elem.from_database_name))
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot move table {} to another database {} because a database-wide row policy "
                        "(ON {}.*) applies to it and cannot follow it across databases",
                        backQuoteIfNeed(elem.from_database_name) + "." + backQuoteIfNeed(elem.from_table_name),
                        backQuoteIfNeed(elem.to_database_name),
                        backQuoteIfNeed(elem.from_database_name));
                /// EXCHANGE swaps data both ways, so the destination's `db.*` would likewise fail to
                /// follow the table arriving from the other database.
                if (exchange_tables && hasDatabaseWideRowPolicy(access_control, elem.to_database_name))
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot exchange table {} with {} because a database-wide row policy (ON {}.*) "
                        "applies to it and cannot follow it across databases",
                        backQuoteIfNeed(elem.to_database_name) + "." + backQuoteIfNeed(elem.to_table_name),
                        backQuoteIfNeed(elem.from_database_name) + "." + backQuoteIfNeed(elem.from_table_name),
                        backQuoteIfNeed(elem.to_database_name));
            }

            /// Collect and PREFLIGHT the per-table re-keys here, before the first mutation below
            /// (`removeDependencies` / `renameTable`): if a policy cannot be moved, the rename is
            /// rejected now with nothing changed. The actual re-key runs after the rename commits,
            /// where a throw could not be rolled back and would leave the table unfiltered.
            row_policy_rekeys = collectRowPolicyRekeys(
                access_control, elem.from_database_name, elem.from_table_name, elem.to_database_name, elem.to_table_name);
            if (exchange_tables)
            {
                auto to_rekeys = collectRowPolicyRekeys(
                    access_control, elem.to_database_name, elem.to_table_name, elem.from_database_name, elem.from_table_name);
                row_policy_rekeys.insert(row_policy_rekeys.end(), to_rekeys.begin(), to_rekeys.end());
            }
            preflightRowPolicyRekeys(access_control, row_policy_rekeys);
        }

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
            database->checkTableNameLength(to_table_id.table_name);

            DatabaseCatalog::instance().checkTableCanBeRenamedWithNoCyclicDependencies(from_table_id, to_table_id);
            bool check_ref_deps = getContext()->getSettingsRef()[Setting::check_referential_table_dependencies];
            bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef()[Setting::check_table_dependencies];
            std::tie(from_ref_dependencies, from_loading_dependencies, from_mv_dependencies) = database_catalog.removeDependencies(from_table_id, check_ref_deps, check_loading_deps, false, /*is_mv*/ true);
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
                /// across databases, see `01155_rename_move_materialized_view`).
                DatabaseCatalog::instance().addSourceViewDependencies(to_table_id, from_dependent_views);
            }

            NamedCollectionFactory::instance().renameDependencies(from_table_id, to_table_id);
            if (exchange_tables)
                NamedCollectionFactory::instance().renameDependencies(to_table_id, from_table_id);

            /// Re-key row policies last. Preflight above already rejected the unrecoverable cases
            /// (read-only storage, destination or transient-name collision); this still has its own
            /// rollback for any residual error, and nothing after it can throw.
            applyRowPolicyRekeys(access_control, row_policy_rekeys);
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
        catalog.assertDatabaseDoesntExist(new_name);

        /// See executeToTables: a copied context shares the same AccessControl singleton.
        auto mutable_context = Context::createCopy(getContext());
        auto & access_control = mutable_context->getAccessControl();

        /// Row policies bound to the database (both `ON db.*` and `ON db.tbl`) must follow it,
        /// otherwise they are orphaned on the old database name (a row-policy escape).
        /// Preflight before the rename commits (see executeToTables): reject the rename now if a
        /// policy cannot be moved, instead of committing the rename and then failing the re-key.
        auto row_policy_rekeys = collectRowPolicyRekeysForDatabase(access_control, old_name, new_name);
        preflightRowPolicyRekeys(access_control, row_policy_rekeys);

        db->renameDatabase(getContext(), new_name);

        applyRowPolicyRekeys(access_control, row_policy_rekeys);
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
