#include <Parsers/ASTRenameQuery.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMaterializedView.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/QueryLog.h>
#include <Access/AccessControl.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/ReplicatedAccessStorage.h>
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
    /// Moves the policy `id` to (`new_database`, `new_table`), keeping its short name.
    struct RowPolicyRekey
    {
        UUID id;
        String new_database;
        String new_table; /// `RowPolicyName::ANY_TABLE_MARK` ("") means a database-wide policy.
    };

    /// Transient name a policy is parked under while its binding is moved. Unique per (policy,
    /// position), so two re-keys of the same rename cannot collide on it.
    String tempRekeyTableName(const UUID & id, size_t index)
    {
        return ".tmp_rename_row_policy_" + toString(id) + "_" + std::to_string(index);
    }

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

    /// A database rename moves both the database-wide `ON db.*` policies and the per-table
    /// `ON db.tbl` ones, so the match is on the database alone and the table name is preserved.
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

    /// Verifies every planned re-key is applicable before the rename commits, so a policy that
    /// cannot follow its table rejects the rename instead of failing after the commit. `rekeys` is
    /// pruned in place, so a later apply skips exactly what was dropped. `may_refuse` false means the
    /// caller cannot surface a rejection to a user, so such a case declines the plan instead.
    void preflightRowPolicyRekeys(
        const AccessControl & access_control, std::vector<RowPolicyRekey> & rekeys, bool log_declined = true,
        bool may_refuse = true)
    {
        if (rekeys.empty())
            return;

        /// (1) Read-only storage: `AccessControl::update` cannot move the policy at all. Checked
        /// before the decline below, so a read-only policy in a shared storage still fails the rename.
        for (const auto & rekey : rekeys)
        {
            auto policy = access_control.tryRead<RowPolicy>(rekey.id);
            if (!policy || !access_control.isReadOnly(rekey.id))
                continue;

            if (!may_refuse)
            {
                if (log_declined)
                    LOG_INFO(
                        getLogger("InterpreterRenameQuery"),
                        "Not moving {} to follow this rename: it is stored in a read-only access storage. "
                        "It keeps its current name, so the renamed object is no longer covered by it. "
                        "Recreate the policy on the new name in a writable storage.",
                        policy->formatTypeWithName());
                rekeys.clear();
                return;
            }

            throw Exception(
                ErrorCodes::ACCESS_STORAGE_READONLY,
                "Cannot rename because {} is stored in a read-only access storage "
                "and cannot follow the renamed object to its new name",
                policy->formatTypeWithName());
        }

        /// (2) A replicated access storage is shared with servers this rename does not apply to, so a
        /// re-key there would be published globally. Drops the whole plan: a partial one can leave a
        /// name unfiltered that the rename would otherwise have kept covered.
        if (access_control.containsStorage(ReplicatedAccessStorage::STORAGE_TYPE))
        {
            if (log_declined)
                LOG_INFO(
                    getLogger("InterpreterRenameQuery"),
                    "Not moving {} row polic{} to follow this rename: this server has a replicated access storage "
                    "configured, and such a storage is shared with servers that this rename does not apply to. "
                    "The policies keep their current names; recreate them on the new name if the rename is meant "
                    "to be visible on every server sharing the storage.",
                    rekeys.size(),
                    rekeys.size() == 1 ? "y" : "ies");
            rekeys.clear();
            return;
        }

        /// (3) Two re-keys of this plan sharing one destination name. Each is applicable alone, so
        /// only a check on the plan as a whole catches the pair.
        std::unordered_map<String, RowPolicyPtr> destinations;
        destinations.reserve(rekeys.size());
        for (const auto & rekey : rekeys)
        {
            auto policy = access_control.tryRead<RowPolicy>(rekey.id);
            if (!policy)
                continue;

            RowPolicyName dst_name;
            dst_name.short_name = policy->getShortName();
            dst_name.database = rekey.new_database;
            dst_name.table_name = rekey.new_table;

            auto [it, inserted] = destinations.emplace(dst_name.toString(), policy);
            if (!inserted)
                throw Exception(
                    ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS,
                    "Cannot rename because {} and {} would both have to occupy the name {} "
                    "after the rename",
                    it->second->formatTypeWithName(),
                    policy->formatTypeWithName(),
                    backQuoteIfNeed(dst_name.toString()));
        }

        /// Policies that are moving, so their current name is about to be vacated.
        std::unordered_set<UUID> moving_ids;
        moving_ids.reserve(rekeys.size());
        for (const auto & rekey : rekeys)
            moving_ids.insert(rekey.id);

        /// `allow_moving_occupant` is true only for a final destination: an `EXCHANGE` swaps two
        /// same-short-name policies, so each one's destination is the other's current name. A parking
        /// name has no such excuse, so an occupant there is a real collision.
        const auto reject_if_taken =
            [&](const RowPolicyName & name, const UUID & moving_id, const RowPolicyPtr & moving_policy,
                bool allow_moving_occupant, const char * what)
        {
            if (auto existing_id = access_control.find<RowPolicy>(name.toString());
                existing_id && (*existing_id != moving_id)
                && !(allow_moving_occupant && moving_ids.contains(*existing_id)))
            {
                throw Exception(
                    ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS,
                    "Cannot rename because {} would have to follow the renamed object, "
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

            /// (4) Transient parking name (phase 1 of the apply) is taken by a non-moving policy.
            RowPolicyName parking_name;
            parking_name.short_name = policy->getShortName();
            parking_name.database = policy->getDatabase();
            parking_name.table_name = tempRekeyTableName(rekey.id, i);
            reject_if_taken(parking_name, rekey.id, policy, /*allow_moving_occupant*/ false, "transient name used while renaming");

            /// (5) Final destination name is taken by a policy that is NOT itself moving.
            RowPolicyName dst_name;
            dst_name.short_name = policy->getShortName();
            dst_name.database = rekey.new_database;
            dst_name.table_name = rekey.new_table;
            reject_if_taken(dst_name, rekey.id, policy, /*allow_moving_occupant*/ true, "destination");
        }
    }

    /// A database-wide policy (`ON db.*`) is bound to no table name, so it cannot follow an object to
    /// another database: the destination lookup is `new_db.tbl` then `new_db.*` and never sees the old
    /// `db.*`. A same-database rename is unaffected, the `ANY_TABLE_MARK` fallback still covers it.
    void rejectCrossDatabaseMoveUnderDatabaseWidePolicy(
        const AccessControl & access_control,
        const String & from_db, const String & from_name, const String & to_db)
    {
        if (from_db == to_db || !hasDatabaseWideRowPolicy(access_control, from_db))
            return;

        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot move {} to another database {} because a database-wide row policy (ON {}.*) "
            "applies to it and cannot follow it across databases",
            backQuoteIfNeed(from_db) + "." + backQuoteIfNeed(from_name),
            backQuoteIfNeed(to_db),
            backQuoteIfNeed(from_db));
    }

    /// True if this rename replaces the storage behind a name while keeping that name, so the
    /// policies bound to it stay there. The AST flag does not survive a `Replicated` database's DDL
    /// queue (see `ASTRenameQuery::replaces_storage_keeping_name`), so that site is matched by UUID.
    bool keepsNameOfReplacedStorage(const ASTRenameQuery & rename, const ContextPtr & context, const RenameDescription & elem)
    {
        if (rename.replaces_storage_keeping_name)
            return true;

        if (!context->getClientInfo().is_replicated_database_internal)
            return false;

        auto parent_table_uuid = context->getParentTable();
        if (!parent_table_uuid.has_value())
            return false;

        /// Exact match, not a prefix: a `.tmp.inner_id.` name is user-creatable.
        return elem.from_table_name == StorageMaterializedView::generateRefreshTempTableName(*parent_table_uuid);
    }

    /// Decides everything about a rename's row policies before it commits: rejects the moves that
    /// cannot be applied and returns the plan for the rest. Empty when no policy moves.
    std::vector<RowPolicyRekey> collectAndPreflightRowPolicyRekeys(
        const AccessControl & access_control,
        const ASTRenameQuery & rename,
        const ContextPtr & context,
        const RenameDescription & elem,
        bool exchange_tables)
    {
        /// Row policies are keyed by (database, table), so one must follow its table on rename.
        /// Otherwise it stays orphaned on the old name and the table is unfiltered under the new one.
        std::vector<RowPolicyRekey> rekeys;

        /// The `Ordinary` to `Atomic` conversion is name-preserving for its outer moves. Its nested
        /// inner-table renames do change the name (`.inner.<name>` to `.inner_id.<uuid>`) and re-key.
        const bool converting_database_engine = context->isConvertingDatabaseEngine();
        const bool conversion_keeps_table_name = converting_database_engine && elem.from_table_name == elem.to_table_name;

        /// `EXCHANGE TABLES t AND t` is a no-op that succeeds, and the same policy is collected on
        /// both sides of it, so evaluating a transition for such an element could only invent a failure.
        const bool same_name = elem.from_database_name == elem.to_database_name && elem.from_table_name == elem.to_table_name;

        if (keepsNameOfReplacedStorage(rename, context, elem) || conversion_keeps_table_name || same_name)
            return rekeys;

        if (!converting_database_engine)
        {
            rejectCrossDatabaseMoveUnderDatabaseWidePolicy(
                access_control, elem.from_database_name, elem.from_table_name, elem.to_database_name);
            /// An `EXCHANGE` swaps data both ways, so the destination's `db.*` would likewise fail
            /// to follow the object arriving from the other database.
            if (exchange_tables)
                rejectCrossDatabaseMoveUnderDatabaseWidePolicy(
                    access_control, elem.to_database_name, elem.to_table_name, elem.from_database_name);
        }

        rekeys = collectRowPolicyRekeys(
            access_control, elem.from_database_name, elem.from_table_name, elem.to_database_name, elem.to_table_name);
        if (exchange_tables)
        {
            auto to_rekeys = collectRowPolicyRekeys(
                access_control, elem.to_database_name, elem.to_table_name, elem.from_database_name, elem.from_table_name);
            rekeys.insert(rekeys.end(), to_rekeys.begin(), to_rekeys.end());
        }
        /// The conversion runs at startup, where a rejection aborts the server instead of reaching a user.
        preflightRowPolicyRekeys(
            access_control, rekeys, /*log_declined*/ true, /*may_refuse*/ !converting_database_engine);
        return rekeys;
    }

    /// Applies re-keyings through a unique parking name first, then to the final destination: an
    /// `EXCHANGE` swaps two same-short-name policies, which would otherwise collide mid-move.
    /// `preflightRowPolicyRekeys` must have proven each step applicable; the rollback covers residuals.
    void applyRowPolicyRekeys(AccessControl & access_control, const std::vector<RowPolicyRekey> & rekeys)
    {
        if (rekeys.empty())
            return;

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
            /// Phase 1: park every policy under its transient name, preflighted as free.
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

            /// Phase 2: move every policy to its final destination.
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

void preflightRowPolicyRekeysForRenames(const ContextPtr & context, const std::vector<std::pair<StorageID, StorageID>> & renames)
{
    const auto & access_control = context->getAccessControl();

    /// The conversion's staging database is renamed back to the original name.
    const bool converting_database_engine = context->isConvertingDatabaseEngine();

    for (const auto & [from, to] : renames)
    {
        if (!converting_database_engine)
            rejectCrossDatabaseMoveUnderDatabaseWidePolicy(
                access_control, from.database_name, from.table_name, to.database_name);

        /// One vector per pair, as the nested rename will build it: parking names come from a
        /// vector's own indices, so a combined vector would probe names nothing will use.
        auto rekeys = collectRowPolicyRekeys(
            access_control, from.database_name, from.table_name, to.database_name, to.table_name);
        preflightRowPolicyRekeys(
            access_control, rekeys, /*log_declined*/ false, /*may_refuse*/ !converting_database_engine);
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

    /// `getContext` is const, but a row policy is a process-global access entity and updating one
    /// needs a mutable `AccessControl`. A copied context shares the same singleton.
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

        /// Run the caller's pre-swap check while still holding `ddl_guards`. If it
        /// throws, the guards release via RAII, no rename happens, and the caller's
        /// catch path runs. Skip when the destination doesn't exist — there is no
        /// storage to check (this is a plain `RENAME TO new_name`, not an exchange).
        if (pre_swap_check && exchange_tables)
            pre_swap_check(StorageID(elem.to_database_name, elem.to_table_name));

        DatabasePtr database = database_catalog.getDatabase(elem.from_database_name);

        /// Must run above the `Replicated` branch below, so a rejection reaches the initiator before
        /// the DDL entry is in Keeper. A replica whose own state blocks the move still rejects the
        /// entry locally, so this bounds the divergence to that peer rather than removing it.
        std::vector<RowPolicyRekey> row_policy_rekeys = collectAndPreflightRowPolicyRekeys(
            access_control, rename, getContext(), elem, exchange_tables);

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

            /// Last, so nothing after it can throw. The preflight above already rejected the
            /// unrecoverable cases and the rollback inside covers residual errors.
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

        /// See `executeToTables`: a copied context shares the same `AccessControl` singleton.
        auto mutable_context = Context::createCopy(getContext());
        auto & access_control = mutable_context->getAccessControl();

        /// Row policies bound to the database, both `ON db.*` and `ON db.tbl`, must follow it or they
        /// are orphaned on the old database name.
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
