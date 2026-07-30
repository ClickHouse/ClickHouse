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
    ///
    /// One case is declined instead of rejected: on a server with a replicated access storage the
    /// whole plan is dropped with a warning, because a global re-key cannot be correct for servers
    /// that are not renaming (see the comment on that branch). `rekeys` is therefore mutable, and
    /// the caller passes the same vector to `applyRowPolicyRekeys`, so the apply skips exactly what
    /// was dropped and the two cannot disagree.
    void preflightRowPolicyRekeys(const AccessControl & access_control, std::vector<RowPolicyRekey> & rekeys)
    {
        if (rekeys.empty())
            return;

        /// (1) Read-only storage: `AccessControl::update` would throw after the commit point. This
        /// stays first and applies unconditionally -- a policy that is both read-only and shared is
        /// still a genuine cannot-move on this node, so the rename must fail rather than silently
        /// skip below.
        for (const auto & rekey : rekeys)
        {
            if (auto policy = access_control.tryRead<RowPolicy>(rekey.id); policy && access_control.isReadOnly(rekey.id))
                throw Exception(
                    ErrorCodes::ACCESS_STORAGE_READONLY,
                    "Cannot rename because {} is stored in a read-only access storage "
                    "and cannot follow the table to its new name",
                    policy->formatTypeWithName());
        }

        /// (2) A replicated access storage is shared between servers through its ZooKeeper path, and
        /// a re-key there is published globally in one transaction, while a table rename applies
        /// only to the server that runs it. Moving the policy would unbind it from the name every
        /// other server still uses, leaving the table unfiltered there -- worse than not moving it.
        /// Whether those servers rename too is not knowable here: the storage's identity is just its
        /// path, it keeps no registry of the servers mounting it, and the set of servers sharing a
        /// rename (a Replicated database, an ON CLUSTER query) is a different set entirely.
        ///
        /// The condition is on the server's CONFIGURATION, not on the affected policies: no locally
        /// computed predicate can bound a re-key that is published globally in one transaction. A
        /// replicated storage also answers reads from its own copy of the entities, refreshed from
        /// Keeper, so a policy written on another server and not yet observed here is invisible to
        /// `collectRowPolicyRekeys` and an entity-level test would silently find nothing to protect.
        ///
        /// It drops ALL re-keys, not just the shared ones: leaving a shared policy on `a` while
        /// still moving a node-local policy from `b` to `a` would leave `b` unfiltered, and the
        /// collision check cannot catch that because the two have different short names. Clearing
        /// the whole plan leaves exactly the bindings this operation would have had without the
        /// re-key, so no name ends up less filtered than it is without this feature.
        if (access_control.containsStorage(ReplicatedAccessStorage::STORAGE_TYPE))
        {
            LOG_WARNING(
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

        /// IDs that are moving (so their current name is about to be vacated and must not be
        /// treated as a collision with another moving policy's destination).
        std::unordered_set<UUID> moving_ids;
        moving_ids.reserve(rekeys.size());
        for (const auto & rekey : rekeys)
            moving_ids.insert(rekey.id);

        /// `allow_moving_occupant` must be true only for a policy's final destination: an EXCHANGE
        /// legitimately swaps two same-short-name policies, so each one's destination is the other's
        /// current name and both are in `rekeys`. For a transient parking name it must be false --
        /// a later-moving policy sitting on an earlier one's parking name is a real collision that
        /// phase 1 of the apply would hit AFTER the table rename has committed.
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

            /// (3) Transient parking name (phase 1 of the apply) is taken by a non-moving policy.
            RowPolicyName parking_name;
            parking_name.short_name = policy->getShortName();
            parking_name.database = policy->getDatabase();
            parking_name.table_name = tempRekeyTableName(rekey.id, i);
            reject_if_taken(parking_name, rekey.id, policy, /*allow_moving_occupant*/ false, "transient name used while renaming");

            /// (4) Final destination name is taken by a policy that is NOT itself moving.
            RowPolicyName dst_name;
            dst_name.short_name = policy->getShortName();
            dst_name.database = rekey.new_database;
            dst_name.table_name = rekey.new_table;
            reject_if_taken(dst_name, rekey.id, policy, /*allow_moving_occupant*/ true, "destination");
        }
    }

    /// True if this rename replaces the storage behind a name while keeping that name, so the row
    /// policies bound to it must stay there.
    ///
    /// The AST flag covers every same-process site, but a rename that goes through a Replicated
    /// database's DDL queue is executed from the entry's SQL text, and the flag is deliberately not
    /// part of that text, so the re-parsed AST always has it false. Only one of the flag's sites can
    /// cross that boundary: a non-append refreshable materialized view swapping in its fresh target.
    /// It is recognized from the parent table UUID, which does survive as a serialized entry field
    /// and is re-published onto the query context by DatabaseReplicatedTask::makeQueryContext.
    ///
    /// All three conditions are required. Without `is_replicated_database_internal` the predicate
    /// would also hold on the purely local path (RefreshTask sets the parent UUID for every
    /// non-append refresh), which would make the AST flag dead there. Without the name equality it
    /// would hold for any rename issued during a refresh, including a user RENAME.
    bool keepsNameOfReplacedStorage(const ASTRenameQuery & rename, const ContextPtr & context, const RenameDescription & elem)
    {
        if (rename.replaces_storage_keeping_name)
            return true;

        if (!context->getClientInfo().is_replicated_database_internal)
            return false;

        auto parent_table_uuid = context->getParentTable();
        if (!parent_table_uuid.has_value())
            return false;

        /// Match by UUID-derived equality rather than by a `.tmp.inner_id.` prefix: a user can create
        /// a table with such a name, and a prefix would also match another view's temp table. The
        /// non-UUID `.tmp.inner.<name>` spelling only occurs in an Ordinary database, which is never
        /// Replicated, so the AST flag already covers it.
        return elem.from_table_name == StorageMaterializedView::generateRefreshTempTableName(*parent_table_uuid);
    }

    /// Everything about a rename's row policies that must be decided BEFORE the rename commits:
    /// reject the moves that cannot be applied, then return the plan for the ones that can.
    /// Returns an empty plan when this rename keeps the name (so no policy moves) or when the
    /// preflight declined the whole plan.
    ///
    /// In a Replicated database this runs twice for one user query, and the two runs have different
    /// jobs. On the initiator it is a preflight only -- it rejects an inapplicable rename before the
    /// DDL entry is enqueued, while nothing is committed yet, and its return value is discarded.
    /// Every replica then re-executes the entry against its own node-local access state and applies
    /// the plan it computes there, which is what makes the per-replica re-key work. See the two call
    /// sites in executeToTables.
    std::vector<RowPolicyRekey> collectAndPreflightRowPolicyRekeys(
        const AccessControl & access_control,
        const ASTRenameQuery & rename,
        const ContextPtr & context,
        const RenameDescription & elem,
        bool exchange_tables)
    {
        /// Row policies are keyed by (database, table). They must follow the table on rename,
        /// otherwise after the rename the policy stays orphaned on the old name and the table
        /// becomes readable with no filtering under its new name (a row-policy escape).
        std::vector<RowPolicyRekey> rekeys;

        /// A storage-replacing swap (CREATE OR REPLACE / REPLACE TABLE, a non-append refreshable view
        /// installing its fresh target, system log schema rotation) keeps the surviving name, so the
        /// row policies bound to that name must stay there and filter the replacement data. Re-keying
        /// would move them onto the transient side of the swap, which is dropped right after, leaving
        /// the surviving name unfiltered -- the very escape this fix closes. See the flag's doc in
        /// ASTRenameQuery.h for the full list of such sites.
        ///
        /// The startup conversion of an Ordinary database to Atomic is a second kind of
        /// name-preserving move: it relocates every table into a temporary database and then renames
        /// that database back, so each moved table ends up under its original (database, table). Its
        /// outer moves keep the table name, and for those the re-key must be skipped too - the policy
        /// is already on the name the table will have when the conversion finishes. The nested renames
        /// of materialized-view and time-series inner tables are different: those names genuinely
        /// change (`.inner.<name>` -> `.inner_id.<uuid>`), so their policies must follow as usual.
        /// In both cases the cross-database `db.*` rejection below does not apply, because the
        /// destination database is the staging name that is renamed back to the original one.
        const bool converting_database_engine = context->isConvertingDatabaseEngine();
        const bool conversion_keeps_table_name = converting_database_engine && elem.from_table_name == elem.to_table_name;

        /// `EXCHANGE TABLES t AND t` is a documented no-op that succeeds (DatabaseAtomic::renameTable
        /// returns early for it, and 01109_exchange_tables pins that). Such an element moves no
        /// binding, so there is nothing to re-key and nothing that can escape - while evaluating the
        /// transition for it could only invent a failure, because the same policy is collected on both
        /// sides of the swap. This overlaps `conversion_keeps_table_name` only for a same-database
        /// element: the conversion's outer moves are cross-database (staging db -> original db) with
        /// the table name equal, so both conditions are needed.
        const bool same_name = elem.from_database_name == elem.to_database_name && elem.from_table_name == elem.to_table_name;

        if (keepsNameOfReplacedStorage(rename, context, elem) || conversion_keeps_table_name || same_name)
            return rekeys;

        /// A database-wide policy (`ON db.*`) is not bound to any single table name, so it cannot
        /// follow a table that moves to a different database: the destination lookup is `new_db.tbl`
        /// then `new_db.*`, which never sees the old `db.*`, so the moved data would be readable
        /// unfiltered (or under an unrelated destination `db.*`). Reject such cross-database moves
        /// rather than silently dropping the filter. A same-database rename is unaffected: the
        /// `db.*` policy keeps covering the table through the ANY_TABLE_MARK fallback.
        if (elem.from_database_name != elem.to_database_name && !converting_database_engine)
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

        /// Collect and PREFLIGHT the per-table re-keys here, before the first mutation of the rename
        /// (`removeDependencies` / `renameTable`, or the DDL enqueue on a Replicated initiator): if a
        /// policy cannot be moved, the rename is rejected now with nothing changed. The actual re-key
        /// runs after the rename commits, where a throw could not be rolled back and would leave the
        /// table unfiltered.
        rekeys = collectRowPolicyRekeys(
            access_control, elem.from_database_name, elem.from_table_name, elem.to_database_name, elem.to_table_name);
        if (exchange_tables)
        {
            auto to_rekeys = collectRowPolicyRekeys(
                access_control, elem.to_database_name, elem.to_table_name, elem.from_database_name, elem.from_table_name);
            rekeys.insert(rekeys.end(), to_rekeys.begin(), to_rekeys.end());
        }
        preflightRowPolicyRekeys(access_control, rekeys);
        return rekeys;
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

        /// Run the caller's pre-swap check while still holding `ddl_guards`. If it
        /// throws, the guards release via RAII, no rename happens, and the caller's
        /// catch path runs. Skip when the destination doesn't exist — there is no
        /// storage to check (this is a plain `RENAME TO new_name`, not an exchange).
        if (pre_swap_check && exchange_tables)
            pre_swap_check(StorageID(elem.to_database_name, elem.to_table_name));

        DatabasePtr database = database_catalog.getDatabase(elem.from_database_name);

        /// Preflight the row-policy transition before the Replicated branch below enqueues the DDL
        /// entry, so that a rename the initiator itself can see is inapplicable is rejected before
        /// any entry is written to Keeper: the error is reported directly, rather than raised later
        /// from inside the replayed entry.
        ///
        /// Commit-safety on this path does not depend on this hoist. Below the branch the preflight
        /// would still precede the first mutation of the rename (`removeDependencies` /
        /// `renameTable`), so an initiator-visible rejection already left nothing committed either
        /// way; the hoist only avoids writing a transient doomed entry.
        ///
        /// The initiator can only validate what it can see. Row policies in a node-local storage
        /// are not replicated by the DDL queue, so replicas can hold different ones, and a replica
        /// whose own state blocks the move applies the rename anyway and leaves its policy on the
        /// old name: that name keeps filtering there, while the new name on that replica is filtered
        /// by whatever policy already sat on it. The replicas end up filtering differently.
        /// Preflighting here bounds that divergence to the peer-only case, it does not remove it.
        /// In-tree precedent for initiator-side pre-enqueue validation:
        /// `DatabaseReplicated::checkQueryValid`.
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
