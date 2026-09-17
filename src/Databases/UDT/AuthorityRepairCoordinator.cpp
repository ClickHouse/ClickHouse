#include <Databases/UDT/AuthorityRepairCoordinator.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>

#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Common/Stopwatch.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using RepairError = AuthorityRepairCoordinatorError;

[[noreturn]] void fail(RepairError::Code code, std::string_view message)
{
    throw RepairError(code, message);
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(RepairError::Code::ReverificationLimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(RepairError::Code::ReverificationLimitExceeded, message);
    return lhs * rhs;
}

AuthorityRootGraphIdentity identifyRoot(const AuthorityRoot & root)
{
    const auto & state = root.getAuthorityState();
    const auto inventory = root.pinAuthorityInventory();
    const auto graph = root.pinSchemaObjectDependencyGraph();
    if (!inventory || !graph || state.database_uuid == UUIDHelpers::Nil || state.database_catalog_epoch == 0
        || root.getDatabaseUUID() != state.database_uuid || inventory->getSummary() != root.getInventorySummary()
        || inventory->getSummary().leaf_count != state.leaf_count || inventory->getSummary().merkle_radix_root != state.inventory_root
        || graph->getDatabaseUUID() != state.database_uuid || graph->computeRoot() != state.schema_graph_root)
    {
        fail(RepairError::Code::InvalidRoot, "authority exact repair received an inconsistent immutable root");
    }
    return {
        .authority_root = {
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
        },
        .schema_graph_root = state.schema_graph_root,
    };
}

void validateLimits(const AuthorityExactRepairLimits & limits)
{
    constexpr AuthorityVerificationScheduleLimits schedule_maxima;
    if (!limits.reverification_continuation || limits.maximum_reverification_batches == 0
        || limits.maximum_reverification_batches > schedule_maxima.maximum_snapshot_targets
        || limits.maximum_reverification_work_items_per_pass == 0
        || limits.maximum_reverification_work_items_per_pass > schedule_maxima.maximum_snapshot_targets
        || limits.maximum_reverification_retained_bytes == 0 || limits.maximum_reverification_retained_bytes > 64ULL << 20)
    {
        fail(RepairError::Code::InvalidConfiguration, "authority exact-repair re-verification limits or continuation are invalid");
    }
}

bool reverificationBudgetExpired(const AuthorityVerificationBatchExecutorLimits & limits) noexcept
{
    if (limits.cancellation.stop_requested())
        return true;
    if (limits.monotonic_deadline && std::chrono::steady_clock::now() >= *limits.monotonic_deadline)
        return true;
    return limits.thread_cpu_deadline_nanoseconds && clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) >= *limits.thread_cpu_deadline_nanoseconds;
}

bool stagedArtifactLess(const DatabaseSchemaWALStagedArtifact & lhs, const DatabaseSchemaWALStagedArtifact & rhs) noexcept
{
    if (lhs.kind != rhs.kind)
        return static_cast<UInt8>(lhs.kind) < static_cast<UInt8>(rhs.kind);
    if (lhs.object != rhs.object)
        return lhs.object < rhs.object;
    if (lhs.revision != rhs.revision)
        return lhs.revision < rhs.revision;
    return static_cast<UInt8>(lhs.image) < static_cast<UInt8>(rhs.image);
}

bool sameStagedArtifactIdentity(const DatabaseSchemaWALStagedArtifact & lhs, const DatabaseSchemaWALStagedArtifact & rhs) noexcept
{
    return lhs.kind == rhs.kind && lhs.image == rhs.image && lhs.object == rhs.object && lhs.revision == rhs.revision;
}

void addStagedArtifact(std::vector<DatabaseSchemaWALStagedArtifact> & artifacts, DatabaseSchemaWALStagedArtifact artifact)
{
    artifacts.push_back(std::move(artifact));
}

std::vector<DatabaseSchemaWALStagedArtifact> makeRepairArtifacts(
    const AuthorityRoot & root,
    const AuthorityRepairPlan & plan,
    const AuthorityQuarantinePlan & quarantine,
    AuthorityExactRepairResult & result)
{
    const auto selections = plan.getSelections();
    if (selections.empty())
        fail(RepairError::Code::InvalidPlan, "authority exact-repair plan contains no selected source");

    std::vector<DatabaseSchemaWALStagedArtifact> artifacts;
    artifacts.reserve(selections.size() * 2);
    for (const auto & selection : selections)
    {
        if (!quarantine.contains(selection.object))
            fail(RepairError::Code::InvalidPlan, "authority exact-repair target is outside the published quarantine closure");
        switch (selection.source)
        {
            case AuthorityRepairSource::LocalSchemaWAL: ++result.local_wal_sources; break;
            case AuthorityRepairSource::ReplicatedAuthority: ++result.replicated_authority_sources; break;
            case AuthorityRepairSource::VerifiedBackup: ++result.verified_backup_sources; break;
        }

        DatabaseSchemaWALStagedArtifact artifact{
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = selection.object,
            .revision = selection.object_revision,
            .canonical_bytes = selection.canonical_bytes,
        };
        switch (selection.artifact_kind)
        {
            case AuthorityRepairArtifactKind::DefinitionRecord:
                artifact.kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord;
                break;
            case AuthorityRepairArtifactKind::SidecarExpectationRecord:
                artifact.kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord;
                break;
            case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar: {
                artifact.kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar;
                const auto * expectation = root.findExpectationRecord(selection.object);
                if (!expectation || expectation->object_schema_revision != selection.object_revision)
                    fail(RepairError::Code::InvalidRoot, "authority exact sidecar repair has no exact rooted expectation");
                addStagedArtifact(
                    artifacts,
                    {
                        .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
                        .image = DatabaseSchemaWALStagedArtifactImage::After,
                        .object = expectation->object,
                        .revision = expectation->object_schema_revision,
                        .canonical_bytes = encodeSidecarExpectationRecord(*expectation),
                    });
                break;
            }
        }
        addStagedArtifact(artifacts, std::move(artifact));
    }

    std::sort(artifacts.begin(), artifacts.end(), stagedArtifactLess);
    std::vector<DatabaseSchemaWALStagedArtifact> unique;
    unique.reserve(artifacts.size());
    for (auto & artifact : artifacts)
    {
        if (!unique.empty() && sameStagedArtifactIdentity(unique.back(), artifact))
        {
            if (unique.back().canonical_bytes != artifact.canonical_bytes)
                fail(RepairError::Code::InvalidPlan, "authority exact-repair plan selected conflicting bytes for one artifact");
            continue;
        }
        unique.push_back(std::move(artifact));
    }
    result.repaired_artifacts = toUInt64(selections.size());
    return unique;
}

}

class AuthorityRepairReverificationContinuation::Impl final
{
public:
    void reset(const AuthorityRootGraphIdentity & root_, const AuthorityQuarantinePlan::Ptr & quarantine_)
    {
        root = root_;
        quarantine = quarantine_;
        /// A continuation may be reused after a larger root or a lowered
        /// limit. Do not let the old vector capacity escape the new retained
        /// byte admission decision.
        std::vector<AuthorityVerificationTarget>().swap(targets);
        snapshot_offset = 0;
        cursor = {};
        cursor.database_uuid = root_.authority_root.database_uuid;
        cursor.bucket_count = 1;
        next_target = 0;
        plan.reset();
        receipt.reset();
        pending_next_cursor = {};
        pending_next_target = 0;
        completed_batches = 0;
        verified_targets = 0;
    }

    void clear() noexcept
    {
        root.reset();
        quarantine.reset();
        std::vector<AuthorityVerificationTarget>().swap(targets);
        snapshot_offset = 0;
        cursor = {};
        next_target = 0;
        plan.reset();
        receipt.reset();
        pending_next_cursor = {};
        pending_next_target = 0;
        completed_batches = 0;
        verified_targets = 0;
    }

    mutable std::mutex mutex;
    std::optional<AuthorityRootGraphIdentity> root;
    AuthorityQuarantinePlan::Ptr quarantine;
    std::vector<AuthorityVerificationTarget> targets;
    UInt64 snapshot_offset = 0;
    AuthorityVerificationScheduleCursor cursor;
    UInt64 next_target = 0;
    AuthorityVerificationBatchPlan::Ptr plan;
    AuthorityVerificationBatchReceipt::Ptr receipt;
    AuthorityVerificationScheduleCursor pending_next_cursor;
    UInt64 pending_next_target = 0;
    UInt64 completed_batches = 0;
    UInt64 verified_targets = 0;
};

AuthorityRepairReverificationContinuation::AuthorityRepairReverificationContinuation()
    : impl(std::make_unique<Impl>())
{
}

AuthorityRepairReverificationContinuation::~AuthorityRepairReverificationContinuation() = default;

bool AuthorityRepairReverificationContinuation::isActiveFor(const AuthorityQuarantinePlan & quarantine) const noexcept
{
    std::lock_guard lock(impl->mutex);
    return impl->root && *impl->root == quarantine.getRoot() && impl->quarantine.get() == std::addressof(quarantine);
}

AuthorityRepairCoordinatorError::AuthorityRepairCoordinatorError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

bool AuthorityRepairCoordinator::reverifyAndRelease(
    DB::DatabaseAtomic & database,
    AtomicAuthority & authority,
    AtomicDatabaseSchemaMutationStorage & storage,
    AuthorityVerificationRuntimeState & runtime,
    AtomicAuthority::RootSnapshot & repaired_root,
    const AuthorityQuarantinePlan::Ptr & quarantine,
    std::unique_lock<std::mutex> schema_lock,
    const AuthorityExactRepairLimits & limits,
    AuthorityExactRepairResult & result)
{
    if (!schema_lock.owns_lock() || !repaired_root || !quarantine)
        fail(RepairError::Code::InvalidQuarantine, "authority repair re-verification lost its schema lock, root, or quarantine");
    if (quarantine->getRoot() != identifyRoot(repaired_root.get()))
        fail(RepairError::Code::InvalidQuarantine, "authority repair re-verification quarantine is not anchored to its exact root");
    const auto durable_state = storage.getCurrentAuthorityState();
    if (!durable_state || *durable_state != repaired_root->getAuthorityState() || storage.getRecoveryRequiredTransactionID())
        fail(RepairError::Code::InvalidRoot, "authority repaired root differs from the durable schema head");

    const auto root_identity = quarantine->getRoot();
    const UInt64 target_count = repaired_root->getInventorySummary().leaf_count;
    const UInt64 retained_target_bytes = checkedMultiply(
        target_count,
        static_cast<UInt64>(sizeof(AuthorityVerificationTarget)),
        "authority repair re-verification target snapshot byte count overflows UInt64");
    if (retained_target_bytes > limits.maximum_reverification_retained_bytes)
    {
        fail(
            RepairError::Code::ReverificationLimitExceeded,
            "authority repair re-verification target snapshot exceeds its retained byte limit");
    }
    auto & progress = *limits.reverification_continuation->impl;
    std::unique_lock progress_lock(progress.mutex);
    UInt64 remaining_work_items = limits.maximum_reverification_work_items_per_pass;
    if (!progress.root || *progress.root != root_identity || progress.quarantine != quarantine)
    {
        /// Releasing an old retained target/plan/receipt image can scale with
        /// its sealed target count. Establish the setup charge before reset.
        if (progress.root)
        {
            if (remaining_work_items == 0 || reverificationBudgetExpired(limits.verification_executor))
                return false;
            --remaining_work_items;
        }
        progress.reset(root_identity, quarantine);
    }
    if (progress.snapshot_offset > target_count || progress.targets.size() != progress.snapshot_offset)
        fail(RepairError::Code::InvalidRoot, "authority repair re-verification continuation has an invalid snapshot offset");

    const auto current_pass_budget = [&]
    {
        return AuthorityVerificationPassBudget{
            .cancellation = limits.verification_executor.cancellation,
            .monotonic_deadline = limits.verification_executor.monotonic_deadline,
            .thread_cpu_deadline_nanoseconds = limits.verification_executor.thread_cpu_deadline_nanoseconds,
            .maximum_work_items = remaining_work_items,
        };
    };
    const auto retain_result_progress = [&]
    {
        result.reverification_batches = progress.completed_batches;
        result.reverified_inventory_targets = progress.verified_targets;
    };

    if (target_count != 0 && progress.targets.capacity() < static_cast<size_t>(target_count))
    {
        /// The exact retained snapshot allocation is one bounded setup item.
        /// Charge it before work proportional to the rooted target count.
        if (remaining_work_items == 0 || reverificationBudgetExpired(limits.verification_executor))
        {
            retain_result_progress();
            return false;
        }
        --remaining_work_items;
        progress.targets.reserve(static_cast<size_t>(target_count));
    }

    while (progress.snapshot_offset < target_count)
    {
        if (remaining_work_items == 0 || reverificationBudgetExpired(limits.verification_executor))
        {
            retain_result_progress();
            return false;
        }
        const UInt64 remaining = target_count - progress.snapshot_offset;
        UInt64 consumed_work_items = 0;
        auto additions = storage.snapshotAuthorityVerificationTargets(
            repaired_root.get(),
            limits.verification_schedule,
            limits.verification_executor,
            progress.snapshot_offset,
            std::min(remaining, remaining_work_items),
            {},
            current_pass_budget(),
            &consumed_work_items);
        if (consumed_work_items < additions.size() || consumed_work_items > remaining_work_items)
            fail(RepairError::Code::InvalidRoot, "authority repair verification snapshot exceeded its cooperative work quantum");
        remaining_work_items -= consumed_work_items;
        if (additions.empty())
        {
            if (remaining_work_items == 0 || reverificationBudgetExpired(limits.verification_executor))
            {
                retain_result_progress();
                return false;
            }
            fail(RepairError::Code::InvalidRoot, "authority repair complete verification snapshot made no progress");
        }
        if (additions.size() > remaining)
            fail(RepairError::Code::InvalidRoot, "authority repair complete verification snapshot made invalid progress");
        progress.snapshot_offset = checkedAdd(
            progress.snapshot_offset, toUInt64(additions.size()), "authority repair verification snapshot offset overflows UInt64");
        progress.targets.insert(
            progress.targets.end(), std::make_move_iterator(additions.begin()), std::make_move_iterator(additions.end()));
    }

    if (progress.next_target > target_count || progress.verified_targets != progress.next_target)
        fail(RepairError::Code::InvalidRoot, "authority repair re-verification continuation lost its verified prefix");

    AuthorityVerificationTrustedBatch trusted_batch(database, storage, std::move(schema_lock));
    while (progress.next_target < target_count)
    {
        if (remaining_work_items == 0 || reverificationBudgetExpired(limits.verification_executor))
        {
            retain_result_progress();
            return false;
        }
        if (!progress.plan)
        {
            if (progress.completed_batches >= limits.maximum_reverification_batches)
                fail(RepairError::Code::ReverificationLimitExceeded, "authority repair requires too many complete verification batches");
            auto planning = planNextAuthorityRepairReverificationBatch(
                repaired_root.get(),
                progress.targets,
                progress.next_target,
                progress.cursor,
                limits.verification_schedule,
                current_pass_budget());
            if (planning.consumed_work_items > remaining_work_items)
                fail(RepairError::Code::InvalidRoot, "authority repair verification planner exceeded its cooperative work quantum");
            remaining_work_items -= planning.consumed_work_items;
            if (planning.status == AuthorityRepairReverificationBatchPlanningStatus::InProgress)
            {
                if (planning.consumed_work_items == 0 && !reverificationBudgetExpired(limits.verification_executor))
                    fail(RepairError::Code::InvalidRoot, "authority repair verification planner made no cooperative progress");
                retain_result_progress();
                return false;
            }
            if (planning.status != AuthorityRepairReverificationBatchPlanningStatus::Scheduled || !planning.plan
                || planning.next_target <= progress.next_target || planning.next_target > target_count)
            {
                fail(RepairError::Code::InvalidRoot, "authority repair complete verification planner made invalid progress");
            }
            progress.plan = std::move(planning.plan);
            progress.pending_next_cursor = std::move(planning.next_cursor);
            progress.pending_next_target = planning.next_target;
        }

        if (remaining_work_items == 0 || reverificationBudgetExpired(limits.verification_executor))
        {
            retain_result_progress();
            return false;
        }

        const size_t previously_completed = progress.receipt ? progress.receipt->getTerminalCompletions().size() : 0;
        const size_t scheduled_count = progress.plan->getTargets().size();
        if (scheduled_count < previously_completed)
            fail(RepairError::Code::InvalidRoot, "authority repair retained receipt exceeds its sealed plan");
        auto execution_limits = limits.verification_executor;
        /// Charge scalable sealed-prefix validation/copy and trusted execution
        /// setup before entering the executor. A first terminal target shares
        /// this item; later terminal targets consume one item each.
        const UInt64 execution_work_item_budget = remaining_work_items;
        --remaining_work_items;
        execution_limits.maximum_terminal_targets = std::min(execution_limits.maximum_terminal_targets, execution_work_item_budget);
        auto receipt = AuthorityVerificationBatchExecutor::executeTrusted(
            repaired_root, *progress.plan, trusted_batch, execution_limits, progress.receipt.get());
        if (!receipt)
        {
            retain_result_progress();
            return false;
        }
        if (receipt->getRoot() != progress.plan->getRoot() || receipt->getTargetSetDigest() != progress.plan->getTargetSetDigest()
            || receipt->getChargeABI() != progress.plan->getChargeABI())
        {
            fail(RepairError::Code::InvalidRoot, "authority repair executor returned a receipt for another sealed plan");
        }
        const auto scheduled = progress.plan->getTargets();
        const auto completed = receipt->getTerminalCompletions();
        if (completed.size() < previously_completed || completed.size() > scheduled.size())
            fail(RepairError::Code::InvalidRoot, "authority repair receipt is not a cumulative planned-target prefix");
        for (size_t index = 0; index < completed.size(); ++index)
        {
            if (completed[index].leaf != scheduled[index].leaf)
                fail(RepairError::Code::InvalidRoot, "authority repair receipt changed its scheduled target order");
            if (completed[index].disposition != AuthorityVerificationTargetDisposition::Verified)
            {
                retain_result_progress();
                /// Exact repair did not converge. Abandon this sealed
                /// re-verification image so the next automatic-repair pass
                /// performs a fresh exact-root audit/source selection instead
                /// of replaying the same damaged target forever.
                progress.clear();
                return false;
            }
        }
        const UInt64 execution_work_items = toUInt64(completed.size() - previously_completed);
        if (execution_work_items > execution_work_item_budget || execution_work_items > execution_limits.maximum_terminal_targets)
            fail(RepairError::Code::InvalidRoot, "authority repair executor exceeded its cooperative work quantum");
        if (execution_work_items > 1)
            remaining_work_items -= execution_work_items - 1;
        progress.receipt = std::move(receipt);
        if (completed.size() != scheduled.size())
        {
            if (completed.size() == previously_completed && !reverificationBudgetExpired(limits.verification_executor))
            {
                fail(
                    RepairError::Code::InvalidRoot,
                    "authority repair executor made no resumable progress before its execution budget expired");
            }
            retain_result_progress();
            return false;
        }

        progress.verified_targets
            = checkedAdd(progress.verified_targets, toUInt64(completed.size()), "authority repair verified-target count overflows UInt64");
        progress.next_target = progress.pending_next_target;
        progress.cursor = std::move(progress.pending_next_cursor);
        ++progress.completed_batches;
        progress.plan.reset();
        progress.receipt.reset();
    }
    if (progress.verified_targets != target_count || (target_count != 0 && progress.cursor.completed_rotations != 1))
        fail(RepairError::Code::InvalidRoot, "authority repair complete verification did not seal its exact inventory once");

    runtime.releaseQuarantineAfterCompleteVerification(repaired_root.get(), quarantine);
    result.reverification_batches = progress.completed_batches;
    result.reverified_inventory_targets = progress.verified_targets;
    result.released_quarantined_objects = toUInt64(quarantine->getQuarantinedObjects().size());
    progress.clear();
    static_cast<void>(authority);
    return true;
}

AuthorityExactRepairResult AuthorityRepairCoordinator::executeAndRelease(
    DB::DatabaseAtomic & database, const AuthorityRepairPlan & plan, const AuthorityExactRepairLimits & limits)
{
    database.waitDatabaseStarted();
    std::unique_lock schema_lock(database.udt_schema_mutation_mutex);
    return executeAndRelease(database, plan, std::move(schema_lock), limits);
}

AuthorityExactRepairResult AuthorityRepairCoordinator::executeAndRelease(
    DB::DatabaseAtomic & database,
    const AuthorityRepairPlan & plan,
    std::unique_lock<std::mutex> schema_lock,
    const AuthorityExactRepairLimits & limits)
{
    validateLimits(limits);
    if (!schema_lock.owns_lock() || schema_lock.mutex() != &database.udt_schema_mutation_mutex)
        fail(RepairError::Code::InvalidRoot, "Atomic authority exact repair received an invalid schema lock");

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    AuthorityVerificationRuntimeState * runtime = nullptr;
    {
        std::lock_guard authority_lock(database.udt_authority_mutex);
        authority = database.udt_authority.get();
        storage = database.udt_mutation_storage.get();
        runtime = database.udt_verification_runtime.get();
        if (database.udt_authority_mode != DB::DatabaseAtomic::AuthorityMode::Enabled || database.udt_authority_shutdown
            || database.udt_table_startup_state || !authority || !storage || !runtime
            || database.active_udt_authority.load(std::memory_order_acquire) != authority
            || database.active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
        {
            fail(RepairError::Code::InvalidRoot, "Atomic authority exact-repair components are not active and consistent");
        }
    }

    auto planning_root = authority->acquireCurrentRoot();
    if (!planning_root)
        fail(RepairError::Code::InvalidRoot, "Atomic authority exact repair has no published root");
    const AuthorityRootGraphIdentity current_identity = identifyRoot(planning_root.get());
    if (plan.getRoot() != current_identity)
        fail(RepairError::Code::InvalidPlan, "authority exact-repair plan is stale for the published root");
    const auto durable_state = storage->getCurrentAuthorityState();
    if (!durable_state || *durable_state != planning_root->getAuthorityState() || storage->getRecoveryRequiredTransactionID())
        fail(RepairError::Code::InvalidRoot, "authority exact-repair root differs from its durable predecessor");

    AuthorityQuarantinePlan::Ptr quarantine;
    {
        auto runtime_snapshot = runtime->acquireSnapshot();
        if (runtime_snapshot.isFailClosed())
            fail(RepairError::Code::InvalidQuarantine, "authority exact repair cannot release an unbounded fail-closed runtime");
        quarantine = runtime_snapshot.getQuarantine();
    }
    if (!quarantine || quarantine->getRoot() != current_identity)
        fail(RepairError::Code::InvalidQuarantine, "authority exact-repair plan has no matching published quarantine");

    AuthorityExactRepairResult result;
    result.previous_root = current_identity.authority_root;
    result.damaged_artifact_manifest_digest = plan.getDamagedArtifactManifestDigest();
    auto staged_artifacts = makeRepairArtifacts(planning_root.get(), plan, *quarantine, result);
    auto replacement_root = planning_root->cloneForExactRepair(limits.authority_state);
    const AuthorityRootGraphIdentity replacement_identity = identifyRoot(*replacement_root);
    result.repaired_root = replacement_identity.authority_root;

    storage->maintainCheckpointBeforeMutation(planning_root.get());
    auto mutation_guard = storage->issueMutationGuard();
    const UInt64 predecessor = mutation_guard.getDurablePredecessorTransactionID();
    if (predecessor == std::numeric_limits<UInt64>::max())
        fail(RepairError::Code::TransactionIDExhausted, "authority exact-repair transaction ID domain is exhausted");
    result.transaction_id = predecessor + 1;

    DatabaseSchemaWALExactRepairProvenance repair_provenance{
        .transaction_id = result.transaction_id,
        .damaged_artifact_count = plan.getDamagedArtifactCount(),
        .damaged_artifact_manifest_digest = plan.getDamagedArtifactManifestDigest(),
        .local_wal_sources = result.local_wal_sources,
        .replicated_authority_sources = result.replicated_authority_sources,
        .verified_backup_sources = result.verified_backup_sources,
        .previous_catalog_epoch = current_identity.authority_root.database_catalog_epoch,
        .previous_authority_anchor = current_identity.authority_root.authority_anchor,
        .repaired_catalog_epoch = replacement_identity.authority_root.database_catalog_epoch,
        .repaired_authority_anchor = replacement_identity.authority_root.authority_anchor,
    };
    auto published_repair_provenance = std::make_unique<const DatabaseSchemaWALExactRepairProvenance>(repair_provenance);

    auto transition = DatabaseSchemaWALTransitionBuilder::buildExactRepair(
        result.transaction_id,
        planning_root.get(),
        *replacement_root,
        std::move(staged_artifacts),
        limits.wal,
        std::move(repair_provenance));
    auto publication = authority->preparePublication(std::move(replacement_root));
    auto prepared_execution = prepareDatabaseSchemaMutationExecution(transition, limits.wal);
    validatePreparedDatabaseSchemaMutationExecution(*storage, mutation_guard, prepared_execution);

    /// Acquire the diagnostic-owner lock before the durable boundary. The
    /// fixed provenance value and its unique_ptr were both preallocated, so a
    /// successful commit/publication is followed only by no-throw pointer
    /// replacement while this lock is retained.
    {
        std::lock_guard authority_status_lock(database.udt_authority_mutex);
        if (database.udt_authority_shutdown || database.udt_authority.get() != authority || database.udt_mutation_storage.get() != storage
            || database.udt_verification_runtime.get() != runtime)
        {
            fail(RepairError::Code::InvalidRoot, "Atomic authority exact-repair components changed before durable publication");
        }
        try
        {
            static_cast<void>(executePreparedDatabaseSchemaMutation(*storage, mutation_guard, std::move(prepared_execution)));
        }
        catch (...)
        {
            /// The durability phase is now explicitly indeterminate and the
            /// mutation storage is fail-stopped. Do not continue exposing an older
            /// repair as the latest durable repair until startup recovery can
            /// reconstruct the exact committed history.
            database.udt_last_exact_repair_provenance.reset();
            throw;
        }
        authority->publish(std::move(publication));
        database.udt_last_exact_repair_provenance = std::move(published_repair_provenance);
    }

    auto repaired_root = authority->acquireCurrentRoot();
    if (!repaired_root || identifyRoot(repaired_root.get()) != replacement_identity)
        std::terminate();
    {
        /// Authority publication re-anchors the immutable quarantine to the
        /// content-neutral successor before readers can be admitted again.
        /// Release must use that exact runtime object, not the predecessor
        /// pointer captured while planning the repair.
        auto runtime_snapshot = runtime->acquireSnapshot();
        const auto & reanchored = runtime_snapshot.getQuarantine();
        if (runtime_snapshot.isFailClosed() || !reanchored || reanchored->getRoot() != replacement_identity
            || reanchored->getFailingSeeds().size() != quarantine->getFailingSeeds().size()
            || !std::equal(
                reanchored->getFailingSeeds().begin(), reanchored->getFailingSeeds().end(), quarantine->getFailingSeeds().begin())
            || reanchored->getQuarantinedObjects().size() != quarantine->getQuarantinedObjects().size()
            || !std::equal(
                reanchored->getQuarantinedObjects().begin(),
                reanchored->getQuarantinedObjects().end(),
                quarantine->getQuarantinedObjects().begin()))
        {
            std::terminate();
        }
        quarantine = reanchored;
    }
    const bool released
        = reverifyAndRelease(database, *authority, *storage, *runtime, repaired_root, quarantine, std::move(schema_lock), limits, result);
    result.status = released ? AuthorityExactRepairStatus::RepairedAndReleased : AuthorityExactRepairStatus::RepairedQuarantineRetained;
    return result;
}

AuthorityExactRepairResult
AuthorityRepairCoordinator::resumeReverificationAndRelease(DB::DatabaseAtomic & database, const AuthorityExactRepairLimits & limits)
{
    database.waitDatabaseStarted();
    std::unique_lock schema_lock(database.udt_schema_mutation_mutex);
    return resumeReverificationAndRelease(database, std::move(schema_lock), limits);
}

AuthorityExactRepairResult AuthorityRepairCoordinator::resumeReverificationAndRelease(
    DB::DatabaseAtomic & database, std::unique_lock<std::mutex> schema_lock, const AuthorityExactRepairLimits & limits)
{
    validateLimits(limits);
    if (!schema_lock.owns_lock() || schema_lock.mutex() != &database.udt_schema_mutation_mutex)
        fail(RepairError::Code::InvalidRoot, "Atomic authority repair re-verification received an invalid schema lock");

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    AuthorityVerificationRuntimeState * runtime = nullptr;
    {
        std::lock_guard authority_lock(database.udt_authority_mutex);
        authority = database.udt_authority.get();
        storage = database.udt_mutation_storage.get();
        runtime = database.udt_verification_runtime.get();
        if (database.udt_authority_mode != DB::DatabaseAtomic::AuthorityMode::Enabled || database.udt_authority_shutdown
            || database.udt_table_startup_state || !authority || !storage || !runtime
            || database.active_udt_authority.load(std::memory_order_acquire) != authority
            || database.active_udt_verification_runtime.load(std::memory_order_acquire) != runtime)
        {
            fail(RepairError::Code::InvalidRoot, "Atomic authority repair re-verification components are not active and consistent");
        }
    }

    AuthorityQuarantinePlan::Ptr quarantine;
    {
        auto runtime_snapshot = runtime->acquireSnapshot();
        if (runtime_snapshot.isFailClosed())
            fail(RepairError::Code::InvalidQuarantine, "authority repair re-verification cannot release fail-closed state");
        quarantine = runtime_snapshot.getQuarantine();
    }
    if (!quarantine)
        fail(RepairError::Code::InvalidQuarantine, "authority repair re-verification has no published quarantine");

    auto repaired_root = authority->acquireCurrentRoot();
    if (!repaired_root)
        fail(RepairError::Code::InvalidRoot, "authority repair re-verification has no published root");
    const auto repaired_identity = identifyRoot(repaired_root.get());
    AuthorityExactRepairResult result;
    result.previous_root = quarantine->getRoot().authority_root;
    result.repaired_root = repaired_identity.authority_root;
    const bool released
        = reverifyAndRelease(database, *authority, *storage, *runtime, repaired_root, quarantine, std::move(schema_lock), limits, result);
    result.status = released ? AuthorityExactRepairStatus::ReverifiedAndReleased : AuthorityExactRepairStatus::ReverificationFailed;
    return result;
}

}
