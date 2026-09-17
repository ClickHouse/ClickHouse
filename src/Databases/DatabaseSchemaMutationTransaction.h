#pragma once

#include <Databases/DatabaseSchemaWAL.h>

#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

enum class DatabaseSchemaMutationDurabilityPhase : UInt8
{
    PrepareMarker = 1,
    AfterImage = 2,
    InstallationBarrier = 3,
    CommitMarker = 4,
    RecoveryImage = 5,
    RecoveryBarrier = 6,
    RecoveryMarker = 7,
};

/// A storage exception at or after the first Prepare-marker attempt cannot
/// tell the caller which side of the durability boundary was reached. The
/// owning database must remain fail-stopped and run durable-state recovery
/// for `transaction_id` before publishing or starting another mutation.
class DatabaseSchemaMutationIndeterminateDurabilityError final : public std::runtime_error
{
public:
    DatabaseSchemaMutationIndeterminateDurabilityError(UInt64 transaction_id_, DatabaseSchemaMutationDurabilityPhase phase_);

    const UInt64 transaction_id;
    const DatabaseSchemaMutationDurabilityPhase phase;
};

class DatabaseSchemaMutationReplayConflictError final : public std::runtime_error
{
public:
    explicit DatabaseSchemaMutationReplayConflictError(std::string_view message);
};

/// Opaque proof that the caller holds the owning database's exclusive schema
/// mutation guard. A durable-storage implementation issues the token only
/// inside that guard and validates its opaque identity on every lifecycle
/// operation. A token is single-transaction and move-only.
class DatabaseSchemaMutationGuard final
{
public:
    enum class State : UInt8
    {
        Ready = 1,
        RecoveryRequired = 2,
        Finished = 3,
    };

    DatabaseSchemaMutationGuard(const DatabaseSchemaMutationGuard &) = delete;
    DatabaseSchemaMutationGuard & operator=(const DatabaseSchemaMutationGuard &) = delete;
    /// Moving transfers the only usable token. The source becomes a terminal
    /// zero-identity guard and cannot enter a storage lifecycle operation.
    DatabaseSchemaMutationGuard(DatabaseSchemaMutationGuard && other) noexcept;
    /// Assignment could overwrite the only token capable of clearing a
    /// recovery latch, so guards may only be move-constructed.
    DatabaseSchemaMutationGuard & operator=(DatabaseSchemaMutationGuard && other) noexcept = delete;

    /// Storage implementations must use a process-local unguessable/generation
    /// value for `opaque_identity` and reject stale or foreign identities.
    static DatabaseSchemaMutationGuard issue(UUID database_uuid, UInt64 opaque_identity, UInt64 durable_predecessor_transaction_id);

    UUID getDatabaseUUID() const noexcept { return database_uuid; }
    UInt64 getOpaqueIdentity() const noexcept { return opaque_identity; }
    UInt64 getDurablePredecessorTransactionID() const noexcept { return durable_predecessor_transaction_id; }
    State getState() const noexcept { return state; }

private:
    DatabaseSchemaMutationGuard(UUID database_uuid_, UInt64 opaque_identity_, UInt64 durable_predecessor_transaction_id_);
    void invalidateAfterMove() noexcept;

    friend DatabaseSchemaWALCommit executeDatabaseSchemaMutation(
        class IDatabaseSchemaMutationDurableStorage &,
        DatabaseSchemaMutationGuard &,
        const DatabaseSchemaWALValidatedTransition &,
        const DatabaseSchemaWALLimits &);
    friend DatabaseSchemaWALCommit executePreparedDatabaseSchemaMutation(
        class IDatabaseSchemaMutationDurableStorage &, DatabaseSchemaMutationGuard &, class PreparedDatabaseSchemaMutationExecution);
    friend DatabaseSchemaWALRecoveryDecision recoverDatabaseSchemaMutation(
        class IDatabaseSchemaMutationDurableStorage &,
        DatabaseSchemaMutationGuard &,
        const DatabaseSchemaWALValidatedTransition &,
        const std::optional<DatabaseSchemaWALCommit> &,
        const DatabaseSchemaWALLimits &);
    friend void discardUnpreparedDatabaseSchemaMutationStaging(
        class IDatabaseSchemaMutationDurableStorage &, DatabaseSchemaMutationGuard &, UInt64);

    UUID database_uuid;
    UInt64 opaque_identity;
    UInt64 durable_predecessor_transaction_id;
    State state = State::Ready;
};

enum class DatabaseSchemaMutationArtifactActionKind : UInt8
{
    Install = 1,
    Remove = 2,
};

/// One already-validated, allocation-free-at-application action. Install
/// actions name the exact canonical manifest ordinal; remove actions have no
/// ordinal. The planner orders removals dependent-first and installations
/// dependency-first, including shared chains and diamonds.
struct DatabaseSchemaMutationArtifactAction
{
    DatabaseSchemaMutationArtifactActionKind action{};
    DatabaseSchemaWALStagedArtifactKind kind{};
    SchemaObjectID object;
    std::optional<size_t> staged_artifact_ordinal;

    bool operator==(const DatabaseSchemaMutationArtifactAction &) const = default;
};

/// Fully owned, single-use execution image. Construction performs every
/// allocation, encoding and dependency-order check required by the durable
/// transaction. This lets security-sensitive callers consume an operation
/// token only after the transaction is known to be executable without any
/// further fallible orchestration work.
class PreparedDatabaseSchemaMutationExecution final
{
public:
    PreparedDatabaseSchemaMutationExecution(const PreparedDatabaseSchemaMutationExecution &) = delete;
    PreparedDatabaseSchemaMutationExecution & operator=(const PreparedDatabaseSchemaMutationExecution &) = delete;
    PreparedDatabaseSchemaMutationExecution(PreparedDatabaseSchemaMutationExecution && other) noexcept;
    PreparedDatabaseSchemaMutationExecution & operator=(PreparedDatabaseSchemaMutationExecution &&) noexcept = delete;

private:
    PreparedDatabaseSchemaMutationExecution(
        DatabaseSchemaWALPrepare prepare_,
        std::vector<String> staged_artifact_bytes_,
        std::vector<DatabaseSchemaMutationArtifactAction> actions_,
        String prepare_bytes_,
        DatabaseSchemaWALCommit commit_,
        String commit_bytes_,
        std::vector<DatabaseSchemaWALStagedArtifactLocator> locators_);

    friend PreparedDatabaseSchemaMutationExecution
    prepareDatabaseSchemaMutationExecution(const DatabaseSchemaWALValidatedTransition &, const DatabaseSchemaWALLimits &);
    friend void validatePreparedDatabaseSchemaMutationExecution(
        IDatabaseSchemaMutationDurableStorage &, const DatabaseSchemaMutationGuard &, PreparedDatabaseSchemaMutationExecution &);
    friend DatabaseSchemaWALCommit executePreparedDatabaseSchemaMutation(
        IDatabaseSchemaMutationDurableStorage &, DatabaseSchemaMutationGuard &, PreparedDatabaseSchemaMutationExecution);

    DatabaseSchemaWALPrepare prepare;
    std::vector<String> staged_artifact_bytes;
    std::vector<DatabaseSchemaMutationArtifactAction> actions;
    String prepare_bytes;
    DatabaseSchemaWALCommit commit;
    String commit_bytes;
    std::vector<DatabaseSchemaWALStagedArtifactLocator> locators;
    UUID validated_guard_database_uuid = UUIDHelpers::Nil;
    UInt64 validated_guard_opaque_identity = 0;
    UInt64 validated_guard_predecessor = 0;
    const IDatabaseSchemaMutationDurableStorage * validated_storage = nullptr;
    bool durable_preflight_complete = false;
    bool usable = true;
};

/// Durable I/O boundary for one database-owned schema transaction. Each
/// method returns only after the operation named by that method is durable.
/// Implementations may batch writes internally, but must preserve the call
/// order made by executeDatabaseSchemaMutation and
/// recoverDatabaseSchemaMutation.
///
/// Every durable keyed operation is replay-idempotent for the exact same
/// guard/transaction, locator/reference/content and decision. A canonical
/// install/remove accepts only the already-selected image or the exact opposite
/// image named by the durable Prepare. Any other existing value is preserved
/// and reported with DatabaseSchemaMutationReplayConflictError.
class IDatabaseSchemaMutationDurableStorage
{
public:
    virtual ~IDatabaseSchemaMutationDurableStorage() = default;

    /// Validates the opaque guard, checks that its captured durable predecessor
    /// is still current, checks the exact preceding authority state, and
    /// rejects transaction IDs not strictly greater than the durable high-water
    /// mark. Rolled-back prepared IDs remain below that high-water mark.
    virtual void validateMutationGuardAndDurablePredecessor(
        const DatabaseSchemaMutationGuard & guard,
        const std::optional<AuthorityState> & expected_preceding_authority_state,
        UInt64 transaction_id) = 0;

    /// Records a process-local fail-stop latch without throwing. Until exact
    /// recovery or proven-unprepared cleanup clears it, mutation validation
    /// must reject every new transaction for this database.
    virtual void markMutationRecoveryRequired(
        const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id, DatabaseSchemaMutationDurabilityPhase phase) noexcept = 0;

    /// Validates a current guard for replay of this exact durable transaction.
    virtual void validateRecoveryGuard(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) = 0;

    virtual void stageArtifact(
        const DatabaseSchemaWALStagedArtifactLocator & locator,
        const DatabaseSchemaWALStagedArtifactRef & artifact,
        std::string_view canonical_bytes) = 0;

    /// Makes every staged artifact and the staging-directory entries durable.
    virtual void finishStaging(UUID database_uuid, UInt64 transaction_id) = 0;

    /// Persists the exact canonical WAL record and its containing directory.
    virtual void persistPrepare(UInt64 transaction_id, std::string_view canonical_prepare) = 0;

    /// Installs one selected image at its canonical location. The content
    /// address has already been validated by DatabaseSchemaWALTransitionBuilder.
    virtual void installArtifact(const DatabaseSchemaWALStagedArtifactRef & artifact, std::string_view canonical_bytes) = 0;

    /// Removes the canonical location for an object/artifact pair. This is
    /// used for the absent side of add/remove transitions.
    virtual void removeArtifact(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) = 0;

    /// Makes every install/remove and all affected directory entries durable.
    virtual void finishInstallation(UUID database_uuid, UInt64 transaction_id) = 0;

    /// Persists the exact canonical WAL record and its containing directory.
    virtual void persistCommit(UInt64 transaction_id, std::string_view canonical_commit) = 0;

    /// Called after an idempotent recovery image has been made durable. A
    /// committed transaction's staged images cannot be discarded until a
    /// durable checkpoint covers it.
    virtual void finishRecovery(UUID database_uuid, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision) = 0;

    /// Deletes only staging for a transaction for which the implementation has
    /// durably proved that no Prepare marker exists. This operation also clears
    /// a matching process-local fail-stop latch. It is idempotent.
    virtual void discardUnpreparedStaging(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) = 0;

    /// Retires a rolled-back transaction only after finishRecovery has durably
    /// recorded that exact rollback decision. It may delete its Prepare and
    /// staged images, but must retain a durable transaction-ID high-water mark.
    virtual void retireRolledBackTransaction(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) = 0;

    /// Persists the exact validated checkpoint record and both canonical
    /// snapshots. Replaying identical bytes is a no-op; differing content for
    /// the same checkpoint ID is a conflict. This method never removes WAL or
    /// staged transaction images.
    virtual void persistValidatedCheckpoint(
        const DatabaseSchemaMutationGuard & guard,
        const DatabaseSchemaWALCheckpoint & checkpoint,
        std::string_view canonical_checkpoint,
        std::string_view canonical_inventory_snapshot,
        std::string_view canonical_schema_graph_snapshot) = 0;

    /// Verifies that the exact checkpoint and snapshots above are durable,
    /// then compacts through its covered transaction. Only this call may remove
    /// committed staged images, and it must preserve every newer WAL/staging
    /// entry. The exact replay is idempotent.
    virtual void
    compactThroughValidatedCheckpoint(const DatabaseSchemaMutationGuard & guard, const DatabaseSchemaWALCheckpoint & checkpoint) = 0;
};

/// Builds the complete application plan before any storage call. A dependency
/// cycle among changed non-self objects is rejected rather than assigned an
/// arbitrary installation order.
[[nodiscard]] std::vector<DatabaseSchemaMutationArtifactAction> planValidatedDatabaseSchemaArtifactInstallation(
    const DatabaseSchemaWALValidatedTransition & transition, DatabaseSchemaWALStagedArtifactImage selected_image);

/// Completes the entire fallible orchestration phase without touching
/// storage. In particular, dependency cycles and encoding/limit failures are
/// reported here, before a caller consumes a single-use authorization token.
[[nodiscard]] PreparedDatabaseSchemaMutationExecution prepareDatabaseSchemaMutationExecution(
    const DatabaseSchemaWALValidatedTransition & transition, const DatabaseSchemaWALLimits & limits = {});

/// Performs the final fallible guard/durable-predecessor CAS before a caller
/// consumes a single-use authorization token. Successful validation binds the
/// prepared image to this exact guard; no staging or WAL marker is written.
void validatePreparedDatabaseSchemaMutationExecution(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    PreparedDatabaseSchemaMutationExecution & prepared);

/// Performs only durable-storage operations using an image already bound by
/// validatePreparedDatabaseSchemaMutationExecution. The prepared value is
/// consumed exactly once; violating that internal contract terminates rather
/// than opening a new fallible path after authorization-token consumption.
[[nodiscard]] DatabaseSchemaWALCommit executePreparedDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & guard,
    PreparedDatabaseSchemaMutationExecution prepared);

/// All encoding, allocation, and image planning finish before the first I/O
/// call. The returned commit is durable when this function returns and the
/// caller may then publish the already-prepared immutable authority root.
[[nodiscard]] DatabaseSchemaWALCommit executeDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedTransition & transition,
    const DatabaseSchemaWALLimits & limits = {});

/// Reinstalls the exact before image when no valid commit exists, or the exact
/// after image when the commit is present. The supplied transition must have
/// been reconstructed from the durable prepare plus all staged bytes and
/// validated against the preceding committed/checkpoint state.
[[nodiscard]] DatabaseSchemaWALRecoveryDecision recoverDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedTransition & transition,
    const std::optional<DatabaseSchemaWALCommit> & commit,
    const DatabaseSchemaWALLimits & limits = {});

/// Safe only after staging/finishStaging failed or a Prepare attempt was
/// inspected and proved absent. The storage implementation performs that proof
/// again while the mutation guard is held.
void discardUnpreparedDatabaseSchemaMutationStaging(
    IDatabaseSchemaMutationDurableStorage & storage, DatabaseSchemaMutationGuard & guard, UInt64 transaction_id);

void retireRolledBackDatabaseSchemaMutation(
    IDatabaseSchemaMutationDurableStorage & storage, const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id);

/// Encoding and validation complete before persistValidatedCheckpoint is
/// entered. Committed staging remains present after this function returns.
void persistValidatedDatabaseSchemaCheckpoint(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedCheckpoint & checkpoint,
    const DatabaseSchemaWALLimits & limits = {});

void compactDatabaseSchemaWALThroughValidatedCheckpoint(
    IDatabaseSchemaMutationDurableStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedCheckpoint & checkpoint);

}
