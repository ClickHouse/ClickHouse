#pragma once

#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>

#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Disks/IDisk.h>

#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRoot;
class AuthorityVerificationScheduler;
class AuthorityVerificationBatchExecutor;
class AuthorityRepairCoordinator;
class AuthorityAutomaticRepair;
class AuthorityAutomaticRepairAccess;
struct AuthorityVerificationBatchExecutorLimits;
struct AuthorityRootGraphIdentity;

/// Frozen permanent preimage domain for the V1 high-water record checksum.
inline constexpr std::string_view atomic_database_schema_mutation_high_water_hash_domain
    = "ClickHouse UDT Atomic schema transaction high-water V1";
inline constexpr std::string_view authority_activation_marker_hash_domain = "ClickHouse UDT Atomic authority activation marker V1";
inline constexpr std::string_view authority_activation_marker_v2_hash_domain = "ClickHouse UDT Atomic authority activation marker V2";
inline constexpr std::string_view atomic_database_schema_mutation_staged_artifact_hash_domain
    = "ClickHouse UDT Atomic schema staged artifact envelope V1";
inline constexpr std::string_view atomic_database_schema_mutation_recovery_decision_hash_domain
    = "ClickHouse UDT Atomic schema recovery decision V1";
inline constexpr std::string_view authority_verification_cursor_hash_domain = "ClickHouse UDT Atomic authority verification cursor V1";
inline constexpr UInt16 atomic_database_udt_configuration_format_version = 2;
inline constexpr std::string_view atomic_database_udt_configuration_hash_domain
    = "ClickHouse UDT Atomic database policy configuration generation V2";

String encodeAtomicDatabaseSchemaMutationHighWaterMark(UUID database_uuid, UInt64 transaction_id);
UInt64 decodeAtomicDatabaseSchemaMutationHighWaterMark(std::string_view bytes, UUID expected_database_uuid);
String encodeAuthorityActivationMarker(UUID database_uuid, UInt64 activation_transaction_id);
UInt64 decodeAuthorityActivationMarker(std::string_view bytes, UUID expected_database_uuid);
String encodeAtomicDatabaseSchemaMutationRecoveryDecision(
    UUID database_uuid, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision, const Digest & prepare_hash);
DatabaseSchemaWALRecoveryDecision decodeAtomicDatabaseSchemaMutationRecoveryDecision(
    std::string_view bytes, UUID expected_database_uuid, UInt64 expected_transaction_id, const Digest & expected_prepare_hash);
String encodeAuthorityVerificationScheduleCursor(const AuthorityVerificationScheduleCursor & cursor);
AuthorityVerificationScheduleCursor decodeAuthorityVerificationScheduleCursor(std::string_view bytes, UUID expected_database_uuid);

/// Permanent V1 path router for the first Atomic-disk schema transaction
/// backend. The supplied metadata root is database-local and relative to its
/// DiskPtr; every remaining component is derived from durable identities.
class AtomicDatabaseSchemaMutationPaths final
{
public:
    AtomicDatabaseSchemaMutationPaths(String metadata_root_, UUID database_uuid_, String database_name_ = {});

    const String & getMetadataRoot() const noexcept { return metadata_root; }
    UUID getDatabaseUUID() const noexcept { return database_uuid; }
    bool hasDatabaseName() const noexcept { return !database_name.empty(); }

    String typesDirectory() const;
    String authorityDirectory() const;
    String stagingDirectory() const;
    String stagingTransactionDirectory(UInt64 transaction_id) const;
    String stagedArtifactPath(UInt64 transaction_id, UInt64 ordinal) const;
    String walDirectory() const;
    String walTransactionDirectory(UInt64 transaction_id) const;
    String preparePath(UInt64 transaction_id) const;
    String commitPath(UInt64 transaction_id) const;
    String recoveryDecisionPath(UInt64 transaction_id) const;
    String retiredDirectory() const;
    String retiredRollbackDirectory() const;
    String retiredRollbackTransactionDirectory(UInt64 transaction_id) const;
    String retiredCheckpointDirectory() const;
    String retiredCheckpointTransactionDirectory(UInt64 checkpoint_id, UInt64 transaction_id) const;
    String retiredCheckpointImageDirectory(UInt64 checkpoint_id) const;
    String checkpointsDirectory() const;
    String checkpointDirectory(UInt64 checkpoint_id) const;
    String checkpointRecordPath(UInt64 checkpoint_id) const;
    String checkpointInventorySnapshotPath(UInt64 checkpoint_id) const;
    String checkpointSchemaGraphSnapshotPath(UInt64 checkpoint_id) const;
    /// Permanent database-root-level marker. Keeping it outside `types/`
    /// makes deletion of the complete authority namespace observable.
    String activationMarkerPath() const;
    String activationMarkerTemporaryPath() const;
    String verificationCursorPath() const;
    String verificationCursorTemporaryPath() const;
    String udtConfigurationV2Path() const;
    String udtConfigurationV2TemporaryPath() const;
    /// Legacy read-only migration paths. New writers publish only the combined
    /// configuration generation above.
    String verificationSchedulerOverrideV2Path() const;
    String verificationSchedulerOverrideV2TemporaryPath() const;
    String resourceQuotaOverrideV2Path() const;
    String resourceQuotaOverrideV2TemporaryPath() const;
    String highWaterMarkPath() const;
    String authorityRecordPath(const AuthorityInventoryKey & key) const;
    String tableReferencesPath(const SchemaObjectID & object) const;
    String metadataInstallationRecordPath(const SchemaObjectID & object) const;
    String tableMetadataPath(std::string_view object_name) const;
    String droppedTableMetadataPath(std::string_view object_name, UUID object_uuid) const;
    String canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) const;

private:
    String metadata_root;
    UUID database_uuid;
    String database_name;
};

struct AtomicDatabaseSchemaMutationStorageLimits
{
    DatabaseSchemaWALLimits wal;
    UInt64 maximum_directory_entries = 1'400'000;
    UInt64 maximum_checkpoint_namespace_entries = 16;
    UInt64 maximum_metadata_root_bytes = 4'096;
    UInt64 maximum_total_authority_record_bytes = resource_implementation_maximum_deterministic_catalog_bytes;
    UInt64 maximum_total_durable_dependent_object_bytes = resource_implementation_maximum_durable_dependent_object_bytes;
};

class AtomicDatabaseSchemaMutationStorageError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        UnsupportedDisk,
        UnsafePath,
        CorruptDurableState,
        LimitExceeded,
        DirectorySyncUnavailable,
        FaultInjected,
    };

    AtomicDatabaseSchemaMutationStorageError(Code code_, std::string_view message);

    const Code code;
};

struct AtomicDatabaseSchemaMutationRecoveryTransaction
{
    DatabaseSchemaWALPrepare prepare;
    std::optional<DatabaseSchemaWALCommit> commit;
    std::optional<DatabaseSchemaWALRecoveryDecision> recovery_decision;
    std::vector<String> staged_artifact_bytes;
};

/// Opaque process-local cursor for one exact local schema-WAL namespace
/// snapshot. Only the storage-owned automatic-repair adapter can advance it;
/// the cursor pins its root/limits and retains bounded directory iterators and
/// identifier vectors between cooperative scheduler runs.
class AtomicDatabaseSchemaMutationDurableTransactionDiscovery final
{
public:
    AtomicDatabaseSchemaMutationDurableTransactionDiscovery();
    AtomicDatabaseSchemaMutationDurableTransactionDiscovery(const AtomicDatabaseSchemaMutationDurableTransactionDiscovery &) = delete;
    AtomicDatabaseSchemaMutationDurableTransactionDiscovery & operator=(const AtomicDatabaseSchemaMutationDurableTransactionDiscovery &)
        = delete;
    ~AtomicDatabaseSchemaMutationDurableTransactionDiscovery();

private:
    friend class AtomicDatabaseSchemaMutationStorage;
    class Impl;
    std::unique_ptr<Impl> impl;
};

/// Fixed non-result control reservation for the bounded local-only directory
/// iterators, exact path/name scratch, and opaque discovery cursor. The
/// per-transaction charge below independently covers both resumable merge
/// images, every run cursor, and the final durable-ID vector.
inline constexpr UInt64 atomic_database_schema_mutation_discovery_fixed_control_bytes = 64ULL << 10;
inline constexpr UInt64 atomic_database_schema_mutation_discovery_control_bytes_per_transaction = 64;
inline constexpr UInt64 atomic_database_schema_mutation_default_recovery_control_bytes = 2ULL << 30;

struct AtomicDatabaseSchemaMutationCheckpointImage
{
    DatabaseSchemaWALCheckpoint checkpoint;
    String inventory_snapshot_bytes;
    String schema_graph_snapshot_bytes;
};

struct AtomicDatabaseSchemaMutationAuthorityRecordImage
{
    AuthorityInventoryKey key;
    String canonical_bytes;
};

struct AtomicDatabaseSchemaMutationDependentObjectImage
{
    SidecarExpectationRecord expectation;
    String object_name;
    String canonical_metadata_bytes;
    String canonical_sidecar_bytes;
    String canonical_installation_record_bytes;
};

struct AtomicDatabaseSchemaMutationReconciliation
{
    std::vector<AtomicDatabaseSchemaMutationAuthorityRecordImage> authority_records;
    std::vector<AtomicDatabaseSchemaMutationDependentObjectImage> dependent_objects;
};

/// Complete optional V2 database-owned policy extension. Each present byte
/// string is independently UUID-bound, versioned and checksummed by its
/// component codec.
struct AtomicDatabaseUDTPersistedConfigurationV2
{
    std::optional<String> verification_scheduler_override;
    std::optional<String> resource_quota_override;

    bool operator==(const AtomicDatabaseUDTPersistedConfigurationV2 &) const = default;
};

/// One crash-consistent scheduler/quota selection. The combined record is the
/// sole commit point; generation starts at one and advances only when a
/// configured component replacement changes the effective persisted bytes.
struct AtomicDatabaseUDTPersistedConfigurationGenerationV2
{
    UInt16 format_version = atomic_database_udt_configuration_format_version;
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 generation = 0;
    AtomicDatabaseUDTPersistedConfigurationV2 configuration;

    bool operator==(const AtomicDatabaseUDTPersistedConfigurationGenerationV2 &) const = default;
};

[[nodiscard]] String
encodeAtomicDatabaseUDTConfigurationGenerationV2(const AtomicDatabaseUDTPersistedConfigurationGenerationV2 & generation);
[[nodiscard]] AtomicDatabaseUDTPersistedConfigurationGenerationV2
decodeAtomicDatabaseUDTConfigurationGenerationV2(std::string_view bytes, UUID expected_database_uuid);

class AtomicDatabaseSchemaMutationStorage;
struct AtomicDatabaseUDTConfigurationCleanupState;

/// Rollback capability for configuration files staged before the first schema
/// transaction guard is issued. Destruction removes them only after re-proving
/// that no activation marker, Prepare, or other durable authority evidence can
/// own them; indeterminate durability therefore leaves recovery authoritative.
class PreparedAtomicDatabaseUDTConfigurationV2 final
{
public:
    PreparedAtomicDatabaseUDTConfigurationV2(const PreparedAtomicDatabaseUDTConfigurationV2 &) = delete;
    PreparedAtomicDatabaseUDTConfigurationV2 & operator=(const PreparedAtomicDatabaseUDTConfigurationV2 &) = delete;
    PreparedAtomicDatabaseUDTConfigurationV2(PreparedAtomicDatabaseUDTConfigurationV2 && other) noexcept;
    PreparedAtomicDatabaseUDTConfigurationV2 & operator=(PreparedAtomicDatabaseUDTConfigurationV2 &&) = delete;
    ~PreparedAtomicDatabaseUDTConfigurationV2();

    void disarmAfterDurableActivation() noexcept;

private:
    explicit PreparedAtomicDatabaseUDTConfigurationV2(std::shared_ptr<AtomicDatabaseUDTConfigurationCleanupState> cleanup_state_) noexcept;

    friend class AtomicDatabaseSchemaMutationStorage;
    std::shared_ptr<AtomicDatabaseUDTConfigurationCleanupState> cleanup_state;
};

/// Local DiskPtr implementation of the V1 schema-mutation durability
/// boundary. Callers still own the database-wide exclusive schema lock; each
/// issueMutationGuard() invalidates every earlier process-local guard.
class AtomicDatabaseSchemaMutationStorage final : public IDatabaseSchemaMutationDurableStorage
{
public:
    AtomicDatabaseSchemaMutationStorage(
        DiskPtr disk_, UUID database_uuid_, String metadata_root_, AtomicDatabaseSchemaMutationStorageLimits limits_ = {});
    AtomicDatabaseSchemaMutationStorage(
        DiskPtr disk_,
        UUID database_uuid_,
        String metadata_root_,
        String database_name_,
        AtomicDatabaseSchemaMutationStorageLimits limits_ = {});
    ~AtomicDatabaseSchemaMutationStorage() override;

    AtomicDatabaseSchemaMutationStorage(const AtomicDatabaseSchemaMutationStorage &) = delete;
    AtomicDatabaseSchemaMutationStorage & operator=(const AtomicDatabaseSchemaMutationStorage &) = delete;
    AtomicDatabaseSchemaMutationStorage(AtomicDatabaseSchemaMutationStorage &&) = delete;
    AtomicDatabaseSchemaMutationStorage & operator=(AtomicDatabaseSchemaMutationStorage &&) = delete;

    DatabaseSchemaMutationGuard issueMutationGuard();

    /// Read-only never-enabled probe. Absence of both the database-root
    /// activation marker and `types/` is false; a marker without its complete
    /// namespace, a committed namespace without its marker, or any other
    /// partial/unknown authority layout fails instead of silently downgrading.
    bool hasDurableAuthorityMarker() const;
    /// Narrow startup discriminator for policy reconciliation. This validates
    /// only the durable activation-marker image and deliberately returns false
    /// for a temporary-only first-activation marker: that state must enter the
    /// full WAL recovery path so the uncommitted activation can be rolled back.
    /// A true result is still followed by hasBoundedDurableAuthorityHead() in
    /// reconcileUDTConfigurationForActiveStartupV2().
    bool hasCompleteDurableActivationMarker() const;
    /// Bounded live-process probe used while the owning database schema mutex
    /// already serializes publication. It validates the activation marker and
    /// fixed directory spine only; startup/recovery must still use the complete
    /// hasDurableAuthorityMarker() audit of history and unknown entries.
    bool hasBoundedDurableAuthorityHead() const;
    /// Removes only a fully proved empty/partially-created authority scaffold.
    /// If `types/` is absent this performs no writes or directory creation;
    /// an exact empty types-only creation remnant is also safely removed.
    bool cleanupNeverEnabledScaffold();
    /// Stages the configured V2 policy set before the first schema mutation.
    /// The returned capability must be disarmed only after the epoch-1 Commit
    /// and activation marker are durable.
    [[nodiscard]] PreparedAtomicDatabaseUDTConfigurationV2
    prepareUDTConfigurationForFirstActivationV2(const AtomicDatabaseUDTPersistedConfigurationV2 & configured);
    /// Reconciles an already-active authority before constructing its root or
    /// scheduler. An absent configured component means Keep; a present byte
    /// string means Replace. Both components are committed in one generation.
    /// Exact legacy separate V2 records are migrated read-only when the
    /// combined record is absent.
    [[nodiscard]] AtomicDatabaseUDTPersistedConfigurationV2
    reconcileUDTConfigurationForActiveStartupV2(const AtomicDatabaseUDTPersistedConfigurationV2 & configured);
    /// Returns the currently committed selection before applying a configured
    /// Replace. Startup uses this to admission-check a genuinely new scheduler
    /// policy against the prospective quota tuple before committing it.
    [[nodiscard]] AtomicDatabaseUDTPersistedConfigurationV2 readUDTConfigurationForActiveStartupV2();
    /// Recovers an interrupted atomic cursor replacement and returns the last
    /// fully synchronized progress record. Absence is the canonical initial
    /// cursor; a cursor without an active durable authority fails closed.
    std::optional<AuthorityVerificationScheduleCursor> loadAuthorityVerificationCursor();
    /// Persists only structurally valid progress for this exact database. The
    /// caller owns database schema serialization and must publish the matching
    /// process-local cursor only after this barrier succeeds.
    void persistAuthorityVerificationCursor(const AuthorityVerificationScheduleCursor & cursor);
    UInt64 getDurableHighWaterMark() const;
    std::optional<AuthorityState> getCurrentAuthorityState() const;
    /// Process-local fail-stop identity set after an indeterminate durability
    /// phase. Durable recovery remains authoritative; this accessor only lets
    /// the owning live database select the exact transaction whose guard may
    /// clear the latch without admitting another mutation first.
    std::optional<UInt64> getRecoveryRequiredTransactionID() const noexcept;
    /// Complete startup/recovery enumeration. This preserves the original
    /// globally bounded, fully decoded and fail-closed WAL validation contract;
    /// automatic repair uses the private resumable lightweight API below.
    std::vector<UInt64> listDurableTransactionIDs() const;
    AtomicDatabaseSchemaMutationRecoveryTransaction loadTransactionForRecovery(
        UInt64 transaction_id,
        UInt64 maximum_total_staged_artifact_bytes = std::numeric_limits<UInt64>::max(),
        UInt64 maximum_staged_artifacts = std::numeric_limits<UInt64>::max(),
        UInt64 maximum_control_bytes = atomic_database_schema_mutation_default_recovery_control_bytes) const;
    std::optional<AtomicDatabaseSchemaMutationCheckpointImage> loadLatestCheckpoint() const;
    /// Newest authenticated exact-repair summary from the retained checkpoint
    /// plus its committed live WAL tail. No artifact bytes/source references
    /// are retained or exposed.
    std::optional<DatabaseSchemaWALExactRepairProvenance> loadLatestExactRepairProvenance() const;

    /// Read-only startup preflight for an already authenticated local-WAL
    /// exact-repair candidate. It returns true only when installing the exact
    /// After image would change its canonical target; unsafe paths still fail.
    bool startupExactRepairArtifactNeedsInstallation(const DatabaseSchemaWALStagedArtifact & artifact) const;

    /// Before a fresh schema transaction, checkpoint and compact a tail of at
    /// least 1024 committed transactions. The caller must hold the database's
    /// exclusive schema-mutation lock and pass its exact published root. Any
    /// interrupted checkpoint/compaction/GC is completed under an internally
    /// issued guard before this method returns.
    void maintainCheckpointBeforeMutation(const AuthorityRoot & current_root);

    /// Reads exactly the record set named by the anchored inventory and the
    /// dependent metadata/sidecars named by its expectations. Returned bytes
    /// are aggregate-bounded; directory names never become authority. Missing,
    /// extra, malformed, mismatched, or symbolic-link entries fail.
    AtomicDatabaseSchemaMutationReconciliation readAndReconcileAuthorityRecords(
        const AuthorityInventory & anchored_inventory, const SchemaObjectDependencyGraph & anchored_graph) const;

    /// Performs the same complete durable-integrity reconciliation as the
    /// full form above, but retains canonical bytes only for the selected
    /// dependent objects. Authority records and unselected object images are
    /// validated transiently instead of being accumulated in memory.
    AtomicDatabaseSchemaMutationReconciliation readAndReconcileAuthorityRecordsForObjects(
        const AuthorityInventory & anchored_inventory,
        const SchemaObjectDependencyGraph & anchored_graph,
        std::span<const SchemaObjectID> selected_objects) const;

    /// Removes every staging transaction for which the absence of a durable
    /// Prepare marker is reproved while the caller's schema guard is current.
    /// Prepared and committed staging is never removed here.
    UInt64 sweepUnpreparedStaging(const DatabaseSchemaMutationGuard & guard);
    UInt64 sweepRetiredTransactions(const DatabaseSchemaMutationGuard & guard);

    const AtomicDatabaseSchemaMutationPaths & getPaths() const noexcept;

    void validateMutationGuardAndDurablePredecessor(
        const DatabaseSchemaMutationGuard & guard,
        const std::optional<AuthorityState> & expected_preceding_authority_state,
        UInt64 transaction_id) override;
    void markMutationRecoveryRequired(
        const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id, DatabaseSchemaMutationDurabilityPhase phase) noexcept override;
    void validateRecoveryGuard(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) override;
    void stageArtifact(
        const DatabaseSchemaWALStagedArtifactLocator & locator,
        const DatabaseSchemaWALStagedArtifactRef & artifact,
        std::string_view canonical_bytes) override;
    void finishStaging(UUID database_uuid, UInt64 transaction_id) override;
    void persistPrepare(UInt64 transaction_id, std::string_view canonical_prepare) override;
    void installArtifact(const DatabaseSchemaWALStagedArtifactRef & artifact, std::string_view canonical_bytes) override;
    void removeArtifact(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) override;
    void finishInstallation(UUID database_uuid, UInt64 transaction_id) override;
    void persistCommit(UInt64 transaction_id, std::string_view canonical_commit) override;
    void finishRecovery(UUID database_uuid, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision) override;
    void discardUnpreparedStaging(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) override;
    void retireRolledBackTransaction(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) override;
    void persistValidatedCheckpoint(
        const DatabaseSchemaMutationGuard & guard,
        const DatabaseSchemaWALCheckpoint & checkpoint,
        std::string_view canonical_checkpoint,
        std::string_view canonical_inventory_snapshot,
        std::string_view canonical_schema_graph_snapshot) override;
    void
    compactThroughValidatedCheckpoint(const DatabaseSchemaMutationGuard & guard, const DatabaseSchemaWALCheckpoint & checkpoint) override;

private:
    friend class PreparedAtomicDatabaseUDTConfigurationV2;
    friend class AuthorityVerificationBatchExecutor;
    friend class AuthorityVerificationScheduler;
    friend class AuthorityRepairCoordinator;
    friend class AuthorityAutomaticRepair;
    friend class AuthorityAutomaticRepairAccess;

    /// Process-local sealed result of one root-addressed durable read. Only
    /// the owning storage can construct it and only the verification executor
    /// can inspect it; callers cannot substitute paths, bytes, or read costs.
    struct VerificationTargetRead
    {
        enum class State : UInt8
        {
            Complete,
            Damaged,
            Failed,
        };

        State state = State::Failed;
        std::optional<String> authority_record_bytes;
        std::optional<String> installation_record_bytes;
        std::optional<String> persisted_references_bytes;
        std::optional<String> metadata_bytes;
        UInt64 retained_bytes = 0;
        AuthorityVerificationTargetCost charged_cost;
    };

public:
    /// Root-addressed raw inputs for the trusted automatic-repair audit.  In
    /// contrast to periodic verification, every artifact path is derived from
    /// the immutable expectation even when the currently installed
    /// expectation or installation record is malformed.  The ordinary-object
    /// metadata path is accepted only after the rooted installation hash and
    /// record identity have been validated; no caller-provided name is used.
    struct RepairAuditTargetRead
    {
        std::optional<String> authority_record_bytes;
        std::optional<String> installation_record_bytes;
        std::optional<String> persisted_references_bytes;
        std::optional<String> metadata_bytes;
    };

private:
    VerificationTargetRead
    readAuthorityVerificationTarget(const AuthorityRoot & anchored_root, const ScheduledAuthorityVerificationTarget & target) const;
    RepairAuditTargetRead readAuthorityRepairAuditTarget(
        const AuthorityRoot & anchored_root, const AuthorityInventoryLeaf & leaf, UInt64 maximum_retained_bytes) const;
    /// The friend caller holds the database schema-mutation fence and re-proves
    /// this exact durable root before every scheduler invocation. The storage
    /// additionally pins that identity/limits in the cursor and exact-validates
    /// the namespace with a second resumable pass before yielding any IDs.
    std::optional<std::vector<UInt64>> resumeDurableTransactionIDDiscoveryForAuthorityRepair(
        AtomicDatabaseSchemaMutationDurableTransactionDiscovery & continuation,
        const AuthorityRootGraphIdentity & root,
        UInt64 maximum_transactions,
        UInt64 maximum_control_bytes,
        const AuthorityVerificationPassBudget & pass_budget) const;
    std::optional<AtomicDatabaseSchemaMutationRecoveryTransaction> loadCommittedTransactionForAuthorityRepair(
        UInt64 transaction_id,
        UInt64 maximum_total_staged_artifact_bytes,
        UInt64 maximum_staged_artifacts,
        UInt64 maximum_control_bytes) const;
    std::vector<AuthorityVerificationTarget> snapshotAuthorityVerificationTargets(
        const AuthorityRoot & anchored_root,
        const AuthorityVerificationScheduleLimits & schedule_limits,
        const AuthorityVerificationBatchExecutorLimits & executor_limits,
        UInt64 begin_target,
        UInt64 maximum_targets,
        std::span<const AuthorityVerificationTargetHistory> history = {},
        const AuthorityVerificationPassBudget & pass_budget = {},
        UInt64 * consumed_work_items = nullptr) const;

    class Impl;
    std::unique_ptr<Impl> impl;
    std::shared_ptr<AtomicDatabaseUDTConfigurationCleanupState> configuration_cleanup_state;

    void discardPreparedUDTConfigurationV2IfInactiveNoThrow() noexcept;
};

}
