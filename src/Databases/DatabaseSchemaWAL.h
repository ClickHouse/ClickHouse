#pragma once

#include <Databases/SchemaObjectDependencyGraph.h>

#include <DataTypes/UDT/AuthorityInventory.h>
#include <DataTypes/UDT/AuthorityInventorySnapshot.h>
#include <DataTypes/UDT/AuthorityState.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/Record.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRoot;

inline constexpr UInt16 database_schema_wal_format_version = 1;

enum class DatabaseSchemaWALRecordKind : UInt8
{
    Prepare = 1,
    Commit = 2,
    Checkpoint = 3,
};

enum class DatabaseSchemaWALStagedArtifactKind : UInt8
{
    TypeDefinitionRecord = 1,
    SidecarExpectationRecord = 2,
    DependentObjectMetadata = 3,
    PersistedTypeReferencesSidecar = 4,
    DependentObjectMetadataInstallationRecord = 5,
};

enum class DatabaseSchemaWALStagedArtifactImage : UInt8
{
    Before = 1,
    After = 2,
};

struct DatabaseSchemaWALAuthorityRecordState
{
    UInt64 object_revision = 0;
    Digest canonical_record_hash{};

    bool operator==(const DatabaseSchemaWALAuthorityRecordState &) const = default;
};

struct DatabaseSchemaWALAuthorityRecordDelta
{
    AuthorityInventoryKey key;
    std::optional<DatabaseSchemaWALAuthorityRecordState> before;
    std::optional<DatabaseSchemaWALAuthorityRecordState> after;

    bool operator==(const DatabaseSchemaWALAuthorityRecordDelta &) const = default;
};

struct DatabaseSchemaWALDependentObjectState
{
    UInt64 object_schema_revision = 0;
    Digest metadata_hash{};
    std::optional<Digest> sidecar_record_hash;
    std::optional<Digest> expectation_record_hash;

    bool operator==(const DatabaseSchemaWALDependentObjectState &) const = default;
};

struct DatabaseSchemaWALDependentObjectDelta
{
    SchemaObjectID object;
    std::optional<DatabaseSchemaWALDependentObjectState> before;
    std::optional<DatabaseSchemaWALDependentObjectState> after;

    bool operator==(const DatabaseSchemaWALDependentObjectDelta &) const = default;
};

/// Canonical manifest entry. Its zero-based position is the durable ordinal;
/// the storage locator is derived only from database UUID, transaction ID and
/// that ordinal. No path bytes are part of the permanent record.
struct DatabaseSchemaWALStagedArtifactRef
{
    DatabaseSchemaWALStagedArtifactKind kind{};
    DatabaseSchemaWALStagedArtifactImage image{};
    SchemaObjectID object;
    UInt64 revision = 0;
    UInt64 byte_size = 0;
    Digest content_hash{};

    bool operator==(const DatabaseSchemaWALStagedArtifactRef &) const = default;
};

/// Writer-side value. `canonical_bytes` are staged outside the WAL before its
/// Prepare marker is made durable; only the derived reference is serialized.
struct DatabaseSchemaWALStagedArtifact
{
    DatabaseSchemaWALStagedArtifactKind kind{};
    DatabaseSchemaWALStagedArtifactImage image{};
    SchemaObjectID object;
    UInt64 revision = 0;
    String canonical_bytes;
};

struct DatabaseSchemaWALStagedArtifactLocator
{
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 transaction_id = 0;
    UInt64 ordinal = 0;

    bool operator==(const DatabaseSchemaWALStagedArtifactLocator &) const = default;
};

/// Fixed-size, authenticated operator provenance for one content-neutral
/// exact repair. Source references and canonical artifact bytes deliberately
/// stay outside the permanent record; the manifest digest commits the exact
/// damaged target set selected by the trusted repair planner.
struct DatabaseSchemaWALExactRepairProvenance
{
    UInt64 transaction_id = 0;
    UInt64 damaged_artifact_count = 0;
    Digest damaged_artifact_manifest_digest{};
    UInt64 local_wal_sources = 0;
    UInt64 replicated_authority_sources = 0;
    UInt64 verified_backup_sources = 0;
    UInt64 previous_catalog_epoch = 0;
    Digest previous_authority_anchor{};
    UInt64 repaired_catalog_epoch = 0;
    Digest repaired_authority_anchor{};

    bool operator==(const DatabaseSchemaWALExactRepairProvenance &) const = default;
};

struct DatabaseSchemaWALPrepare
{
    UInt16 format_version = database_schema_wal_format_version;
    UInt64 transaction_id = 0;
    /// Explicit extension-bit contract. An exact-repair transition advances
    /// only the authority epoch and durably reinstalls canonical artifacts
    /// already addressed by the unchanged authority inventory. It is never
    /// inferred from an otherwise ordinary empty mutation.
    bool exact_repair = false;
    /// New writers always attach this bounded authenticated summary to an
    /// exact repair. It remains optional so already-written V1 exact-repair
    /// records retain their original canonical meaning and stay recoverable.
    std::optional<DatabaseSchemaWALExactRepairProvenance> exact_repair_provenance;
    std::optional<AuthorityState> before_authority_state;
    AuthorityState after_authority_state;
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_record_deltas;
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_object_deltas;
    SchemaObjectDependencyGraphMutation graph_delta;
    std::vector<DatabaseSchemaWALStagedArtifactRef> staged_artifacts;
    Digest prepare_hash{};

    bool operator==(const DatabaseSchemaWALPrepare & other) const;
};

struct DatabaseSchemaWALCommit
{
    UInt16 format_version = database_schema_wal_format_version;
    UInt64 transaction_id = 0;
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 database_catalog_epoch = 0;
    Digest inventory_root{};
    Digest schema_graph_root{};
    Digest authority_anchor{};
    Digest prepare_hash{};
    Digest commit_hash{};

    bool operator==(const DatabaseSchemaWALCommit &) const = default;
};

/// The full covered commit remains inside the durable marker, so its proof is
/// still checkable after the covered WAL prefix is removed.
struct DatabaseSchemaWALCheckpoint
{
    UInt16 format_version = database_schema_wal_format_version;
    UInt64 checkpoint_id = 0;
    DatabaseSchemaWALCommit covered_commit;
    AuthorityState authority_state;
    Digest inventory_snapshot_hash{};
    Digest schema_graph_snapshot_hash{};
    /// Carries the newest covered exact-repair summary across WAL-prefix
    /// compaction. This is derived authenticated history, never authority.
    std::optional<DatabaseSchemaWALExactRepairProvenance> last_exact_repair_provenance;
    Digest checkpoint_hash{};

    bool operator==(const DatabaseSchemaWALCheckpoint &) const = default;
};

struct DatabaseSchemaWALLimits
{
    AuthorityStateLimits authority_state;
    AuthorityInventorySnapshotLimits inventory_snapshot;
    SchemaObjectDependencyGraphLimits schema_graph;
    RecordLimits definition_record;
    PersistedTypeReferencesLimits persisted_references;
    DependentObjectMetadataInstallationRecordLimits installation_record;
    UInt64 maximum_authority_record_deltas = 200'000;
    UInt64 maximum_dependent_object_deltas = 200'000;
    UInt64 maximum_graph_node_deltas = 200'000;
    UInt64 maximum_graph_edge_deltas = 4'194'304;
    UInt64 maximum_staged_artifacts = 1'200'000;
    UInt64 maximum_staged_artifact_bytes = 16ULL << 20;
    UInt64 maximum_total_staged_artifact_bytes = 512ULL << 20;
    UInt64 maximum_encoded_bytes = 512ULL << 20;
    /// Aggregate logical bytes retained by decoded Prepare vector elements.
    /// The encoded input buffer is charged independently by its owner.
    UInt64 maximum_decode_control_bytes = 1ULL << 30;
};

/// Fixed transient validation/anchor scratch which coexists with the decoded
/// Prepare object and all of its retained vectors.
inline constexpr UInt64 database_schema_wal_prepare_decode_transient_control_bytes = 4ULL << 10;
inline constexpr UInt64 database_schema_wal_prepare_minimum_decode_control_bytes
    = sizeof(DatabaseSchemaWALPrepare) + database_schema_wal_prepare_decode_transient_control_bytes;

class DatabaseSchemaWALError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        Truncated,
        UnsupportedVersion,
        UnknownRecordKind,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        TrailingData,
        DuplicateDelta,
        ConflictingDelta,
        MissingArtifact,
        UnexpectedArtifact,
        ArtifactMismatch,
        DigestMismatch,
        TransitionMismatch,
    };

    DatabaseSchemaWALError(Code code_, std::string_view message);

    const Code code;
};

struct DatabaseSchemaWALTransitionBase
{
    std::optional<AuthorityState> authority_state;
    AuthorityInventory::Ptr authority_inventory;
    SchemaObjectDependencyGraph::Ptr schema_graph;
};

/// Work performed by the specialized content-neutral activation path. These
/// counters stay zero by contract: the pinned immutable root already proves
/// its inventory and graph anchors, so activation must not enumerate either
/// payload.
struct DatabaseSchemaWALDependentObjectActivationStatistics
{
    UInt64 inventory_leaves_visited = 0;
    UInt64 graph_nodes_visited = 0;
    UInt64 graph_edges_visited = 0;

    bool operator==(const DatabaseSchemaWALDependentObjectActivationStatistics &) const = default;
};

class DatabaseSchemaWALValidatedTransition final
{
public:
    DatabaseSchemaWALValidatedTransition(const DatabaseSchemaWALValidatedTransition &) = delete;
    DatabaseSchemaWALValidatedTransition & operator=(const DatabaseSchemaWALValidatedTransition &) = delete;
    DatabaseSchemaWALValidatedTransition(DatabaseSchemaWALValidatedTransition &&) noexcept = default;
    DatabaseSchemaWALValidatedTransition & operator=(DatabaseSchemaWALValidatedTransition &&) noexcept = default;

    const DatabaseSchemaWALPrepare & getPrepare() const noexcept { return prepare; }
    const AuthorityInventory & getAfterInventory() const noexcept { return *after_inventory; }
    const SchemaObjectDependencyGraph & getAfterGraph() const noexcept { return *after_graph; }
    /// O(1) writer/recovery pins for chaining validated transitions. Callers
    /// must never rebuild either root from a directory listing.
    AuthorityInventory::Ptr pinAfterInventory() const noexcept { return after_inventory; }
    SchemaObjectDependencyGraph::Ptr pinAfterGraph() const noexcept { return after_graph; }
    std::span<const String> getStagedArtifactBytes() const noexcept { return staged_artifact_bytes; }

private:
    DatabaseSchemaWALValidatedTransition(
        DatabaseSchemaWALPrepare prepare_,
        AuthorityInventory::Ptr after_inventory_,
        SchemaObjectDependencyGraph::Ptr after_graph_,
        std::vector<String> staged_artifact_bytes_);

    friend class DatabaseSchemaWALTransitionBuilder;

    DatabaseSchemaWALPrepare prepare;
    AuthorityInventory::Ptr after_inventory;
    SchemaObjectDependencyGraph::Ptr after_graph;
    std::vector<String> staged_artifact_bytes;
};

class DatabaseSchemaWALTransitionBuilder final
{
public:
    /// Build the exact permanent definition-only -> dependent-object-capable prepare record while sharing
    /// the trusted root's immutable content pins. Unlike the generic mutation
    /// builder this path never reconstructs inventory or hashes the graph.
    static DatabaseSchemaWALValidatedTransition buildDependentObjectActivation(
        UInt64 transaction_id,
        const AuthorityRoot & definition_only_root,
        const DatabaseSchemaWALLimits & limits = {},
        DatabaseSchemaWALDependentObjectActivationStatistics * statistics = nullptr);

    /// dependent-object-capable physicalization fast path. `after_root` carries an internal
    /// proof that its persistent inventory/graph/record/catalog values were
    /// derived from `before_root` by these exact removal deltas. The WAL
    /// therefore consumes the already-computed pins and does not rebuild the
    /// same mutation.
    static DatabaseSchemaWALValidatedTransition buildPhysicalization(
        UInt64 transaction_id,
        const AuthorityRoot & before_root,
        const AuthorityRoot & after_root,
        std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_record_deltas,
        std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_object_deltas,
        SchemaObjectDependencyGraphMutation graph_delta,
        std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
        const DatabaseSchemaWALLimits & limits = {});

    /// Builds a content-neutral exact-repair transaction. Every supplied
    /// artifact must be an After image matching the unchanged pinned
    /// inventory; a persisted sidecar must be accompanied by its exact rooted
    /// expectation record. Recovery deliberately reinstalls the same exact
    /// images even when the Commit marker is absent, because removing or
    /// restoring a corrupt preimage would violate the anchored authority.
    static DatabaseSchemaWALValidatedTransition buildExactRepair(
        UInt64 transaction_id,
        const AuthorityRoot & before_root,
        const AuthorityRoot & after_root,
        std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
        const DatabaseSchemaWALLimits & limits = {},
        std::optional<DatabaseSchemaWALExactRepairProvenance> provenance = std::nullopt);

    /// Startup variant used before an AuthorityRoot can be activated. The
    /// anchored state/inventory/graph pins are the exact recovered WAL head;
    /// the caller supplies its content-neutral epoch successor. This does not
    /// relax exact-repair validation or reconstruct authority from directory
    /// contents.
    static DatabaseSchemaWALValidatedTransition buildExactRepair(
        UInt64 transaction_id,
        const DatabaseSchemaWALTransitionBase & before,
        AuthorityState after_authority_state,
        std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
        const DatabaseSchemaWALLimits & limits = {},
        std::optional<DatabaseSchemaWALExactRepairProvenance> provenance = std::nullopt);

    static DatabaseSchemaWALValidatedTransition build(
        UInt64 transaction_id,
        const DatabaseSchemaWALTransitionBase & base,
        AuthorityState after_authority_state,
        std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_record_deltas,
        std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_object_deltas,
        SchemaObjectDependencyGraphMutation graph_delta,
        std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts,
        const DatabaseSchemaWALLimits & limits = {});

    static DatabaseSchemaWALValidatedTransition validateDecoded(
        DatabaseSchemaWALPrepare prepare,
        const DatabaseSchemaWALTransitionBase & base,
        std::vector<String> staged_artifact_bytes,
        const DatabaseSchemaWALLimits & limits = {});

private:
    DatabaseSchemaWALTransitionBuilder() = delete;
};

bool isDatabaseSchemaWALExactRepair(const DatabaseSchemaWALPrepare & prepare) noexcept;

class DatabaseSchemaWALValidatedCheckpoint final
{
public:
    DatabaseSchemaWALValidatedCheckpoint(const DatabaseSchemaWALValidatedCheckpoint &) = delete;
    DatabaseSchemaWALValidatedCheckpoint & operator=(const DatabaseSchemaWALValidatedCheckpoint &) = delete;
    DatabaseSchemaWALValidatedCheckpoint(DatabaseSchemaWALValidatedCheckpoint &&) noexcept = default;
    DatabaseSchemaWALValidatedCheckpoint & operator=(DatabaseSchemaWALValidatedCheckpoint &&) noexcept = default;

    const DatabaseSchemaWALCheckpoint & getCheckpoint() const noexcept { return checkpoint; }
    const String & getInventorySnapshotBytes() const noexcept { return inventory_snapshot_bytes; }
    const String & getSchemaGraphSnapshotBytes() const noexcept { return schema_graph_snapshot_bytes; }
    AuthorityInventory::Ptr pinInventory() const noexcept { return inventory; }
    SchemaObjectDependencyGraph::Ptr pinSchemaGraph() const noexcept { return schema_graph; }

private:
    DatabaseSchemaWALValidatedCheckpoint(
        DatabaseSchemaWALCheckpoint checkpoint_,
        String inventory_snapshot_bytes_,
        String schema_graph_snapshot_bytes_,
        AuthorityInventory::Ptr inventory_,
        SchemaObjectDependencyGraph::Ptr schema_graph_);

    friend class DatabaseSchemaWALCheckpointBuilder;

    DatabaseSchemaWALCheckpoint checkpoint;
    String inventory_snapshot_bytes;
    String schema_graph_snapshot_bytes;
    AuthorityInventory::Ptr inventory;
    SchemaObjectDependencyGraph::Ptr schema_graph;
};

class DatabaseSchemaWALCheckpointBuilder final
{
public:
    static DatabaseSchemaWALValidatedCheckpoint build(
        UInt64 checkpoint_id,
        DatabaseSchemaWALCommit covered_commit,
        AuthorityState authority_state,
        AuthorityInventory::Ptr authority_inventory,
        SchemaObjectDependencyGraph::Ptr schema_graph,
        const DatabaseSchemaWALLimits & limits = {},
        std::optional<DatabaseSchemaWALExactRepairProvenance> last_exact_repair_provenance = std::nullopt);

    static DatabaseSchemaWALValidatedCheckpoint validateDecoded(
        DatabaseSchemaWALCheckpoint checkpoint,
        std::string_view inventory_snapshot_bytes,
        std::string_view schema_graph_snapshot_bytes,
        const DatabaseSchemaWALLimits & limits = {});

private:
    DatabaseSchemaWALCheckpointBuilder() = delete;
};

Digest computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind kind, std::string_view canonical_bytes);
/// Canonical fixed-memory fallback manifest for startup repairs which are
/// selected directly from authenticated local WAL images before a full
/// runtime repair audit can exist.
Digest computeDatabaseSchemaWALExactRepairArtifactManifestDigest(
    std::span<const DatabaseSchemaWALStagedArtifact> artifacts, const DatabaseSchemaWALLimits & limits = {});
DatabaseSchemaWALStagedArtifactLocator
makeDatabaseSchemaWALStagedArtifactLocator(UUID database_uuid, UInt64 transaction_id, UInt64 ordinal);

Digest computeDatabaseSchemaWALPrepareHash(const DatabaseSchemaWALPrepare & prepare, const DatabaseSchemaWALLimits & limits = {});
Digest computeDatabaseSchemaWALCommitHash(const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALLimits & limits = {});
Digest
computeDatabaseSchemaWALCheckpointHash(const DatabaseSchemaWALCheckpoint & checkpoint, const DatabaseSchemaWALLimits & limits = {});

DatabaseSchemaWALCommit
makeDatabaseSchemaWALCommit(const DatabaseSchemaWALValidatedTransition & transition, const DatabaseSchemaWALLimits & limits = {});

String encodeDatabaseSchemaWALPrepare(const DatabaseSchemaWALPrepare & prepare, const DatabaseSchemaWALLimits & limits = {});
String encodeDatabaseSchemaWALCommit(const DatabaseSchemaWALCommit & commit, const DatabaseSchemaWALLimits & limits = {});
String encodeDatabaseSchemaWALCheckpoint(const DatabaseSchemaWALCheckpoint & checkpoint, const DatabaseSchemaWALLimits & limits = {});

DatabaseSchemaWALPrepare decodeDatabaseSchemaWALPrepare(std::string_view bytes, const DatabaseSchemaWALLimits & limits = {});
/// Retained decoded Prepare object/vector control charge. The decoder's fixed
/// transient validation scratch and encoded/canonical payload buffer are
/// independently bounded and deliberately excluded.
UInt64 getDatabaseSchemaWALPrepareDecodedControlBytes(const DatabaseSchemaWALPrepare & prepare);
DatabaseSchemaWALCommit decodeDatabaseSchemaWALCommit(std::string_view bytes, const DatabaseSchemaWALLimits & limits = {});
DatabaseSchemaWALCheckpoint decodeDatabaseSchemaWALCheckpoint(std::string_view bytes, const DatabaseSchemaWALLimits & limits = {});

void validateDatabaseSchemaWALCommit(
    const DatabaseSchemaWALValidatedTransition & transition,
    const DatabaseSchemaWALCommit & commit,
    const DatabaseSchemaWALLimits & limits = {});

enum class DatabaseSchemaWALRecoveryDecision : UInt8
{
    RollBackPrepared = 1,
    CompleteCommitted = 2,
};

DatabaseSchemaWALRecoveryDecision decideDatabaseSchemaWALRecovery(
    const DatabaseSchemaWALValidatedTransition & transition,
    const std::optional<DatabaseSchemaWALCommit> & commit,
    const DatabaseSchemaWALLimits & limits = {});

}
