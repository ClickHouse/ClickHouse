#pragma once

#include <Databases/UDT/AuthorityResourceUsageIndex.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>
#include <Databases/UDT/DatabaseResourceQuota.h>

#include <Databases/SchemaObjectDependencyGraph.h>

#include <DataTypes/UDT/AuthorityInventory.h>
#include <DataTypes/UDT/AuthorityState.h>
#include <DataTypes/UDT/Catalog.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Core/Types.h>

#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

struct AuthorityRecordStore;

class AuthorityRootError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidAuthorityState,
        DatabaseMismatch,
        InvalidDefinition,
        InvalidRecord,
        DuplicateRecordIdentity,
        RecordDefinitionMismatch,
        InventoryMismatch,
        GraphMismatch,
        LimitExceeded,
    };

    AuthorityRootError(Code code_, std::string_view message);

    const Code code;
};

/// Limits for one complete immutable database authority value. Canonical
/// record bytes count both definition records and fixed-size expectations.
struct AuthorityRootBuildLimits
{
    TypeCatalogBuildLimits type_catalog;
    RecordLimits definition_record;
    AuthorityInventoryLimits inventory;
    AuthorityStateLimits authority_state;
    AuthorityResourceUsageIndexLimits resource_usage_index;
    UInt64 maximum_definition_records = 100'000;
    UInt64 maximum_expectation_records = 100'000;
    UInt64 maximum_canonical_record_bytes = resource_implementation_maximum_deterministic_catalog_bytes;
    /// Process-local policy domain used to account the exact planner/bucket
    /// requirement in the immutable root quota image. It is not canonical
    /// authority data and therefore does not alter the durable root ABI.
    AuthorityVerificationSchedulePolicy verification_policy;
    UInt64 verification_maximum_targets_per_batch = 1'024;
};

/// Exact work of one dependent-object admission edit. Every counter is
/// bounded by the fixed V1 key depths plus the number of added dependency
/// edges; none depends on the number of untouched authority records.
struct DependentObjectAdmissionDeltaStatistics
{
    AuthorityInventoryMutationStatistics inventory;
    SchemaObjectDependencyGraphMutationStatistics graph;
    UInt64 expectation_record_deltas_applied = 0;
    UInt64 expectation_record_nodes_visited = 0;
    UInt64 expectation_record_nodes_created = 0;
    UInt64 expectation_record_nodes_hashed = 0;
    UInt64 expectation_records_materialized = 0;
};

/// Complete immutable definition-only or dependent-object-capable database authority value. The database
/// catalog epoch and the in-memory type-index generation are intentionally
/// independent: capability activation may advance the former without
/// rebuilding or advancing the latter.
class AuthorityRoot final
{
    struct ContentPayload final
    {
        struct PhysicalizationProvenance final
        {
            Digest base_authority_anchor{};
            Digest authority_removal_keys_digest{};
            Digest graph_delta_digest{};
        };

        UInt64 type_index_generation;
        Digest type_index_content_digest;
        std::shared_ptr<const TypeCatalogRoot> type_catalog;
        AuthorityInventory::Ptr inventory;
        SchemaObjectDependencyGraph::Ptr schema_graph;
        std::shared_ptr<const AuthorityRecordStore> records;
        AuthorityResourceUsageIndex::Ptr resource_usage_index;
        DatabaseResourceQuotaSnapshot::Ptr database_resource_quota;
        AuthorityVerificationSchedulePolicy verification_policy;
        UInt64 verification_maximum_targets_per_batch;
        UInt64 logical_charge;
        std::optional<PhysicalizationProvenance> physicalization_provenance;
    };

    using ContentPayloadPtr = std::shared_ptr<const ContentPayload>;

public:
    using Ptr = std::unique_ptr<const AuthorityRoot>;

    AuthorityRoot(const AuthorityRoot &) = delete;
    AuthorityRoot & operator=(const AuthorityRoot &) = delete;
    AuthorityRoot(AuthorityRoot &&) = delete;
    AuthorityRoot & operator=(AuthorityRoot &&) = delete;
    ~AuthorityRoot();

    const UUID & getDatabaseUUID() const noexcept { return authority_state.database_uuid; }
    UInt64 getDatabaseCatalogEpoch() const noexcept { return authority_state.database_catalog_epoch; }
    UInt64 getTypeIndexGeneration() const noexcept { return content_payload->type_index_generation; }
    const Digest & getTypeIndexContentDigest() const noexcept { return content_payload->type_index_content_digest; }
    UInt64 getPersistentCapabilityMask() const noexcept { return authority_state.persistent_capability_mask; }
    const AuthorityState & getAuthorityState() const noexcept { return authority_state; }
    const AuthorityInventorySummary & getInventorySummary() const noexcept { return content_payload->inventory->getSummary(); }
    const AuthorityResourceUsageSummary & getResourceUsageSummary() const noexcept
    {
        return content_payload->resource_usage_index->getSummary();
    }
    const DatabaseResourceQuotaSnapshot & getDatabaseResourceQuota() const noexcept { return *content_payload->database_resource_quota; }

    /// Writer/admin pins used while preparing one schema mutation. They keep
    /// the exact anchored components from this immutable root; callers must
    /// never reconstruct either component from a directory listing.
    AuthorityInventory::Ptr pinAuthorityInventory() const noexcept { return content_payload->inventory; }
    SchemaObjectDependencyGraph::Ptr pinSchemaObjectDependencyGraph() const noexcept { return content_payload->schema_graph; }
    DatabaseResourceQuotaSnapshot::Ptr pinDatabaseResourceQuota() const noexcept { return content_payload->database_resource_quota; }
    AuthorityResourceUsageIndex::Ptr pinResourceUsageIndex() const noexcept { return content_payload->resource_usage_index; }

    Definition::Ptr findByIdentity(const DefinitionIdentity & identity) const;
    Definition::Ptr findByName(std::string_view normalized_local_name) const;

    std::span<const Record> getDefinitionRecords() const;
    std::span<const SidecarExpectationRecord> getExpectationRecords() const;
    const Record * findDefinitionRecord(const DefinitionIdentity & identity) const noexcept;
    const Record * findDefinitionRecord(UUID type_uuid) const noexcept;
    const SidecarExpectationRecord * findExpectationRecord(const SchemaObjectID & object) const noexcept;
    /// Resolves the object kind omitted from an inventory sidecar key by
    /// probing the closed V1 kind registry against the immutable radix store.
    /// A corrupt ambiguous store fails closed with nullptr.
    const SidecarExpectationRecord * findExpectationRecord(UUID object_uuid) const noexcept;
    UInt64 getDefinitionRecordCount() const noexcept;
    UInt64 getExpectationRecordCount() const noexcept;

    const SchemaObjectDependencyGraph & getSchemaObjectDependencyGraph() const noexcept { return *content_payload->schema_graph; }

    /// Returns the only V1 content-neutral activation value. The clone owns a
    /// new authority-state wrapper but shares every immutable content object,
    /// record vector, generation, and content digest with this root.
    [[nodiscard]] Ptr cloneWithAuthorityState(AuthorityState next_authority_state, const AuthorityStateLimits & limits = {}) const;

    /// Creates the exact epoch successor used after durable canonical-artifact
    /// repair. All content pins, capability bits, inventory/graph anchors,
    /// catalog generations, quota state and record stores are shared; only the
    /// authority epoch and its derived anchor advance.
    [[nodiscard]] Ptr cloneForExactRepair(const AuthorityStateLimits & limits = {}) const;

    /// Pure quota-policy successor. It keeps every canonical authority
    /// component and exact usage, advances the catalog epoch, and applies the
    /// next effective database tuple through the ordinary OVER_QUOTA
    /// transition. The caller must durably bind the separate V2 policy record
    /// to the same schema transaction before publication.
    [[nodiscard]] Ptr cloneWithDatabaseResourceLimits(
        UInt64 next_database_catalog_epoch, EffectiveResourceLimits next_limits, const AuthorityStateLimits & limits = {}) const;

    /// Rebinds the process-local quota accounting domain during startup,
    /// before the root is published. Canonical authority state and quota
    /// revision/limits stay unchanged; usage is recomputed for the selected
    /// durable scheduler policy and may become OVER_QUOTA after a decrease.
    [[nodiscard]] Ptr cloneWithVerificationPlanningDomainForStartup(
        const AuthorityVerificationSchedulePolicy & policy, UInt64 maximum_targets_per_batch) const;

    /// O(1) publication/accounting predicate. Equal rebuilt content does not
    /// qualify: capability activation must preserve this exact payload.
    bool sharesContentPayloadWith(const AuthorityRoot & other) const noexcept { return content_payload == other.content_payload; }

    /// O(1) structural-sharing predicate for dependent-object-only edits.
    /// The type catalog and definition-record radix root must be reused, not
    /// reconstructed into merely equal values.
    bool sharesDefinitionContentWith(const AuthorityRoot & other) const noexcept;

    /// O(touched) proof consumed by the schema-WAL builder so it can reuse the
    /// already-computed persistent roots without applying the same delta a
    /// second time.
    bool provesPhysicalizationDeltaFrom(
        const AuthorityRoot & base,
        std::span<const AuthorityInventoryKey> sorted_removal_keys,
        const SchemaObjectDependencyGraphMutation & graph_delta) const;

    UInt64 getContentPayloadLogicalCharge() const noexcept { return content_payload->logical_charge; }
    static constexpr UInt64 getWrapperLogicalCharge() noexcept { return sizeof(AuthorityRoot); }
    static constexpr UInt64 getContentPayloadBaseLogicalCharge() noexcept { return sizeof(ContentPayload) + 2 * sizeof(void *); }

private:
    AuthorityRoot(AuthorityState authority_state_, ContentPayloadPtr content_payload_);

    friend class AuthorityRootBuilder;

    const AuthorityState authority_state;
    const ContentPayloadPtr content_payload;
};

class AuthorityRootBuilder final
{
public:
    /// Maximum incremental record-store logical charge of adding one
    /// dependent-object expectation. The replacement-root estimator combines
    /// this with the independently bounded inventory and graph mutations.
    static UInt64 getExpectationRecordInsertionLogicalChargeUpperBound();

    /// Reconstructs already-durable authority content. Usage above the
    /// currently supported default layer is retained as OVER_QUOTA so only
    /// neutral or shrinking successors remain admissible.
    [[nodiscard]] static AuthorityRoot::Ptr build(
        AuthorityState authority_state,
        UInt64 type_index_generation,
        std::span<const Definition::Ptr> checked_definitions,
        std::span<const Record> definition_records,
        std::span<const SidecarExpectationRecord> expectation_records,
        SchemaObjectDependencyGraph::Ptr schema_graph,
        const AuthorityRootBuildLimits & limits = {},
        std::span<const AuthorityDependentObjectResourceImage> dependent_objects = {});

    /// Recovery with the exact implementation/server/persisted-database/
    /// authority-adapter minimum selected by the owning database. Existing
    /// usage may exceed it and is then retained as OVER_QUOTA.
    [[nodiscard]] static AuthorityRoot::Ptr build(
        AuthorityState authority_state,
        UInt64 type_index_generation,
        std::span<const Definition::Ptr> checked_definitions,
        std::span<const Record> definition_records,
        std::span<const SidecarExpectationRecord> expectation_records,
        SchemaObjectDependencyGraph::Ptr schema_graph,
        const EffectiveResourceLimits & effective_database_limits,
        const AuthorityRootBuildLimits & limits = {},
        std::span<const AuthorityDependentObjectResourceImage> dependent_objects = {});

    /// Admits the first authority root for a never-enabled database. Unlike
    /// recovery, new durable content must be within every effective quota.
    [[nodiscard]] static AuthorityRoot::Ptr buildInitialAdmission(
        AuthorityState authority_state,
        UInt64 type_index_generation,
        std::span<const Definition::Ptr> checked_definitions,
        std::span<const Record> definition_records,
        std::span<const SidecarExpectationRecord> expectation_records,
        SchemaObjectDependencyGraph::Ptr schema_graph,
        const AuthorityRootBuildLimits & limits = {},
        std::span<const AuthorityDependentObjectResourceImage> dependent_objects = {});

    [[nodiscard]] static AuthorityRoot::Ptr buildInitialAdmission(
        AuthorityState authority_state,
        UInt64 type_index_generation,
        std::span<const Definition::Ptr> checked_definitions,
        std::span<const Record> definition_records,
        std::span<const SidecarExpectationRecord> expectation_records,
        SchemaObjectDependencyGraph::Ptr schema_graph,
        const EffectiveResourceLimits & effective_database_limits,
        const AuthorityRootBuildLimits & limits = {},
        std::span<const AuthorityDependentObjectResourceImage> dependent_objects = {});

    /// Full content replacement from one exact published base. Unlike the
    /// recovery-only `build`, this path advances the immutable database quota
    /// image through one prospective base-to-replacement transition.
    [[nodiscard]] static AuthorityRoot::Ptr buildReplacement(
        const AuthorityRoot & base,
        AuthorityState authority_state,
        UInt64 type_index_generation,
        std::span<const Definition::Ptr> checked_definitions,
        std::span<const Record> definition_records,
        std::span<const SidecarExpectationRecord> expectation_records,
        SchemaObjectDependencyGraph::Ptr schema_graph,
        const AuthorityRootBuildLimits & limits = {},
        std::span<const AuthorityDependentObjectResourceImage> dependent_objects = {});

    /// dependent-object-capable physicalization-only persistent delta. It path-copies the
    /// inventory, graph, record stores and affected catalog shards from the
    /// exact pinned root; untouched catalog/record/radix state is shared.
    [[nodiscard]] static AuthorityRoot::Ptr buildPhysicalizationDelta(
        const AuthorityRoot & base,
        UInt64 next_database_catalog_epoch,
        std::span<const DefinitionIdentity> definition_removals,
        std::span<const SchemaObjectID> expectation_removals,
        const SchemaObjectDependencyGraphMutation & graph_delta,
        const AuthorityRootBuildLimits & limits = {},
        AuthorityInventoryMutationStatistics * inventory_statistics = nullptr,
        SchemaObjectDependencyGraphMutationStatistics * graph_statistics = nullptr);

    /// Adds one dependent-object expectation and its graph node/edges by
    /// path-copying only the touched persistent-radix paths. Definition
    /// records, checked definitions, the type catalog, and untouched
    /// expectation-record subtrees remain shared with `base`.
    [[nodiscard]] static AuthorityRoot::Ptr buildDependentObjectAdmissionDelta(
        const AuthorityRoot & base,
        UInt64 next_database_catalog_epoch,
        const SidecarExpectationRecord & expectation_addition,
        const SchemaObjectDependencyGraphMutation & graph_delta,
        const AuthorityDependentObjectResourceImage & dependent_object,
        const AuthorityRootBuildLimits & limits = {},
        DependentObjectAdmissionDeltaStatistics * statistics = nullptr);

    /// Replaces the durable expectation for one existing dependent object
    /// while preserving its stable object identity. The optional graph delta
    /// may replace only this object's retained definition/object edges;
    /// definition nodes and all unrelated graph state remain shared.
    [[nodiscard]] static AuthorityRoot::Ptr buildDependentObjectExpectationReplacementDelta(
        const AuthorityRoot & base,
        UInt64 next_database_catalog_epoch,
        const SidecarExpectationRecord & expectation_replacement,
        const SchemaObjectDependencyGraphMutation & graph_delta,
        const AuthorityDependentObjectResourceImage & dependent_object,
        const AuthorityRootBuildLimits & limits = {});

private:
    [[nodiscard]] static AuthorityRoot::Ptr buildImpl(
        const AuthorityRoot * base,
        bool require_initial_usage_within_quota,
        const EffectiveResourceLimits * initial_effective_database_limits,
        AuthorityState authority_state,
        UInt64 type_index_generation,
        std::span<const Definition::Ptr> checked_definitions,
        std::span<const Record> definition_records,
        std::span<const SidecarExpectationRecord> expectation_records,
        SchemaObjectDependencyGraph::Ptr schema_graph,
        const AuthorityRootBuildLimits & limits,
        std::span<const AuthorityDependentObjectResourceImage> dependent_objects);

    AuthorityRootBuilder() = delete;
};

}
