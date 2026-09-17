#pragma once

#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/PersistedTypeReferences.h>

#include <Core/Types.h>

#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

inline constexpr UInt16 physicalization_plan_format_version = 1;
inline constexpr std::string_view physicalization_scope_hash_domain = "ClickHouse UDT physicalization scope V1";
inline constexpr std::string_view physicalization_manifest_hash_domain = "ClickHouse UDT physicalization loss manifest V1";
inline constexpr UInt64 physicalization_default_maximum_selected_objects = 10'000;
inline constexpr UInt64 physicalization_default_maximum_validation_definitions = 10'000;
inline constexpr UInt64 physicalization_maximum_selected_objects = 100'000;
inline constexpr UInt64 physicalization_maximum_validation_definitions = 100'000;

enum class PhysicalizationScope : UInt8
{
    Object = 1,
    DependentClosure = 2,
    Database = 3,
};

struct PhysicalizationSelector
{
    PhysicalizationScope scope{};
    /// Absent only for database scope. Production admits registered Table,
    /// shared View/MaterializedView, and Dictionary adapters in addition to
    /// synthetic object providers used by focused validation.
    std::optional<SchemaObjectID> object;
    bool drop_unused_types = false;

    bool operator==(const PhysicalizationSelector &) const = default;
};

/// Immutable object-side input supplied by the registered object-kind adapter.
/// `selected_semantic_capabilities` is copied from the already-bound index in
/// exact persisted-use order; dry run never reopens a catalog or rebinds it.
struct PhysicalizationObject
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    String diagnostic_name;
    Digest canonical_metadata_hash{};
    PersistedTypeReferences references;
    std::vector<SemanticCapabilityMask> selected_semantic_capabilities;
};

class IPhysicalizationObjectProvider
{
public:
    virtual ~IPhysicalizationObjectProvider() = default;
    virtual void checkCancellation() const { }

    /// The provider must return the exact immutable snapshot named by the
    /// authority expectation. Missing, stale, or unbound state fails closed.
    virtual PhysicalizationObject load(const SidecarExpectationRecord & expectation) const = 0;
};

struct PhysicalizationManifestObject
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    String diagnostic_name;
    Digest canonical_metadata_hash{};
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};
    PersistedTypeReferences references;
    std::vector<SemanticCapabilityMask> selected_semantic_capabilities;
};

struct PhysicalizationManifestDefinition
{
    DefinitionIdentity identity;
    String normalized_name;
    Digest definition_hash{};
    Digest canonical_record_hash{};
    /// Complete canonical authority record required to audit the definition,
    /// parameters, template/certificate, dependencies, and administrative
    /// fields whose logical provenance may become unreachable after apply.
    String canonical_record_bytes;
    bool selected_for_drop = false;
};

struct PhysicalizationPlanLimits
{
    /// Per-item codec limits bound transient materialization independently.
    /// `maximum_manifest_bytes` below bounds the complete retained canonical
    /// plan/manifest, not the peak temporary bytes of one already-bounded
    /// sidecar or definition-record encoder.
    PersistedTypeReferencesLimits persisted_references{
        .maximum_sidecar_bytes = 16ULL << 20,
        .maximum_descriptors = 4'096,
        .maximum_occurrence_paths = 65'536,
        .maximum_path_depth = 64,
        .maximum_canonical_arguments_bytes = 64ULL << 10,
        .maximum_canonical_physical_type_bytes = 64ULL << 10,
        .maximum_qualified_name_bytes = 4ULL << 10,
    };
    RecordLimits definition_record;
    UInt64 maximum_selected_objects = physicalization_default_maximum_selected_objects;
    UInt64 maximum_validation_definitions = physicalization_default_maximum_validation_definitions;
    UInt64 maximum_walked_edges = 262'144;
    UInt64 maximum_manifest_entries = 262'144;
    UInt64 maximum_scope_bytes = 4ULL << 20;
    UInt64 maximum_manifest_bytes = 64ULL << 20;
    UInt64 maximum_diagnostic_name_bytes = 4ULL << 10;
};

class PhysicalizationPlanError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidSelector,
        UnsupportedObjectKind,
        ObjectNotFound,
        IncompleteScope,
        IntegrityMismatch,
        GraphMismatch,
        LimitExceeded,
    };

    PhysicalizationPlanError(Code code_, std::string_view message);

    const Code code;
};

/// Fully materialized dry-run result. It owns only compact/canonical values;
/// it retains neither the authority root nor a resolution-session lease.
class PhysicalizationPlan final
{
public:
    const PhysicalizationSelector & getSelector() const noexcept { return selector; }
    const UUID & getDatabaseUUID() const noexcept { return database_uuid; }
    UInt64 getDatabaseCatalogEpoch() const noexcept { return database_catalog_epoch; }
    const Digest & getInventoryRoot() const noexcept { return inventory_root; }

    std::span<const PhysicalizationManifestObject> getObjects() const noexcept { return objects; }
    std::span<const PhysicalizationManifestDefinition> getDefinitions() const noexcept { return definitions; }

    const String & getCanonicalScopeBytes() const noexcept { return canonical_scope_bytes; }
    const Digest & getScopeDigest() const noexcept { return scope_digest; }
    UInt64 getScopeCount() const noexcept { return scope_count; }
    UInt64 getScopeBytes() const noexcept { return scope_bytes; }

    const String & getCanonicalManifestBytes() const noexcept { return canonical_manifest_bytes; }
    const Digest & getManifestDigest() const noexcept { return manifest_digest; }
    UInt64 getManifestCount() const noexcept { return manifest_count; }
    UInt64 getManifestBytes() const noexcept { return manifest_bytes; }

private:
    PhysicalizationPlan(
        PhysicalizationSelector selector_,
        UUID database_uuid_,
        UInt64 database_catalog_epoch_,
        Digest inventory_root_,
        std::vector<PhysicalizationManifestObject> objects_,
        std::vector<PhysicalizationManifestDefinition> definitions_,
        String canonical_scope_bytes_,
        Digest scope_digest_,
        UInt64 scope_count_,
        String canonical_manifest_bytes_,
        Digest manifest_digest_,
        UInt64 manifest_count_);

    friend class PhysicalizationPlanner;

    PhysicalizationSelector selector;
    UUID database_uuid;
    UInt64 database_catalog_epoch;
    Digest inventory_root;
    std::vector<PhysicalizationManifestObject> objects;
    std::vector<PhysicalizationManifestDefinition> definitions;
    String canonical_scope_bytes;
    Digest scope_digest;
    UInt64 scope_count;
    UInt64 scope_bytes;
    String canonical_manifest_bytes;
    Digest manifest_digest;
    UInt64 manifest_count;
    UInt64 manifest_bytes;
};

class PhysicalizationPlanner final
{
public:
    /// Computes only the authority-root-selected object identities. It does
    /// not load sidecars or metadata and is used to authorize the complete
    /// closure before any hidden durable record is decoded.
    static std::vector<SchemaObjectID> selectObjectIdentities(
        const AuthorityRoot & root, const PhysicalizationSelector & selector, const PhysicalizationPlanLimits & limits = {});

    static PhysicalizationPlan build(
        const AuthorityRoot & root,
        PhysicalizationSelector selector,
        const IPhysicalizationObjectProvider & object_provider,
        const PhysicalizationPlanLimits & limits = {});

private:
    PhysicalizationPlanner() = delete;
};

static_assert(static_cast<UInt8>(PhysicalizationScope::Object) == 1);
static_assert(static_cast<UInt8>(PhysicalizationScope::DependentClosure) == 2);
static_assert(static_cast<UInt8>(PhysicalizationScope::Database) == 3);

}
