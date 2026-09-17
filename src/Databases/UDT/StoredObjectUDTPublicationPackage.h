#pragma once

#include <Databases/SchemaObjectDependencyGraph.h>
#include <Databases/UDT/AtomicAuthority.h>

#include <DataTypes/UDT/AuthorityInventory.h>
#include <DataTypes/UDT/AuthorityState.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Core/Types.h>

#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB
{
class ASTCreateQuery;
}

namespace DB::UDT
{

class PreparedViewOutputTypeBindingAdmission;
class PreparedDictionaryAttributeTypeBindingAdmission;
class StoredObjectUDTPublicationCoordinator;
struct PreparedViewOutputTypeBindings;
struct PreparedDictionaryAttributeTypeBindings;
class StoredObjectPhysicalizationAdapterRegistry;
enum class StoredObjectKind : UInt8;

class StoredObjectUDTPublicationPackageError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidMetadata,
        InvalidBase,
        DatabaseMismatch,
        InvalidRevision,
        InvalidSidecar,
        DefinitionNotFound,
        DefinitionMismatch,
        ObjectAlreadyExists,
        IntegrityMismatch,
        LimitExceeded,
        InvalidTransition,
    };

    StoredObjectUDTPublicationPackageError(Code code_, std::string_view message);

    const Code code;
};

struct StoredObjectUDTMetadataValidationLimits
{
    UInt64 maximum_candidate_metadata_bytes = 16ULL << 20;
    UInt64 maximum_canonical_metadata_bytes = 16ULL << 20;
    UInt64 maximum_sidecar_bytes = 16ULL << 20;
    UInt64 maximum_object_name_bytes = 4ULL << 10;
};

/// Opaque proof emitted by the database-owned parser and schema validator for
/// one exact stored-object metadata image. Generic UDT publication code never
/// reparses SQL or infers logical identity from physical type equality.
class ValidatedStoredObjectUDTMetadata final
{
public:
    ValidatedStoredObjectUDTMetadata(const ValidatedStoredObjectUDTMetadata &) = delete;
    ValidatedStoredObjectUDTMetadata & operator=(const ValidatedStoredObjectUDTMetadata &) = delete;
    ValidatedStoredObjectUDTMetadata(ValidatedStoredObjectUDTMetadata &&) noexcept = default;
    ValidatedStoredObjectUDTMetadata & operator=(ValidatedStoredObjectUDTMetadata &&) noexcept = default;

    const SchemaObjectID & getObject() const noexcept { return object; }
    UInt64 getObjectSchemaRevision() const noexcept { return object_schema_revision; }
    const String & getObjectName() const noexcept { return object_name; }
    const Digest & getSidecarHash() const noexcept { return sidecar_hash; }
    const Digest & getPhysicalSchemaFingerprint() const noexcept { return physical_schema_fingerprint; }
    const String & getCanonicalMetadataBytes() const noexcept { return canonical_metadata_bytes; }
    const Digest & getCanonicalMetadataHash() const noexcept { return canonical_metadata_hash; }

private:
    ValidatedStoredObjectUDTMetadata(
        SchemaObjectID object_,
        UInt64 object_schema_revision_,
        String object_name_,
        Digest sidecar_hash_,
        Digest physical_schema_fingerprint_,
        String canonical_metadata_bytes_,
        Digest canonical_metadata_hash_);

    friend class IStoredObjectUDTMetadataValidator;

    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    String object_name;
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};
    String canonical_metadata_bytes;
    Digest canonical_metadata_hash{};
};

/// Database-engine boundary for an ordinary metadata image. Implementations
/// must parse the candidate with the owning object's grammar, validate the
/// exact UUID/revision and ordered physical schema, and return canonical bytes.
class IStoredObjectUDTMetadataValidator
{
public:
    virtual ~IStoredObjectUDTMetadataValidator() = default;

protected:
    struct DecodedMetadata
    {
        SchemaObjectID object;
        UInt64 object_schema_revision = 0;
        String object_name;
        Digest sidecar_hash{};
        Digest physical_schema_fingerprint{};
        String canonical_metadata_bytes;
    };

    /// The implementation must honor `limits` before materializing an output.
    virtual DecodedMetadata decodeAndCanonicalize(
        std::string_view candidate_metadata_bytes,
        std::string_view canonical_sidecar_bytes,
        const StoredObjectUDTMetadataValidationLimits & limits) const = 0;

private:
    /// The durable publication coordinator, rather than an arbitrary caller,
    /// owns conversion of a database parser result into an authorization proof.
    [[nodiscard]] ValidatedStoredObjectUDTMetadata validateAndCanonicalize(
        const SidecarExpectationRecord & expectation,
        std::string_view candidate_metadata_bytes,
        std::string_view canonical_sidecar_bytes,
        const StoredObjectUDTMetadataValidationLimits & limits = {}) const;

    friend class StoredObjectUDTPublicationCoordinator;
};

/// One-shot proof that the exact sidecar was produced by a closed stored-object
/// binder and accepted together with a complete physicalization adapter. Only
/// the indivisible View/Dictionary binding packages may issue this capability.
class StoredObjectUDTPublicationAdmissionProof final
{
public:
    StoredObjectUDTPublicationAdmissionProof(const StoredObjectUDTPublicationAdmissionProof &) = delete;
    StoredObjectUDTPublicationAdmissionProof & operator=(const StoredObjectUDTPublicationAdmissionProof &) = delete;
    StoredObjectUDTPublicationAdmissionProof(StoredObjectUDTPublicationAdmissionProof && other) noexcept
        : object(other.object)
        , object_schema_revision(other.object_schema_revision)
        , sidecar_hash(other.sidecar_hash)
        , physical_schema_fingerprint(other.physical_schema_fingerprint)
        , exact_descriptor_count(other.exact_descriptor_count)
    {
        other.object = {};
        other.object_schema_revision = 0;
        other.exact_descriptor_count = 0;
    }
    StoredObjectUDTPublicationAdmissionProof & operator=(StoredObjectUDTPublicationAdmissionProof &&) = delete;

private:
    StoredObjectUDTPublicationAdmissionProof(
        SchemaObjectID object_,
        UInt64 object_schema_revision_,
        Digest sidecar_hash_,
        Digest physical_schema_fingerprint_,
        UInt64 exact_descriptor_count_) noexcept
        : object(object_)
        , object_schema_revision(object_schema_revision_)
        , sidecar_hash(sidecar_hash_)
        , physical_schema_fingerprint(physical_schema_fingerprint_)
        , exact_descriptor_count(exact_descriptor_count_)
    {
    }

    friend class PreparedViewOutputTypeBindingAdmission;
    friend class PreparedDictionaryAttributeTypeBindingAdmission;
    friend class StoredObjectUDTPublicationPackage;
    friend StoredObjectUDTPublicationAdmissionProof authorizePreparedViewOutputTypeBindings(
        StoredObjectKind,
        const ASTCreateQuery &,
        const PreparedViewOutputTypeBindings &,
        const StoredObjectPhysicalizationAdapterRegistry &,
        bool);
    friend StoredObjectUDTPublicationAdmissionProof authorizePreparedDictionaryAttributeTypeBindings(
        const ASTCreateQuery &, const PreparedDictionaryAttributeTypeBindings &, const StoredObjectPhysicalizationAdapterRegistry &);

    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};
    UInt64 exact_descriptor_count = 0;
};

struct StoredObjectUDTPublicationPackageLimits
{
    PersistedTypeReferencesLimits persisted_references;
    DependentObjectMetadataInstallationRecordLimits installation_record;
    AuthorityRootBuildLimits authority_root;
    SchemaObjectDependencyGraphLimits schema_graph;
    UInt64 maximum_metadata_bytes = 16ULL << 20;
    UInt64 maximum_definition_dependencies = 65'536;
    UInt64 maximum_object_dependencies = 65'536;
    UInt64 maximum_work_units = 4ULL << 20;
    UInt64 maximum_provenance_scratch_bytes = 64ULL << 20;
    UInt64 maximum_owned_canonical_bytes = 64ULL << 20;
    UInt64 maximum_retained_logical_bytes = 8ULL << 30;
};

struct StoredObjectUDTPublicationPackageStatistics
{
    UInt64 descriptors_validated = 0;
    UInt64 occurrences_validated = 0;
    UInt64 definition_dependencies = 0;
    UInt64 object_dependencies = 0;
    UInt64 work_units = 0;
    UInt64 provenance_scratch_bytes_upper_bound = 0;
    UInt64 owned_canonical_bytes = 0;
    UInt64 pinned_root_logical_bytes = 0;
    /// Conservative logical-accounting bounds. They include structurally
    /// shared state once, but are not allocator-resident byte measurements.
    UInt64 after_inventory_logical_bytes_upper_bound = 0;
    UInt64 after_graph_logical_bytes = 0;
    UInt64 replacement_root_logical_bytes_upper_bound = 0;
    UInt64 retained_logical_bytes_upper_bound = 0;
    AuthorityInventoryMutationStatistics inventory_mutation;
    SchemaObjectDependencyGraphMutationStatistics graph_mutation;
};

/// Move-only, create-only and pre-I/O package for one exact authority pin. It
/// owns the already-built composite replacement root but has no publication
/// method: a database transaction must bind that root to the durable metadata
/// and WAL transition before taking ownership and publishing it.
class StoredObjectUDTPublicationPackage final
{
public:
    StoredObjectUDTPublicationPackage(const StoredObjectUDTPublicationPackage &) = delete;
    StoredObjectUDTPublicationPackage & operator=(const StoredObjectUDTPublicationPackage &) = delete;
    StoredObjectUDTPublicationPackage(StoredObjectUDTPublicationPackage &&) noexcept = default;
    StoredObjectUDTPublicationPackage & operator=(StoredObjectUDTPublicationPackage &&) = delete;

    [[nodiscard]] static StoredObjectUDTPublicationPackage prepareCreate(
        AtomicAuthority::RootSnapshot planning_root,
        StoredObjectUDTPublicationAdmissionProof admission_proof,
        ValidatedStoredObjectUDTMetadata validated_metadata,
        String canonical_sidecar_bytes,
        SidecarExpectationRecord expected_expectation,
        std::span<const SchemaObjectID> object_dependencies,
        const StoredObjectUDTPublicationPackageLimits & limits = {});

    const AuthorityRoot & getPlanningRoot() const { return planning_root.get(); }
    bool wasPreparedFrom(const AuthorityRoot & root) const noexcept { return planning_root.operator->() == &root; }
    const AuthorityState & getBeforeAuthorityState() const noexcept { return before_authority_state; }
    bool hasReplacementRoot() const noexcept { return static_cast<bool>(replacement_root); }
    const AuthorityRoot & getReplacementRoot() const;
    const AuthorityState & getAfterAuthorityState() const;
    const AuthorityInventory & getAfterInventory() const;
    const SchemaObjectDependencyGraph & getAfterSchemaGraph() const;

    const ValidatedStoredObjectUDTMetadata & getValidatedMetadata() const noexcept { return validated_metadata; }
    const PersistedTypeReferences & getPersistedReferences() const noexcept { return persisted_references; }
    const String & getCanonicalSidecarBytes() const noexcept { return canonical_sidecar_bytes; }
    const DependentObjectMetadataInstallationRecord & getInstallationRecord() const noexcept { return installation_record; }
    const String & getCanonicalInstallationRecordBytes() const noexcept { return canonical_installation_record_bytes; }
    const Digest & getInstallationRecordHash() const noexcept { return installation_record_hash; }
    const SidecarExpectationRecord & getExpectationRecord() const noexcept { return expectation_record; }
    const String & getCanonicalExpectationRecordBytes() const noexcept { return canonical_expectation_record_bytes; }
    const Digest & getExpectationRecordHash() const noexcept { return expectation_record_hash; }
    std::span<const AuthorityInventoryLeafDelta> getInventoryLeafDeltas() const noexcept { return inventory_leaf_deltas; }
    const SchemaObjectDependencyGraphMutation & getSchemaGraphDelta() const noexcept { return schema_graph_delta; }
    const StoredObjectUDTPublicationPackageStatistics & getStatistics() const noexcept { return statistics; }

private:
    /// Only the durable transaction coordinator may convert a validated plan
    /// into an authority-publication capability.
    AuthorityRoot::Ptr releaseReplacementRoot() noexcept { return std::move(replacement_root); }

    friend class StoredObjectUDTPublicationCoordinator;

    StoredObjectUDTPublicationPackage(
        AtomicAuthority::RootSnapshot planning_root_,
        AuthorityState before_authority_state_,
        ValidatedStoredObjectUDTMetadata validated_metadata_,
        PersistedTypeReferences persisted_references_,
        String canonical_sidecar_bytes_,
        DependentObjectMetadataInstallationRecord installation_record_,
        String canonical_installation_record_bytes_,
        Digest installation_record_hash_,
        SidecarExpectationRecord expectation_record_,
        String canonical_expectation_record_bytes_,
        Digest expectation_record_hash_,
        std::vector<AuthorityInventoryLeafDelta> inventory_leaf_deltas_,
        SchemaObjectDependencyGraphMutation schema_graph_delta_,
        AuthorityRoot::Ptr replacement_root_,
        StoredObjectUDTPublicationPackageStatistics statistics_);

    AtomicAuthority::RootSnapshot planning_root;
    AuthorityState before_authority_state;
    ValidatedStoredObjectUDTMetadata validated_metadata;
    PersistedTypeReferences persisted_references;
    String canonical_sidecar_bytes;
    DependentObjectMetadataInstallationRecord installation_record;
    String canonical_installation_record_bytes;
    Digest installation_record_hash{};
    SidecarExpectationRecord expectation_record;
    String canonical_expectation_record_bytes;
    Digest expectation_record_hash{};
    std::vector<AuthorityInventoryLeafDelta> inventory_leaf_deltas;
    SchemaObjectDependencyGraphMutation schema_graph_delta;
    AuthorityRoot::Ptr replacement_root;
    StoredObjectUDTPublicationPackageStatistics statistics;
};

}
