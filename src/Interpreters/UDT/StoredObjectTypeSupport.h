#pragma once

#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <Interpreters/UDT/StoredObjectTableFunctionSources.h>

#include <Core/Types.h>

#include <optional>
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

class IPhysicalizationObjectProvider;
class IPhysicalizationRewriteAdapter;
class BoundObjectTypeReferences;
enum class StoredObjectAdmissionRejection : UInt8;

/// Closed in-memory inventory. These ordinals are not a persistence format.
enum class StoredObjectKind : UInt8
{
    Unclassified,
    Table,
    View,
    MaterializedView,
    Dictionary,
};

enum class StoredObjectOccurrenceSite : UInt8
{
    Unclassified,
    TableColumnDeclaration,
    ViewOutputDeclaration,
    ViewStoredCast,
    MaterializedViewOutputDeclaration,
    MaterializedViewStoredCast,
    DictionaryAttribute,
    TableFunctionSchemaString,
    FormatSchemaString,
    StorageEngineTypeString,
    GlobalSQLUDFBody,
    ExecutableFunctionBody,
    ExternalMutableAuthority,
    TemporaryObject,
    DefaultExpression,
    ConstraintExpression,
    PolicyExpression,
    UnclassifiedTypeString,
};

enum class StoredObjectSourceMode : UInt8
{
    Unclassified,
    ObjectDefinition,
    ExplicitColumns,
    AsSourceTable,
    CloneAsSourceTable,
    AsSelect,
    EmptyAsSelect,
    AsTableFunction,
    SchemaInference,
    DialectLike,
    AttachMetadata,
};

enum class StoredObjectOccurrenceDisposition : UInt8
{
    ExactPersistedPath,
    PhysicalOnly,
    Unsupported,
};

enum class StoredObjectProvenanceRule : UInt8
{
    ExplicitTargetBinding,
    ExactSourceSidecar,
    ExactSelectedOutput,
    PhysicalInference,
    ExactAttachedSidecar,
    PhysicalOnly,
};

using StoredObjectKindMask = UInt16;
using StoredObjectOccurrenceSiteMask = UInt64;
using StoredObjectSourceModeMask = UInt32;

[[nodiscard]] constexpr StoredObjectKindMask storedObjectKindMask(StoredObjectKind kind) noexcept
{
    switch (kind)
    {
        case StoredObjectKind::Table: return 1U << 0;
        case StoredObjectKind::View: return 1U << 1;
        case StoredObjectKind::MaterializedView: return 1U << 2;
        case StoredObjectKind::Dictionary: return 1U << 3;
        case StoredObjectKind::Unclassified: return 0;
    }
    return 0;
}

[[nodiscard]] constexpr StoredObjectOccurrenceSiteMask storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite site) noexcept
{
    switch (site)
    {
        case StoredObjectOccurrenceSite::TableColumnDeclaration: return 1ULL << 0;
        case StoredObjectOccurrenceSite::ViewOutputDeclaration: return 1ULL << 1;
        case StoredObjectOccurrenceSite::ViewStoredCast: return 1ULL << 2;
        case StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration: return 1ULL << 3;
        case StoredObjectOccurrenceSite::MaterializedViewStoredCast: return 1ULL << 4;
        case StoredObjectOccurrenceSite::DictionaryAttribute: return 1ULL << 5;
        case StoredObjectOccurrenceSite::TableFunctionSchemaString: return 1ULL << 6;
        case StoredObjectOccurrenceSite::FormatSchemaString: return 1ULL << 7;
        case StoredObjectOccurrenceSite::StorageEngineTypeString: return 1ULL << 8;
        case StoredObjectOccurrenceSite::GlobalSQLUDFBody: return 1ULL << 9;
        case StoredObjectOccurrenceSite::ExecutableFunctionBody: return 1ULL << 10;
        case StoredObjectOccurrenceSite::ExternalMutableAuthority: return 1ULL << 11;
        case StoredObjectOccurrenceSite::TemporaryObject: return 1ULL << 12;
        case StoredObjectOccurrenceSite::DefaultExpression: return 1ULL << 13;
        case StoredObjectOccurrenceSite::ConstraintExpression: return 1ULL << 14;
        case StoredObjectOccurrenceSite::PolicyExpression: return 1ULL << 15;
        case StoredObjectOccurrenceSite::UnclassifiedTypeString: return 1ULL << 16;
        case StoredObjectOccurrenceSite::Unclassified: return 0;
    }
    return 0;
}

[[nodiscard]] constexpr StoredObjectSourceModeMask storedObjectSourceModeMask(StoredObjectSourceMode mode) noexcept
{
    switch (mode)
    {
        case StoredObjectSourceMode::ObjectDefinition: return 1U << 0;
        case StoredObjectSourceMode::ExplicitColumns: return 1U << 1;
        case StoredObjectSourceMode::AsSourceTable: return 1U << 2;
        case StoredObjectSourceMode::CloneAsSourceTable: return 1U << 3;
        case StoredObjectSourceMode::AsSelect: return 1U << 4;
        case StoredObjectSourceMode::EmptyAsSelect: return 1U << 5;
        case StoredObjectSourceMode::AsTableFunction: return 1U << 6;
        case StoredObjectSourceMode::SchemaInference: return 1U << 7;
        case StoredObjectSourceMode::DialectLike: return 1U << 8;
        case StoredObjectSourceMode::AttachMetadata: return 1U << 9;
        case StoredObjectSourceMode::Unclassified: return 0;
    }
    return 0;
}

struct StoredObjectOccurrenceSiteContract
{
    StoredObjectOccurrenceSite site{};
    StoredObjectKindMask owner_kinds = 0;
    StoredObjectOccurrenceDisposition disposition = StoredObjectOccurrenceDisposition::Unsupported;
    std::optional<PersistedTypePathSection> persisted_path_section;
};

struct StoredObjectSourceModeContract
{
    StoredObjectKind object_kind{};
    StoredObjectSourceMode source_mode{};
    StoredObjectProvenanceRule implicit_provenance = StoredObjectProvenanceRule::PhysicalOnly;
    bool physical_only_allowed = false;
    bool explicit_destination_columns_allowed = false;
    bool cross_database_physical_allowed = false;
    StoredObjectOccurrenceSiteMask required_logical_occurrence_sites = 0;
};

/// Allocation-free structural classification of one native CREATE/ATTACH AST.
/// `has_explicit_destination_columns` is deliberately independent from
/// source_mode: CREATE ... (columns) AS SELECT remains AsSelect. Metadata-load
/// declarations are not an explicit target-binding input and leave it false.
struct StoredObjectCreateQueryClassification
{
    StoredObjectKind object_kind = StoredObjectKind::Unclassified;
    StoredObjectSourceMode source_mode = StoredObjectSourceMode::Unclassified;
    bool has_explicit_destination_columns = false;
    StoredObjectOccurrenceSiteMask structured_udt_occurrence_sites = 0;
    StoredObjectOccurrenceSiteMask qualified_type_reference_candidate_sites = 0;
    StoredObjectOccurrenceSiteMask unresolved_type_string_occurrence_sites = 0;
    bool source_query_has_structured_udt_reference = false;
    bool source_query_requires_exact_logical_authority = false;
    bool source_query_has_unclassified_table_function = false;
    StoredObjectTableFunctionSourceProvenance source_table_function_provenance = StoredObjectTableFunctionSourceProvenance::Unclassified;
    bool has_unclassified_udt_reference = false;
    bool structured_udt_scan_complete = false;
    bool type_string_scan_complete = true;

    [[nodiscard]] bool isClassified() const noexcept
    {
        return object_kind != StoredObjectKind::Unclassified && source_mode != StoredObjectSourceMode::Unclassified
            && structured_udt_scan_complete && type_string_scan_complete && !has_unclassified_udt_reference;
    }

    [[nodiscard]] bool hasStructuredUDTReference(StoredObjectOccurrenceSite site) const noexcept
    {
        return (structured_udt_occurrence_sites & storedObjectOccurrenceSiteMask(site)) != 0;
    }

    [[nodiscard]] bool hasQualifiedTypeReferenceCandidate(StoredObjectOccurrenceSite site) const noexcept
    {
        return (qualified_type_reference_candidate_sites & storedObjectOccurrenceSiteMask(site)) != 0;
    }

    [[nodiscard]] bool hasUnresolvedTypeStringSlot(StoredObjectOccurrenceSite site) const noexcept
    {
        return (unresolved_type_string_occurrence_sites & storedObjectOccurrenceSiteMask(site)) != 0;
    }
};

/// Interpreter-side route chosen before distributed/replicated DDL can publish
/// an unbound logical CREATE. `Prepare*` routes authorize only exact in-memory
/// binding preparation. The database-owned durable publication boundary must
/// consume the prepared package; generic CREATE is never an admission path.
enum class StoredObjectCreatePreparationRoute : UInt8
{
    PhysicalOnly,
    PhysicalizeTableFunctionSchema,
    TableExplicitColumns,
    PrepareTableSelectedOutputs,
    PrepareViewExplicitOutputs,
    PrepareViewSelectedOutputs,
    PrepareMaterializedViewExplicitOutputs,
    PrepareMaterializedViewSelectedOutputs,
    PrepareDictionaryAttributes,
    Unsupported,
};

struct StoredObjectCreatePreparationDecision
{
    StoredObjectCreatePreparationRoute route = StoredObjectCreatePreparationRoute::PhysicalOnly;
    StoredObjectAdmissionRejection rejection{};
    StoredObjectOccurrenceSite occurrence_site = StoredObjectOccurrenceSite::Unclassified;
    bool has_positive_udt_evidence = false;

    [[nodiscard]] bool isUnsupported() const noexcept { return route == StoredObjectCreatePreparationRoute::Unsupported; }
    [[nodiscard]] bool requiresLocalUDTResolution() const noexcept
    {
        return route == StoredObjectCreatePreparationRoute::PhysicalizeTableFunctionSchema || requiresLogicalPreparation();
    }
    [[nodiscard]] bool requiresLogicalPreparation() const noexcept
    {
        return route == StoredObjectCreatePreparationRoute::TableExplicitColumns
            || route == StoredObjectCreatePreparationRoute::PrepareTableSelectedOutputs
            || route == StoredObjectCreatePreparationRoute::PrepareViewExplicitOutputs
            || route == StoredObjectCreatePreparationRoute::PrepareViewSelectedOutputs
            || route == StoredObjectCreatePreparationRoute::PrepareMaterializedViewExplicitOutputs
            || route == StoredObjectCreatePreparationRoute::PrepareMaterializedViewSelectedOutputs
            || route == StoredObjectCreatePreparationRoute::PrepareDictionaryAttributes;
    }
};

/// `metadata_load` covers a database-owned ATTACH/restart/restore parse whose
/// AST does not necessarily retain the public ATTACH bit. Unknown or
/// overlapping source forms remain Unclassified. The walk allocates no memory
/// and returns an incomplete fail-closed result at its fixed work/depth bound.
[[nodiscard]] StoredObjectCreateQueryClassification
classifyStoredObjectCreateQuery(const ASTCreateQuery & create, bool metadata_load = false) noexcept;

/// Closed, allocation-free interpreter routing decision over the structural
/// classification and CREATE-family flags. Built-in-only input takes the
/// PhysicalOnly route even when the UDT feature is disabled. Unknown logical
/// provenance never falls through to generic CREATE.
[[nodiscard]] StoredObjectCreatePreparationDecision classifyStoredObjectCreatePreparation(
    const ASTCreateQuery & create, const StoredObjectCreateQueryClassification & classification, bool udt_feature_enabled) noexcept;

/// The returned spans are the authoritative inventories. An absent lookup is
/// unsupported; callers must not infer a neighboring contract.
[[nodiscard]] std::span<const StoredObjectOccurrenceSiteContract> getStoredObjectOccurrenceSiteContracts() noexcept;
[[nodiscard]] std::span<const StoredObjectSourceModeContract> getStoredObjectSourceModeContracts() noexcept;
[[nodiscard]] const StoredObjectOccurrenceSiteContract * tryGetStoredObjectOccurrenceSiteContract(StoredObjectOccurrenceSite site) noexcept;
[[nodiscard]] const StoredObjectSourceModeContract *
tryGetStoredObjectSourceModeContract(StoredObjectKind object_kind, StoredObjectSourceMode source_mode) noexcept;
[[nodiscard]] std::optional<SchemaObjectKind> tryGetSchemaObjectKind(StoredObjectKind object_kind) noexcept;

class StoredObjectSelectedOutput final
{
public:
    enum class Kind : UInt8
    {
        Physical,
        ExactDescriptor,
    };

    [[nodiscard]] static StoredObjectSelectedOutput physical();
    [[nodiscard]] static StoredObjectSelectedOutput exactDescriptor(PersistedTypeDescriptor descriptor);

    Kind getKind() const noexcept { return kind; }
    const PersistedTypeDescriptor * tryGetExactDescriptor() const noexcept;

private:
    StoredObjectSelectedOutput(Kind kind_, std::optional<PersistedTypeDescriptor> descriptor_);

    Kind kind;
    std::optional<PersistedTypeDescriptor> descriptor;
};

/// An exact non-output occurrence already proved by the owning binder or
/// analyzer. This carries no physical IDataType, name-only identity, or result
/// header, so the admission API cannot infer UDT identity from physical
/// equality.
struct StoredObjectExactOccurrence
{
    StoredObjectOccurrenceSite site{};
    PersistedTypeDescriptor descriptor;
};

struct StoredObjectPhysicalizationAdapterRegistration
{
    StoredObjectKind object_kind{};
    SchemaObjectKind schema_object_kind{};
    StoredObjectSourceModeMask source_modes = 0;
    StoredObjectOccurrenceSiteMask occurrence_sites = 0;
    const IPhysicalizationObjectProvider * object_provider = nullptr;
    const IPhysicalizationRewriteAdapter * rewrite_adapter = nullptr;
};

/// Dispatch-only projection. It deliberately exposes neither source-mode nor
/// occurrence-site masks, because View and MaterializedView share one durable
/// SchemaObjectKind while retaining different admission inventories. Registered
/// adapters must outlive the immutable registry and every copied dispatch.
class StoredObjectPhysicalizationDispatch final
{
public:
    SchemaObjectKind getSchemaObjectKind() const noexcept { return schema_object_kind; }
    const IPhysicalizationObjectProvider & getObjectProvider() const noexcept { return *object_provider; }
    const IPhysicalizationRewriteAdapter & getRewriteAdapter() const noexcept { return *rewrite_adapter; }

private:
    StoredObjectPhysicalizationDispatch(
        SchemaObjectKind schema_object_kind_,
        const IPhysicalizationObjectProvider & object_provider_,
        const IPhysicalizationRewriteAdapter & rewrite_adapter_) noexcept;

    friend class StoredObjectPhysicalizationAdapterRegistry;

    SchemaObjectKind schema_object_kind;
    const IPhysicalizationObjectProvider * object_provider;
    const IPhysicalizationRewriteAdapter * rewrite_adapter;
};

class StoredObjectPhysicalizationAdapterRegistryError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        LimitExceeded,
        InvalidRegistration,
        DuplicateRegistration,
        IncompleteRegistration,
        ConflictingDispatch,
    };

    StoredObjectPhysicalizationAdapterRegistryError(Code code_, std::string_view message);

    const Code code;
};

/// Immutable database-local registration set. Registration is closed to the
/// source-mode and occurrence-site inventories above. Object kinds sharing a
/// persisted SchemaObjectKind must share one provider/rewrite dispatch pair,
/// because source mode is deliberately not durable object identity.
class StoredObjectPhysicalizationAdapterRegistry final
{
public:
    [[nodiscard]] static StoredObjectPhysicalizationAdapterRegistry
    create(std::span<const StoredObjectPhysicalizationAdapterRegistration> registrations);

    [[nodiscard]] const StoredObjectPhysicalizationAdapterRegistration *
    tryGet(StoredObjectKind object_kind, StoredObjectSourceMode source_mode) const noexcept;
    [[nodiscard]] std::optional<StoredObjectPhysicalizationDispatch> tryGetDispatch(
        StoredObjectKind object_kind,
        StoredObjectSourceMode source_mode,
        StoredObjectOccurrenceSiteMask required_occurrence_sites) const noexcept;
    [[nodiscard]] std::optional<StoredObjectPhysicalizationDispatch> tryGetDispatch(SchemaObjectKind schema_object_kind) const noexcept;
    [[nodiscard]] bool hasCompleteAdapter(
        StoredObjectKind object_kind,
        StoredObjectSourceMode source_mode,
        StoredObjectOccurrenceSiteMask required_occurrence_sites) const noexcept;

private:
    explicit StoredObjectPhysicalizationAdapterRegistry(std::vector<StoredObjectPhysicalizationAdapterRegistration> registrations_);

    std::vector<StoredObjectPhysicalizationAdapterRegistration> registrations;
};

enum class StoredObjectAdmissionStatus : UInt8
{
    PhysicalOnly,
    Logical,
    UnsupportedContext,
};

enum class StoredObjectAdmissionRejection : UInt8
{
    None,
    UnclassifiedObjectKind,
    UnclassifiedSourceMode,
    UnsupportedObjectSourceMode,
    UnsupportedOccurrenceSite,
    OccurrenceOwnerMismatch,
    InvalidTargetDatabase,
    InvalidProvenanceSource,
    IncompleteOutputClassification,
    IncompleteTypeStringClassification,
    CrossDatabaseDescriptor,
    ConflictingDescriptorIdentity,
    SourceSidecarMismatch,
    MissingPhysicalizationAdapter,
    LimitExceeded,
};

class StoredObjectAdmissionResult final
{
public:
    StoredObjectAdmissionStatus getStatus() const noexcept { return status; }
    StoredObjectAdmissionRejection getRejection() const noexcept { return rejection; }
    StoredObjectKind getObjectKind() const noexcept { return object_kind; }
    StoredObjectSourceMode getSourceMode() const noexcept { return source_mode; }
    StoredObjectOccurrenceSite getOccurrenceSite() const noexcept { return occurrence_site; }
    UInt64 getExactDescriptorCount() const noexcept { return exact_descriptor_count; }
    bool isAccepted() const noexcept { return status != StoredObjectAdmissionStatus::UnsupportedContext; }
    bool hasLogicalReferences() const noexcept { return status == StoredObjectAdmissionStatus::Logical; }

private:
    StoredObjectAdmissionResult(
        StoredObjectAdmissionStatus status_,
        StoredObjectAdmissionRejection rejection_,
        StoredObjectKind object_kind_,
        StoredObjectSourceMode source_mode_,
        StoredObjectOccurrenceSite occurrence_site_,
        UInt64 exact_descriptor_count_);

    friend StoredObjectAdmissionResult rejectUnsupportedStoredObjectContext(
        StoredObjectKind, StoredObjectSourceMode, StoredObjectOccurrenceSite, StoredObjectAdmissionRejection) noexcept;
    friend StoredObjectAdmissionResult admitStoredObjectPhysicalOnly(StoredObjectKind, StoredObjectSourceMode, const UUID &) noexcept;
    friend StoredObjectAdmissionResult admitStoredObjectExplicitDestination(
        StoredObjectKind,
        StoredObjectSourceMode,
        const UUID &,
        std::span<const PersistedTypeDescriptor>,
        const StoredObjectPhysicalizationAdapterRegistry &);
    friend StoredObjectAdmissionResult admitStoredObjectSourceSidecar(
        StoredObjectKind,
        StoredObjectSourceMode,
        const UUID &,
        const PersistedTypeReferences &,
        const BoundObjectTypeReferences &,
        const StoredObjectPhysicalizationAdapterRegistry &);
    friend StoredObjectAdmissionResult admitStoredObjectSelectedOutputs(
        StoredObjectKind,
        StoredObjectSourceMode,
        const UUID &,
        std::span<const StoredObjectSelectedOutput>,
        std::span<const StoredObjectExactOccurrence>,
        bool,
        const StoredObjectPhysicalizationAdapterRegistry &);

    StoredObjectAdmissionStatus status;
    StoredObjectAdmissionRejection rejection;
    StoredObjectKind object_kind;
    StoredObjectSourceMode source_mode;
    StoredObjectOccurrenceSite occurrence_site;
    UInt64 exact_descriptor_count;
};

/// Central routing decision. Logical admission always carries the exact
/// physicalization dispatch that made it admissible; rejected and physical-only
/// decisions never do. This value retains no descriptors and is not an atomic
/// publication package.
class StoredObjectAdmissionDispatch final
{
public:
    const StoredObjectAdmissionResult & getAdmission() const noexcept { return admission; }
    const StoredObjectPhysicalizationDispatch * tryGetPhysicalizationDispatch() const noexcept
    {
        return physicalization_dispatch ? &*physicalization_dispatch : nullptr;
    }

private:
    StoredObjectAdmissionDispatch(
        StoredObjectAdmissionResult admission_, std::optional<StoredObjectPhysicalizationDispatch> physicalization_dispatch_);

    friend StoredObjectAdmissionDispatch admitStoredObjectExplicitViewOutputCreate(
        const ASTCreateQuery &, const UUID &, std::span<const PersistedTypeDescriptor>, const StoredObjectPhysicalizationAdapterRegistry &);
    friend StoredObjectAdmissionDispatch admitStoredObjectExplicitMaterializedViewOutputCreate(
        const ASTCreateQuery &, const UUID &, std::span<const PersistedTypeDescriptor>, const StoredObjectPhysicalizationAdapterRegistry &);
    friend StoredObjectAdmissionDispatch admitStoredObjectDictionaryAttributeCreate(
        const ASTCreateQuery &, const UUID &, std::span<const PersistedTypeDescriptor>, const StoredObjectPhysicalizationAdapterRegistry &);

    StoredObjectAdmissionResult admission;
    std::optional<StoredObjectPhysicalizationDispatch> physicalization_dispatch;
};

/// Central fail-closed result for any unclassified or unsupported persisted
/// type context. DDL hooks should map this result to their public
/// UNSUPPORTED_USER_DEFINED_TYPE_CONTEXT error before durable mutation.
[[nodiscard]] StoredObjectAdmissionResult rejectUnsupportedStoredObjectContext(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    StoredObjectOccurrenceSite occurrence_site,
    StoredObjectAdmissionRejection rejection = StoredObjectAdmissionRejection::UnsupportedOccurrenceSite) noexcept;

/// Explicit physical-only intent is valid only after the caller has checked
/// both explicit UDT syntax and retained source-sidecar presence bits and found
/// neither. No DataTypePtr/header input is accepted here.
[[nodiscard]] StoredObjectAdmissionResult
admitStoredObjectPhysicalOnly(StoredObjectKind object_kind, StoredObjectSourceMode source_mode, const UUID & target_database_uuid) noexcept;

/// Explicit destination declarations are already resolved by the target
/// database authority. Empty descriptors mean that all declarations are
/// physical. Exact descriptors must all belong to target_database_uuid.
[[nodiscard]] StoredObjectAdmissionResult admitStoredObjectExplicitDestination(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry);

/// Native AS source-table, CLONE, and ATTACH consume the exact source sidecar
/// plus its immutable bound derivative. Logical cross-database copies reject;
/// a caller with no source sidecar must use the explicit physical-only API.
[[nodiscard]] StoredObjectAdmissionResult admitStoredObjectSourceSidecar(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    const UUID & target_database_uuid,
    const PersistedTypeReferences & source_references,
    const BoundObjectTypeReferences & bound_source_references,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry);

/// AS SELECT/EMPTY consumes only the compact output proof captured before
/// semantic-role erasure plus exact stored-expression descriptors. It accepts
/// neither executable headers nor physical types. `classification_complete`
/// must be explicit so an unclassified provenance-capable form fails closed.
[[nodiscard]] StoredObjectAdmissionResult admitStoredObjectSelectedOutputs(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    const UUID & target_database_uuid,
    std::span<const StoredObjectSelectedOutput> selected_outputs,
    std::span<const StoredObjectExactOccurrence> exact_stored_occurrences,
    bool classification_complete,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry);

/// Admission vertical for a fresh ordinary VIEW whose explicit output type
/// declarations are already resolved under the target database authority. It
/// admits only output declarations (no stored CAST), and returns a logical
/// result only together with a complete View physicalization dispatch. A DDL
/// caller must provide the matching exact sidecar construction and atomic
/// durable/runtime publication path before exposing this context.
[[nodiscard]] StoredObjectAdmissionDispatch admitStoredObjectExplicitViewOutputCreate(
    const ASTCreateQuery & create,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_output_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry);

/// Materialized View equivalent of the output-only admission above. It uses
/// the distinct MaterializedView source/occurrence inventory while sharing the
/// durable SchemaObjectKind::View dispatch with ordinary Views. External `TO`
/// targets and POPULATE are admitted; refreshable and inner-table MVs remain
/// fail-closed because their extra durable object is not part of this commit.
[[nodiscard]] StoredObjectAdmissionDispatch admitStoredObjectExplicitMaterializedViewOutputCreate(
    const ASTCreateQuery & create,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_output_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry);

/// Feature-inert fresh Dictionary admission for exact attribute declarations.
/// It accepts only ObjectDefinition/DictionaryAttribute provenance and returns
/// a logical result only with a complete Dictionary physicalization dispatch.
[[nodiscard]] StoredObjectAdmissionDispatch admitStoredObjectDictionaryAttributeCreate(
    const ASTCreateQuery & create,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_attribute_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry);

[[nodiscard]] std::string_view getStoredObjectAdmissionRejectionName(StoredObjectAdmissionRejection rejection) noexcept;

}
