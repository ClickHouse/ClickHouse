#pragma once

#include <DataTypes/UDT/TableColumnTypeBindings.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/UDT/DictionaryAttributeTypeBindings.h>
#include <Interpreters/UDT/StoredObjectTypeSupport.h>
#include <Interpreters/UDT/ViewOutputTypeBindings.h>

#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB
{
class ASTCreateQuery;
}

namespace DB::UDT
{

class IAuthorityAdapter;
class PreparedStoredObjectTypeBindingHandoff;

/// Move-only first half of inferred View/MV preparation. Exact schema-string
/// endpoints must be bound and rewritten before the selected-output analyzer
/// instantiates their table functions. Stored CAST endpoints deliberately stay
/// logical until that analyzer has captured their semantic output role.
class PreparedViewSchemaStringBindingHandoff final
{
public:
    PreparedViewSchemaStringBindingHandoff(const PreparedViewSchemaStringBindingHandoff &) = delete;
    PreparedViewSchemaStringBindingHandoff & operator=(const PreparedViewSchemaStringBindingHandoff &) = delete;
    PreparedViewSchemaStringBindingHandoff(PreparedViewSchemaStringBindingHandoff &&) noexcept;
    PreparedViewSchemaStringBindingHandoff & operator=(PreparedViewSchemaStringBindingHandoff &&) noexcept;
    ~PreparedViewSchemaStringBindingHandoff();

    /// Validates the retained original endpoint generation, clones the complete
    /// CREATE, applies every schema-string replacement to that clone, and proves
    /// by fresh classification that its SELECT has no qualified/context-owned
    /// schema string. The stored SELECT remains logical until final preparation.
    [[nodiscard]] ASTPtr clonePhysicalizedSelectForAnalysis();
    bool hasPreparedPhysicalizedAnalysisAST() const noexcept;

private:
    struct Impl;
    explicit PreparedViewSchemaStringBindingHandoff(std::unique_ptr<Impl> impl_);

    friend PreparedViewSchemaStringBindingHandoff prepareStoredObjectSelectedOutputSchemaStringBindings(
        ASTCreateQuery &,
        const StoredObjectCreateQueryClassification &,
        StoredObjectKind,
        UUID,
        std::string_view,
        const ContextPtr &,
        const IAuthorityAdapter &,
        const ViewOutputTypeBindingLimits &);
    friend PreparedStoredObjectTypeBindingHandoff prepareStoredObjectSelectedOutputBindings(
        ASTCreateQuery &,
        const StoredObjectCreateQueryClassification &,
        StoredObjectKind,
        const SchemaObjectID &,
        UInt64,
        std::string_view,
        const ContextPtr &,
        const IAuthorityAdapter &,
        std::span<const SelectedOutputTypeBinding>,
        PreparedViewSchemaStringBindingHandoff *,
        const ViewOutputTypeBindingLimits &);

    std::unique_ptr<Impl> impl;
};

struct ViewAuxiliaryTypePresentation
{
    PersistedTypeOccurrenceSite site = PersistedTypeOccurrenceSite::StoredExpression;
    UInt64 object_ordinal = 0;
    String runtime_owner_key;
    DataTypePtr physical_type;
    ASTPtr declared_type;
    bool has_logical_references = false;
};

class StoredObjectTypeBindingPreparationError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidDecision,
        InvalidObject,
        InvalidDeclaration,
        CrossDatabaseReference,
        SourceSidecarMismatch,
        LimitExceeded,
        QueryChanged,
        NormalizedSchemaMismatch,
        MissingLogicalBinding,
        InvalidState,
    };

    StoredObjectTypeBindingPreparationError(Code code_, std::string_view message);

    const Code code;
};

/// Move-only interpreter-to-database handoff for exact declaration bindings.
/// Preparation resolves under one target authority, constructs the permanent
/// versioned sidecar package, and prebuilds physical AST replacements. This value is
/// deliberately not a publication/admission proof: generic CREATE must not
/// consume it, and DatabaseAtomic must still bind a complete physicalization
/// adapter and publish the package transactionally.
class PreparedStoredObjectTypeBindingHandoff final
{
public:
    PreparedStoredObjectTypeBindingHandoff(const PreparedStoredObjectTypeBindingHandoff &) = delete;
    PreparedStoredObjectTypeBindingHandoff & operator=(const PreparedStoredObjectTypeBindingHandoff &) = delete;
    PreparedStoredObjectTypeBindingHandoff(PreparedStoredObjectTypeBindingHandoff &&) noexcept;
    PreparedStoredObjectTypeBindingHandoff & operator=(PreparedStoredObjectTypeBindingHandoff &&) noexcept;
    ~PreparedStoredObjectTypeBindingHandoff();

    StoredObjectKind getObjectKind() const noexcept;
    StoredObjectSourceMode getSourceMode() const noexcept;
    const SchemaObjectID & getObject() const noexcept;
    bool hasAppliedPhysicalTypeASTs() const noexcept;
    bool usesSelectedOutputClassification() const noexcept;

    const PreparedViewOutputTypeBindings * tryGetViewBindings() const noexcept;
    const PreparedDictionaryAttributeTypeBindings * tryGetDictionaryBindings() const noexcept;

    /// Validates every original declaration pointer before changing any AST,
    /// then applies all prebuilt physical types without another lookup.
    void applyPhysicalTypeASTs();

    /// View normalization must preserve the exact ordered physical outputs
    /// used by the sidecar fingerprint. No physical header can establish or
    /// repair logical identity after this check.
    void validateNormalizedViewOutputs(const NamesAndTypesList & normalized_outputs) const;

    [[nodiscard]] PreparedViewOutputTypeBindings releaseViewBindings() &&;
    [[nodiscard]] PreparedDictionaryAttributeTypeBindings releaseDictionaryBindings() &&;

private:
    struct Impl;
    explicit PreparedStoredObjectTypeBindingHandoff(std::unique_ptr<Impl> impl_);

    friend PreparedStoredObjectTypeBindingHandoff prepareStoredObjectExactDeclarationBindings(
        ASTCreateQuery &,
        const StoredObjectCreateQueryClassification &,
        const StoredObjectCreatePreparationDecision &,
        const SchemaObjectID &,
        UInt64,
        std::string_view,
        const ContextPtr &,
        const IAuthorityAdapter &,
        const ViewOutputTypeBindingLimits &,
        const DictionaryAttributeTypeBindingLimits &);
    friend PreparedStoredObjectTypeBindingHandoff prepareStoredObjectSelectedOutputBindings(
        ASTCreateQuery &,
        const StoredObjectCreateQueryClassification &,
        StoredObjectKind,
        const SchemaObjectID &,
        UInt64,
        std::string_view,
        const ContextPtr &,
        const IAuthorityAdapter &,
        std::span<const SelectedOutputTypeBinding>,
        PreparedViewSchemaStringBindingHandoff *,
        const ViewOutputTypeBindingLimits &);
    friend PreparedStoredObjectTypeBindingHandoff prepareStoredObjectSelectedOutputAlterBindings(
        ASTPtr &,
        StoredObjectKind,
        const SchemaObjectID &,
        UInt64,
        std::string_view,
        const ContextPtr &,
        const IAuthorityAdapter &,
        std::span<const SelectedOutputTypeBinding>,
        const ViewOutputTypeBindingLimits &);

    std::unique_ptr<Impl> impl;
};

/// Activated only for the three `Prepare*` routes. Built-in-only CREATEs never
/// enter this function. The route and a fresh structural reclassification must
/// agree exactly, closing AST changes between early routing and preparation.
[[nodiscard]] PreparedStoredObjectTypeBindingHandoff prepareStoredObjectExactDeclarationBindings(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    const StoredObjectCreatePreparationDecision & decision,
    const SchemaObjectID & object,
    UInt64 object_schema_revision,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    const ViewOutputTypeBindingLimits & view_limits = {},
    const DictionaryAttributeTypeBindingLimits & dictionary_limits = {});

/// Exact inferred-output counterpart for View/MV AS SELECT. The analyzer
/// classification was captured before normalization and contains every output
/// in order. This function binds the original stored SELECT's approved
/// auxiliary endpoints, builds one mixed package, and prepares its physical
/// AST replacements; it never infers identity from the normalized header.
[[nodiscard]] PreparedStoredObjectTypeBindingHandoff prepareStoredObjectSelectedOutputBindings(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    StoredObjectKind object_kind,
    const SchemaObjectID & object,
    UInt64 object_schema_revision,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    PreparedViewSchemaStringBindingHandoff * prepared_schema_strings = nullptr,
    const ViewOutputTypeBindingLimits & view_limits = {});

/// Pre-analyzer half for an inferred View/MV whose exact schema-string
/// endpoint contains a qualified UDT. This fixes the target Atomic database
/// identity and authority session without retaining a DDL/mutation guard,
/// resolves every approved schema endpoint in one scope, and returns a handoff
/// that the final selected-output preparation must consume exactly once.
[[nodiscard]] PreparedViewSchemaStringBindingHandoff prepareStoredObjectSelectedOutputSchemaStringBindings(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    StoredObjectKind object_kind,
    UUID target_database_uuid,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    const ViewOutputTypeBindingLimits & view_limits = {});

/// ALTER MODIFY QUERY counterpart for an already mapped external-target
/// MaterializedView. The selected-output proof is captured by the analyzer
/// before this function is called; the exact mutable stored SELECT is walked
/// for auxiliary endpoints and then physicalized in place. Unlike fresh CREATE
/// preparation, an all-physical result is valid and deliberately represents
/// removal of the last logical occurrence.
[[nodiscard]] PreparedStoredObjectTypeBindingHandoff prepareStoredObjectSelectedOutputAlterBindings(
    ASTPtr & stored_select,
    StoredObjectKind object_kind,
    const SchemaObjectID & object,
    UInt64 object_schema_revision,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    const ViewOutputTypeBindingLimits & view_limits = {});

/// Resolves an exact literal schema slot owned by `CREATE TABLE ... AS
/// table_function()` and rewrites it to canonical physical column types before
/// the table function is instantiated or its AST can be persisted.  This is a
/// transient-resolution operation: inferred destination columns remain
/// physical and no descriptor, sidecar, dependency edge, or binding handoff is
/// produced.  The preparation decision and a fresh structural classification
/// must designate the dedicated physicalization route.
void physicalizeInferredTableFunctionSchema(
    ASTCreateQuery & create,
    const StoredObjectCreateQueryClassification & classification,
    const StoredObjectCreatePreparationDecision & decision,
    const ContextPtr & context);

/// Replays the closed View/MV auxiliary-endpoint inventory over already
/// physical canonical CREATE metadata. It supplies the exact endpoint table
/// required by V2 fingerprint/path reconstruction; unknown, context-owned,
/// shared, or malformed slots fail closed.
[[nodiscard]] std::vector<ViewAuxiliaryPhysicalTypeBindingInput>
collectViewAuxiliaryPhysicalTypeBindings(const ASTCreateQuery & create, const ViewOutputTypeBindingLimits & limits = {});

/// Runtime/startup replay over the exact stored SELECT owned by a View/MV.
/// Besides returning the canonical physical auxiliary table, this annotates
/// every approved CAST AST with its deterministic StoredExpression ordinal so
/// analyzer lookup can consume the already-bound V2 occurrence without a
/// catalog lookup. The annotation is runtime-only and is not formatted or
/// serialized.
[[nodiscard]] std::vector<ViewAuxiliaryPhysicalTypeBindingInput>
collectViewAuxiliaryPhysicalTypeBindingsFromStoredSelect(const ASTPtr & stored_select, const ViewOutputTypeBindingLimits & limits = {});

/// Converts the runtime-only stored-expression annotations of one private
/// View/MV SELECT clone into ordinary physical CAST targets before its owning
/// logical binding is erased. The traversal is closed and bounded, requires
/// tagged CAST ordinals to be contiguous in owner-walk order, and applies no
/// AST mutation until every tagged endpoint has been validated.
void physicalizeViewStoredSelectRuntimeAnnotations(const ASTPtr & stored_select);

/// Applies current-name, introspection-only presentations to the exact
/// physical auxiliary endpoints of a trusted View/MV CREATE AST. Endpoint
/// identity comes solely from site/ordinal/owner key; physical equality is
/// used only to detect that the fetched CREATE snapshot changed.
void applyViewAuxiliaryTypePresentations(
    ASTCreateQuery & create, std::span<const ViewAuxiliaryTypePresentation> presentations, const ViewOutputTypeBindingLimits & limits = {});

/// Rebuilds the canonical source sidecar solely from an immutable bound
/// snapshot and verifies its hash. This is the only native AS/CLONE bridge;
/// no physical header, name, or IDataType equality can create provenance.
[[nodiscard]] PersistedTypeReferences
reconstructPersistedTableSourceReferences(const BoundObjectTypeReferences & source, const PersistedTypeReferencesLimits & limits = {});

/// Retargets an already verified same-authority Table sidecar to a fresh target
/// UUID and reconstructs the target's exact physical-schema binder input.
[[nodiscard]] PreparedTableColumnTypeBindings prepareTableSourceSidecarCopyBindings(
    const SchemaObjectID & target,
    UInt64 target_object_schema_revision,
    const NamesAndTypesList & target_physical_columns,
    const PersistedTypeReferences & source_references,
    const TableColumnTypeBindingLimits & limits = {});

}
