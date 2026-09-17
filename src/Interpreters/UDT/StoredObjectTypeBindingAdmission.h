#pragma once

#include <Interpreters/UDT/DictionaryAttributeTypeBindings.h>
#include <Interpreters/UDT/StoredObjectTypeSupport.h>
#include <Interpreters/UDT/ViewOutputTypeBindings.h>

namespace DB::UDT
{

class StoredObjectUDTPublicationAdmissionProof;

/// In-memory, pre-publication result for ordinary and materialized View output
/// declarations. A logical admission always carries a complete physicalization
/// dispatch. The binding package is not durable and does not authorize a caller
/// to publish metadata, authority state, or runtime objects independently.
class PreparedViewOutputTypeBindingAdmission final
{
public:
    PreparedViewOutputTypeBindingAdmission(const PreparedViewOutputTypeBindingAdmission &) = delete;
    PreparedViewOutputTypeBindingAdmission & operator=(const PreparedViewOutputTypeBindingAdmission &) = delete;
    PreparedViewOutputTypeBindingAdmission(PreparedViewOutputTypeBindingAdmission && other) noexcept;
    PreparedViewOutputTypeBindingAdmission & operator=(PreparedViewOutputTypeBindingAdmission &&) = delete;

    const PreparedViewOutputTypeBindings & getTypeBindings() const noexcept { return type_bindings; }
    const StoredObjectAdmissionDispatch & getAdmission() const noexcept { return admission; }

    /// One-shot capability binding these exact logical references to the
    /// complete physicalization adapter admission that accepted them.
    [[nodiscard]] StoredObjectUDTPublicationAdmissionProof releasePublicationAdmissionProof() &&;

private:
    PreparedViewOutputTypeBindingAdmission(PreparedViewOutputTypeBindings type_bindings_, StoredObjectAdmissionDispatch admission_);

    friend PreparedViewOutputTypeBindingAdmission prepareViewOutputTypeBindingAdmission(
        StoredObjectKind,
        const ASTCreateQuery &,
        const SchemaObjectID &,
        UInt64,
        std::span<const ViewOutputTypeBindingInput>,
        const StoredObjectPhysicalizationAdapterRegistry &,
        const ViewOutputTypeBindingLimits &);

    PreparedViewOutputTypeBindings type_bindings;
    StoredObjectAdmissionDispatch admission;
    bool publication_proof_available = true;
};

/// `object_kind` must be View or MaterializedView. Both use a durable
/// SchemaObjectKind::View identity, while admission retains their separate
/// source-mode and occurrence-site inventories.
[[nodiscard]] PreparedViewOutputTypeBindingAdmission prepareViewOutputTypeBindingAdmission(
    StoredObjectKind object_kind,
    const ASTCreateQuery & create,
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const ViewOutputTypeBindingInput> outputs,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry,
    const ViewOutputTypeBindingLimits & limits = {});

/// Authorizes an already-prepared exact View binding without recomputing the
/// resolver/binder result. The returned proof is detached from the dispatch;
/// the caller must consume it together with this same binding package at the
/// database-owned durable publication boundary.
[[nodiscard]] StoredObjectUDTPublicationAdmissionProof authorizePreparedViewOutputTypeBindings(
    StoredObjectKind object_kind,
    const ASTCreateQuery & create,
    const PreparedViewOutputTypeBindings & type_bindings,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry,
    bool uses_selected_output_classification = false);

/// In-memory, pre-publication result for exact Dictionary attribute bindings.
/// Logical admission requires the ObjectDefinition/DictionaryAttribute route
/// and a complete Dictionary physicalization dispatch.
class PreparedDictionaryAttributeTypeBindingAdmission final
{
public:
    PreparedDictionaryAttributeTypeBindingAdmission(const PreparedDictionaryAttributeTypeBindingAdmission &) = delete;
    PreparedDictionaryAttributeTypeBindingAdmission & operator=(const PreparedDictionaryAttributeTypeBindingAdmission &) = delete;
    PreparedDictionaryAttributeTypeBindingAdmission(PreparedDictionaryAttributeTypeBindingAdmission && other) noexcept;
    PreparedDictionaryAttributeTypeBindingAdmission & operator=(PreparedDictionaryAttributeTypeBindingAdmission &&) = delete;

    const PreparedDictionaryAttributeTypeBindings & getTypeBindings() const noexcept { return type_bindings; }
    const StoredObjectAdmissionDispatch & getAdmission() const noexcept { return admission; }

    /// One-shot capability binding these exact logical references to the
    /// complete physicalization adapter admission that accepted them.
    [[nodiscard]] StoredObjectUDTPublicationAdmissionProof releasePublicationAdmissionProof() &&;

private:
    PreparedDictionaryAttributeTypeBindingAdmission(
        PreparedDictionaryAttributeTypeBindings type_bindings_, StoredObjectAdmissionDispatch admission_);

    friend PreparedDictionaryAttributeTypeBindingAdmission prepareDictionaryAttributeTypeBindingAdmission(
        const ASTCreateQuery &,
        const SchemaObjectID &,
        UInt64,
        std::span<const DictionaryAttributeTypeBindingInput>,
        const StoredObjectPhysicalizationAdapterRegistry &,
        const DictionaryAttributeTypeBindingLimits &);

    PreparedDictionaryAttributeTypeBindings type_bindings;
    StoredObjectAdmissionDispatch admission;
    bool publication_proof_available = true;
};

[[nodiscard]] PreparedDictionaryAttributeTypeBindingAdmission prepareDictionaryAttributeTypeBindingAdmission(
    const ASTCreateQuery & create,
    const SchemaObjectID & dictionary,
    UInt64 object_schema_revision,
    std::span<const DictionaryAttributeTypeBindingInput> attributes,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry,
    const DictionaryAttributeTypeBindingLimits & limits = {});

[[nodiscard]] StoredObjectUDTPublicationAdmissionProof authorizePreparedDictionaryAttributeTypeBindings(
    const ASTCreateQuery & create,
    const PreparedDictionaryAttributeTypeBindings & type_bindings,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry);
}
