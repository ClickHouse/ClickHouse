#include <Interpreters/UDT/StoredObjectTypeBindingAdmission.h>

#include <Databases/UDT/StoredObjectUDTPublicationPackage.h>

#include <Parsers/ASTCreateQuery.h>

#include <stdexcept>
#include <utility>

namespace DB::UDT
{
namespace
{

std::span<const PersistedTypeDescriptor> exactDescriptors(const std::optional<PersistedTypeReferences> & references) noexcept
{
    if (!references)
        return {};
    return references->descriptors;
}

StoredObjectOccurrenceSiteMask requiredViewOccurrenceSites(StoredObjectKind object_kind, const PersistedTypeReferences & references)
{
    if (object_kind != StoredObjectKind::View && object_kind != StoredObjectKind::MaterializedView)
        throw std::logic_error("View occurrence-site admission requires a View object kind");

    StoredObjectOccurrenceSiteMask required = 0;
    for (const auto & path : references.occurrence_paths)
    {
        switch (path.site)
        {
            case PersistedTypeOccurrenceSite::Declaration:
                required |= storedObjectOccurrenceSiteMask(
                    object_kind == StoredObjectKind::View ? StoredObjectOccurrenceSite::ViewOutputDeclaration
                                                          : StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration);
                break;
            case PersistedTypeOccurrenceSite::StoredExpression:
                required |= storedObjectOccurrenceSiteMask(
                    object_kind == StoredObjectKind::View ? StoredObjectOccurrenceSite::ViewStoredCast
                                                          : StoredObjectOccurrenceSite::MaterializedViewStoredCast);
                break;
            case PersistedTypeOccurrenceSite::SchemaString:
                /// V2 deliberately persists the stable endpoint class rather
                /// than a parser/function implementation detail. Require both
                /// closed schema-string adapter capabilities so a partial
                /// registry cannot admit a path it cannot later enumerate.
                required |= storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
                    | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
                break;
        }
    }
    if (!required)
        throw std::logic_error("logical View sidecar has no persisted occurrence sites");
    return required;
}

void validatePreparedBindingPackage(
    const std::optional<PersistedTypeReferences> & references,
    const std::optional<BoundObjectPhysicalSchema> & physical_schema,
    const std::optional<SidecarExpectationRecord> & expectation,
    std::span<const SchemaObjectDependencyEdge> dependency_edges,
    const StoredObjectAdmissionDispatch & admission)
{
    const bool has_logical_bindings = references.has_value();
    const bool has_dependency_edges = !dependency_edges.empty();
    if (physical_schema.has_value() != has_logical_bindings || expectation.has_value() != has_logical_bindings
        || has_dependency_edges != has_logical_bindings)
        throw std::logic_error("prepared stored-object type-binding package is not indivisible");

    const auto & result = admission.getAdmission();
    if (result.isAccepted() && result.hasLogicalReferences() != has_logical_bindings)
        throw std::logic_error("accepted stored-object admission disagrees with its exact type bindings");
    if (result.hasLogicalReferences())
    {
        const auto * physicalization_dispatch = admission.tryGetPhysicalizationDispatch();
        if (!physicalization_dispatch)
            throw std::logic_error("logical stored-object admission has no physicalization dispatch");
        if (result.getExactDescriptorCount() != references->descriptors.size())
            throw std::logic_error("logical stored-object admission descriptor count disagrees with its sidecar");
        if (physicalization_dispatch->getSchemaObjectKind() != references->object.kind)
            throw std::logic_error("logical stored-object admission adapter disagrees with its durable object kind");
    }
}

}

PreparedViewOutputTypeBindingAdmission::PreparedViewOutputTypeBindingAdmission(
    PreparedViewOutputTypeBindings type_bindings_, StoredObjectAdmissionDispatch admission_)
    : type_bindings(std::move(type_bindings_))
    , admission(std::move(admission_))
{
}

PreparedViewOutputTypeBindingAdmission::PreparedViewOutputTypeBindingAdmission(PreparedViewOutputTypeBindingAdmission && other) noexcept
    : type_bindings(std::move(other.type_bindings))
    , admission(std::move(other.admission))
    , publication_proof_available(other.publication_proof_available)
{
    other.publication_proof_available = false;
}

StoredObjectUDTPublicationAdmissionProof PreparedViewOutputTypeBindingAdmission::releasePublicationAdmissionProof() &&
{
    if (!publication_proof_available)
        throw std::logic_error("view UDT publication admission proof was already consumed");
    publication_proof_available = false;

    const auto & result = admission.getAdmission();
    const auto * dispatch = admission.tryGetPhysicalizationDispatch();
    if (!result.hasLogicalReferences() || !dispatch || !type_bindings.persisted_references || !type_bindings.sidecar_expectation)
        throw std::logic_error("view UDT publication requires complete logical binding admission");
    const auto & references = *type_bindings.persisted_references;
    const auto & expectation = *type_bindings.sidecar_expectation;
    if (references.object != expectation.object || references.object_schema_revision != expectation.object_schema_revision
        || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || dispatch->getSchemaObjectKind() != references.object.kind || result.getExactDescriptorCount() != references.descriptors.size())
        throw std::logic_error("view UDT publication admission no longer matches its exact bindings");
    return StoredObjectUDTPublicationAdmissionProof(
        expectation.object,
        expectation.object_schema_revision,
        expectation.sidecar_hash,
        expectation.physical_schema_fingerprint,
        result.getExactDescriptorCount());
}

PreparedViewOutputTypeBindingAdmission prepareViewOutputTypeBindingAdmission(
    StoredObjectKind object_kind,
    const ASTCreateQuery & create,
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const ViewOutputTypeBindingInput> outputs,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry,
    const ViewOutputTypeBindingLimits & limits)
{
    if (object_kind != StoredObjectKind::View && object_kind != StoredObjectKind::MaterializedView)
        throw std::invalid_argument("view output type-binding admission requires a View object kind");

    auto type_bindings = prepareViewOutputTypeBindings(view, object_schema_revision, outputs, limits);
    const auto descriptors = exactDescriptors(type_bindings.persisted_references);
    auto admission = object_kind == StoredObjectKind::View
        ? admitStoredObjectExplicitViewOutputCreate(create, view.database_uuid, descriptors, adapter_registry)
        : admitStoredObjectExplicitMaterializedViewOutputCreate(create, view.database_uuid, descriptors, adapter_registry);
    validatePreparedBindingPackage(
        type_bindings.persisted_references,
        type_bindings.bound_physical_schema,
        type_bindings.sidecar_expectation,
        type_bindings.dependency_edges,
        admission);
    return PreparedViewOutputTypeBindingAdmission(std::move(type_bindings), std::move(admission));
}

StoredObjectUDTPublicationAdmissionProof authorizePreparedViewOutputTypeBindings(
    StoredObjectKind object_kind,
    const ASTCreateQuery & create,
    const PreparedViewOutputTypeBindings & type_bindings,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry,
    bool uses_selected_output_classification)
{
    if (object_kind != StoredObjectKind::View && object_kind != StoredObjectKind::MaterializedView)
        throw std::invalid_argument("prepared View output admission requires a View object kind");
    if (!type_bindings.persisted_references || !type_bindings.sidecar_expectation)
        throw std::logic_error("prepared View output admission has no complete logical binding package");

    const auto classification = classifyStoredObjectCreateQuery(create);
    if (classification.object_kind != object_kind || !classification.structured_udt_scan_complete
        || !classification.type_string_scan_complete || !classification.has_explicit_destination_columns
        || (classification.source_mode != StoredObjectSourceMode::AsSelect
            && classification.source_mode != StoredObjectSourceMode::EmptyAsSelect)
        || classification.structured_udt_occurrence_sites != 0 || classification.qualified_type_reference_candidate_sites != 0
        || classification.source_query_has_structured_udt_reference || classification.has_unclassified_udt_reference
        || classification.source_query_has_unclassified_table_function || create.attach || create.if_not_exists || create.replace_view
        || create.replace_table || create.create_or_replace || create.has_attach_from_path || create.attach_short_syntax
        || create.attach_as_replicated.has_value() || !create.cluster.empty()
        || (object_kind != StoredObjectKind::MaterializedView && (create.targets || create.is_populate)) || create.refresh_strategy)
        throw std::logic_error("physicalized View CREATE changed after exact declaration preparation");

    const auto descriptors = exactDescriptors(type_bindings.persisted_references);
    const auto result = [&]
    {
        if (!uses_selected_output_classification)
        {
            return admitStoredObjectExplicitDestination(
                object_kind,
                classification.source_mode,
                type_bindings.persisted_references->object.database_uuid,
                descriptors,
                adapter_registry);
        }

        std::vector<StoredObjectSelectedOutput> outputs;
        outputs.reserve(type_bindings.physical_outputs.size());
        for (size_t index = 0; index < type_bindings.physical_outputs.size(); ++index)
            outputs.push_back(StoredObjectSelectedOutput::physical());
        std::vector<std::optional<StoredObjectOccurrenceSite>> descriptor_sites(descriptors.size());
        const auto output_site = object_kind == StoredObjectKind::View ? StoredObjectOccurrenceSite::ViewOutputDeclaration
                                                                       : StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration;
        const auto stored_expression_site = object_kind == StoredObjectKind::View ? StoredObjectOccurrenceSite::ViewStoredCast
                                                                                  : StoredObjectOccurrenceSite::MaterializedViewStoredCast;
        for (const auto & use : type_bindings.persisted_references->uses)
        {
            if (use.descriptor_id >= descriptors.size() || use.path_id >= type_bindings.persisted_references->occurrence_paths.size())
                throw std::logic_error("prepared selected View sidecar has an invalid descriptor use");
            const auto persisted_site = type_bindings.persisted_references->occurrence_paths[use.path_id].site;
            const auto site = [&]
            {
                switch (persisted_site)
                {
                    case PersistedTypeOccurrenceSite::Declaration: return output_site;
                    case PersistedTypeOccurrenceSite::StoredExpression: return stored_expression_site;
                    case PersistedTypeOccurrenceSite::SchemaString: return StoredObjectOccurrenceSite::TableFunctionSchemaString;
                }
                throw std::logic_error("prepared selected View sidecar has an unknown occurrence site");
            }();
            auto & retained_site = descriptor_sites[use.descriptor_id];
            if (!retained_site)
                retained_site = site;
        }
        std::vector<StoredObjectExactOccurrence> exact_occurrences;
        exact_occurrences.reserve(descriptors.size());
        for (size_t descriptor_index = 0; descriptor_index < descriptors.size(); ++descriptor_index)
        {
            if (!descriptor_sites[descriptor_index])
                throw std::logic_error("prepared selected View sidecar has an unused descriptor");
            exact_occurrences.push_back({.site = *descriptor_sites[descriptor_index], .descriptor = descriptors[descriptor_index]});
        }
        return admitStoredObjectSelectedOutputs(
            object_kind,
            classification.source_mode,
            type_bindings.persisted_references->object.database_uuid,
            outputs,
            exact_occurrences,
            true,
            adapter_registry);
    }();
    const auto required_sites = requiredViewOccurrenceSites(object_kind, *type_bindings.persisted_references);
    const auto dispatch = adapter_registry.tryGetDispatch(object_kind, classification.source_mode, required_sites);
    const auto & references = *type_bindings.persisted_references;
    const auto & expectation = *type_bindings.sidecar_expectation;
    if (!type_bindings.bound_physical_schema || type_bindings.dependency_edges.empty() || !result.isAccepted()
        || !result.hasLogicalReferences() || !dispatch || references.object != expectation.object
        || references.object_schema_revision != expectation.object_schema_revision
        || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || dispatch->getSchemaObjectKind() != references.object.kind || result.getExactDescriptorCount() != references.descriptors.size())
        throw std::logic_error("prepared View output publication admission differs from its exact bindings");
    return StoredObjectUDTPublicationAdmissionProof(
        expectation.object,
        expectation.object_schema_revision,
        expectation.sidecar_hash,
        expectation.physical_schema_fingerprint,
        result.getExactDescriptorCount());
}

PreparedDictionaryAttributeTypeBindingAdmission::PreparedDictionaryAttributeTypeBindingAdmission(
    PreparedDictionaryAttributeTypeBindings type_bindings_, StoredObjectAdmissionDispatch admission_)
    : type_bindings(std::move(type_bindings_))
    , admission(std::move(admission_))
{
}

PreparedDictionaryAttributeTypeBindingAdmission::PreparedDictionaryAttributeTypeBindingAdmission(
    PreparedDictionaryAttributeTypeBindingAdmission && other) noexcept
    : type_bindings(std::move(other.type_bindings))
    , admission(std::move(other.admission))
    , publication_proof_available(other.publication_proof_available)
{
    other.publication_proof_available = false;
}

StoredObjectUDTPublicationAdmissionProof PreparedDictionaryAttributeTypeBindingAdmission::releasePublicationAdmissionProof() &&
{
    if (!publication_proof_available)
        throw std::logic_error("dictionary UDT publication admission proof was already consumed");
    publication_proof_available = false;

    const auto & result = admission.getAdmission();
    const auto * dispatch = admission.tryGetPhysicalizationDispatch();
    if (!result.hasLogicalReferences() || !dispatch || !type_bindings.persisted_references || !type_bindings.sidecar_expectation)
        throw std::logic_error("dictionary UDT publication requires complete logical binding admission");
    const auto & references = *type_bindings.persisted_references;
    const auto & expectation = *type_bindings.sidecar_expectation;
    if (references.object != expectation.object || references.object_schema_revision != expectation.object_schema_revision
        || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || dispatch->getSchemaObjectKind() != references.object.kind || result.getExactDescriptorCount() != references.descriptors.size())
        throw std::logic_error("dictionary UDT publication admission no longer matches its exact bindings");
    return StoredObjectUDTPublicationAdmissionProof(
        expectation.object,
        expectation.object_schema_revision,
        expectation.sidecar_hash,
        expectation.physical_schema_fingerprint,
        result.getExactDescriptorCount());
}

PreparedDictionaryAttributeTypeBindingAdmission prepareDictionaryAttributeTypeBindingAdmission(
    const ASTCreateQuery & create,
    const SchemaObjectID & dictionary,
    UInt64 object_schema_revision,
    std::span<const DictionaryAttributeTypeBindingInput> attributes,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry,
    const DictionaryAttributeTypeBindingLimits & limits)
{
    auto type_bindings = prepareDictionaryAttributeTypeBindings(dictionary, object_schema_revision, attributes, limits);
    const auto descriptors = exactDescriptors(type_bindings.persisted_references);
    auto admission = admitStoredObjectDictionaryAttributeCreate(create, dictionary.database_uuid, descriptors, adapter_registry);
    validatePreparedBindingPackage(
        type_bindings.persisted_references,
        type_bindings.bound_physical_schema,
        type_bindings.sidecar_expectation,
        type_bindings.dependency_edges,
        admission);
    return PreparedDictionaryAttributeTypeBindingAdmission(std::move(type_bindings), std::move(admission));
}

StoredObjectUDTPublicationAdmissionProof authorizePreparedDictionaryAttributeTypeBindings(
    const ASTCreateQuery & create,
    const PreparedDictionaryAttributeTypeBindings & type_bindings,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    if (!type_bindings.persisted_references || !type_bindings.sidecar_expectation)
        throw std::logic_error("prepared Dictionary attribute admission has no complete logical binding package");

    const auto classification = classifyStoredObjectCreateQuery(create);
    if (classification.object_kind != StoredObjectKind::Dictionary || classification.source_mode != StoredObjectSourceMode::ObjectDefinition
        || !classification.structured_udt_scan_complete || !classification.type_string_scan_complete
        || classification.structured_udt_occurrence_sites != 0 || classification.qualified_type_reference_candidate_sites != 0
        || classification.source_query_has_structured_udt_reference || classification.has_unclassified_udt_reference || create.attach
        || create.if_not_exists || create.replace_view || create.replace_table || create.create_or_replace || create.has_attach_from_path
        || create.attach_short_syntax || create.attach_as_replicated.has_value() || !create.cluster.empty() || create.targets
        || create.is_populate)
        throw std::logic_error("physicalized Dictionary CREATE changed after exact attribute preparation");

    const auto descriptors = exactDescriptors(type_bindings.persisted_references);
    const auto result = admitStoredObjectExplicitDestination(
        StoredObjectKind::Dictionary,
        classification.source_mode,
        type_bindings.persisted_references->object.database_uuid,
        descriptors,
        adapter_registry);
    const auto dispatch = adapter_registry.tryGetDispatch(
        StoredObjectKind::Dictionary,
        classification.source_mode,
        storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::DictionaryAttribute));
    const auto & references = *type_bindings.persisted_references;
    const auto & expectation = *type_bindings.sidecar_expectation;
    if (!type_bindings.bound_physical_schema || type_bindings.dependency_edges.empty() || !result.isAccepted()
        || !result.hasLogicalReferences() || !dispatch || references.object != expectation.object
        || references.object_schema_revision != expectation.object_schema_revision
        || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || dispatch->getSchemaObjectKind() != references.object.kind || result.getExactDescriptorCount() != references.descriptors.size())
        throw std::logic_error("prepared Dictionary publication admission differs from its exact bindings");
    return StoredObjectUDTPublicationAdmissionProof(
        expectation.object,
        expectation.object_schema_revision,
        expectation.sidecar_hash,
        expectation.physical_schema_fingerprint,
        result.getExactDescriptorCount());
}
}
