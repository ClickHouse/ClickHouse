#pragma once

#include <Databases/UDT/AuthorityResourceUsageIndex.h>

#include <DataTypes/UDT/PersistedTypeReferences.h>

#include <Core/Types.h>

#include <cstddef>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace DB::UDT::Test
{

struct DependentObjectResourceImageInput
{
    String canonical_metadata_bytes;
    PersistedTypeReferences references;
    String canonical_installation_record_bytes;
};

/// Owns every byte range exposed through AuthorityDependentObjectResourceImage.
/// AuthorityRootBuilder consumes the views synchronously and retains only the
/// decoded contribution, so this batch needs to live only through build().
class DependentObjectResourceImageBatch final
{
public:
    DependentObjectResourceImageBatch(
        std::span<const SidecarExpectationRecord> expectations, std::vector<DependentObjectResourceImageInput> inputs)
    {
        if (expectations.size() != inputs.size())
            throw std::logic_error("test dependent-object resource image count differs from its expectation count");

        canonical_metadata_bytes.reserve(inputs.size());
        canonical_sidecar_bytes.reserve(inputs.size());
        canonical_installation_record_bytes.reserve(inputs.size());
        for (size_t index = 0; index < inputs.size(); ++index)
        {
            auto & input = inputs[index];
            const auto & expectation = expectations[index];
            if (input.canonical_metadata_bytes.empty() || input.references.object != expectation.object
                || input.references.object_schema_revision != expectation.object_schema_revision
                || input.references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
                || input.references.semantic_extension_version != expectation.semantic_extension_version
                || input.references.semantic_extension_flags != expectation.semantic_extension_flags
                || computePersistedTypeReferencesSidecarHash(input.references) != expectation.sidecar_hash
                || (expectation.installation_record_hash.has_value() != !input.canonical_installation_record_bytes.empty()))
            {
                throw std::logic_error("test dependent-object resource image differs from its exact expectation");
            }

            canonical_metadata_bytes.push_back(std::move(input.canonical_metadata_bytes));
            canonical_sidecar_bytes.push_back(encodePersistedTypeReferences(input.references));
            canonical_installation_record_bytes.push_back(std::move(input.canonical_installation_record_bytes));
        }

        resource_images.reserve(inputs.size());
        for (size_t index = 0; index < inputs.size(); ++index)
        {
            resource_images.push_back({
                .object = expectations[index].object,
                .canonical_metadata_bytes = canonical_metadata_bytes[index],
                .canonical_sidecar_bytes = canonical_sidecar_bytes[index],
                .canonical_installation_record_bytes = canonical_installation_record_bytes[index],
            });
        }
    }

    DependentObjectResourceImageBatch(const DependentObjectResourceImageBatch &) = delete;
    DependentObjectResourceImageBatch & operator=(const DependentObjectResourceImageBatch &) = delete;
    DependentObjectResourceImageBatch(DependentObjectResourceImageBatch &&) = delete;
    DependentObjectResourceImageBatch & operator=(DependentObjectResourceImageBatch &&) = delete;

    std::span<const AuthorityDependentObjectResourceImage> get() const noexcept { return resource_images; }

private:
    std::vector<String> canonical_metadata_bytes;
    std::vector<String> canonical_sidecar_bytes;
    std::vector<String> canonical_installation_record_bytes;
    std::vector<AuthorityDependentObjectResourceImage> resource_images;
};

inline PersistedTypeReferences singleDefinitionPersistedTypeReferences(
    const SchemaObjectID & object,
    UInt64 object_schema_revision,
    const Digest & physical_schema_fingerprint,
    const Definition::Ptr & definition,
    DataTypePtr physical_type,
    PersistedTypePathSection section,
    UInt16 semantic_extension_version = 1,
    UInt16 semantic_extension_flags = 0)
{
    if (!object.isValid() || object.kind == SchemaObjectKind::TypeDefinition || object_schema_revision == 0 || !definition
        || definition->getIdentity().database_uuid != object.database_uuid || !physical_type)
    {
        throw std::logic_error("test single-definition sidecar input is invalid");
    }

    const auto instantiated = InstantiatedTypeDescriptor::create(
        definition, CanonicalTypeArguments::validate(definition->getParameters(), {}), std::move(physical_type));
    PersistedTypeReferences references;
    references.object = object;
    references.object_schema_revision = object_schema_revision;
    references.physical_schema_fingerprint = physical_schema_fingerprint;
    references.semantic_extension_version = semantic_extension_version;
    references.semantic_extension_flags = semantic_extension_flags;
    references.descriptors.push_back(instantiated->getPersistedDescriptor());
    references.occurrence_paths.push_back({
        .section = section,
        .site = PersistedTypeOccurrenceSite::Declaration,
        .object_ordinal = 0,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    });
    references.uses.push_back({.path_id = 0, .descriptor_id = 0});
    return references;
}

inline SidecarExpectationRecord sidecarExpectationFor(const PersistedTypeReferences & references)
{
    return {
        .object = references.object,
        .object_schema_revision = references.object_schema_revision,
        .sidecar_hash = computePersistedTypeReferencesSidecarHash(references),
        .physical_schema_fingerprint = references.physical_schema_fingerprint,
        .semantic_extension_version = references.semantic_extension_version,
        .semantic_extension_flags = references.semantic_extension_flags,
    };
}

inline std::vector<DependentObjectResourceImageInput>
dependentObjectResourceImageInputs(std::span<const PersistedTypeReferences> references)
{
    std::vector<DependentObjectResourceImageInput> result;
    result.reserve(references.size());
    for (const auto & current_references : references)
    {
        result.push_back({
            .canonical_metadata_bytes = "synthetic-test-metadata",
            .references = current_references,
            .canonical_installation_record_bytes = {},
        });
    }
    return result;
}

}
