#include <DataTypes/UDT/BoundObjectTypeReferences.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/isUDTResourceOrControlExceptionCode.h>

#include <Common/Exception.h>

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = BoundObjectTypeReferencesError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

bool pathLess(const PersistedTypeOccurrencePath & lhs, const PersistedTypeOccurrencePath & rhs) noexcept
{
    const UInt8 lhs_section = static_cast<UInt8>(lhs.section);
    const UInt8 rhs_section = static_cast<UInt8>(rhs.section);
    if (lhs_section != rhs_section)
        return lhs_section < rhs_section;
    const UInt8 lhs_site = static_cast<UInt8>(lhs.site);
    const UInt8 rhs_site = static_cast<UInt8>(rhs.site);
    if (lhs_site != rhs_site)
        return lhs_site < rhs_site;
    if (lhs.object_ordinal != rhs.object_ordinal)
        return lhs.object_ordinal < rhs.object_ordinal;
    if (lhs.occurrence_ordinal != rhs.occurrence_ordinal)
        return lhs.occurrence_ordinal < rhs.occurrence_ordinal;
    return std::lexicographical_compare(
        lhs.type_child_ordinals.begin(), lhs.type_child_ordinals.end(), rhs.type_child_ordinals.begin(), rhs.type_child_ordinals.end());
}

UInt64 checkedSize(std::size_t value)
{
    static_assert(sizeof(std::size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

void validatePersistedLimits(const PersistedTypeReferencesLimits & limits)
{
    constexpr PersistedTypeReferencesLimits implementation_maxima;
    if (!limits.maximum_sidecar_bytes || !limits.maximum_descriptors || !limits.maximum_occurrence_paths
        || !limits.maximum_canonical_arguments_bytes || !limits.maximum_canonical_physical_type_bytes
        || !limits.maximum_qualified_name_bytes)
        fail(Error::Code::InvalidConfiguration, "every persisted type references limit except path depth must be nonzero");
    if (limits.maximum_sidecar_bytes > implementation_maxima.maximum_sidecar_bytes
        || limits.maximum_descriptors > implementation_maxima.maximum_descriptors
        || limits.maximum_occurrence_paths > implementation_maxima.maximum_occurrence_paths
        || limits.maximum_path_depth > implementation_maxima.maximum_path_depth
        || limits.maximum_canonical_arguments_bytes > implementation_maxima.maximum_canonical_arguments_bytes
        || limits.maximum_canonical_physical_type_bytes > implementation_maxima.maximum_canonical_physical_type_bytes
        || limits.maximum_qualified_name_bytes > implementation_maxima.maximum_qualified_name_bytes)
        fail(Error::Code::InvalidConfiguration, "a persisted type references limit exceeds the V1 implementation maximum");
}

void validateLimits(const BoundObjectTypeReferencesLimits & limits)
{
    validatePersistedLimits(limits.persisted);
    try
    {
        validateCanonicalTypeArgumentLimits(limits.type_arguments);
        validateTypeDescriptorLimits(limits.descriptor);
        validateTemplateSpecializerLimits(limits.specializer);
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const Exception & error)
    {
        if (isUDTResourceOrControlExceptionCode(error.code()))
            throw;
        fail(Error::Code::InvalidConfiguration, error.message());
    }
    catch (const DescriptorError & error)
    {
        fail(Error::Code::InvalidConfiguration, error.what());
    }
    catch (const TemplateSpecializerError & error)
    {
        fail(Error::Code::InvalidConfiguration, error.what());
    }

    if (!limits.maximum_retained_path_components || limits.maximum_retained_path_components > (4ULL << 20))
        fail(Error::Code::InvalidConfiguration, "bound object path-component limit is invalid");
    if (!limits.maximum_single_runtime_owner_key_bytes || limits.maximum_single_runtime_owner_key_bytes > (1ULL << 20)
        || !limits.maximum_retained_runtime_owner_key_bytes || limits.maximum_retained_runtime_owner_key_bytes > (64ULL << 20)
        || limits.maximum_single_runtime_owner_key_bytes > limits.maximum_retained_runtime_owner_key_bytes)
        fail(Error::Code::InvalidConfiguration, "bound object runtime owner-key limits are invalid");
    if (limits.persisted.maximum_descriptors > limits.specializer.maximum_distinct_specializations
        || limits.persisted.maximum_descriptors > limits.descriptor.maximum_descriptors
        || limits.persisted.maximum_occurrence_paths > limits.descriptor.maximum_occurrences
        || limits.persisted.maximum_path_depth > limits.descriptor.maximum_path_depth
        || limits.persisted.maximum_canonical_arguments_bytes > limits.descriptor.maximum_canonical_arguments_bytes
        || limits.persisted.maximum_canonical_arguments_bytes > limits.specializer.maximum_canonical_argument_bytes
        || limits.persisted.maximum_canonical_physical_type_bytes > limits.descriptor.maximum_canonical_physical_type_bytes
        || limits.persisted.maximum_qualified_name_bytes > limits.descriptor.maximum_qualified_name_bytes)
        fail(Error::Code::InvalidConfiguration, "bound object limits disagree on descriptor or occurrence maxima");
}

Error::Code mapSpecializerError(const TemplateSpecializerError & error) noexcept
{
    switch (error.code)
    {
        case TemplateSpecializerError::Code::LimitExceeded: return Error::Code::LimitExceeded;
        case TemplateSpecializerError::Code::MissingCapability:
        case TemplateSpecializerError::Code::AuthorityFailure:
        case TemplateSpecializerError::Code::InvalidIdentity:
        case TemplateSpecializerError::Code::DefinitionNotFound:
        case TemplateSpecializerError::Code::DependencyMismatch: return Error::Code::AuthorityFailure;
        case TemplateSpecializerError::Code::InvalidAttemptState:
        case TemplateSpecializerError::Code::InvalidArguments:
        case TemplateSpecializerError::Code::InvalidTemplate:
        case TemplateSpecializerError::Code::ActiveCycle:
        case TemplateSpecializerError::Code::NonDecreasingRecursion: return Error::Code::DescriptorMismatch;
    }
    return Error::Code::DescriptorMismatch;
}

}

BoundObjectTypeReferencesError::BoundObjectTypeReferencesError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

BoundObjectTypeReferenceUse::BoundObjectTypeReferenceUse(
    PersistedTypeOccurrencePath path_,
    UInt32 descriptor_index_,
    DataTypePtr physical_type_,
    String runtime_owner_key_,
    SemanticCapabilityMask semantic_capabilities_)
    : path(std::move(path_))
    , descriptor_index(descriptor_index_)
    , physical_type(std::move(physical_type_))
    , runtime_owner_key(std::move(runtime_owner_key_))
    , semantic_capabilities(semantic_capabilities_)
{
}

static std::vector<UInt32> makeRuntimeUseIndices(const std::vector<BoundObjectTypeReferenceUse> & uses)
{
    std::vector<UInt32> result;
    result.reserve(uses.size());
    for (size_t index = 0; index < uses.size(); ++index)
    {
        if (index > std::numeric_limits<UInt32>::max())
            fail(Error::Code::LimitExceeded, "bound runtime use index exceeds UInt32");
        if (!uses[index].getRuntimeOwnerKey().empty())
            result.push_back(static_cast<UInt32>(index));
    }

    std::sort(
        result.begin(),
        result.end(),
        [&](UInt32 lhs_index, UInt32 rhs_index)
        {
            const auto & lhs = uses[lhs_index];
            const auto & rhs = uses[rhs_index];
            if (lhs.getPath().section != rhs.getPath().section)
                return static_cast<UInt8>(lhs.getPath().section) < static_cast<UInt8>(rhs.getPath().section);
            if (lhs.getPath().site != rhs.getPath().site)
                return static_cast<UInt8>(lhs.getPath().site) < static_cast<UInt8>(rhs.getPath().site);
            if (lhs.getRuntimeOwnerKey() != rhs.getRuntimeOwnerKey())
                return lhs.getRuntimeOwnerKey() < rhs.getRuntimeOwnerKey();
            if (lhs.getPath().type_child_ordinals != rhs.getPath().type_child_ordinals)
                return std::lexicographical_compare(
                    lhs.getPath().type_child_ordinals.begin(),
                    lhs.getPath().type_child_ordinals.end(),
                    rhs.getPath().type_child_ordinals.begin(),
                    rhs.getPath().type_child_ordinals.end());
            return lhs.getPath().occurrence_ordinal < rhs.getPath().occurrence_ordinal;
        });
    return result;
}

BoundObjectTypeReferences::BoundObjectTypeReferences(
    UInt16 format_version_,
    UInt16 path_dictionary_version_,
    SchemaObjectID object_,
    UInt64 object_schema_revision_,
    Digest sidecar_hash_,
    Digest physical_schema_fingerprint_,
    std::vector<InstantiatedTypeDescriptor::Ptr> descriptors_,
    std::vector<Definition::Ptr> definition_handles_,
    std::vector<BoundObjectTypeReferenceUse> uses_,
    SemanticCapabilityMask semantic_capabilities_)
    : format_version(format_version_)
    , path_dictionary_version(path_dictionary_version_)
    , object(object_)
    , object_schema_revision(object_schema_revision_)
    , sidecar_hash(sidecar_hash_)
    , physical_schema_fingerprint(physical_schema_fingerprint_)
    , descriptors(std::move(descriptors_))
    , definition_handles(std::move(definition_handles_))
    , uses(std::move(uses_))
    , runtime_use_indices(makeRuntimeUseIndices(uses))
    , semantic_capabilities(semantic_capabilities_)
{
}

BoundObjectTypeReferences::Ptr BoundObjectTypeReferences::bind(
    const PersistedTypeReferences & references,
    BoundObjectPhysicalSchema physical_schema,
    const IAuthorityAdapter & authority,
    const BoundObjectTypeReferencesLimits & limits)
{
    validateLimits(limits);
    Digest sidecar_hash{};
    try
    {
        sidecar_hash = computePersistedTypeReferencesSidecarHash(references, limits.persisted);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        const auto code = error.code == PersistedTypeReferencesError::Code::LimitExceeded ? Error::Code::LimitExceeded
                                                                                          : Error::Code::DescriptorMismatch;
        fail(code, error.what());
    }

    if (physical_schema.object != references.object || physical_schema.object_schema_revision != references.object_schema_revision)
        fail(Error::Code::ObjectMismatch, "bound physical schema identity or revision differs from its sidecar");
    if (physical_schema.physical_schema_fingerprint != references.physical_schema_fingerprint)
        fail(Error::Code::PhysicalSchemaMismatch, "bound physical schema fingerprint differs from its sidecar");
    if (physical_schema.occurrences.size() != references.occurrence_paths.size())
        fail(Error::Code::PathMismatch, "bound physical occurrence count differs from its sidecar");
    if (references.descriptors.size() > limits.specializer.maximum_distinct_specializations)
        fail(Error::Code::LimitExceeded, "bound descriptor count exceeds the specialization limit");

    UInt64 retained_path_components = 0;
    UInt64 retained_runtime_owner_key_bytes = 0;
    for (std::size_t index = 0; index < physical_schema.occurrences.size(); ++index)
    {
        const auto & occurrence = physical_schema.occurrences[index];
        if (occurrence.path != references.occurrence_paths[index])
            fail(Error::Code::PathMismatch, "bound physical occurrence path differs from its sidecar");
        if (!occurrence.physical_type)
            fail(Error::Code::PhysicalSchemaMismatch, "bound physical occurrence has no physical type");
        const UInt64 depth = checkedSize(occurrence.path.type_child_ordinals.size());
        if (depth > limits.maximum_retained_path_components - retained_path_components)
            fail(Error::Code::LimitExceeded, "bound occurrence path components exceed their retained limit");
        retained_path_components += depth;
        const UInt64 owner_key_bytes = checkedSize(occurrence.runtime_owner_key.size());
        if (owner_key_bytes > limits.maximum_single_runtime_owner_key_bytes
            || owner_key_bytes > limits.maximum_retained_runtime_owner_key_bytes - retained_runtime_owner_key_bytes)
            fail(Error::Code::LimitExceeded, "bound occurrence runtime owner keys exceed their retained byte limit");
        retained_runtime_owner_key_bytes += owner_key_bytes;
    }

    std::vector<TemplateSpecializationID> specialization_ids;
    specialization_ids.reserve(references.descriptors.size());
    FinishedTemplateSpecializations finished;
    try
    {
        auto attempt = TemplateSpecializer::Attempt::begin(authority, limits.specializer);
        for (const auto & descriptor : references.descriptors)
        {
            specialization_ids.push_back(attempt.specializeEncoded(
                descriptor.getDefinitionIdentity(), descriptor.getCanonicalArgumentsEncoding(), limits.type_arguments));
        }
        finished = attempt.finish();
    }
    catch (const TemplateSpecializerError & error)
    {
        fail(mapSpecializerError(error), error.what());
    }

    std::vector<InstantiatedTypeDescriptor::Ptr> descriptors;
    descriptors.reserve(references.descriptors.size());
    for (std::size_t descriptor_index = 0; descriptor_index < references.descriptors.size(); ++descriptor_index)
    {
        const auto specialization_id = specialization_ids[descriptor_index];
        if (specialization_id >= finished.specializations.size())
            fail(Error::Code::AuthorityFailure, "bound specialization result omitted a requested descriptor");
        auto & specialization = finished.specializations[specialization_id];
        if (specialization.definition_handle_index >= finished.definition_handles.size()
            || specialization.definition_identity != references.descriptors[descriptor_index].getDefinitionIdentity())
            fail(Error::Code::AuthorityFailure, "bound specialization result has an invalid definition handle");

        try
        {
            auto physical_type = DataTypeFactory::instance().get(specialization.canonical_physical_ast);
            auto descriptor = InstantiatedTypeDescriptor::create(
                finished.definition_handles[specialization.definition_handle_index],
                specialization.canonical_arguments,
                std::move(physical_type),
                limits.descriptor);
            if (!descriptor->getPersistedDescriptor().hasSameInstantiation(references.descriptors[descriptor_index]))
                fail(Error::Code::DescriptorMismatch, "persisted descriptor differs from its checked specialization");
            descriptors.push_back(std::move(descriptor));
        }
        catch (const Error &)
        {
            throw;
        }
        catch (const DescriptorError & error)
        {
            fail(Error::Code::DescriptorMismatch, error.what());
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const Exception & error)
        {
            if (isUDTResourceOrControlExceptionCode(error.code()))
                throw;
            fail(Error::Code::DescriptorMismatch, error.message());
        }
    }

    std::vector<BoundObjectTypeReferenceUse> uses;
    uses.reserve(references.uses.size());
    SemanticCapabilityMask semantic_capabilities = 0;
    for (std::size_t index = 0; index < references.uses.size(); ++index)
    {
        const UInt64 descriptor_id = references.uses[index].descriptor_id;
        if (descriptor_id >= descriptors.size() || descriptor_id > std::numeric_limits<UInt32>::max())
            fail(Error::Code::DescriptorMismatch, "bound occurrence references an invalid descriptor index");
        const auto & descriptor = descriptors[static_cast<std::size_t>(descriptor_id)];
        const auto & persisted = descriptor->getPersistedDescriptor();
        auto & actual_type = physical_schema.occurrences[index].physical_type;
        try
        {
            if (!actual_type->equals(*descriptor->getPhysicalType()) || actual_type->getName() != persisted.getCanonicalPhysicalType()
                || physicalTypeFingerprint(actual_type) != persisted.getStorageFingerprint())
                fail(Error::Code::PhysicalSchemaMismatch, "bound physical occurrence differs from its checked descriptor");
        }
        catch (const Error &)
        {
            throw;
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const Exception & error)
        {
            if (isUDTResourceOrControlExceptionCode(error.code()))
                throw;
            fail(Error::Code::PhysicalSchemaMismatch, error.message());
        }

        const SemanticCapabilityMask use_capabilities = physical_schema.occurrences[index].selected_semantic_capabilities;
        if ((use_capabilities & static_cast<SemanticCapabilityMask>(~all_semantic_capabilities)) != 0
            || (use_capabilities & persisted.getSemanticCapabilities()) != use_capabilities)
            fail(Error::Code::PhysicalSchemaMismatch, "bound occurrence semantic capabilities disagree with its checked descriptor");
        semantic_capabilities = static_cast<SemanticCapabilityMask>(semantic_capabilities | use_capabilities);
        uses.push_back(BoundObjectTypeReferenceUse(
            references.occurrence_paths[index],
            static_cast<UInt32>(descriptor_id),
            std::move(actual_type),
            std::move(physical_schema.occurrences[index].runtime_owner_key),
            use_capabilities));
    }

    return Ptr(new BoundObjectTypeReferences(
        references.format_version,
        references.path_dictionary_version,
        references.object,
        references.object_schema_revision,
        sidecar_hash,
        references.physical_schema_fingerprint,
        std::move(descriptors),
        std::move(finished.definition_handles),
        std::move(uses),
        semantic_capabilities));
}

const BoundObjectTypeReferenceUse * BoundObjectTypeReferences::findUse(const PersistedTypeOccurrencePath & path) const noexcept
{
    const auto found = std::lower_bound(
        uses.begin(),
        uses.end(),
        path,
        [](const BoundObjectTypeReferenceUse & use, const PersistedTypeOccurrencePath & key) { return pathLess(use.getPath(), key); });
    return found != uses.end() && found->getPath() == path ? std::addressof(*found) : nullptr;
}

BoundObjectTypeReferenceUseLookup BoundObjectTypeReferences::findUniqueRuntimeUse(
    PersistedTypePathSection section, std::string_view runtime_owner_key, std::span<const UInt64> type_child_ordinals) const noexcept
{
    return findUniqueRuntimeUse(section, PersistedTypeOccurrenceSite::Declaration, runtime_owner_key, type_child_ordinals);
}

BoundObjectTypeReferenceUseLookup BoundObjectTypeReferences::findUniqueRuntimeUse(
    PersistedTypePathSection section,
    PersistedTypeOccurrenceSite site,
    std::string_view runtime_owner_key,
    std::span<const UInt64> type_child_ordinals) const noexcept
{
    if (runtime_owner_key.empty())
        return {};

    const auto before_key = [&](UInt32 use_index)
    {
        const auto & use = uses[use_index];
        if (use.getPath().section != section)
            return static_cast<UInt8>(use.getPath().section) < static_cast<UInt8>(section);
        if (use.getPath().site != site)
            return static_cast<UInt8>(use.getPath().site) < static_cast<UInt8>(site);
        if (use.getRuntimeOwnerKey() != runtime_owner_key)
            return std::string_view(use.getRuntimeOwnerKey()) < runtime_owner_key;
        return std::lexicographical_compare(
            use.getPath().type_child_ordinals.begin(),
            use.getPath().type_child_ordinals.end(),
            type_child_ordinals.begin(),
            type_child_ordinals.end());
    };
    const auto matches_key = [&](UInt32 use_index)
    {
        const auto & use = uses[use_index];
        return use.getPath().section == section && use.getPath().site == site && use.getRuntimeOwnerKey() == runtime_owner_key
            && std::equal(
                   use.getPath().type_child_ordinals.begin(),
                   use.getPath().type_child_ordinals.end(),
                   type_child_ordinals.begin(),
                   type_child_ordinals.end());
    };

    const auto found = std::lower_bound(
        runtime_use_indices.begin(), runtime_use_indices.end(), true, [&](UInt32 use_index, bool) { return before_key(use_index); });
    if (found == runtime_use_indices.end() || !matches_key(*found))
        return {};
    SemanticCapabilityMask endpoint_capabilities = 0;
    size_t match_count = 0;
    for (auto current = found; current != runtime_use_indices.end() && matches_key(*current); ++current)
    {
        endpoint_capabilities = static_cast<SemanticCapabilityMask>(endpoint_capabilities | uses[*current].getSemanticCapabilities());
        ++match_count;
    }
    if (match_count != 1)
        return {.use = nullptr, .ambiguous = true, .semantic_capabilities = endpoint_capabilities};
    return {
        .use = std::addressof(uses[*found]),
        .ambiguous = false,
        .semantic_capabilities = endpoint_capabilities,
    };
}

std::vector<const BoundObjectTypeReferenceUse *> BoundObjectTypeReferences::findRuntimeUsesByPrefix(
    PersistedTypePathSection section, std::string_view runtime_owner_key, std::span<const UInt64> type_child_prefix) const
{
    return findRuntimeUsesByPrefix(section, PersistedTypeOccurrenceSite::Declaration, runtime_owner_key, type_child_prefix);
}

std::vector<const BoundObjectTypeReferenceUse *> BoundObjectTypeReferences::findRuntimeUsesByPrefix(
    PersistedTypePathSection section,
    PersistedTypeOccurrenceSite site,
    std::string_view runtime_owner_key,
    std::span<const UInt64> type_child_prefix) const
{
    std::vector<const BoundObjectTypeReferenceUse *> result;
    if (runtime_owner_key.empty())
        return result;

    const auto before_prefix = [&](UInt32 use_index)
    {
        const auto & use = uses[use_index];
        if (use.getPath().section != section)
            return static_cast<UInt8>(use.getPath().section) < static_cast<UInt8>(section);
        if (use.getPath().site != site)
            return static_cast<UInt8>(use.getPath().site) < static_cast<UInt8>(site);
        if (use.getRuntimeOwnerKey() != runtime_owner_key)
            return std::string_view(use.getRuntimeOwnerKey()) < runtime_owner_key;
        return std::lexicographical_compare(
            use.getPath().type_child_ordinals.begin(),
            use.getPath().type_child_ordinals.end(),
            type_child_prefix.begin(),
            type_child_prefix.end());
    };
    const auto found = std::lower_bound(
        runtime_use_indices.begin(), runtime_use_indices.end(), true, [&](UInt32 use_index, bool) { return before_prefix(use_index); });
    for (auto current = found; current != runtime_use_indices.end(); ++current)
    {
        const auto & use = uses[*current];
        if (use.getPath().section != section || use.getPath().site != site || use.getRuntimeOwnerKey() != runtime_owner_key)
            break;
        const auto & path = use.getPath().type_child_ordinals;
        if (path.size() < type_child_prefix.size() || !std::equal(type_child_prefix.begin(), type_child_prefix.end(), path.begin()))
            break;
        result.push_back(std::addressof(use));
    }
    return result;
}
}
