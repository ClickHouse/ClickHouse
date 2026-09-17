#include <Databases/UDT/AuthorityVerificationStampPublication.h>

#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>

#include <algorithm>
#include <tuple>

namespace DB::UDT
{
namespace
{

bool identityLess(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    return std::tuple{uuidToCanonicalBytes(lhs.database_uuid), uuidToCanonicalBytes(lhs.type_uuid), lhs.revision}
    < std::tuple{uuidToCanonicalBytes(rhs.database_uuid), uuidToCanonicalBytes(rhs.type_uuid), rhs.revision};
}

[[noreturn]] void invalidStamp(std::string_view message)
{
    throw AuthorityVerificationStampError(AuthorityVerificationStampError::Code::InvalidVerification, message);
}

}

std::vector<DefinitionIdentity>
collectAuthorityVerificationRequiredDefinitions(const BoundObjectTypeReferences & bound_references, UInt64 maximum_required_definitions)
{
    const auto descriptors = bound_references.getDescriptors();
    if (maximum_required_definitions == 0 || maximum_required_definitions > AuthorityVerificationStampLimits{}.maximum_required_definitions)
    {
        throw AuthorityVerificationStampError(
            AuthorityVerificationStampError::Code::InvalidConfiguration, "bound-object verification-stamp definition limit is invalid");
    }
    std::vector<DefinitionIdentity> identities;
    identities.reserve(descriptors.size());
    for (const auto & descriptor : descriptors)
    {
        if (!descriptor)
            invalidStamp("bound object contains an empty type descriptor");
        identities.push_back(descriptor->getPersistedDescriptor().getDefinitionIdentity());
    }
    std::sort(identities.begin(), identities.end(), identityLess);
    for (size_t index = 1; index < identities.size(); ++index)
    {
        if (identities[index - 1].type_uuid == identities[index].type_uuid && identities[index - 1] != identities[index])
            invalidStamp("bound object contains conflicting revisions of one required definition");
    }
    identities.erase(std::unique(identities.begin(), identities.end()), identities.end());
    if (identities.size() > maximum_required_definitions)
    {
        throw AuthorityVerificationStampError(
            AuthorityVerificationStampError::Code::LimitExceeded, "bound object exceeds its verification-stamp definition limit");
    }
    if (identities.empty())
        invalidStamp("mapped bound object has an empty required-definition set");
    return identities;
}

AuthorityVerificationStamp::Ptr verifyAndCreateAuthorityVerificationStamp(
    const AuthorityRoot & root,
    const SidecarExpectationRecord & expectation,
    std::string_view canonical_sidecar_bytes,
    const BoundObjectTypeReferences & bound_references,
    const AuthorityIntegrityVerifierLimits & verifier_limits,
    const AuthorityVerificationStampLimits & stamp_limits)
{
    if (bound_references.getObject() != expectation.object
        || bound_references.getObjectSchemaRevision() != expectation.object_schema_revision
        || bound_references.getSidecarHash() != expectation.sidecar_hash
        || bound_references.getPhysicalSchemaFingerprint() != expectation.physical_schema_fingerprint)
        invalidStamp("bound object differs from its exact verification expectation");

    auto references = decodePersistedTypeReferences(canonical_sidecar_bytes, verifier_limits.persisted_references);
    if (encodePersistedTypeReferences(references, verifier_limits.persisted_references) != canonical_sidecar_bytes)
        invalidStamp("verification-stamp sidecar is not canonical");
    const auto required_definitions
        = collectAuthorityVerificationRequiredDefinitions(bound_references, stamp_limits.maximum_required_definitions);

    const auto verified = verifyAuthorityObjectIntegrity(
        root,
        expectation,
        references,
        canonical_sidecar_bytes,
        bound_references.getObjectSchemaRevision(),
        bound_references.getPhysicalSchemaFingerprint(),
        verifier_limits);
    const auto & state = root.getAuthorityState();
    return AuthorityVerificationStamp::create(
        verified,
        required_definitions,
        {
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
        },
        stamp_limits);
}

AuthorityVerificationStamp::Ptr validateAndRebaseAuthorityVerificationStamp(
    const AuthorityRoot & root,
    const SidecarExpectationRecord & expectation,
    const BoundObjectTypeReferences & bound_references,
    const AuthorityVerificationStamp & previous_stamp,
    const AuthorityVerificationStampLimits & stamp_limits)
{
    const AuthorityObjectImageIdentity object_image{
        .object = bound_references.getObject(),
        .object_schema_revision = bound_references.getObjectSchemaRevision(),
        .sidecar_hash = bound_references.getSidecarHash(),
        .physical_schema_fingerprint = bound_references.getPhysicalSchemaFingerprint(),
    };
    const auto required_definitions
        = collectAuthorityVerificationRequiredDefinitions(bound_references, stamp_limits.maximum_required_definitions);
    if (expectation.object != object_image.object || expectation.object_schema_revision != object_image.object_schema_revision
        || expectation.sidecar_hash != object_image.sidecar_hash
        || expectation.physical_schema_fingerprint != object_image.physical_schema_fingerprint
        || previous_stamp.getVerifiedObject() != object_image
        || previous_stamp.getRequiredDefinitions().size() != required_definitions.size()
        || !std::equal(required_definitions.begin(), required_definitions.end(), previous_stamp.getRequiredDefinitions().begin()))
        invalidStamp("verification stamp cannot be rebased from a different object image or definition closure");

    const auto * rooted_expectation = root.findExpectationRecord(object_image.object);
    if (!rooted_expectation || *rooted_expectation != expectation)
        invalidStamp("verification stamp cannot be rebased without its exact rooted expectation");

    const auto descriptors = bound_references.getDescriptors();
    const auto definition_handles = bound_references.getDefinitionHandles();
    if (definition_handles.empty())
        invalidStamp("verification stamp bound definition handles are empty");

    /// The two dictionaries deliberately have different identities and ordering contracts:
    /// descriptors are distinct instantiations sorted by their semantic hash, while definition
    /// handles are distinct checked definitions sorted by immutable identity and may additionally
    /// contain transitive dependencies.  Consequently neither their sizes nor their indexes are
    /// related.  Validate every retained definition once, then resolve each descriptor by identity.
    for (size_t index = 0; index < definition_handles.size(); ++index)
    {
        const auto & retained_definition = definition_handles[index];
        if (!retained_definition)
            invalidStamp("verification stamp bound definition evidence is inconsistent");
        if (index && !identityLess(definition_handles[index - 1]->getIdentity(), retained_definition->getIdentity()))
            invalidStamp("verification stamp bound definition handles are not strictly sorted and unique");
        const auto * rooted_record = root.findDefinitionRecord(retained_definition->getIdentity());
        const auto rooted_definition = root.findByIdentity(retained_definition->getIdentity());
        if (!rooted_record || rooted_record->definition_hash != retained_definition->getDefinitionHash() || !rooted_definition
            || !rooted_definition->hasSameCheckedSemantics(*retained_definition))
            invalidStamp("verification stamp required definition changed in the replacement root");
    }
    for (const auto & descriptor : descriptors)
    {
        if (!descriptor)
            invalidStamp("verification stamp bound definition evidence is inconsistent");
        const auto & persisted_descriptor = descriptor->getPersistedDescriptor();
        const auto & identity = persisted_descriptor.getDefinitionIdentity();
        const auto handle = std::lower_bound(
            definition_handles.begin(),
            definition_handles.end(),
            identity,
            [](const Definition::Ptr & candidate, const DefinitionIdentity & key) { return identityLess(candidate->getIdentity(), key); });
        if (handle == definition_handles.end() || (*handle)->getIdentity() != identity
            || persisted_descriptor.getDefinitionHash() != (*handle)->getDefinitionHash())
        {
            invalidStamp("verification stamp bound definition evidence is inconsistent");
        }
    }

    const auto & graph = root.getSchemaObjectDependencyGraph();
    const UInt64 dependency_count = graph.getDependencyCount(object_image.object);
    const auto dependencies = graph.getDependencies(object_image.object);
    if (dependencies.size() != dependency_count)
        invalidStamp("verification stamp replacement root dependency cardinality is inconsistent");

    UInt64 definition_dependency_count = 0;
    for (const auto & dependency : dependencies)
    {
        if (dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnObject)
            continue;
        if (dependency.kind != SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition
            || dependency.object.kind != SchemaObjectKind::TypeDefinition
            || dependency.object.database_uuid != object_image.object.database_uuid)
            invalidStamp("verification stamp replacement root contains an invalid object dependency edge");
        ++definition_dependency_count;
        if (definition_dependency_count > required_definitions.size())
            invalidStamp("verification stamp dependency graph closure changed in the replacement root");
    }
    if (definition_dependency_count != required_definitions.size())
        invalidStamp("verification stamp dependency graph closure changed in the replacement root");
    for (const auto & definition : required_definitions)
    {
        const SchemaObjectDependencyEdge edge{
            .dependent = object_image.object,
            .dependency = {
                .kind = SchemaObjectKind::TypeDefinition,
                .database_uuid = definition.database_uuid,
                .object_uuid = definition.type_uuid,
            },
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        };
        if (!graph.containsEdge(edge))
            invalidStamp("verification stamp required dependency edge changed in the replacement root");
    }

    const auto & state = root.getAuthorityState();
    const VerifiedAuthorityObjectIntegrity verified{
        .database_uuid = state.database_uuid,
        .database_catalog_epoch = state.database_catalog_epoch,
        .authority_anchor = state.anchor_hash,
        .object = object_image.object,
        .object_schema_revision = object_image.object_schema_revision,
        .sidecar_hash = object_image.sidecar_hash,
        .physical_schema_fingerprint = object_image.physical_schema_fingerprint,
        .required_definitions_digest
        = computeVerifiedRequiredDefinitionsDigest(required_definitions, stamp_limits.maximum_required_definitions),
        .required_definition_count = static_cast<UInt64>(required_definitions.size()),
        .statistics = {},
    };
    return AuthorityVerificationStamp::create(
        verified,
        required_definitions,
        {
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
        },
        stamp_limits);
}

}
