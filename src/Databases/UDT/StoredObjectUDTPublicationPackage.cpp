#include <Databases/UDT/StoredObjectUDTPublicationPackage.h>

#include <Databases/DatabaseSchemaWAL.h>

#include <DataTypes/UDT/Record.h>

#include <algorithm>
#include <limits>
#include <new>
#include <tuple>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = StoredObjectUDTPublicationPackageError;

constexpr UInt64 implementation_maximum_metadata_bytes = 256ULL << 20;
constexpr UInt64 implementation_maximum_work_units = 1ULL << 24;
constexpr UInt64 implementation_maximum_provenance_scratch_bytes = 512ULL << 20;
constexpr UInt64 implementation_maximum_owned_canonical_bytes = 512ULL << 20;
constexpr UInt64 implementation_maximum_retained_logical_bytes = 64ULL << 30;
/// Conservative charge for a canonical inventory leaf and both compressed
/// radix paths retained by a before/after package. This is logical accounting,
/// not an allocator-resident measurement.
constexpr UInt64 inventory_logical_bytes_per_leaf_upper_bound = 8ULL << 10;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedSize(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(Error::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(Error::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(Error::Code::LimitExceeded, message);
    return lhs * rhs;
}

void charge(UInt64 & total, UInt64 addition, std::string_view message)
{
    total = checkedAdd(total, addition, message);
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 result = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++result;
    }
    return result;
}

UInt64 sortWorkUnits(UInt64 item_count)
{
    UInt64 levels = 0;
    UInt64 capacity = 1;
    while (capacity < item_count)
    {
        capacity <<= 1;
        ++levels;
    }
    return checkedMultiply(
        item_count,
        checkedAdd(checkedMultiply(levels, 2, "stored-object sort work overflows UInt64"), 1, "stored-object sort work overflows UInt64"),
        "stored-object sort work overflows UInt64");
}

bool isProductionStoredObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary;
}

void validateMetadataLimits(const StoredObjectUDTMetadataValidationLimits & limits)
{
    if (!limits.maximum_candidate_metadata_bytes || !limits.maximum_canonical_metadata_bytes || !limits.maximum_sidecar_bytes
        || !limits.maximum_object_name_bytes)
        fail(Error::Code::InvalidConfiguration, "stored-object metadata validation limits contain a zero bound");
    if (limits.maximum_candidate_metadata_bytes > implementation_maximum_metadata_bytes
        || limits.maximum_canonical_metadata_bytes > implementation_maximum_metadata_bytes
        || limits.maximum_sidecar_bytes > PersistedTypeReferencesLimits{}.maximum_sidecar_bytes
        || limits.maximum_object_name_bytes > DependentObjectMetadataInstallationRecordLimits{}.maximum_object_name_bytes)
        fail(Error::Code::InvalidConfiguration, "stored-object metadata validation limits exceed implementation maxima");
}

void validatePersistedReferencesLimits(const PersistedTypeReferencesLimits & limits)
{
    const PersistedTypeReferencesLimits maximum;
    if (!limits.maximum_sidecar_bytes || !limits.maximum_descriptors || !limits.maximum_occurrence_paths || !limits.maximum_path_depth
        || !limits.maximum_canonical_arguments_bytes || !limits.maximum_canonical_physical_type_bytes
        || !limits.maximum_qualified_name_bytes || !limits.maximum_text_bytes)
        fail(Error::Code::InvalidConfiguration, "stored-object sidecar limits contain a zero bound");
    if (limits.maximum_sidecar_bytes > maximum.maximum_sidecar_bytes || limits.maximum_descriptors > maximum.maximum_descriptors
        || limits.maximum_occurrence_paths > maximum.maximum_occurrence_paths || limits.maximum_path_depth > maximum.maximum_path_depth
        || limits.maximum_canonical_arguments_bytes > maximum.maximum_canonical_arguments_bytes
        || limits.maximum_canonical_physical_type_bytes > maximum.maximum_canonical_physical_type_bytes
        || limits.maximum_qualified_name_bytes > maximum.maximum_qualified_name_bytes
        || limits.maximum_text_bytes > maximum.maximum_text_bytes)
        fail(Error::Code::InvalidConfiguration, "stored-object sidecar limits exceed implementation maxima");
}

void validatePackageLimits(const StoredObjectUDTPublicationPackageLimits & limits)
{
    validatePersistedReferencesLimits(limits.persisted_references);
    if (!limits.installation_record.maximum_encoded_bytes || !limits.installation_record.maximum_object_name_bytes
        || limits.installation_record.maximum_encoded_bytes > 16ULL << 20
        || limits.installation_record.maximum_object_name_bytes > 4ULL << 20)
        fail(Error::Code::InvalidConfiguration, "stored-object installation-record limits are invalid");
    if (!limits.authority_root.inventory.maximum_leaves || !limits.authority_root.inventory.maximum_leaf_bytes
        || limits.authority_root.inventory.maximum_leaves > (1ULL << 24)
        || limits.authority_root.inventory.maximum_leaf_bytes > (1ULL << 20))
        fail(Error::Code::InvalidConfiguration, "stored-object inventory limits are invalid");
    if (!limits.schema_graph.maximum_nodes || !limits.schema_graph.maximum_edges || !limits.schema_graph.maximum_edges_per_node
        || !limits.schema_graph.maximum_mutation_nodes || !limits.schema_graph.maximum_mutation_edges
        || !limits.schema_graph.maximum_retained_bytes || limits.schema_graph.maximum_nodes > schema_object_dependency_graph_maximum_nodes
        || limits.schema_graph.maximum_edges > schema_object_dependency_graph_maximum_edges
        || limits.schema_graph.maximum_edges_per_node > limits.schema_graph.maximum_edges
        || limits.schema_graph.maximum_mutation_nodes > 2 * schema_object_dependency_graph_maximum_nodes
        || limits.schema_graph.maximum_mutation_edges > 2 * schema_object_dependency_graph_maximum_edges)
        fail(Error::Code::InvalidConfiguration, "stored-object schema-graph limits are invalid");
    if (!limits.authority_root.authority_state.maximum_leaves || !limits.authority_root.authority_state.maximum_encoded_bytes
        || limits.authority_root.authority_state.maximum_leaves > (1ULL << 24)
        || limits.authority_root.authority_state.maximum_encoded_bytes > (1ULL << 20) || !limits.authority_root.maximum_definition_records
        || !limits.authority_root.maximum_expectation_records || !limits.authority_root.maximum_canonical_record_bytes)
        fail(Error::Code::InvalidConfiguration, "stored-object authority-state limits are invalid");
    if (!limits.maximum_metadata_bytes || !limits.maximum_definition_dependencies || !limits.maximum_object_dependencies
        || !limits.maximum_work_units || !limits.maximum_provenance_scratch_bytes || !limits.maximum_owned_canonical_bytes
        || !limits.maximum_retained_logical_bytes)
        fail(Error::Code::InvalidConfiguration, "stored-object publication limits contain a zero bound");
    if (limits.maximum_metadata_bytes > implementation_maximum_metadata_bytes
        || limits.maximum_definition_dependencies > PersistedTypeReferencesLimits{}.maximum_descriptors
        || limits.maximum_object_dependencies > schema_object_dependency_graph_maximum_mutation_edges
        || limits.maximum_work_units > implementation_maximum_work_units
        || limits.maximum_provenance_scratch_bytes > implementation_maximum_provenance_scratch_bytes
        || limits.maximum_owned_canonical_bytes > implementation_maximum_owned_canonical_bytes
        || limits.maximum_retained_logical_bytes > implementation_maximum_retained_logical_bytes)
        fail(Error::Code::InvalidConfiguration, "stored-object publication limits exceed implementation maxima");
}

AuthorityInventoryKey expectationInventoryKey(const SchemaObjectID & object)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = object.object_uuid,
    };
}

SchemaObjectID definitionObject(const DefinitionIdentity & identity)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = identity.database_uuid,
        .object_uuid = identity.type_uuid,
    };
}

bool descriptorMatchesRecord(const PersistedTypeDescriptor & descriptor, const Record & record) noexcept
{
    return descriptor.getDefinitionIdentity() == record.identity && descriptor.getDefinitionHash() == record.definition_hash
        && descriptor.getCheckerABI() == record.checker_abi && descriptor.getCheckerChargeABI() == record.checker_charge_abi
        && descriptor.getPolicyABI() == record.policy_abi && descriptor.getFunctionRegistryABI() == record.function_registry_abi
        && descriptor.getPolicySemanticHash() == record.policy_semantic_hash
        && descriptor.getSemanticCapabilities() == record.semantic_capabilities;
}

struct DefinitionRevision
{
    DefinitionIdentity identity;

    friend bool operator<(const DefinitionRevision & lhs, const DefinitionRevision & rhs) noexcept
    {
        return std::tuple{
                   UUIDHelpers::getHighBytes(lhs.identity.type_uuid),
                   UUIDHelpers::getLowBytes(lhs.identity.type_uuid),
                   lhs.identity.revision}
        < std::tuple{
            UUIDHelpers::getHighBytes(rhs.identity.type_uuid), UUIDHelpers::getLowBytes(rhs.identity.type_uuid), rhs.identity.revision};
    }
};

std::vector<SchemaObjectDependencyEdge> validateDefinitionsAndMakeEdges(
    const AuthorityRoot & root,
    const PersistedTypeReferences & references,
    UInt64 maximum_dependencies,
    UInt64 maximum_provenance_scratch_bytes,
    UInt64 & descriptors_validated,
    UInt64 & provenance_scratch_bytes_upper_bound)
{
    if (references.descriptors.size() > maximum_dependencies)
        fail(Error::Code::LimitExceeded, "stored-object sidecar exceeds its definition-dependency limit");

    const UInt64 descriptor_count = checkedSize(references.descriptors.size(), "stored-object descriptor count exceeds UInt64");
    provenance_scratch_bytes_upper_bound = checkedAdd(
        checkedMultiply(descriptor_count, sizeof(DefinitionRevision), "stored-object provenance scratch charge overflows UInt64"),
        checkedMultiply(descriptor_count, sizeof(SchemaObjectDependencyEdge), "stored-object dependency scratch charge overflows UInt64"),
        "stored-object publication scratch charge overflows UInt64");
    if (provenance_scratch_bytes_upper_bound > maximum_provenance_scratch_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication exceeds its scratch-byte limit");

    std::vector<DefinitionRevision> revisions;
    revisions.reserve(references.descriptors.size());
    for (const auto & descriptor : references.descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        if (identity.database_uuid != references.object.database_uuid)
            fail(Error::Code::DatabaseMismatch, "stored-object descriptor belongs to another database");
        const auto * record = root.findDefinitionRecord(identity);
        if (!record)
            fail(Error::Code::DefinitionNotFound, "stored-object sidecar references an absent exact definition revision");
        if (!descriptorMatchesRecord(descriptor, *record))
            fail(Error::Code::DefinitionMismatch, "stored-object descriptor differs from its exact authority definition");
        revisions.push_back({.identity = identity});
        ++descriptors_validated;
    }

    std::sort(revisions.begin(), revisions.end());
    std::vector<SchemaObjectDependencyEdge> edges;
    edges.reserve(revisions.size());
    for (size_t index = 0; index < revisions.size(); ++index)
    {
        if (index && revisions[index - 1].identity.type_uuid == revisions[index].identity.type_uuid)
        {
            if (revisions[index - 1].identity.revision != revisions[index].identity.revision)
                fail(Error::Code::DefinitionMismatch, "stored-object sidecar retains multiple revisions of one definition identity");
            continue;
        }
        const auto * record = root.findDefinitionRecord(revisions[index].identity);
        const auto definition = root.findByIdentity(revisions[index].identity);
        if (!record || !definition || !recordMatchesCheckedDefinition(*record, *definition))
            fail(Error::Code::InvalidBase, "stored-object authority root contains an unmatched definition record");
        const SchemaObjectID dependency = definitionObject(revisions[index].identity);
        if (!root.getSchemaObjectDependencyGraph().containsNode(dependency))
            fail(Error::Code::InvalidBase, "stored-object definition has no node in the pinned schema graph");
        edges.push_back({
            .dependent = references.object,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        });
    }
    if (edges.size() > maximum_dependencies)
        fail(Error::Code::LimitExceeded, "stored-object dependency edge count exceeds its limit");
    return edges;
}

std::vector<SchemaObjectDependencyEdge> validateObjectDependenciesAndMakeEdges(
    const AuthorityRoot & root, const SchemaObjectID & dependent, std::span<const SchemaObjectID> dependencies, UInt64 maximum_dependencies)
{
    if (dependencies.size() > maximum_dependencies)
        fail(Error::Code::LimitExceeded, "stored-object object-dependency edge count exceeds its limit");

    const auto & graph = root.getSchemaObjectDependencyGraph();
    std::vector<SchemaObjectDependencyEdge> edges;
    edges.reserve(dependencies.size());
    const SchemaObjectID * previous = nullptr;
    for (const auto & dependency : dependencies)
    {
        if (!dependency.isValid() || !isProductionStoredObjectKind(dependency.kind))
            fail(Error::Code::InvalidMetadata, "stored-object publication has an invalid object dependency identity");
        if (dependency.database_uuid != root.getDatabaseUUID())
            fail(Error::Code::DatabaseMismatch, "stored-object publication object dependency belongs to another database");
        if (dependency == dependent || (previous && !(*previous < dependency)))
            fail(Error::Code::InvalidMetadata, "stored-object publication object dependencies are not canonical");
        const auto * expectation = root.findExpectationRecord(dependency);
        if (!expectation || expectation->object != dependency || !graph.containsNode(dependency))
            fail(Error::Code::InvalidBase, "stored-object publication object dependency is not mapped in the pinned root");
        edges.push_back({
            .dependent = dependent,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject,
        });
        previous = &dependency;
    }
    return edges;
}

UInt64 computeDecodedReferencesLogicalBytes(const PersistedTypeReferences & references)
{
    UInt64 result = sizeof(PersistedTypeReferences);
    charge(
        result,
        checkedMultiply(
            references.descriptors.capacity(), sizeof(PersistedTypeDescriptor), "stored-object descriptor charge overflows UInt64"),
        "stored-object decoded-sidecar charge overflows UInt64");
    charge(
        result,
        checkedMultiply(
            references.occurrence_paths.capacity(),
            sizeof(PersistedTypeOccurrencePath),
            "stored-object occurrence-path charge overflows UInt64"),
        "stored-object decoded-sidecar charge overflows UInt64");
    charge(
        result,
        checkedMultiply(
            references.uses.capacity(), sizeof(PersistedTypeOccurrenceUse), "stored-object occurrence-use charge overflows UInt64"),
        "stored-object decoded-sidecar charge overflows UInt64");
    for (const auto & descriptor : references.descriptors)
    {
        charge(
            result,
            checkedSize(descriptor.getCanonicalArgumentsEncoding().capacity(), "stored-object descriptor charge exceeds UInt64"),
            "stored-object decoded-sidecar charge overflows UInt64");
        charge(
            result,
            checkedSize(descriptor.getCanonicalPhysicalType().capacity(), "stored-object descriptor charge exceeds UInt64"),
            "stored-object decoded-sidecar charge overflows UInt64");
        charge(
            result,
            checkedSize(descriptor.getLastKnownQualifiedName().capacity(), "stored-object descriptor charge exceeds UInt64"),
            "stored-object decoded-sidecar charge overflows UInt64");
    }
    for (const auto & path : references.occurrence_paths)
    {
        charge(
            result,
            checkedMultiply(path.type_child_ordinals.capacity(), sizeof(UInt64), "stored-object occurrence-path charge overflows UInt64"),
            "stored-object decoded-sidecar charge overflows UInt64");
    }
    return result;
}

UInt64 computePackageOwnedLogicalBytes(
    const ValidatedStoredObjectUDTMetadata & metadata,
    const PersistedTypeReferences & references,
    const DependentObjectMetadataInstallationRecord & installation_record,
    const String & canonical_sidecar_bytes,
    const String & canonical_installation_record_bytes,
    const String & canonical_expectation_record_bytes,
    const std::vector<AuthorityInventoryLeafDelta> & inventory_deltas,
    const SchemaObjectDependencyGraphMutation & graph_delta)
{
    UInt64 result = sizeof(StoredObjectUDTPublicationPackage);
    charge(
        result,
        checkedSize(metadata.getCanonicalMetadataBytes().capacity(), "stored-object metadata charge exceeds UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedSize(metadata.getObjectName().capacity(), "stored-object name charge exceeds UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedSize(installation_record.object_name.capacity(), "stored-object installation-record name charge exceeds UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedSize(canonical_sidecar_bytes.capacity(), "stored-object sidecar charge exceeds UInt64"),
        "stored-object package charge overflows UInt64");
    charge(result, computeDecodedReferencesLogicalBytes(references), "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedSize(canonical_installation_record_bytes.capacity(), "stored-object installation-record charge exceeds UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedSize(canonical_expectation_record_bytes.capacity(), "stored-object expectation-record charge exceeds UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedMultiply(
            inventory_deltas.capacity(), sizeof(AuthorityInventoryLeafDelta), "stored-object inventory-delta charge overflows UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedMultiply(graph_delta.node_additions.capacity(), sizeof(SchemaObjectID), "stored-object graph-node charge overflows UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedMultiply(graph_delta.node_removals.capacity(), sizeof(SchemaObjectID), "stored-object graph-node charge overflows UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedMultiply(
            graph_delta.edge_additions.capacity(), sizeof(SchemaObjectDependencyEdge), "stored-object graph-edge charge overflows UInt64"),
        "stored-object package charge overflows UInt64");
    charge(
        result,
        checkedMultiply(
            graph_delta.edge_removals.capacity(), sizeof(SchemaObjectDependencyEdge), "stored-object graph-edge charge overflows UInt64"),
        "stored-object package charge overflows UInt64");
    return result;
}

void validateProspectiveGraphMutation(
    const SchemaObjectDependencyGraph & base,
    const SchemaObjectDependencyGraphMutation & mutation,
    const SchemaObjectDependencyGraphLimits & candidate_limits)
{
    const auto & retained_limits = base.getLimits();
    if (mutation.node_additions.size() > candidate_limits.maximum_mutation_nodes
        || mutation.node_additions.size() > retained_limits.maximum_mutation_nodes
        || mutation.edge_additions.size() > candidate_limits.maximum_mutation_edges
        || mutation.edge_additions.size() > retained_limits.maximum_mutation_edges)
        fail(Error::Code::LimitExceeded, "stored-object graph delta exceeds its mutation limit");
    if (base.getNodeCount() >= candidate_limits.maximum_nodes || base.getNodeCount() >= retained_limits.maximum_nodes)
        fail(Error::Code::LimitExceeded, "stored-object graph has no capacity for its object node");
    if (mutation.edge_additions.size() > candidate_limits.maximum_edges - base.getEdgeCount()
        || mutation.edge_additions.size() > retained_limits.maximum_edges - base.getEdgeCount())
        fail(Error::Code::LimitExceeded, "stored-object graph has no capacity for its definition edges");
    if (mutation.edge_additions.size() > candidate_limits.maximum_edges_per_node
        || mutation.edge_additions.size() > retained_limits.maximum_edges_per_node)
        fail(Error::Code::LimitExceeded, "stored-object graph object degree exceeds its limit");
    for (const auto & edge : mutation.edge_additions)
    {
        const UInt64 reverse_degree = base.getDependentCount(edge.dependency);
        if (reverse_degree >= candidate_limits.maximum_edges_per_node || reverse_degree >= retained_limits.maximum_edges_per_node)
            fail(Error::Code::LimitExceeded, "stored-object graph definition degree exceeds its limit");
    }
}

}

StoredObjectUDTPublicationPackageError::StoredObjectUDTPublicationPackageError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

const AuthorityRoot & StoredObjectUDTPublicationPackage::getReplacementRoot() const
{
    if (!replacement_root)
        fail(Error::Code::InvalidTransition, "stored-object publication replacement root was already released");
    return *replacement_root;
}

const AuthorityState & StoredObjectUDTPublicationPackage::getAfterAuthorityState() const
{
    return getReplacementRoot().getAuthorityState();
}

const AuthorityInventory & StoredObjectUDTPublicationPackage::getAfterInventory() const
{
    const auto inventory = getReplacementRoot().pinAuthorityInventory();
    if (!inventory)
        fail(Error::Code::InvalidTransition, "stored-object publication replacement root has no inventory");
    return *inventory;
}

const SchemaObjectDependencyGraph & StoredObjectUDTPublicationPackage::getAfterSchemaGraph() const
{
    return getReplacementRoot().getSchemaObjectDependencyGraph();
}

ValidatedStoredObjectUDTMetadata::ValidatedStoredObjectUDTMetadata(
    SchemaObjectID object_,
    UInt64 object_schema_revision_,
    String object_name_,
    Digest sidecar_hash_,
    Digest physical_schema_fingerprint_,
    String canonical_metadata_bytes_,
    Digest canonical_metadata_hash_)
    : object(object_)
    , object_schema_revision(object_schema_revision_)
    , object_name(std::move(object_name_))
    , sidecar_hash(sidecar_hash_)
    , physical_schema_fingerprint(physical_schema_fingerprint_)
    , canonical_metadata_bytes(std::move(canonical_metadata_bytes_))
    , canonical_metadata_hash(canonical_metadata_hash_)
{
}

ValidatedStoredObjectUDTMetadata IStoredObjectUDTMetadataValidator::validateAndCanonicalize(
    const SidecarExpectationRecord & expectation,
    std::string_view candidate_metadata_bytes,
    std::string_view canonical_sidecar_bytes,
    const StoredObjectUDTMetadataValidationLimits & limits) const
{
    validateMetadataLimits(limits);
    if (!expectation.object.isValid() || !isProductionStoredObjectKind(expectation.object.kind) || !expectation.object_schema_revision)
        fail(Error::Code::InvalidMetadata, "stored-object metadata expectation identity is invalid");
    try
    {
        static_cast<void>(encodeSidecarExpectationRecord(expectation));
    }
    catch (const SidecarExpectationRecordError &)
    {
        fail(Error::Code::InvalidMetadata, "stored-object metadata expectation is not canonical");
    }
    if (candidate_metadata_bytes.empty() || canonical_sidecar_bytes.empty())
        fail(Error::Code::InvalidMetadata, "stored-object metadata validation input is empty");
    if (checkedSize(candidate_metadata_bytes.size(), "stored-object metadata candidate exceeds UInt64")
            > limits.maximum_candidate_metadata_bytes
        || checkedSize(canonical_sidecar_bytes.size(), "stored-object sidecar candidate exceeds UInt64") > limits.maximum_sidecar_bytes)
        fail(Error::Code::LimitExceeded, "stored-object metadata validation input exceeds its byte limit");

    DecodedMetadata decoded = [&]
    {
        try
        {
            return decodeAndCanonicalize(candidate_metadata_bytes, canonical_sidecar_bytes, limits);
        }
        catch (const std::bad_alloc &)
        {
            throw;
        }
        catch (const Error &)
        {
            throw;
        }
        catch (...)
        {
            fail(Error::Code::InvalidMetadata, "database-owned stored-object metadata validation failed");
        }
    }();

    if (decoded.object != expectation.object || decoded.object_schema_revision != expectation.object_schema_revision
        || decoded.sidecar_hash != expectation.sidecar_hash
        || decoded.physical_schema_fingerprint != expectation.physical_schema_fingerprint)
        fail(Error::Code::InvalidMetadata, "validated stored-object metadata differs from its exact sidecar expectation");
    if (decoded.object_name.empty() || decoded.object_name.find('\0') != String::npos)
        fail(Error::Code::InvalidMetadata, "validated stored-object metadata has an invalid database-local object name");
    if (checkedSize(decoded.object_name.size(), "stored-object metadata name exceeds UInt64") > limits.maximum_object_name_bytes)
        fail(Error::Code::LimitExceeded, "validated stored-object metadata name exceeds its byte limit");
    if (decoded.canonical_metadata_bytes.empty())
        fail(Error::Code::InvalidMetadata, "validated canonical stored-object metadata is empty");
    if (checkedSize(decoded.canonical_metadata_bytes.size(), "canonical stored-object metadata exceeds UInt64")
        > limits.maximum_canonical_metadata_bytes)
        fail(Error::Code::LimitExceeded, "validated canonical stored-object metadata exceeds its byte limit");
    String normalized_object_name(decoded.object_name.data(), decoded.object_name.size());
    String normalized_canonical_metadata(decoded.canonical_metadata_bytes.data(), decoded.canonical_metadata_bytes.size());
    const Digest canonical_hash = computeDatabaseSchemaWALStagedArtifactHash(
        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, normalized_canonical_metadata);
    return ValidatedStoredObjectUDTMetadata(
        decoded.object,
        decoded.object_schema_revision,
        std::move(normalized_object_name),
        decoded.sidecar_hash,
        decoded.physical_schema_fingerprint,
        std::move(normalized_canonical_metadata),
        canonical_hash);
}

StoredObjectUDTPublicationPackage::StoredObjectUDTPublicationPackage(
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
    StoredObjectUDTPublicationPackageStatistics statistics_)
    : planning_root(std::move(planning_root_))
    , before_authority_state(std::move(before_authority_state_))
    , validated_metadata(std::move(validated_metadata_))
    , persisted_references(std::move(persisted_references_))
    , canonical_sidecar_bytes(std::move(canonical_sidecar_bytes_))
    , installation_record(std::move(installation_record_))
    , canonical_installation_record_bytes(std::move(canonical_installation_record_bytes_))
    , installation_record_hash(installation_record_hash_)
    , expectation_record(std::move(expectation_record_))
    , canonical_expectation_record_bytes(std::move(canonical_expectation_record_bytes_))
    , expectation_record_hash(expectation_record_hash_)
    , inventory_leaf_deltas(std::move(inventory_leaf_deltas_))
    , schema_graph_delta(std::move(schema_graph_delta_))
    , replacement_root(std::move(replacement_root_))
    , statistics(std::move(statistics_))
{
}

StoredObjectUDTPublicationPackage StoredObjectUDTPublicationPackage::prepareCreate(
    AtomicAuthority::RootSnapshot planning_root,
    StoredObjectUDTPublicationAdmissionProof admission_proof,
    ValidatedStoredObjectUDTMetadata validated_metadata,
    String canonical_sidecar_bytes,
    SidecarExpectationRecord expected_expectation,
    std::span<const SchemaObjectID> object_dependencies,
    const StoredObjectUDTPublicationPackageLimits & limits)
{
    validatePackageLimits(limits);
    if (!planning_root)
        fail(Error::Code::InvalidBase, "stored-object publication has no pinned authority root");
    const AuthorityRoot & root = planning_root.get();
    const AuthorityState before_authority_state = root.getAuthorityState();
    try
    {
        static_cast<void>(encodeAuthorityState(before_authority_state, limits.authority_root.authority_state));
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "stored-object publication base authority state exceeds its limit");
        fail(Error::Code::InvalidBase, "stored-object publication base authority state is invalid");
    }
    if (before_authority_state.persistent_capability_mask != dependent_object_authority_capability_mask)
        fail(Error::Code::InvalidBase, "stored-object publication requires the complete dependent-object authority capability set");
    if (before_authority_state.database_catalog_epoch == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "stored-object publication authority epoch cannot advance");

    const auto base_inventory = root.pinAuthorityInventory();
    const auto base_graph = root.pinSchemaObjectDependencyGraph();
    if (!base_inventory || !base_graph || base_graph->getDatabaseUUID() != root.getDatabaseUUID())
        fail(Error::Code::InvalidBase, "stored-object publication root pins are incomplete");
    const auto & base_inventory_summary = base_inventory->getSummary();
    if (before_authority_state.database_uuid != root.getDatabaseUUID()
        || before_authority_state.leaf_count != base_inventory_summary.leaf_count
        || before_authority_state.inventory_root != base_inventory_summary.merkle_radix_root
        || before_authority_state.schema_graph_root != base_graph->computeRoot())
        fail(Error::Code::InvalidBase, "stored-object publication root components disagree with its authority identity");
    try
    {
        base_graph->validateAgainstLimits(limits.schema_graph);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "stored-object publication base graph exceeds its limit");
        fail(Error::Code::InvalidConfiguration, "stored-object publication schema-graph limit profile is invalid");
    }

    const SchemaObjectID object = validated_metadata.getObject();
    if (!object.isValid() || (object.kind != SchemaObjectKind::View && object.kind != SchemaObjectKind::Dictionary))
        fail(Error::Code::InvalidMetadata, "stored-object publication metadata proof has an invalid object identity");
    if (object.database_uuid != root.getDatabaseUUID())
        fail(Error::Code::DatabaseMismatch, "stored-object publication metadata belongs to another database");
    if (validated_metadata.getObjectSchemaRevision() != 1)
        fail(Error::Code::InvalidRevision, "first stored-object UDT publication must start at schema revision one");
    if (validated_metadata.getCanonicalMetadataBytes().empty())
        fail(Error::Code::InvalidMetadata, "stored-object publication metadata proof has empty canonical metadata");
    if (checkedSize(validated_metadata.getCanonicalMetadataBytes().size(), "stored-object metadata exceeds UInt64")
        > limits.maximum_metadata_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication metadata exceeds its byte limit");
    if (validated_metadata.getObjectName().empty())
        fail(Error::Code::InvalidMetadata, "stored-object publication metadata proof has an invalid object name");
    if (checkedSize(validated_metadata.getObjectName().size(), "stored-object name exceeds UInt64")
        > limits.installation_record.maximum_object_name_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication object name exceeds its byte limit");
    if (!admission_proof.object.isValid() || !admission_proof.object_schema_revision || admission_proof.object != object
        || admission_proof.object_schema_revision != validated_metadata.getObjectSchemaRevision()
        || admission_proof.sidecar_hash != validated_metadata.getSidecarHash()
        || admission_proof.physical_schema_fingerprint != validated_metadata.getPhysicalSchemaFingerprint()
        || !admission_proof.exact_descriptor_count)
        fail(Error::Code::IntegrityMismatch, "stored-object publication admission proof differs from its metadata proof");

    try
    {
        static_cast<void>(encodeSidecarExpectationRecord(expected_expectation));
    }
    catch (const SidecarExpectationRecordError &)
    {
        fail(Error::Code::InvalidSidecar, "stored-object publication expectation record is invalid");
    }
    if (expected_expectation.object != object || expected_expectation.object_schema_revision != validated_metadata.getObjectSchemaRevision()
        || expected_expectation.sidecar_hash != validated_metadata.getSidecarHash()
        || expected_expectation.physical_schema_fingerprint != validated_metadata.getPhysicalSchemaFingerprint())
        fail(Error::Code::IntegrityMismatch, "stored-object metadata proof differs from its expected sidecar identity");

    if (canonical_sidecar_bytes.empty())
        fail(Error::Code::InvalidSidecar, "stored-object publication sidecar is empty");
    if (checkedSize(canonical_sidecar_bytes.size(), "stored-object sidecar exceeds UInt64")
        > limits.persisted_references.maximum_sidecar_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication sidecar exceeds its byte limit");
    UInt64 prospective_installation_bytes = 2 + 1 + 2 * sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(Digest) + 2 * sizeof(UInt16);
    const UInt64 object_name_bytes = checkedSize(validated_metadata.getObjectName().size(), "stored-object name exceeds UInt64");
    prospective_installation_bytes = checkedAdd(
        prospective_installation_bytes, varUIntSize(object_name_bytes), "stored-object installation-record size overflows UInt64");
    prospective_installation_bytes
        = checkedAdd(prospective_installation_bytes, object_name_bytes, "stored-object installation-record size overflows UInt64");
    if (prospective_installation_bytes > limits.installation_record.maximum_encoded_bytes)
        fail(Error::Code::LimitExceeded, "stored-object installation record exceeds its prospective byte limit");
    UInt64 prospective_canonical_bytes = checkedAdd(
        checkedSize(validated_metadata.getCanonicalMetadataBytes().capacity(), "stored-object metadata capacity exceeds UInt64"),
        checkedSize(canonical_sidecar_bytes.capacity(), "stored-object sidecar capacity exceeds UInt64"),
        "stored-object canonical artifact bytes overflow UInt64");
    prospective_canonical_bytes
        = checkedAdd(prospective_canonical_bytes, prospective_installation_bytes, "stored-object canonical artifact bytes overflow UInt64");
    prospective_canonical_bytes = checkedAdd(
        prospective_canonical_bytes,
        sidecar_expectation_record_extended_encoded_bytes,
        "stored-object canonical artifact bytes overflow UInt64");
    if (prospective_canonical_bytes > limits.maximum_owned_canonical_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication canonical artifacts exceed their byte limit");

    PersistedTypeReferences references = [&]
    {
        try
        {
            auto decoded = decodePersistedTypeReferences(canonical_sidecar_bytes, limits.persisted_references);
            if (encodePersistedTypeReferences(decoded, limits.persisted_references) != canonical_sidecar_bytes)
                fail(Error::Code::InvalidSidecar, "stored-object publication sidecar is not canonical");
            return decoded;
        }
        catch (const PersistedTypeReferencesError & error)
        {
            if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "stored-object publication sidecar exceeds its structural limit");
            fail(Error::Code::InvalidSidecar, "stored-object publication sidecar is invalid");
        }
    }();
    const Digest sidecar_hash = [&]
    {
        try
        {
            return computePersistedTypeReferencesSidecarHash(references, limits.persisted_references);
        }
        catch (const PersistedTypeReferencesError & error)
        {
            if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "stored-object publication sidecar hash input exceeds its limit");
            fail(Error::Code::InvalidSidecar, "stored-object publication sidecar cannot be hashed canonically");
        }
    }();
    if (references.object != object || references.object_schema_revision != validated_metadata.getObjectSchemaRevision()
        || references.physical_schema_fingerprint != validated_metadata.getPhysicalSchemaFingerprint()
        || sidecar_hash != validated_metadata.getSidecarHash() || sidecar_hash != expected_expectation.sidecar_hash
        || sidecar_hash != admission_proof.sidecar_hash)
        fail(Error::Code::IntegrityMismatch, "stored-object sidecar differs from its metadata proof and expectation");

    const UInt64 descriptor_count = checkedSize(references.descriptors.size(), "stored-object descriptor count exceeds UInt64");
    if (descriptor_count != admission_proof.exact_descriptor_count)
        fail(Error::Code::IntegrityMismatch, "stored-object sidecar descriptor set differs from its adapter admission proof");
    UInt64 canonical_argument_bytes = 0;
    UInt64 canonical_physical_type_bytes = 0;
    for (const auto & descriptor : references.descriptors)
    {
        canonical_argument_bytes = checkedAdd(
            canonical_argument_bytes,
            checkedSize(descriptor.getCanonicalArgumentsEncoding().size(), "stored-object canonical arguments exceed UInt64"),
            "stored-object canonical argument bytes overflow UInt64");
        canonical_physical_type_bytes = checkedAdd(
            canonical_physical_type_bytes,
            checkedSize(descriptor.getCanonicalPhysicalType().size(), "stored-object canonical physical type exceeds UInt64"),
            "stored-object canonical physical type bytes overflow UInt64");
    }
    const UInt64 occurrence_count = checkedSize(references.occurrence_paths.size(), "stored-object occurrence count exceeds UInt64");
    const UInt64 use_count = checkedSize(references.uses.size(), "stored-object occurrence-use count exceeds UInt64");
    UInt64 work_units = checkedAdd(descriptor_count, occurrence_count, "stored-object publication work overflows UInt64");
    work_units = checkedAdd(work_units, use_count, "stored-object publication work overflows UInt64");
    work_units = checkedAdd(
        work_units,
        checkedMultiply(descriptor_count, 2, "stored-object publication provenance work overflows UInt64"),
        "stored-object publication work overflows UInt64");
    work_units = checkedAdd(work_units, sortWorkUnits(descriptor_count), "stored-object publication work overflows UInt64");
    if (work_units > limits.maximum_work_units)
        fail(Error::Code::LimitExceeded, "stored-object publication exceeds its work limit");

    DependentObjectMetadataInstallationRecord installation_record{
        .object = object,
        .object_schema_revision = validated_metadata.getObjectSchemaRevision(),
        .object_name = validated_metadata.getObjectName(),
        .metadata_artifact_hash = validated_metadata.getCanonicalMetadataHash(),
    };
    String canonical_installation_record_bytes;
    Digest installation_record_hash{};
    try
    {
        canonical_installation_record_bytes
            = encodeDependentObjectMetadataInstallationRecord(installation_record, limits.installation_record);
        if (canonical_installation_record_bytes.size() != prospective_installation_bytes
            || decodeDependentObjectMetadataInstallationRecord(canonical_installation_record_bytes, limits.installation_record)
                != installation_record)
            fail(Error::Code::IntegrityMismatch, "stored-object installation record does not round-trip canonically");
        installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installation_record, limits.installation_record);
    }
    catch (const DependentObjectMetadataInstallationRecordError & error)
    {
        if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "stored-object installation record exceeds its limit");
        fail(Error::Code::InvalidMetadata, "stored-object installation record is invalid");
    }

    if (expected_expectation.installation_record_hash && *expected_expectation.installation_record_hash != installation_record_hash)
        fail(Error::Code::IntegrityMismatch, "stored-object expectation names another metadata installation record");
    expected_expectation.installation_record_hash = installation_record_hash;
    String canonical_expectation_record_bytes;
    Digest expectation_record_hash{};
    try
    {
        canonical_expectation_record_bytes = encodeSidecarExpectationRecord(expected_expectation);
        if (canonical_expectation_record_bytes.size() != sidecar_expectation_record_extended_encoded_bytes
            || decodeSidecarExpectationRecord(canonical_expectation_record_bytes) != expected_expectation)
            fail(Error::Code::IntegrityMismatch, "stored-object expectation record does not round-trip canonically");
        expectation_record_hash = computeSidecarExpectationRecordHash(expected_expectation);
    }
    catch (const SidecarExpectationRecordError &)
    {
        fail(Error::Code::InvalidSidecar, "stored-object expectation record is invalid");
    }
    UInt64 owned_canonical_bytes = checkedAdd(
        checkedSize(validated_metadata.getCanonicalMetadataBytes().capacity(), "stored-object metadata capacity exceeds UInt64"),
        checkedSize(canonical_sidecar_bytes.capacity(), "stored-object sidecar capacity exceeds UInt64"),
        "stored-object canonical artifact capacity overflows UInt64");
    owned_canonical_bytes = checkedAdd(
        owned_canonical_bytes,
        checkedSize(canonical_installation_record_bytes.capacity(), "stored-object installation-record capacity exceeds UInt64"),
        "stored-object canonical artifact capacity overflows UInt64");
    owned_canonical_bytes = checkedAdd(
        owned_canonical_bytes,
        checkedSize(canonical_expectation_record_bytes.capacity(), "stored-object expectation-record capacity exceeds UInt64"),
        "stored-object canonical artifact capacity overflows UInt64");
    if (owned_canonical_bytes > limits.maximum_owned_canonical_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication canonical artifacts retain capacity beyond their byte limit");

    UInt64 descriptors_validated = 0;
    UInt64 provenance_scratch_bytes_upper_bound = 0;
    auto definition_dependency_edges = validateDefinitionsAndMakeEdges(
        root,
        references,
        limits.maximum_definition_dependencies,
        limits.maximum_provenance_scratch_bytes,
        descriptors_validated,
        provenance_scratch_bytes_upper_bound);
    auto dependency_edges = validateObjectDependenciesAndMakeEdges(root, object, object_dependencies, limits.maximum_object_dependencies);
    if (dependency_edges.size() > limits.schema_graph.maximum_mutation_edges
        || dependency_edges.size() > base_graph->getLimits().maximum_mutation_edges
        || definition_dependency_edges.size() > limits.schema_graph.maximum_mutation_edges - dependency_edges.size()
        || definition_dependency_edges.size() > base_graph->getLimits().maximum_mutation_edges - dependency_edges.size())
        fail(Error::Code::LimitExceeded, "stored-object dependencies exceed the graph mutation limit");
    dependency_edges.insert(dependency_edges.end(), definition_dependency_edges.begin(), definition_dependency_edges.end());
    std::sort(dependency_edges.begin(), dependency_edges.end());
    work_units = checkedAdd(
        work_units,
        checkedSize(dependency_edges.size(), "stored-object dependency count exceeds UInt64"),
        "stored-object publication work overflows UInt64");
    work_units = checkedAdd(
        work_units,
        sortWorkUnits(checkedSize(dependency_edges.size(), "stored-object dependency count exceeds UInt64")),
        "stored-object publication work overflows UInt64");
    if (work_units > limits.maximum_work_units)
        fail(Error::Code::LimitExceeded, "stored-object publication exceeds its work limit");

    const AuthorityInventoryKey inventory_key = expectationInventoryKey(object);
    if (root.findExpectationRecord(object) || base_inventory->find(inventory_key) || base_graph->containsNode(object))
        fail(Error::Code::ObjectAlreadyExists, "stored-object identity already exists in the pinned authority root");
    const AuthorityInventoryLeaf inventory_leaf{
        .key = inventory_key,
        .object_revision = validated_metadata.getObjectSchemaRevision(),
        .canonical_record_hash = expectation_record_hash,
    };
    std::vector<AuthorityInventoryLeafDelta> inventory_leaf_deltas{{
        .key = inventory_key,
        .before = std::nullopt,
        .after = inventory_leaf,
    }};
    SchemaObjectDependencyGraphMutation schema_graph_delta{
        .node_additions = {object},
        .node_removals = {},
        .edge_additions = std::move(dependency_edges),
        .edge_removals = {},
    };
    validateProspectiveGraphMutation(*base_graph, schema_graph_delta, limits.schema_graph);

    const UInt64 next_leaf_count = checkedAdd(base_inventory_summary.leaf_count, 1, "stored-object inventory count overflows UInt64");
    if (next_leaf_count > limits.authority_root.inventory.maximum_leaves
        || next_leaf_count > limits.authority_root.authority_state.maximum_leaves)
        fail(Error::Code::LimitExceeded, "stored-object inventory has no capacity for its expectation leaf");
    const UInt64 pinned_root_logical_bytes = checkedAdd(
        AuthorityRoot::getWrapperLogicalCharge(),
        root.getContentPayloadLogicalCharge(),
        "stored-object pinned-root charge overflows UInt64");
    const UInt64 after_inventory_logical_bytes_upper_bound = checkedMultiply(
        next_leaf_count, inventory_logical_bytes_per_leaf_upper_bound, "stored-object inventory logical charge overflows UInt64");
    const UInt64 package_owned_logical_bytes = computePackageOwnedLogicalBytes(
        validated_metadata,
        references,
        installation_record,
        canonical_sidecar_bytes,
        canonical_installation_record_bytes,
        canonical_expectation_record_bytes,
        inventory_leaf_deltas,
        schema_graph_delta);
    UInt64 prospective_replacement_root_logical_bytes = checkedAdd(
        AuthorityRoot::getWrapperLogicalCharge(),
        root.getContentPayloadLogicalCharge(),
        "stored-object replacement-root charge overflows UInt64");
    prospective_replacement_root_logical_bytes = checkedAdd(
        prospective_replacement_root_logical_bytes,
        limits.schema_graph.maximum_retained_bytes,
        "stored-object replacement-root charge overflows UInt64");
    prospective_replacement_root_logical_bytes = checkedAdd(
        prospective_replacement_root_logical_bytes,
        AuthorityInventory::getSingleLeafInsertionAccountedBytesUpperBound(),
        "stored-object replacement-root charge overflows UInt64");
    prospective_replacement_root_logical_bytes = checkedAdd(
        prospective_replacement_root_logical_bytes,
        AuthorityRootBuilder::getExpectationRecordInsertionLogicalChargeUpperBound(),
        "stored-object replacement-root charge overflows UInt64");
    prospective_replacement_root_logical_bytes = checkedAdd(
        prospective_replacement_root_logical_bytes,
        AuthorityResourceUsageIndex::getObjectInsertionAccountedBytesUpperBound(
            descriptor_count, canonical_argument_bytes, canonical_physical_type_bytes),
        "stored-object resource-index replacement charge overflows UInt64");
    UInt64 prospective_retained_bytes = checkedAdd(
        pinned_root_logical_bytes, after_inventory_logical_bytes_upper_bound, "stored-object publication retained charge overflows UInt64");
    prospective_retained_bytes = checkedAdd(
        prospective_retained_bytes,
        prospective_replacement_root_logical_bytes,
        "stored-object publication retained charge overflows UInt64");
    prospective_retained_bytes
        = checkedAdd(prospective_retained_bytes, package_owned_logical_bytes, "stored-object publication retained charge overflows UInt64");
    if (prospective_retained_bytes > limits.maximum_retained_logical_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication package exceeds its retained logical-byte limit");

    StoredObjectUDTPublicationPackageStatistics statistics{
        .descriptors_validated = descriptors_validated,
        .occurrences_validated = occurrence_count,
        .definition_dependencies
        = checkedSize(definition_dependency_edges.size(), "stored-object definition-dependency count exceeds UInt64"),
        .object_dependencies = checkedSize(object_dependencies.size(), "stored-object object-dependency count exceeds UInt64"),
        .work_units = work_units,
        .provenance_scratch_bytes_upper_bound = provenance_scratch_bytes_upper_bound,
        .owned_canonical_bytes = owned_canonical_bytes,
        .pinned_root_logical_bytes = pinned_root_logical_bytes,
        .after_inventory_logical_bytes_upper_bound = after_inventory_logical_bytes_upper_bound,
        .inventory_mutation = {},
        .graph_mutation = {},
    };
    DependentObjectAdmissionDeltaStatistics delta_statistics;
    AuthorityRoot::Ptr replacement_root;
    try
    {
        replacement_root = AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
            root,
            before_authority_state.database_catalog_epoch + 1,
            expected_expectation,
            schema_graph_delta,
            {
                .object = object,
                .canonical_metadata_bytes = validated_metadata.getCanonicalMetadataBytes(),
                .canonical_sidecar_bytes = canonical_sidecar_bytes,
                .canonical_installation_record_bytes = canonical_installation_record_bytes,
            },
            limits.authority_root,
            &delta_statistics);
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::InvalidConfiguration)
            fail(Error::Code::InvalidConfiguration, "stored-object replacement-root limits are invalid");
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "stored-object replacement root is invalid");
    }
    statistics.inventory_mutation = delta_statistics.inventory;
    statistics.graph_mutation = delta_statistics.graph;

    const auto after_inventory = replacement_root ? replacement_root->pinAuthorityInventory() : AuthorityInventory::Ptr{};
    const auto after_schema_graph
        = replacement_root ? replacement_root->pinSchemaObjectDependencyGraph() : SchemaObjectDependencyGraph::Ptr{};
    try
    {
        if (!after_schema_graph)
            fail(Error::Code::InvalidTransition, "stored-object replacement root has no schema graph");
        after_schema_graph->validateAgainstLimits(limits.schema_graph);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "stored-object schema-graph delta exceeds its limit");
        fail(Error::Code::InvalidTransition, "stored-object schema-graph delta is invalid");
    }

    if (!replacement_root || !replacement_root->sharesDefinitionContentWith(root) || !after_inventory
        || after_inventory->getSummary().leaf_count != next_leaf_count || !after_inventory->find(inventory_key)
        || *after_inventory->find(inventory_key) != inventory_leaf || !after_schema_graph
        || after_schema_graph->getNodeCount() != base_graph->getNodeCount() + 1
        || after_schema_graph->getEdgeCount() != base_graph->getEdgeCount() + schema_graph_delta.edge_additions.size()
        || !after_schema_graph->containsNode(object) || !replacement_root->findExpectationRecord(object)
        || *replacement_root->findExpectationRecord(object) != expected_expectation)
        fail(Error::Code::InvalidTransition, "stored-object replacement root disagrees with its exact delta");
    for (const auto & edge : schema_graph_delta.edge_additions)
        if (!after_schema_graph->containsEdge(edge))
            fail(Error::Code::InvalidTransition, "stored-object after-graph is missing a definition edge");

    const auto & after_authority_state = replacement_root->getAuthorityState();
    if (after_authority_state.database_catalog_epoch != before_authority_state.database_catalog_epoch + 1
        || after_authority_state.database_uuid != before_authority_state.database_uuid
        || after_authority_state.persistent_capability_mask != before_authority_state.persistent_capability_mask
        || after_authority_state.leaf_count != after_inventory->getSummary().leaf_count
        || after_authority_state.inventory_root != after_inventory->getSummary().merkle_radix_root
        || after_authority_state.schema_graph_root != after_schema_graph->computeRoot())
        fail(Error::Code::InvalidTransition, "stored-object after-authority identity changes unrelated fields");

    statistics.after_graph_logical_bytes = after_schema_graph->getAccountedBytes();
    statistics.replacement_root_logical_bytes_upper_bound = checkedAdd(
        AuthorityRoot::getWrapperLogicalCharge(),
        replacement_root->getContentPayloadLogicalCharge(),
        "stored-object replacement-root charge overflows UInt64");
    statistics.retained_logical_bytes_upper_bound = checkedAdd(
        pinned_root_logical_bytes, after_inventory_logical_bytes_upper_bound, "stored-object publication retained charge overflows UInt64");
    statistics.retained_logical_bytes_upper_bound = checkedAdd(
        statistics.retained_logical_bytes_upper_bound,
        statistics.replacement_root_logical_bytes_upper_bound,
        "stored-object publication retained charge overflows UInt64");
    statistics.retained_logical_bytes_upper_bound = checkedAdd(
        statistics.retained_logical_bytes_upper_bound,
        package_owned_logical_bytes,
        "stored-object publication retained charge overflows UInt64");
    if (statistics.retained_logical_bytes_upper_bound > limits.maximum_retained_logical_bytes)
        fail(Error::Code::LimitExceeded, "stored-object publication result exceeds its retained logical-byte limit");

    return StoredObjectUDTPublicationPackage(
        std::move(planning_root),
        before_authority_state,
        std::move(validated_metadata),
        std::move(references),
        std::move(canonical_sidecar_bytes),
        std::move(installation_record),
        std::move(canonical_installation_record_bytes),
        installation_record_hash,
        expected_expectation,
        std::move(canonical_expectation_record_bytes),
        expectation_record_hash,
        std::move(inventory_leaf_deltas),
        std::move(schema_graph_delta),
        std::move(replacement_root),
        statistics);
}

}
