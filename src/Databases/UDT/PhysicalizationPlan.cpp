#include <Databases/UDT/PhysicalizationPlan.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <algorithm>
#include <deque>
#include <map>
#include <set>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = PhysicalizationPlanError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedSize(size_t value)
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

void checkedIncrement(UInt64 & value, UInt64 maximum, std::string_view message)
{
    if (value == maximum)
        fail(Error::Code::LimitExceeded, message);
    ++value;
}

void checkedCharge(UInt64 & value, UInt64 additional, UInt64 maximum, std::string_view message)
{
    if (value > maximum || additional > maximum - value)
        fail(Error::Code::LimitExceeded, message);
    value += additional;
}

bool containsZero(std::string_view value) noexcept
{
    return value.find('\0') != std::string_view::npos;
}

bool definitionIdentityLess(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    const auto lhs_database = uuidToCanonicalBytes(lhs.database_uuid);
    const auto rhs_database = uuidToCanonicalBytes(rhs.database_uuid);
    if (lhs_database != rhs_database)
        return lhs_database < rhs_database;
    const auto lhs_type = uuidToCanonicalBytes(lhs.type_uuid);
    const auto rhs_type = uuidToCanonicalBytes(rhs.type_uuid);
    if (lhs_type != rhs_type)
        return lhs_type < rhs_type;
    return lhs.revision < rhs.revision;
}

bool hasPhysicalizationAdapter(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::SyntheticTestObject || kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View
        || kind == SchemaObjectKind::Dictionary;
}

SchemaObjectID definitionObject(const DefinitionIdentity & identity)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = identity.database_uuid,
        .object_uuid = identity.type_uuid,
    };
}

void validateLimits(const PhysicalizationPlanLimits & limits)
{
    const PersistedTypeReferencesLimits maximum_persisted;
    const RecordLimits maximum_record;
    if (!limits.persisted_references.maximum_sidecar_bytes
        || limits.persisted_references.maximum_sidecar_bytes > maximum_persisted.maximum_sidecar_bytes
        || !limits.persisted_references.maximum_descriptors
        || limits.persisted_references.maximum_descriptors > maximum_persisted.maximum_descriptors
        || !limits.persisted_references.maximum_occurrence_paths
        || limits.persisted_references.maximum_occurrence_paths > maximum_persisted.maximum_occurrence_paths
        || !limits.persisted_references.maximum_path_depth
        || limits.persisted_references.maximum_path_depth > maximum_persisted.maximum_path_depth
        || !limits.persisted_references.maximum_canonical_arguments_bytes
        || limits.persisted_references.maximum_canonical_arguments_bytes > maximum_persisted.maximum_canonical_arguments_bytes
        || !limits.persisted_references.maximum_canonical_physical_type_bytes
        || limits.persisted_references.maximum_canonical_physical_type_bytes > maximum_persisted.maximum_canonical_physical_type_bytes
        || !limits.persisted_references.maximum_qualified_name_bytes
        || limits.persisted_references.maximum_qualified_name_bytes > maximum_persisted.maximum_qualified_name_bytes)
        fail(Error::Code::InvalidConfiguration, "physicalization sidecar limits are invalid");

    if (!limits.definition_record.maximum_record_bytes
        || limits.definition_record.maximum_record_bytes > maximum_record.maximum_record_bytes
        || !limits.definition_record.maximum_name_bytes || limits.definition_record.maximum_name_bytes > maximum_record.maximum_name_bytes
        || !limits.definition_record.maximum_parameter_count
        || limits.definition_record.maximum_parameter_count > maximum_record.maximum_parameter_count
        || !limits.definition_record.maximum_parameter_name_bytes
        || limits.definition_record.maximum_parameter_name_bytes > maximum_record.maximum_parameter_name_bytes
        || !limits.definition_record.maximum_canonical_sql_bytes
        || limits.definition_record.maximum_canonical_sql_bytes > maximum_record.maximum_canonical_sql_bytes
        || !limits.definition_record.maximum_template_ir_bytes
        || limits.definition_record.maximum_template_ir_bytes > maximum_record.maximum_template_ir_bytes
        || !limits.definition_record.maximum_dependency_count
        || limits.definition_record.maximum_dependency_count > maximum_record.maximum_dependency_count
        || !limits.definition_record.maximum_checker_certificate_bytes
        || limits.definition_record.maximum_checker_certificate_bytes > maximum_record.maximum_checker_certificate_bytes
        || !limits.definition_record.maximum_owner_display_name_bytes
        || limits.definition_record.maximum_owner_display_name_bytes > maximum_record.maximum_owner_display_name_bytes
        || !limits.definition_record.maximum_comment_bytes
        || limits.definition_record.maximum_comment_bytes > maximum_record.maximum_comment_bytes)
        fail(Error::Code::InvalidConfiguration, "physicalization definition-record limits are invalid");

    if (!limits.maximum_selected_objects || limits.maximum_selected_objects > physicalization_maximum_selected_objects
        || !limits.maximum_validation_definitions || limits.maximum_validation_definitions > physicalization_maximum_validation_definitions
        || !limits.maximum_walked_edges || limits.maximum_walked_edges > schema_object_dependency_graph_maximum_edges
        || !limits.maximum_manifest_entries || limits.maximum_manifest_entries > schema_object_dependency_graph_maximum_edges
        || !limits.maximum_scope_bytes || limits.maximum_scope_bytes > (16ULL << 20) || !limits.maximum_manifest_bytes
        || limits.maximum_manifest_bytes > (512ULL << 20) || !limits.maximum_diagnostic_name_bytes
        || limits.maximum_diagnostic_name_bytes > (4ULL << 10))
        fail(Error::Code::InvalidConfiguration, "physicalization plan limits are invalid");
}

void validateSelector(const PhysicalizationSelector & selector, const AuthorityRoot & root)
{
    if (root.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(Error::Code::InvalidSelector, "physicalization requires the complete dependent-object-capable authority capability set");

    switch (selector.scope)
    {
        case PhysicalizationScope::Object:
        case PhysicalizationScope::DependentClosure:
            if (!selector.object || !selector.object->isValid() || selector.object->database_uuid != root.getDatabaseUUID())
                fail(Error::Code::InvalidSelector, "physicalization object selector is invalid");
            if (!hasPhysicalizationAdapter(selector.object->kind))
                fail(Error::Code::UnsupportedObjectKind, "physicalization object kind has no registered adapter");
            return;
        case PhysicalizationScope::Database:
            if (selector.object)
                fail(Error::Code::InvalidSelector, "database physicalization selector contains an object");
            return;
    }
    fail(Error::Code::InvalidSelector, "physicalization selector scope is unknown");
}

class Encoder final
{
public:
    explicit Encoder(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    void byte(UInt8 value)
    {
        reserve(1);
        output.push_back(static_cast<char>(value));
    }

    void uint16(UInt16 value)
    {
        reserve(sizeof(value));
        for (size_t index = 0; index < sizeof(value); ++index)
            output.push_back(static_cast<char>(value >> (8 * index)));
    }

    void uint64(UInt64 value)
    {
        reserve(sizeof(value));
        for (size_t index = 0; index < sizeof(value); ++index)
            output.push_back(static_cast<char>(value >> (8 * index)));
    }

    void varUInt(UInt64 value)
    {
        do
        {
            UInt8 current = static_cast<UInt8>(value & 0x7f);
            value >>= 7;
            if (value)
                current = static_cast<UInt8>(current | 0x80);
            byte(current);
        } while (value);
    }

    void uuid(const UUID & value)
    {
        const auto bytes = uuidToCanonicalBytes(value);
        raw(std::span<const CanonicalByte>(bytes.data(), bytes.size()));
    }

    void digest(const Digest & value) { raw(std::span<const CanonicalByte>(value.data(), value.size())); }

    void object(const SchemaObjectID & value)
    {
        byte(static_cast<UInt8>(value.kind));
        uuid(value.database_uuid);
        uuid(value.object_uuid);
    }

    void identity(const DefinitionIdentity & value)
    {
        uuid(value.database_uuid);
        uuid(value.type_uuid);
        uint64(value.revision);
    }

    void string(std::string_view value)
    {
        varUInt(checkedSize(value.size()));
        reserve(checkedSize(value.size()));
        output.append(value.data(), value.size());
    }

    String release() && { return std::move(output); }

private:
    void raw(std::span<const CanonicalByte> value)
    {
        reserve(checkedSize(value.size()));
        output.append(reinterpret_cast<const char *>(value.data()), value.size());
    }

    void reserve(UInt64 additional)
    {
        const UInt64 current = checkedSize(output.size());
        if (current > maximum_bytes || additional > maximum_bytes - current)
            fail(Error::Code::LimitExceeded, "physicalization canonical bytes exceed their limit");
    }

    const UInt64 maximum_bytes;
    String output;
};

std::vector<SchemaObjectID> selectObjects(
    const AuthorityRoot & root, const PhysicalizationSelector & selector, const PhysicalizationPlanLimits & limits, UInt64 & walked_edges)
{
    const auto & graph = root.getSchemaObjectDependencyGraph();
    std::set<SchemaObjectID> selected;

    if (selector.scope == PhysicalizationScope::Database)
    {
        const auto expectations = root.getExpectationRecords();
        if (checkedSize(expectations.size()) > limits.maximum_selected_objects)
            fail(Error::Code::LimitExceeded, "physicalization selected-object count exceeds its limit");
        for (const auto & expectation : expectations)
        {
            if (!hasPhysicalizationAdapter(expectation.object.kind))
                fail(Error::Code::UnsupportedObjectKind, "database scope contains an object kind without a registered adapter");
            selected.insert(expectation.object);
        }
    }
    else
    {
        if (!root.findExpectationRecord(*selector.object))
            fail(Error::Code::ObjectNotFound, "physicalization seed has no active sidecar expectation");
        selected.insert(*selector.object);
    }

    std::deque<SchemaObjectID> pending;
    if (selector.scope == PhysicalizationScope::DependentClosure)
        pending.push_back(*selector.object);

    while (!pending.empty())
    {
        const SchemaObjectID dependency = pending.front();
        pending.pop_front();
        for (const auto & dependent : graph.getDependents(dependency))
        {
            if (dependent.kind != SchemaObjectDependencyEdgeKind::ObjectDependsOnObject)
                continue;
            checkedIncrement(walked_edges, limits.maximum_walked_edges, "physicalization graph walk exceeds its edge limit");
            if (!hasPhysicalizationAdapter(dependent.object.kind))
                fail(Error::Code::UnsupportedObjectKind, "dependent closure contains an object kind without a registered adapter");
            if (!root.findExpectationRecord(dependent.object))
                fail(Error::Code::IncompleteScope, "dependent closure contains an object without a sidecar expectation");
            if (!selected.contains(dependent.object))
            {
                if (checkedSize(selected.size()) >= limits.maximum_selected_objects)
                    fail(Error::Code::LimitExceeded, "physicalization selected-object count exceeds its limit");
                selected.insert(dependent.object);
                pending.push_back(dependent.object);
            }
        }
    }

    if (selector.scope != PhysicalizationScope::DependentClosure)
    {
        for (const auto & object : selected)
        {
            for (const auto & dependent : graph.getDependents(object))
            {
                if (dependent.kind != SchemaObjectDependencyEdgeKind::ObjectDependsOnObject)
                    continue;
                checkedIncrement(walked_edges, limits.maximum_walked_edges, "physicalization graph walk exceeds its edge limit");
                if (!selected.contains(dependent.object))
                    fail(Error::Code::IncompleteScope, "physicalization scope omits a dependent object");
            }
        }
    }

    return {selected.begin(), selected.end()};
}

UInt64 validateObject(
    const AuthorityRoot & root,
    const SidecarExpectationRecord & expectation,
    const PhysicalizationObject & object,
    const PhysicalizationPlanLimits & limits,
    UInt64 & walked_edges)
{
    if (object.object != expectation.object || object.object_schema_revision != expectation.object_schema_revision
        || object.references.object != expectation.object || object.references.object_schema_revision != expectation.object_schema_revision)
        fail(Error::Code::IntegrityMismatch, "physicalization object identity or revision differs from its expectation");
    if (object.diagnostic_name.empty() || containsZero(object.diagnostic_name)
        || checkedSize(object.diagnostic_name.size()) > limits.maximum_diagnostic_name_bytes)
        fail(Error::Code::IntegrityMismatch, "physicalization object diagnostic name is invalid");
    if (object.references.physical_schema_fingerprint != expectation.physical_schema_fingerprint)
        fail(Error::Code::IntegrityMismatch, "physicalization object fingerprint differs from its expectation");

    UInt64 encoded_sidecar_bytes = 0;
    try
    {
        const Digest sidecar_hash = computePersistedTypeReferencesSidecarHash(object.references, limits.persisted_references);
        if (sidecar_hash != expectation.sidecar_hash)
            fail(Error::Code::IntegrityMismatch, "physicalization sidecar differs from its expectation hash");
        encoded_sidecar_bytes = checkedSize(encodePersistedTypeReferences(object.references, limits.persisted_references).size());
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "physicalization sidecar exceeds its limit");
        fail(Error::Code::IntegrityMismatch, "physicalization sidecar is not canonical");
    }
    if (object.selected_semantic_capabilities.size() != object.references.uses.size())
        fail(Error::Code::IntegrityMismatch, "physicalization bound-use count differs from its sidecar");

    for (size_t index = 0; index < object.references.uses.size(); ++index)
    {
        const UInt64 descriptor_id = object.references.uses[index].descriptor_id;
        if (descriptor_id >= object.references.descriptors.size())
            fail(Error::Code::IntegrityMismatch, "physicalization use references an invalid descriptor");
        const auto capabilities = object.selected_semantic_capabilities[index];
        const auto descriptor_capabilities = object.references.descriptors[static_cast<size_t>(descriptor_id)].getSemanticCapabilities();
        if ((capabilities & static_cast<SemanticCapabilityMask>(~all_semantic_capabilities)) != 0
            || (capabilities & descriptor_capabilities) != capabilities)
            fail(Error::Code::IntegrityMismatch, "physicalization bound-use capabilities differ from their descriptor");
    }

    std::vector<SchemaObjectID> expected_definition_dependencies;
    expected_definition_dependencies.reserve(object.references.descriptors.size());
    for (const auto & descriptor : object.references.descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        if (identity.database_uuid != root.getDatabaseUUID())
            fail(Error::Code::IntegrityMismatch, "physicalization descriptor belongs to another database");
        expected_definition_dependencies.push_back(definitionObject(identity));
    }
    std::sort(expected_definition_dependencies.begin(), expected_definition_dependencies.end());
    expected_definition_dependencies.erase(
        std::unique(expected_definition_dependencies.begin(), expected_definition_dependencies.end()),
        expected_definition_dependencies.end());

    std::vector<SchemaObjectID> actual_definition_dependencies;
    for (const auto & dependency : root.getSchemaObjectDependencyGraph().getDependencies(object.object))
    {
        if (dependency.kind != SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition)
            continue;
        checkedIncrement(walked_edges, limits.maximum_walked_edges, "physicalization graph walk exceeds its edge limit");
        actual_definition_dependencies.push_back(dependency.object);
    }
    if (actual_definition_dependencies != expected_definition_dependencies)
        fail(Error::Code::GraphMismatch, "physicalization sidecar and object definition edges differ");
    return encoded_sidecar_bytes;
}

std::vector<PhysicalizationManifestDefinition> collectDefinitions(
    const AuthorityRoot & root,
    std::span<const PhysicalizationManifestObject> objects,
    bool drop_unused_types,
    bool include_all_definitions,
    const PhysicalizationPlanLimits & limits,
    UInt64 & walked_edges,
    UInt64 & retained_manifest_bytes,
    UInt64 & prospective_manifest_entries)
{
    const auto & graph = root.getSchemaObjectDependencyGraph();
    std::set<SchemaObjectID> validation_objects;
    std::map<SchemaObjectID, UInt64> selected_object_dependent_counts;
    std::map<SchemaObjectID, std::vector<SchemaObjectID>> definition_dependencies;
    std::deque<SchemaObjectID> pending;
    const auto enqueue_definition = [&](const SchemaObjectID & definition)
    {
        if (validation_objects.contains(definition))
            return;
        if (checkedSize(validation_objects.size()) >= limits.maximum_validation_definitions)
            fail(Error::Code::LimitExceeded, "physicalization validation-definition count exceeds its limit");
        checkedIncrement(
            prospective_manifest_entries, limits.maximum_manifest_entries, "physicalization manifest entry count exceeds its limit");
        validation_objects.insert(definition);
        pending.push_back(definition);
    };
    for (const auto & object : objects)
    {
        std::set<SchemaObjectID> object_definitions;
        for (const auto & descriptor : object.references.descriptors)
        {
            const auto & identity = descriptor.getDefinitionIdentity();
            const auto * record = root.findDefinitionRecord(identity);
            if (!record || record->definition_hash != descriptor.getDefinitionHash() || !root.findByIdentity(identity))
                fail(Error::Code::IntegrityMismatch, "physicalization descriptor does not match the active definition record");
            const SchemaObjectID definition = definitionObject(identity);
            object_definitions.insert(definition);
            enqueue_definition(definition);
        }
        for (const auto & definition : object_definitions)
            ++selected_object_dependent_counts[definition];
    }

    if (include_all_definitions)
    {
        for (const auto & definition : root.getDefinitionRecords())
            enqueue_definition(definitionObject(definition.identity));
    }

    while (!pending.empty())
    {
        const SchemaObjectID definition = pending.front();
        pending.pop_front();
        auto & retained_dependencies = definition_dependencies[definition];
        for (const auto & dependency : graph.getDependencies(definition))
        {
            if (dependency.kind != SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition)
                continue;
            checkedIncrement(walked_edges, limits.maximum_walked_edges, "physicalization graph walk exceeds its edge limit");
            retained_dependencies.push_back(dependency.object);
            enqueue_definition(dependency.object);
        }
    }

    if (checkedSize(validation_objects.size()) > limits.maximum_validation_definitions)
        fail(Error::Code::LimitExceeded, "physicalization validation-definition count exceeds its limit");

    std::set<SchemaObjectID> blocked;
    std::deque<SchemaObjectID> blocked_pending;
    if (drop_unused_types)
    {
        std::map<SchemaObjectID, UInt64> internal_definition_dependent_counts;
        for (const auto & [dependent, dependencies] : definition_dependencies)
        {
            static_cast<void>(dependent);
            for (const auto & dependency : dependencies)
                ++internal_definition_dependent_counts[dependency];
        }
        for (const auto & definition : validation_objects)
        {
            const UInt64 selected_object_dependents = selected_object_dependent_counts[definition];
            const UInt64 internal_definition_dependents = internal_definition_dependent_counts[definition];
            const UInt64 accounted_dependents = selected_object_dependents + internal_definition_dependents;
            const UInt64 actual_dependents = checkedSize(graph.getDependents(definition).size());
            if (actual_dependents < accounted_dependents)
                fail(Error::Code::GraphMismatch, "physicalization dependent counts disagree with the schema graph");
            const bool has_external_dependent = actual_dependents > accounted_dependents;
            if (has_external_dependent && blocked.insert(definition).second)
                blocked_pending.push_back(definition);
        }

        while (!blocked_pending.empty())
        {
            const SchemaObjectID retained_dependent = blocked_pending.front();
            blocked_pending.pop_front();
            for (const auto & dependency : definition_dependencies[retained_dependent])
            {
                if (dependency == retained_dependent || !validation_objects.contains(dependency))
                    continue;
                if (blocked.insert(dependency).second)
                    blocked_pending.push_back(dependency);
            }
        }
    }

    std::vector<PhysicalizationManifestDefinition> result;
    const UInt64 validation_count = checkedSize(validation_objects.size());
    if (validation_count > limits.maximum_manifest_bytes / 160)
        fail(Error::Code::LimitExceeded, "physicalization retained definition bytes exceed the manifest limit");
    checkedCharge(
        retained_manifest_bytes,
        validation_count * 160,
        limits.maximum_manifest_bytes,
        "physicalization retained definition bytes exceed the manifest limit");
    result.reserve(validation_objects.size());
    for (const auto & definition : validation_objects)
    {
        const auto * record = root.findDefinitionRecord(definition.object_uuid);
        if (!record)
            fail(Error::Code::GraphMismatch, "physicalization graph references a missing definition record");
        String canonical_record_bytes;
        try
        {
            canonical_record_bytes = encodeRecord(*record, limits.definition_record);
        }
        catch (const RecordError & error)
        {
            if (error.code == RecordError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "physicalization definition record exceeds its limit");
            fail(Error::Code::IntegrityMismatch, "physicalization definition record is not canonical");
        }
        UInt64 retained_definition_bytes = checkedSize(canonical_record_bytes.size());
        checkedCharge(
            retained_definition_bytes,
            checkedSize(record->normalized_name.size()),
            limits.maximum_manifest_bytes,
            "physicalization retained definition bytes exceed the manifest limit");
        checkedCharge(
            retained_manifest_bytes,
            retained_definition_bytes,
            limits.maximum_manifest_bytes,
            "physicalization retained definition bytes exceed the manifest limit");
        result.push_back({
            .identity = record->identity,
            .normalized_name = record->normalized_name,
            .definition_hash = record->definition_hash,
            .canonical_record_hash = computeRecordHash(*record, limits.definition_record),
            .canonical_record_bytes = std::move(canonical_record_bytes),
            .selected_for_drop = drop_unused_types && !blocked.contains(definition),
        });
    }
    std::sort(
        result.begin(),
        result.end(),
        [](const auto & lhs, const auto & rhs) { return definitionIdentityLess(lhs.identity, rhs.identity); });
    return result;
}

String encodeScope(const AuthorityRoot & root, std::span<const SchemaObjectID> selected_objects, UInt64 maximum_bytes)
{
    Encoder encoder(maximum_bytes);
    encoder.uint16(physicalization_plan_format_version);
    encoder.uuid(root.getDatabaseUUID());
    encoder.uint64(root.getDatabaseCatalogEpoch());
    encoder.digest(root.getInventorySummary().merkle_radix_root);
    encoder.varUInt(checkedSize(selected_objects.size()));
    for (const auto & object : selected_objects)
    {
        const auto * expectation = root.findExpectationRecord(object);
        if (!expectation)
            fail(Error::Code::ObjectNotFound, "physicalization selected object has no active expectation");
        encoder.object(expectation->object);
        encoder.uint64(expectation->object_schema_revision);
        encoder.digest(expectation->sidecar_hash);
    }
    return std::move(encoder).release();
}

String encodeManifest(
    const AuthorityRoot & root,
    std::span<const PhysicalizationManifestObject> objects,
    std::span<const PhysicalizationManifestDefinition> definitions,
    const PhysicalizationPlanLimits & limits)
{
    Encoder encoder(limits.maximum_manifest_bytes);
    encoder.uint16(physicalization_plan_format_version);
    encoder.uuid(root.getDatabaseUUID());
    encoder.uint64(root.getDatabaseCatalogEpoch());
    encoder.digest(root.getInventorySummary().merkle_radix_root);
    encoder.varUInt(checkedSize(objects.size()));
    for (const auto & object : objects)
    {
        encoder.object(object.object);
        encoder.uint64(object.object_schema_revision);
        encoder.string(object.diagnostic_name);
        encoder.digest(object.canonical_metadata_hash);
        encoder.digest(object.sidecar_hash);
        encoder.digest(object.physical_schema_fingerprint);
        encoder.string(encodePersistedTypeReferences(object.references, limits.persisted_references));
        encoder.varUInt(checkedSize(object.selected_semantic_capabilities.size()));
        for (const auto capabilities : object.selected_semantic_capabilities)
            encoder.uint64(capabilities);
        /// V1 compatibility consequence: physical schema bytes remain, while
        /// logical provenance and its selected semantic capabilities are lost.
        encoder.byte(1);
    }
    encoder.varUInt(checkedSize(definitions.size()));
    for (const auto & definition : definitions)
    {
        encoder.identity(definition.identity);
        encoder.string(definition.normalized_name);
        encoder.digest(definition.definition_hash);
        encoder.digest(definition.canonical_record_hash);
        encoder.string(definition.canonical_record_bytes);
        encoder.byte(definition.selected_for_drop ? 1 : 0);
    }
    return std::move(encoder).release();
}

UInt64 countManifestEntries(
    std::span<const PhysicalizationManifestObject> objects, std::span<const PhysicalizationManifestDefinition> definitions, UInt64 maximum)
{
    UInt64 result = checkedSize(definitions.size());
    if (result > maximum)
        fail(Error::Code::LimitExceeded, "physicalization manifest entry count exceeds its limit");
    for (const auto & object : objects)
    {
        const UInt64 descriptors = checkedSize(object.references.descriptors.size());
        const UInt64 uses = checkedSize(object.references.uses.size());
        if (result == maximum || descriptors > maximum - result - 1)
            fail(Error::Code::LimitExceeded, "physicalization manifest entry count exceeds its limit");
        result += 1 + descriptors;
        if (uses > maximum - result)
            fail(Error::Code::LimitExceeded, "physicalization manifest entry count exceeds its limit");
        result += uses;
    }
    return result;
}

}

PhysicalizationPlanError::PhysicalizationPlanError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

std::vector<SchemaObjectID> PhysicalizationPlanner::selectObjectIdentities(
    const AuthorityRoot & root, const PhysicalizationSelector & selector, const PhysicalizationPlanLimits & limits)
{
    validateLimits(limits);
    validateSelector(selector, root);
    UInt64 walked_edges = 0;
    return selectObjects(root, selector, limits, walked_edges);
}

PhysicalizationPlan::PhysicalizationPlan(
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
    UInt64 manifest_count_)
    : selector(std::move(selector_))
    , database_uuid(database_uuid_)
    , database_catalog_epoch(database_catalog_epoch_)
    , inventory_root(inventory_root_)
    , objects(std::move(objects_))
    , definitions(std::move(definitions_))
    , canonical_scope_bytes(std::move(canonical_scope_bytes_))
    , scope_digest(scope_digest_)
    , scope_count(scope_count_)
    , scope_bytes(checkedSize(canonical_scope_bytes.size()))
    , canonical_manifest_bytes(std::move(canonical_manifest_bytes_))
    , manifest_digest(manifest_digest_)
    , manifest_count(manifest_count_)
    , manifest_bytes(checkedSize(canonical_manifest_bytes.size()))
{
}

PhysicalizationPlan PhysicalizationPlanner::build(
    const AuthorityRoot & root,
    PhysicalizationSelector selector,
    const IPhysicalizationObjectProvider & object_provider,
    const PhysicalizationPlanLimits & limits)
{
    validateLimits(limits);
    validateSelector(selector, root);

    UInt64 walked_edges = 0;
    const auto selected_ids = selectObjects(root, selector, limits, walked_edges);
    if (checkedSize(selected_ids.size()) > limits.maximum_manifest_entries)
        fail(Error::Code::LimitExceeded, "physicalization manifest entry count exceeds its limit");
    String scope_bytes = encodeScope(root, selected_ids, limits.maximum_scope_bytes);
    const UInt64 selected_count = checkedSize(selected_ids.size());
    if (selected_count > limits.maximum_manifest_bytes / 256)
        fail(Error::Code::LimitExceeded, "physicalization retained object bytes exceed the manifest limit");
    UInt64 retained_manifest_bytes = 128;
    checkedCharge(
        retained_manifest_bytes,
        selected_count * 256,
        limits.maximum_manifest_bytes,
        "physicalization retained object bytes exceed the manifest limit");
    std::vector<PhysicalizationManifestObject> objects;
    objects.reserve(selected_ids.size());
    UInt64 prospective_manifest_entries = 0;
    for (const auto & selected : selected_ids)
    {
        object_provider.checkCancellation();
        const auto * expectation = root.findExpectationRecord(selected);
        if (!expectation)
            fail(Error::Code::ObjectNotFound, "physicalization selected object has no active expectation");
        auto object = object_provider.load(*expectation);
        const UInt64 encoded_sidecar_bytes = validateObject(root, *expectation, object, limits, walked_edges);
        const UInt64 descriptor_count = checkedSize(object.references.descriptors.size());
        const UInt64 use_count = checkedSize(object.references.uses.size());
        checkedCharge(
            prospective_manifest_entries,
            1 + descriptor_count + use_count,
            limits.maximum_manifest_entries,
            "physicalization manifest entry count exceeds its limit");
        UInt64 retained_bytes = encoded_sidecar_bytes;
        checkedCharge(
            retained_bytes,
            checkedSize(object.diagnostic_name.size()),
            limits.maximum_manifest_bytes,
            "physicalization retained object bytes exceed the manifest limit");
        checkedCharge(
            retained_bytes,
            use_count * sizeof(SemanticCapabilityMask),
            limits.maximum_manifest_bytes,
            "physicalization retained object bytes exceed the manifest limit");
        checkedCharge(
            retained_manifest_bytes,
            retained_bytes,
            limits.maximum_manifest_bytes,
            "physicalization retained object bytes exceed the manifest limit");
        objects.push_back({
            .object = object.object,
            .object_schema_revision = object.object_schema_revision,
            .diagnostic_name = std::move(object.diagnostic_name),
            .canonical_metadata_hash = object.canonical_metadata_hash,
            .sidecar_hash = expectation->sidecar_hash,
            .physical_schema_fingerprint = expectation->physical_schema_fingerprint,
            .references = std::move(object.references),
            .selected_semantic_capabilities = std::move(object.selected_semantic_capabilities),
        });
    }

    const bool include_all_definitions = selector.scope == PhysicalizationScope::Database && selector.drop_unused_types;
    auto definitions = collectDefinitions(
        root,
        objects,
        selector.drop_unused_types,
        include_all_definitions,
        limits,
        walked_edges,
        retained_manifest_bytes,
        prospective_manifest_entries);
    object_provider.checkCancellation();
    const UInt64 manifest_count = countManifestEntries(objects, definitions, limits.maximum_manifest_entries);
    if (manifest_count != prospective_manifest_entries)
        fail(Error::Code::IntegrityMismatch, "physicalization manifest entry accounting is inconsistent");
    String manifest_bytes = encodeManifest(root, objects, definitions, limits);
    object_provider.checkCancellation();
    const Digest scope_digest = hashFramedDomainSeparated(physicalization_scope_hash_domain, scope_bytes);
    const Digest manifest_digest = hashFramedDomainSeparated(physicalization_manifest_hash_domain, manifest_bytes);

    return PhysicalizationPlan(
        std::move(selector),
        root.getDatabaseUUID(),
        root.getDatabaseCatalogEpoch(),
        root.getInventorySummary().merkle_radix_root,
        std::move(objects),
        std::move(definitions),
        std::move(scope_bytes),
        scope_digest,
        checkedSize(selected_ids.size()),
        std::move(manifest_bytes),
        manifest_digest,
        manifest_count);
}

}
