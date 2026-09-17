#include <Databases/UDT/PhysicalizationMutationPlanner.h>

#include <Databases/UDT/AtomicAuthority.h>

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <algorithm>
#include <deque>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = PhysicalizationMutationPlannerError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedSize(size_t value, std::string_view message)
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    static_cast<void>(message);
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

bool isDurableStorageObject(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary;
}

SchemaObjectID definitionObject(const DefinitionIdentity & identity)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = identity.database_uuid,
        .object_uuid = identity.type_uuid,
    };
}

AuthorityInventoryKey definitionInventoryKey(const DefinitionIdentity & identity)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
        .object_uuid = identity.type_uuid,
    };
}

AuthorityInventoryKey expectationInventoryKey(const SchemaObjectID & object)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = object.object_uuid,
    };
}

DatabaseSchemaWALAuthorityRecordState definitionRecordState(const Record & record, const Digest & canonical_record_hash)
{
    return {
        .object_revision = record.identity.revision,
        .canonical_record_hash = canonical_record_hash,
    };
}

DatabaseSchemaWALAuthorityRecordState expectationRecordState(const SidecarExpectationRecord & expectation)
{
    return {
        .object_revision = expectation.object_schema_revision,
        .canonical_record_hash = computeSidecarExpectationRecordHash(expectation),
    };
}

DatabaseSchemaWALStagedArtifact definitionArtifact(const Record & record, const String & canonical_record_bytes)
{
    return {
        .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
        .image = DatabaseSchemaWALStagedArtifactImage::Before,
        .object = definitionObject(record.identity),
        .revision = record.identity.revision,
        .canonical_bytes = canonical_record_bytes,
    };
}

void validateBasicRequest(
    const AuthorityRoot & root,
    const PhysicalizationPlan & plan,
    UInt64 transaction_id,
    UInt64 expected_epoch,
    std::span<const PhysicalizationRewriteImage> images,
    const PhysicalizationMutationPlannerLimits & limits)
{
    const UInt64 atomic_definition_limit = atomicDatabaseAuthorityCapabilities().limits.maximum_definition_bytes;
    if (!limits.maximum_definition_retained_bytes || limits.maximum_definition_retained_bytes > atomic_definition_limit)
        fail(Error::Code::InvalidConfiguration, "physicalization retained-definition limit is invalid");
    if (!transaction_id)
        fail(Error::Code::InvalidRequest, "physicalization transaction ID must be nonzero");
    if (root.getDatabaseCatalogEpoch() != expected_epoch)
        fail(Error::Code::ExpectedEpochMismatch, "physicalization expected database epoch is stale");
    if (root.getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "physicalization database epoch cannot advance");
    if (root.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(Error::Code::InvalidRequest, "physicalization requires the complete dependent-object-capable authority capability set");
    if (plan.getDatabaseUUID() != root.getDatabaseUUID())
        fail(Error::Code::DatabaseMismatch, "physicalization plan belongs to another database");
    if (plan.getDatabaseCatalogEpoch() != root.getDatabaseCatalogEpoch()
        || plan.getInventoryRoot() != root.getInventorySummary().merkle_radix_root)
        fail(Error::Code::StalePlan, "physicalization plan is not anchored to the current authority root");
    if (plan.getScopeBytes() != checkedSize(plan.getCanonicalScopeBytes().size(), "physicalization scope size does not fit UInt64")
        || plan.getManifestBytes()
            != checkedSize(plan.getCanonicalManifestBytes().size(), "physicalization manifest size does not fit UInt64")
        || hashFramedDomainSeparated(physicalization_scope_hash_domain, plan.getCanonicalScopeBytes()) != plan.getScopeDigest()
        || hashFramedDomainSeparated(physicalization_manifest_hash_domain, plan.getCanonicalManifestBytes()) != plan.getManifestDigest())
    {
        fail(Error::Code::InvalidPlan, "physicalization plan canonical bytes do not match their content addresses");
    }

    const auto objects = plan.getObjects();
    const auto definitions = plan.getDefinitions();
    const bool selects_definition_for_drop
        = std::any_of(definitions.begin(), definitions.end(), [](const auto & definition) { return definition.selected_for_drop; });
    const bool definition_only_database_drop = objects.empty() && plan.getSelector().scope == PhysicalizationScope::Database
        && plan.getSelector().drop_unused_types && selects_definition_for_drop;
    if (objects.empty() && !definition_only_database_drop)
        fail(Error::Code::InvalidRequest, "physicalization plan contains no durable object rewrite or definition removal");
    if (plan.getScopeCount() != checkedSize(objects.size(), "physicalization object count does not fit UInt64"))
        fail(Error::Code::InvalidPlan, "physicalization scope count differs from its selected objects");
    if (images.size() != objects.size())
        fail(Error::Code::InvalidRewriteImages, "physicalization rewrite images are missing or extra");
    if (objects.size() > limits.schema_wal.maximum_dependent_object_deltas)
        fail(Error::Code::LimitExceeded, "physicalization dependent-object count exceeds the WAL limit");

    const auto & selector = plan.getSelector();
    const bool known_scope = selector.scope == PhysicalizationScope::Object || selector.scope == PhysicalizationScope::DependentClosure
        || selector.scope == PhysicalizationScope::Database;
    if (!known_scope)
        fail(Error::Code::InvalidPlan, "physicalization selector scope is unknown");
    switch (selector.scope)
    {
        case PhysicalizationScope::Object:
            if (!selector.object || objects.size() != 1 || objects.front().object != *selector.object)
                fail(Error::Code::InvalidPlan, "physicalization object scope is inconsistent");
            break;
        case PhysicalizationScope::DependentClosure:
            if (!selector.object
                || std::none_of(objects.begin(), objects.end(), [&](const auto & object) { return object.object == *selector.object; }))
                fail(Error::Code::InvalidPlan, "physicalization dependent-closure seed is absent");
            break;
        case PhysicalizationScope::Database:
            if (selector.object || objects.size() != root.getExpectationRecordCount())
                fail(Error::Code::InvalidPlan, "physicalization database scope is incomplete");
            break;
    }

    UInt64 staged_metadata_bytes = 0;
    for (size_t index = 0; index < objects.size(); ++index)
    {
        const auto & object = objects[index];
        const auto & image = images[index];
        if (!object.object.isValid() || !hasPhysicalizationAdapter(object.object.kind)
            || object.object.database_uuid != root.getDatabaseUUID())
            fail(Error::Code::InvalidPlan, "physicalization plan contains an unsupported object identity");
        if (index && !(objects[index - 1].object < object.object))
            fail(Error::Code::InvalidPlan, "physicalization plan objects are not in strict canonical order");
        if (image.object != object.object || image.before_object_schema_revision != object.object_schema_revision)
            fail(Error::Code::InvalidRewriteImages, "physicalization rewrite image identity or before revision differs from the plan");
        if (object.object_schema_revision == std::numeric_limits<UInt64>::max()
            || image.after_object_schema_revision != object.object_schema_revision + 1)
            fail(Error::Code::InvalidRewriteImages, "physicalization rewrite must advance the object revision exactly once");
        if (image.before_physical_schema_fingerprint != object.physical_schema_fingerprint
            || image.after_physical_schema_fingerprint != object.physical_schema_fingerprint)
            fail(Error::Code::IntegrityMismatch, "physicalization rewrite changes the physical schema fingerprint");
        if (image.before_canonical_metadata_bytes.empty() || image.after_canonical_metadata_bytes.empty())
            fail(Error::Code::InvalidRewriteImages, "physicalization rewrite metadata image is empty");
        if (computeDatabaseSchemaWALStagedArtifactHash(
                DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, image.before_canonical_metadata_bytes)
            != object.canonical_metadata_hash)
            fail(Error::Code::StalePlan, "physicalization before metadata differs from the recomputed loss manifest");
        const UInt64 before_size
            = checkedSize(image.before_canonical_metadata_bytes.size(), "physicalization before metadata size does not fit UInt64");
        const UInt64 after_size
            = checkedSize(image.after_canonical_metadata_bytes.size(), "physicalization after metadata size does not fit UInt64");
        if (before_size > limits.schema_wal.maximum_staged_artifact_bytes || after_size > limits.schema_wal.maximum_staged_artifact_bytes)
            fail(Error::Code::LimitExceeded, "physicalization metadata image exceeds the WAL artifact limit");
        staged_metadata_bytes = checkedAdd(staged_metadata_bytes, before_size, "physicalization staged metadata bytes overflow");
        staged_metadata_bytes = checkedAdd(staged_metadata_bytes, after_size, "physicalization staged metadata bytes overflow");
        if (staged_metadata_bytes > limits.schema_wal.maximum_total_staged_artifact_bytes)
            fail(Error::Code::LimitExceeded, "physicalization staged metadata bytes exceed the WAL aggregate limit");
    }
}

struct ValidatedPlan
{
    std::set<SchemaObjectID> selected_objects;
    std::set<SchemaObjectID> validation_definitions;
    std::set<SchemaObjectID> dropped_definitions;
};

void validateSelectedObjectScope(
    const AuthorityRoot & root, const PhysicalizationPlan & plan, const std::set<SchemaObjectID> & selected_objects)
{
    const auto & graph = root.getSchemaObjectDependencyGraph();
    std::set<SchemaObjectID> expected_objects;
    std::deque<SchemaObjectID> pending;
    const auto & selector = plan.getSelector();

    if (selector.scope == PhysicalizationScope::Database)
    {
        for (const auto & expectation : root.getExpectationRecords())
            expected_objects.insert(expectation.object);
    }
    else
    {
        expected_objects.insert(*selector.object);
        if (selector.scope == PhysicalizationScope::DependentClosure)
            pending.push_back(*selector.object);
    }

    while (!pending.empty())
    {
        const auto dependency = pending.front();
        pending.pop_front();
        for (const auto & dependent : graph.getDependents(dependency))
        {
            if (dependent.kind != SchemaObjectDependencyEdgeKind::ObjectDependsOnObject)
                continue;
            if (!hasPhysicalizationAdapter(dependent.object.kind) || !root.findExpectationRecord(dependent.object))
                fail(Error::Code::StalePlan, "physicalization dependent closure is no longer adapter-complete");
            if (expected_objects.insert(dependent.object).second)
                pending.push_back(dependent.object);
        }
    }

    if (selector.scope == PhysicalizationScope::Object)
    {
        for (const auto & dependent : graph.getDependents(*selector.object))
        {
            if (dependent.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnObject && !expected_objects.contains(dependent.object))
                fail(Error::Code::StalePlan, "physicalization object scope gained an omitted dependent");
        }
    }
    if (expected_objects != selected_objects)
        fail(Error::Code::StalePlan, "physicalization selected-object scope changed after dry run");
}

ValidatedPlan
validatePlanAgainstRoot(const AuthorityRoot & root, const PhysicalizationPlan & plan, const PhysicalizationMutationPlannerLimits & limits)
{
    ValidatedPlan result;
    const auto & graph = root.getSchemaObjectDependencyGraph();

    std::deque<SchemaObjectID> pending_definitions;
    for (const auto & object : plan.getObjects())
    {
        const auto * expectation = root.findExpectationRecord(object.object);
        if (!expectation || expectation->object_schema_revision != object.object_schema_revision
            || expectation->sidecar_hash != object.sidecar_hash
            || expectation->physical_schema_fingerprint != object.physical_schema_fingerprint)
            fail(Error::Code::StalePlan, "physicalization object expectation changed after dry run");
        if (object.references.object != object.object || object.references.object_schema_revision != object.object_schema_revision
            || object.references.physical_schema_fingerprint != object.physical_schema_fingerprint)
            fail(Error::Code::InvalidPlan, "physicalization manifest sidecar identity is inconsistent");

        Digest sidecar_hash;
        try
        {
            sidecar_hash = computePersistedTypeReferencesSidecarHash(object.references, limits.schema_wal.persisted_references);
        }
        catch (const PersistedTypeReferencesError & error)
        {
            if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "physicalization sidecar exceeds the WAL codec limit");
            fail(Error::Code::InvalidPlan, "physicalization manifest sidecar is not canonical");
        }
        if (sidecar_hash != object.sidecar_hash)
            fail(Error::Code::IntegrityMismatch, "physicalization manifest sidecar differs from its expectation");
        if (object.selected_semantic_capabilities.size() != object.references.uses.size())
            fail(Error::Code::InvalidPlan, "physicalization manifest use capabilities are incomplete");
        result.selected_objects.insert(object.object);

        std::set<SchemaObjectID> expected_object_dependencies;
        for (const auto & descriptor : object.references.descriptors)
        {
            const auto & identity = descriptor.getDefinitionIdentity();
            const auto * record = root.findDefinitionRecord(identity);
            if (!record || record->definition_hash != descriptor.getDefinitionHash() || !root.findByIdentity(identity))
                fail(Error::Code::StalePlan, "physicalization descriptor no longer matches the active definition");
            const auto definition = definitionObject(identity);
            expected_object_dependencies.insert(definition);
            if (result.validation_definitions.insert(definition).second)
                pending_definitions.push_back(definition);
        }

        std::vector<SchemaObjectID> actual_object_dependencies;
        for (const auto & dependency : graph.getDependencies(object.object))
        {
            if (dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition)
                actual_object_dependencies.push_back(dependency.object);
        }
        if (!std::equal(
                actual_object_dependencies.begin(),
                actual_object_dependencies.end(),
                expected_object_dependencies.begin(),
                expected_object_dependencies.end()))
            fail(Error::Code::IntegrityMismatch, "physicalization object definition edges differ from its sidecar");
    }

    validateSelectedObjectScope(root, plan, result.selected_objects);

    /// DATABASE ... DROP UNUSED TYPES is the only selector that is allowed to
    /// collect definitions which are not reachable from a selected object.
    /// The dry run includes the complete active definition inventory so that
    /// definition-only databases and never-referenced definitions are part of
    /// the reviewed loss manifest. Reconstruct that exact validation universe
    /// here instead of accepting extra manifest entries on trust.
    if (plan.getSelector().scope == PhysicalizationScope::Database && plan.getSelector().drop_unused_types)
    {
        for (const auto & definition : root.getDefinitionRecords())
        {
            const SchemaObjectID object = definitionObject(definition.identity);
            if (result.validation_definitions.insert(object).second)
                pending_definitions.push_back(object);
        }
    }

    while (!pending_definitions.empty())
    {
        const SchemaObjectID definition = pending_definitions.front();
        pending_definitions.pop_front();
        for (const auto & dependency : graph.getDependencies(definition))
        {
            if (dependency.kind != SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition)
                continue;
            if (result.validation_definitions.insert(dependency.object).second)
                pending_definitions.push_back(dependency.object);
        }
    }

    const auto manifest_definitions = plan.getDefinitions();
    if (manifest_definitions.size() != result.validation_definitions.size())
        fail(Error::Code::InvalidPlan, "physicalization definition manifest is not the exact validation closure");
    std::set<SchemaObjectID> manifest_definition_objects;
    for (size_t index = 0; index < manifest_definitions.size(); ++index)
    {
        const auto & definition = manifest_definitions[index];
        if (index && !definitionIdentityLess(manifest_definitions[index - 1].identity, definition.identity))
            fail(Error::Code::InvalidPlan, "physicalization manifest definitions are not in strict canonical order");
        if (definition.identity.database_uuid != root.getDatabaseUUID() || definition.identity.type_uuid == UUIDHelpers::Nil
            || definition.identity.revision == 0)
            fail(Error::Code::InvalidPlan, "physicalization manifest definition identity is invalid");
        const SchemaObjectID object = definitionObject(definition.identity);
        if (!manifest_definition_objects.insert(object).second)
            fail(Error::Code::InvalidPlan, "physicalization manifest repeats a stable definition identity");
        const auto * record = root.findDefinitionRecord(definition.identity);
        auto checked_definition = root.findByIdentity(definition.identity);
        if (!record || !checked_definition || !recordMatchesCheckedDefinition(*record, *checked_definition))
            fail(Error::Code::StalePlan, "physicalization manifest definition is no longer active");
        if (!tryCountLogicalRetainedDefinitionBytes(*checked_definition, limits.maximum_definition_retained_bytes))
            fail(Error::Code::LimitExceeded, "physicalization definition exceeds the Atomic retained-definition byte limit");
        String canonical_record;
        try
        {
            canonical_record = encodeRecord(*record, limits.schema_wal.definition_record);
        }
        catch (const RecordError & error)
        {
            if (error.code == RecordError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "physicalization definition record exceeds the WAL codec limit");
            fail(Error::Code::IntegrityMismatch, "physicalization active definition record is not canonical");
        }
        if (definition.normalized_name != record->normalized_name || definition.definition_hash != record->definition_hash
            || definition.canonical_record_hash != computeRecordHash(*record, limits.schema_wal.definition_record)
            || definition.canonical_record_bytes != canonical_record)
            fail(Error::Code::StalePlan, "physicalization definition manifest differs from the authority root");
        if (definition.selected_for_drop)
        {
            if (!plan.getSelector().drop_unused_types)
                fail(Error::Code::InvalidPlan, "physicalization manifest selects a definition without DROP UNUSED");
            result.dropped_definitions.insert(object);
        }
    }
    if (manifest_definition_objects != result.validation_definitions)
        fail(Error::Code::InvalidPlan, "physicalization definition manifest differs from the graph closure");

    std::set<SchemaObjectID> blocked_definitions;
    std::deque<SchemaObjectID> blocked_pending;
    if (plan.getSelector().drop_unused_types)
    {
        for (const auto & definition : result.validation_definitions)
        {
            bool blocked = false;
            for (const auto & dependent : graph.getDependents(definition))
            {
                const bool selected_object_dependent = dependent.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition
                    && result.selected_objects.contains(dependent.object);
                const bool validation_definition_dependent = dependent.kind == SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition
                    && result.validation_definitions.contains(dependent.object);
                if (!selected_object_dependent && !validation_definition_dependent)
                {
                    blocked = true;
                    break;
                }
            }
            if (blocked && blocked_definitions.insert(definition).second)
                blocked_pending.push_back(definition);
        }
        while (!blocked_pending.empty())
        {
            const auto retained_definition = blocked_pending.front();
            blocked_pending.pop_front();
            for (const auto & dependency : graph.getDependencies(retained_definition))
            {
                if (dependency.kind != SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition
                    || dependency.object == retained_definition || !result.validation_definitions.contains(dependency.object))
                    continue;
                if (blocked_definitions.insert(dependency.object).second)
                    blocked_pending.push_back(dependency.object);
            }
        }
    }

    std::set<SchemaObjectID> expected_drops;
    if (plan.getSelector().drop_unused_types)
    {
        std::set_difference(
            result.validation_definitions.begin(),
            result.validation_definitions.end(),
            blocked_definitions.begin(),
            blocked_definitions.end(),
            std::inserter(expected_drops, expected_drops.end()));
    }
    for (const auto & dropped : result.dropped_definitions)
    {
        if (blocked_definitions.contains(dropped))
            fail(Error::Code::RemainingDependent, "physicalization DROP UNUSED definition gained a remaining dependent");
    }
    if (expected_drops != result.dropped_definitions)
        fail(Error::Code::InvalidPlan, "physicalization DROP UNUSED set is not the exact zero-reference closure");
    return result;
}

SchemaObjectDependencyGraphMutation makeGraphDelta(const SchemaObjectDependencyGraph & graph, const ValidatedPlan & plan)
{
    std::set<SchemaObjectDependencyEdge> removals;
    for (const auto & object : plan.selected_objects)
    {
        const bool remove_object_node = isDurableStorageObject(object.kind);
        for (const auto & dependency : graph.getDependencies(object))
        {
            if (remove_object_node || dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition)
            {
                removals.insert({
                    .dependent = object,
                    .dependency = dependency.object,
                    .kind = dependency.kind,
                });
            }
        }
        if (remove_object_node)
        {
            for (const auto & dependent : graph.getDependents(object))
            {
                removals.insert({
                    .dependent = dependent.object,
                    .dependency = object,
                    .kind = dependent.kind,
                });
            }
        }
    }

    for (const auto & definition : plan.dropped_definitions)
    {
        for (const auto & dependency : graph.getDependencies(definition))
        {
            removals.insert({
                .dependent = definition,
                .dependency = dependency.object,
                .kind = dependency.kind,
            });
        }
        for (const auto & dependent : graph.getDependents(definition))
        {
            const bool removed_object_edge = dependent.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition
                && plan.selected_objects.contains(dependent.object);
            const bool removed_definition_edge = dependent.kind == SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition
                && plan.dropped_definitions.contains(dependent.object);
            if (!removed_object_edge && !removed_definition_edge)
                fail(Error::Code::RemainingDependent, "physicalization cannot drop a definition with a remaining graph dependent");
            removals.insert({
                .dependent = dependent.object,
                .dependency = definition,
                .kind = dependent.kind,
            });
        }
    }

    SchemaObjectDependencyGraphMutation result;
    result.node_removals.assign(plan.dropped_definitions.begin(), plan.dropped_definitions.end());
    for (const auto & object : plan.selected_objects)
    {
        /// A physicalized production table has no remaining authority-owned
        /// metadata package, so retaining its graph node would make the durable
        /// graph claim an anchored table that no expectation can install.
        /// Synthetic objects deliberately keep their graph-only node so the
        /// generic graph semantics remain covered independently of table I/O.
        if (isDurableStorageObject(object.kind))
            result.node_removals.push_back(object);
    }
    std::sort(result.node_removals.begin(), result.node_removals.end());
    result.edge_removals.assign(removals.begin(), removals.end());
    return result;
}

UInt64 chargeArtifactBytes(UInt64 total, UInt64 artifact_bytes, const DatabaseSchemaWALLimits & limits)
{
    if (artifact_bytes > limits.maximum_staged_artifact_bytes)
        fail(Error::Code::LimitExceeded, "physicalization staged artifact exceeds its byte limit");
    total = checkedAdd(total, artifact_bytes, "physicalization aggregate staged-artifact bytes overflow");
    if (total > limits.maximum_total_staged_artifact_bytes)
        fail(Error::Code::LimitExceeded, "physicalization aggregate staged-artifact bytes exceed their limit");
    return total;
}

String canonicalTableInstallationBytes(
    const PhysicalizationManifestObject & object, const SidecarExpectationRecord & expectation, const DatabaseSchemaWALLimits & limits)
{
    if (!isDurableStorageObject(object.object.kind) || !expectation.installation_record_hash)
        fail(Error::Code::InvalidPlan, "physicalization stored-object manifest has no metadata installation record");

    DependentObjectMetadataInstallationRecord installation{
        .object = object.object,
        .object_schema_revision = object.object_schema_revision,
        .object_name = object.diagnostic_name,
        .metadata_artifact_hash = object.canonical_metadata_hash,
    };
    String bytes;
    try
    {
        bytes = encodeDependentObjectMetadataInstallationRecord(installation, limits.installation_record);
        if (computeDependentObjectMetadataInstallationRecordHash(installation, limits.installation_record)
            != *expectation.installation_record_hash)
            fail(Error::Code::IntegrityMismatch, "physicalization table metadata installation record differs from its expectation");
    }
    catch (const DependentObjectMetadataInstallationRecordError & error)
    {
        if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "physicalization table metadata installation record exceeds its limit");
        fail(Error::Code::InvalidPlan, "physicalization table metadata installation record is invalid");
    }
    return bytes;
}

void validateArtifactBudget(
    const AuthorityRoot & current_root,
    const PhysicalizationPlan & plan,
    std::span<const PhysicalizationRewriteImage> rewrite_images,
    UInt64 selected_definition_count,
    const DatabaseSchemaWALLimits & limits)
{
    const UInt64 object_count = checkedSize(plan.getObjects().size(), "physicalization selected-object count does not fit UInt64");
    const UInt64 definition_count = selected_definition_count;
    const UInt64 authority_delta_count = checkedAdd(object_count, definition_count, "physicalization authority delta count overflows");
    if (authority_delta_count > limits.maximum_authority_record_deltas)
        fail(Error::Code::LimitExceeded, "physicalization authority deltas exceed the WAL limit");
    const UInt64 durable_object_count = checkedSize(
        std::count_if(
            plan.getObjects().begin(),
            plan.getObjects().end(),
            [](const auto & object) { return isDurableStorageObject(object.object.kind); }),
        "physicalization durable-object count does not fit UInt64");
    const UInt64 artifact_count = checkedAdd(
        checkedAdd(
            checkedMultiply(object_count, 4, "physicalization staged-artifact count overflows"),
            durable_object_count,
            "physicalization staged-artifact count overflows"),
        definition_count,
        "physicalization staged-artifact count overflows");
    if (artifact_count > limits.maximum_staged_artifacts)
        fail(Error::Code::LimitExceeded, "physicalization staged-artifact count exceeds the WAL limit");

    UInt64 total_bytes = 0;
    const auto objects = plan.getObjects();
    for (size_t index = 0; index < objects.size(); ++index)
    {
        total_bytes = chargeArtifactBytes(
            total_bytes,
            checkedSize(
                rewrite_images[index].before_canonical_metadata_bytes.size(), "physicalization before metadata size does not fit UInt64"),
            limits);
        total_bytes = chargeArtifactBytes(
            total_bytes,
            checkedSize(
                rewrite_images[index].after_canonical_metadata_bytes.size(), "physicalization after metadata size does not fit UInt64"),
            limits);

        UInt64 sidecar_bytes = 0;
        try
        {
            sidecar_bytes = checkedSize(
                encodePersistedTypeReferences(objects[index].references, limits.persisted_references).size(),
                "physicalization sidecar size does not fit UInt64");
        }
        catch (const PersistedTypeReferencesError & error)
        {
            if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "physicalization sidecar exceeds the WAL codec limit");
            fail(Error::Code::InvalidPlan, "physicalization sidecar is not canonical");
        }
        total_bytes = chargeArtifactBytes(total_bytes, sidecar_bytes, limits);
        const auto * expectation = current_root.findExpectationRecord(objects[index].object);
        if (!expectation)
            fail(Error::Code::StalePlan, "physicalization expectation disappeared during artifact preflight");
        total_bytes = chargeArtifactBytes(
            total_bytes,
            checkedSize(encodeSidecarExpectationRecord(*expectation).size(), "physicalization expectation size does not fit UInt64"),
            limits);
        if (isDurableStorageObject(objects[index].object.kind))
        {
            total_bytes = chargeArtifactBytes(
                total_bytes,
                checkedSize(
                    canonicalTableInstallationBytes(objects[index], *expectation, limits).size(),
                    "physicalization table metadata installation size does not fit UInt64"),
                limits);
        }
    }

    UInt64 counted_definitions = 0;
    for (const auto & definition : plan.getDefinitions())
    {
        if (!definition.selected_for_drop)
            continue;
        total_bytes = chargeArtifactBytes(
            total_bytes,
            checkedSize(definition.canonical_record_bytes.size(), "physicalization definition artifact size does not fit UInt64"),
            limits);
        ++counted_definitions;
    }
    if (counted_definitions != selected_definition_count)
        fail(Error::Code::InvalidPlan, "physicalization artifact preflight definition count is inconsistent");
}

}

PhysicalizationMutationPlannerError::PhysicalizationMutationPlannerError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

PhysicalizationMutationPlannerLimits::PhysicalizationMutationPlannerLimits()
{
    const auto atomic_limits = atomicDatabaseAuthorityCapabilities().limits;
    authority_root.type_catalog.maximum_definitions = atomic_limits.maximum_definitions;
    authority_root.maximum_definition_records = atomic_limits.maximum_definitions;
    maximum_definition_retained_bytes = atomic_limits.maximum_definition_bytes;
}

PreparedPhysicalizationMutation::PreparedPhysicalizationMutation(
    AuthorityRoot::Ptr replacement_root_, DatabaseSchemaWALValidatedTransition transition_)
    : replacement_root(std::move(replacement_root_))
    , transition(std::move(transition_))
{
}

PreparedPhysicalizationMutation PhysicalizationMutationPlanner::plan(
    const AuthorityRoot & current_root,
    const PhysicalizationPlan & freshly_recomputed_plan,
    UInt64 transaction_id,
    UInt64 expected_database_catalog_epoch,
    std::span<const PhysicalizationRewriteImage> rewrite_images,
    const PhysicalizationMutationPlannerLimits & limits)
{
    validateBasicRequest(current_root, freshly_recomputed_plan, transaction_id, expected_database_catalog_epoch, rewrite_images, limits);
    const ValidatedPlan validated_plan = validatePlanAgainstRoot(current_root, freshly_recomputed_plan, limits);
    validateArtifactBudget(
        current_root,
        freshly_recomputed_plan,
        rewrite_images,
        checkedSize(validated_plan.dropped_definitions.size(), "physicalization dropped-definition count does not fit UInt64"),
        limits.schema_wal);

    const auto graph_delta = makeGraphDelta(current_root.getSchemaObjectDependencyGraph(), validated_plan);
    std::vector<DefinitionIdentity> definition_removals;
    definition_removals.reserve(validated_plan.dropped_definitions.size());
    for (const auto & definition : freshly_recomputed_plan.getDefinitions())
        if (definition.selected_for_drop)
            definition_removals.push_back(definition.identity);
    std::vector<SchemaObjectID> expectation_removals(validated_plan.selected_objects.begin(), validated_plan.selected_objects.end());

    AuthorityRoot::Ptr replacement_root;
    try
    {
        replacement_root = AuthorityRootBuilder::buildPhysicalizationDelta(
            current_root,
            current_root.getDatabaseCatalogEpoch() + 1,
            definition_removals,
            expectation_removals,
            graph_delta,
            limits.authority_root);
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "physicalization replacement root is invalid");
    }

    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas;
    authority_deltas.reserve(validated_plan.selected_objects.size() + validated_plan.dropped_definitions.size());
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_deltas;
    dependent_deltas.reserve(validated_plan.selected_objects.size());
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts;
    staged_artifacts.reserve(validated_plan.selected_objects.size() * 5 + validated_plan.dropped_definitions.size());

    const auto plan_objects = freshly_recomputed_plan.getObjects();
    for (size_t index = 0; index < plan_objects.size(); ++index)
    {
        const auto & object = plan_objects[index];
        const auto & image = rewrite_images[index];
        const auto * expectation = current_root.findExpectationRecord(object.object);
        if (!expectation)
            fail(Error::Code::StalePlan, "physicalization expectation disappeared during preparation");

        String sidecar_bytes;
        try
        {
            sidecar_bytes = encodePersistedTypeReferences(object.references, limits.schema_wal.persisted_references);
        }
        catch (const PersistedTypeReferencesError & error)
        {
            if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "physicalization sidecar exceeds the WAL codec limit");
            fail(Error::Code::InvalidPlan, "physicalization sidecar is not canonical");
        }
        const String expectation_bytes = encodeSidecarExpectationRecord(*expectation);
        const Digest expectation_hash = computeSidecarExpectationRecordHash(*expectation);

        authority_deltas.push_back({
            .key = expectationInventoryKey(object.object),
            .before = expectationRecordState(*expectation),
            .after = std::nullopt,
        });
        dependent_deltas.push_back({
            .object = object.object,
            .before = DatabaseSchemaWALDependentObjectState{
                .object_schema_revision = image.before_object_schema_revision,
                .metadata_hash = object.canonical_metadata_hash,
                .sidecar_record_hash = object.sidecar_hash,
                .expectation_record_hash = expectation_hash,
            },
            .after = DatabaseSchemaWALDependentObjectState{
                .object_schema_revision = image.after_object_schema_revision,
                .metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
                    DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, image.after_canonical_metadata_bytes),
                .sidecar_record_hash = std::nullopt,
                .expectation_record_hash = std::nullopt,
            },
        });
        staged_artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = object.object,
            .revision = image.before_object_schema_revision,
            .canonical_bytes = image.before_canonical_metadata_bytes,
        });
        staged_artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = object.object,
            .revision = image.after_object_schema_revision,
            .canonical_bytes = image.after_canonical_metadata_bytes,
        });
        staged_artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = object.object,
            .revision = image.before_object_schema_revision,
            .canonical_bytes = std::move(sidecar_bytes),
        });
        staged_artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = object.object,
            .revision = image.before_object_schema_revision,
            .canonical_bytes = expectation_bytes,
        });
        if (isDurableStorageObject(object.object.kind))
        {
            staged_artifacts.push_back({
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::Before,
                .object = object.object,
                .revision = image.before_object_schema_revision,
                .canonical_bytes = canonicalTableInstallationBytes(object, *expectation, limits.schema_wal),
            });
        }
    }

    for (const auto & definition : freshly_recomputed_plan.getDefinitions())
    {
        if (!definition.selected_for_drop)
            continue;
        const auto * record = current_root.findDefinitionRecord(definition.identity);
        if (!record)
            fail(Error::Code::StalePlan, "physicalization dropped definition disappeared during preparation");
        authority_deltas.push_back({
            .key = definitionInventoryKey(record->identity),
            .before = definitionRecordState(*record, definition.canonical_record_hash),
            .after = std::nullopt,
        });
        staged_artifacts.push_back(definitionArtifact(*record, definition.canonical_record_bytes));
    }

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::buildPhysicalization(
                transaction_id,
                current_root,
                *replacement_root,
                std::move(authority_deltas),
                std::move(dependent_deltas),
                graph_delta,
                std::move(staged_artifacts),
                limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "physicalization WAL transition exceeds its limit");
            fail(Error::Code::InvalidTransition, "physicalization WAL transition is invalid");
        }
    }();

    if (replacement_root->getTypeIndexGeneration()
        != current_root.getTypeIndexGeneration() + (validated_plan.dropped_definitions.empty() ? 0 : 1))
        fail(Error::Code::InvalidTransition, "physicalization replacement type-index generation is inconsistent");
    if (validated_plan.dropped_definitions.empty()
        && replacement_root->getTypeIndexContentDigest() != current_root.getTypeIndexContentDigest())
        fail(Error::Code::InvalidTransition, "physicalization without definition drops changed the type index");

    return PreparedPhysicalizationMutation(std::move(replacement_root), std::move(transition));
}

}
