#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/tests/UDTTestResourceImages.h>

#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <optional>
#include <string>
#include <utility>

namespace DB::UDT
{
namespace
{

UUID uuid(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Digest digest(UInt8 first)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(first + index);
    return result;
}

String toHex(std::span<const CanonicalByte> bytes)
{
    constexpr std::array<char, 16> alphabet{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    String result;
    result.reserve(bytes.size() * 2);
    for (const auto byte : bytes)
    {
        result.push_back(alphabet[byte >> 4]);
        result.push_back(alphabet[byte & 0x0f]);
    }
    return result;
}

SchemaObjectID objectID(SchemaObjectKind kind, UUID database_uuid, UUID object_uuid)
{
    return {.kind = kind, .database_uuid = database_uuid, .object_uuid = object_uuid};
}

Record definitionRecord(
    UUID database_uuid, UUID type_uuid, UInt64 revision, String qualified_name, String local_name, String built_in, String comment)
{
    Record result;
    result.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = revision};
    result.normalized_name = std::move(qualified_name);
    result.normalized_local_name = std::move(local_name);
    result.policy_semantic_hash = CheckerProof::empty_policy_semantic_hash;
    result.canonical_definition_sql = "CREATE TYPE " + result.normalized_name + " AS " + built_in;
    result.canonical_physical_template_sql = built_in;
    result.canonical_template_ir = "ir:" + built_in;
    result.semantic_definition_digest = hashFramedDomainSeparated("test semantic definition", built_in);
    result.definition_hash = hashFramedDomainSeparated("test definition", built_in);
    result.compositional_dependency_closure_digest = hashFramedDomainSeparated("test dependency closure", built_in);
    result.encoded_checker_certificate = "certificate:" + built_in;
    result.checker_certificate_digest = hashDomainSeparated(CheckerProof::checker_proof_domain, result.encoded_checker_certificate);
    result.charged_work = 1;
    result.logical_node_count = 1;
    result.maximum_template_depth = 0;
    result.owner_uuid = uuid(0x9000, 1);
    result.owner_display_name = "owner";
    result.comment = std::move(comment);
    result.creation_time_us_utc = 100;
    static_cast<void>(encodeRecord(result));
    return result;
}

AuthorityInventoryLeaf definitionLeaf(const Record & record)
{
    return {
        .key = {
            .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
            .object_uuid = record.identity.type_uuid,
        },
        .object_revision = record.identity.revision,
        .canonical_record_hash = computeRecordHash(record),
    };
}

AuthorityInventoryLeaf expectationLeaf(const SidecarExpectationRecord & record)
{
    return {
        .key = {
            .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
            .object_uuid = record.object.object_uuid,
        },
        .object_revision = record.object_schema_revision,
        .canonical_record_hash = computeSidecarExpectationRecordHash(record),
    };
}

AuthorityInventory::Ptr inventory(std::vector<AuthorityInventoryLeaf> leaves)
{
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    const auto summary = buildAuthorityInventorySummary(leaves);
    return AuthorityInventory::create(summary, std::move(leaves));
}

AuthorityState state(
    UUID database_uuid,
    UInt64 epoch,
    UInt64 capabilities,
    const AuthorityInventory::Ptr & authority_inventory,
    const SchemaObjectDependencyGraph::Ptr & graph)
{
    return makeAuthorityState(
        database_uuid,
        epoch,
        capabilities,
        authority_inventory->getSummary().leaf_count,
        authority_inventory->getSummary().merkle_radix_root,
        graph->computeRoot());
}

String noArguments()
{
    constexpr std::array<char, 3> bytes{1, 0, 0};
    return {bytes.data(), bytes.size()};
}

PersistedTypeDescriptor descriptor(const Record & definition)
{
    const String canonical_arguments = noArguments();
    const String canonical_physical_type = "UInt64";
    const Digest storage_fingerprint = digest(0x40);
    const Digest instantiation_hash = computeInstantiationSemanticHash({
        .definition_identity = definition.identity,
        .definition_hash = definition.definition_hash,
        .canonical_arguments_encoding = canonical_arguments,
        .canonical_physical_type = canonical_physical_type,
        .storage_fingerprint = storage_fingerprint,
        .checker_abi = definition.checker_abi,
        .checker_charge_abi = definition.checker_charge_abi,
        .policy_abi = definition.policy_abi,
        .function_registry_abi = definition.function_registry_abi,
        .policy_semantic_hash = definition.policy_semantic_hash,
        .semantic_capabilities = definition.semantic_capabilities,
    });
    return PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        definition.identity,
        definition.definition_hash,
        canonical_arguments,
        canonical_physical_type,
        instantiation_hash,
        storage_fingerprint,
        definition.checker_abi,
        definition.checker_charge_abi,
        definition.policy_abi,
        definition.function_registry_abi,
        definition.policy_semantic_hash,
        definition.semantic_capabilities,
        definition.normalized_name);
}

PersistedTypeReferences
references(const SchemaObjectID & object, UInt64 revision, const Record & definition, Digest physical_fingerprint)
{
    PersistedTypePathSection path_section;
    switch (object.kind)
    {
        case SchemaObjectKind::Table: path_section = PersistedTypePathSection::ColumnType; break;
        case SchemaObjectKind::View: path_section = PersistedTypePathSection::ViewExpression; break;
        case SchemaObjectKind::Dictionary: path_section = PersistedTypePathSection::DictionaryAttribute; break;
        case SchemaObjectKind::SyntheticTestObject: path_section = PersistedTypePathSection::SyntheticPayload; break;
        case SchemaObjectKind::TypeDefinition: throw std::logic_error("type definition cannot own a persisted references sidecar");
    }

    PersistedTypeReferences result;
    result.object = object;
    result.object_schema_revision = revision;
    result.physical_schema_fingerprint = physical_fingerprint;
    result.descriptors = {descriptor(definition)};
    result.occurrence_paths = {{
        .section = path_section,
        .object_ordinal = 0,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    }};
    result.uses = {{.path_id = 0, .descriptor_id = 0}};
    return result;
}

template <typename Function>
void expectError(DatabaseSchemaWALError::Code code, Function && function)
{
    try
    {
        function();
        FAIL() << "expected DatabaseSchemaWALError";
    }
    catch (const DatabaseSchemaWALError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

std::vector<String> copyArtifactBytes(const DatabaseSchemaWALValidatedTransition & transition)
{
    return {transition.getStagedArtifactBytes().begin(), transition.getStagedArtifactBytes().end()};
}

std::pair<UInt64, UInt64> artifactBudget(std::span<const String> bytes)
{
    UInt64 maximum = 0;
    UInt64 total = 0;
    for (const auto & value : bytes)
    {
        const auto size = static_cast<UInt64>(value.size());
        maximum = std::max(maximum, size);
        total += size;
    }
    return {maximum, total};
}

struct Fixture
{
    UUID database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL);
    UUID type_uuid = uuid(0x1021324354657687ULL, 0x98a9bacbdcedfe01ULL);
    UUID dependent_uuid = uuid(0xfedcba9876543210ULL, 0x0123456789abcdefULL);
    SchemaObjectID type_object = objectID(SchemaObjectKind::TypeDefinition, database_uuid, type_uuid);
    SchemaObjectID dependent_object = objectID(SchemaObjectKind::SyntheticTestObject, database_uuid, dependent_uuid);
    Record definition = definitionRecord(database_uuid, type_uuid, 7, "db.Alpha", "Alpha", "UInt64", "initial");
    String definition_bytes = encodeRecord(definition);
    AuthorityInventoryLeaf definition_leaf = definitionLeaf(definition);
    AuthorityInventory::Ptr empty_inventory = inventory({});
    AuthorityInventory::Ptr definition_inventory = inventory({definition_leaf});
    SchemaObjectDependencyGraph::Ptr empty_graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    SchemaObjectDependencyGraph::Ptr definition_graph = SchemaObjectDependencyGraph::build(database_uuid, {&type_object, 1}, {});
    AuthorityState definition_only_state = state(database_uuid, 1, definition_authority_capability_mask, definition_inventory, definition_graph);
    AuthorityState dependent_object_state = activateDependentObjectAuthority(definition_only_state);

    DatabaseSchemaWALTransitionBase firstBase() const
    {
        return {.authority_state = std::nullopt, .authority_inventory = empty_inventory, .schema_graph = empty_graph};
    }

    DatabaseSchemaWALTransitionBase definition_onlyBase() const
    {
        return {.authority_state = definition_only_state, .authority_inventory = definition_inventory, .schema_graph = definition_graph};
    }

    DatabaseSchemaWALTransitionBase dependent_objectBase() const
    {
        return {.authority_state = dependent_object_state, .authority_inventory = definition_inventory, .schema_graph = definition_graph};
    }

    DatabaseSchemaWALValidatedTransition firstTransition() const
    {
        return DatabaseSchemaWALTransitionBuilder::build(
            100,
            firstBase(),
            definition_only_state,
            {DatabaseSchemaWALAuthorityRecordDelta{
                .key = definition_leaf.key,
                .before = std::nullopt,
                .after = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = definition_leaf.object_revision,
                    .canonical_record_hash = definition_leaf.canonical_record_hash,
                },
            }},
            {},
            SchemaObjectDependencyGraphMutation{
                .node_additions = {type_object},
                .node_removals = {},
                .edge_additions = {},
                .edge_removals = {},
            },
            {DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = type_object,
                .revision = definition.identity.revision,
                .canonical_bytes = definition_bytes,
            }});
    }

    DatabaseSchemaWALValidatedTransition activationTransition() const
    {
        return DatabaseSchemaWALTransitionBuilder::build(101, definition_onlyBase(), dependent_object_state, {}, {}, {}, {});
    }
};

struct ObjectTransitionCase
{
    DatabaseSchemaWALValidatedTransition transition;
    DatabaseSchemaWALTransitionBase base;
    AuthorityInventory::Ptr after_inventory;
    SchemaObjectDependencyGraph::Ptr after_graph;
};

enum class DependentImageKind : UInt8
{
    Absent,
    PhysicalOnly,
    Logical,
};

struct DependentImageData
{
    std::optional<DatabaseSchemaWALDependentObjectState> state;
    std::optional<AuthorityInventoryLeaf> expectation_leaf;
    std::vector<DatabaseSchemaWALStagedArtifact> artifacts;
};

ObjectTransitionCase objectTransition(
    const Fixture & fixture,
    const DatabaseSchemaWALLimits & limits = {},
    bool include_installation_record = false,
    bool installation_metadata_mismatch = false,
    DependentImageKind before_image = DependentImageKind::PhysicalOnly,
    DependentImageKind after_image = DependentImageKind::Logical,
    String before_object_name = "events",
    String after_object_name = "events",
    bool retain_physical_after_graph_node = false)
{
    constexpr UInt64 before_revision = 8;
    constexpr UInt64 after_revision = 9;
    const String before_metadata = "metadata-before";
    const String after_metadata = "metadata-after";
    const Digest physical_fingerprint = digest(0xc0);

    const auto make_image = [&](DependentImageKind image_kind,
                                DatabaseSchemaWALStagedArtifactImage image,
                                UInt64 revision,
                                const String & metadata,
                                const String & object_name)
    {
        DependentImageData result;
        if (image_kind == DependentImageKind::Absent)
            return result;

        const Digest metadata_hash
            = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata);
        result.state = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = revision,
            .metadata_hash = metadata_hash,
            .sidecar_record_hash = std::nullopt,
            .expectation_record_hash = std::nullopt,
        };
        result.artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = image,
            .object = fixture.dependent_object,
            .revision = revision,
            .canonical_bytes = metadata,
        });
        if (image_kind == DependentImageKind::PhysicalOnly)
            return result;

        const auto sidecar = references(fixture.dependent_object, revision, fixture.definition, physical_fingerprint);
        const String sidecar_bytes = encodePersistedTypeReferences(sidecar);
        const Digest sidecar_hash = computePersistedTypeReferencesSidecarHash(sidecar);
        SidecarExpectationRecord expectation{
            .object = fixture.dependent_object,
            .object_schema_revision = revision,
            .sidecar_hash = sidecar_hash,
            .physical_schema_fingerprint = physical_fingerprint,
        };
        std::optional<DependentObjectMetadataInstallationRecord> installation_record;
        if (include_installation_record)
        {
            installation_record = DependentObjectMetadataInstallationRecord{
                .object = fixture.dependent_object,
                .object_schema_revision = revision,
                .object_name = object_name,
                .metadata_artifact_hash
                = installation_metadata_mismatch && image == DatabaseSchemaWALStagedArtifactImage::After ? digest(0xd0) : metadata_hash,
            };
            expectation.installation_record_hash
                = computeDependentObjectMetadataInstallationRecordHash(*installation_record, limits.installation_record);
        }
        const String expectation_bytes = encodeSidecarExpectationRecord(expectation);
        result.expectation_leaf = expectationLeaf(expectation);
        result.state->sidecar_record_hash = sidecar_hash;
        result.state->expectation_record_hash = result.expectation_leaf->canonical_record_hash;
        result.artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = image,
            .object = fixture.dependent_object,
            .revision = revision,
            .canonical_bytes = sidecar_bytes,
        });
        result.artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = image,
            .object = fixture.dependent_object,
            .revision = revision,
            .canonical_bytes = expectation_bytes,
        });
        if (installation_record)
        {
            result.artifacts.push_back({
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
                .image = image,
                .object = fixture.dependent_object,
                .revision = revision,
                .canonical_bytes = encodeDependentObjectMetadataInstallationRecord(*installation_record, limits.installation_record),
            });
        }
        return result;
    };

    auto before
        = make_image(before_image, DatabaseSchemaWALStagedArtifactImage::Before, before_revision, before_metadata, before_object_name);
    auto after = make_image(after_image, DatabaseSchemaWALStagedArtifactImage::After, after_revision, after_metadata, after_object_name);

    const auto image_inventory = [&](const DependentImageData & image)
    {
        std::vector<AuthorityInventoryLeaf> leaves{fixture.definition_leaf};
        if (image.expectation_leaf)
            leaves.push_back(*image.expectation_leaf);
        return inventory(std::move(leaves));
    };
    auto before_inventory = image_inventory(before);
    auto after_inventory = image_inventory(after);

    const SchemaObjectDependencyEdge dependency_edge{
        .dependent = fixture.dependent_object,
        .dependency = fixture.type_object,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const SchemaObjectDependencyGraphMutation graph_addition{
        .node_additions = {fixture.dependent_object},
        .node_removals = {},
        .edge_additions = {dependency_edge},
        .edge_removals = {},
    };
    const auto mapped_graph = SchemaObjectDependencyGraph::applyMutation(fixture.definition_graph, graph_addition);
    const bool before_is_mapped = before.expectation_leaf.has_value();
    const bool after_is_mapped = after.expectation_leaf.has_value();
    auto before_graph = before_is_mapped ? mapped_graph : fixture.definition_graph;
    auto after_graph = after_is_mapped || retain_physical_after_graph_node ? mapped_graph : fixture.definition_graph;
    SchemaObjectDependencyGraphMutation graph_delta;
    if (!before_is_mapped && after_is_mapped)
    {
        graph_delta = graph_addition;
    }
    else if (before_is_mapped && !after_is_mapped)
    {
        if (!retain_physical_after_graph_node)
        {
            graph_delta = {
                .node_additions = {},
                .node_removals = {fixture.dependent_object},
                .edge_additions = {},
                .edge_removals = {dependency_edge},
            };
        }
    }

    const auto before_state = state(
        fixture.database_uuid,
        fixture.dependent_object_state.database_catalog_epoch,
        dependent_object_authority_capability_mask,
        before_inventory,
        before_graph);
    auto after_state = state(
        fixture.database_uuid, before_state.database_catalog_epoch + 1, dependent_object_authority_capability_mask, after_inventory, after_graph);
    auto base = fixture.dependent_objectBase();
    if (before_is_mapped)
    {
        base = {
            .authority_state = before_state,
            .authority_inventory = before_inventory,
            .schema_graph = before_graph,
        };
    }

    const auto authority_state_from_leaf
        = [](const std::optional<AuthorityInventoryLeaf> & leaf) -> std::optional<DatabaseSchemaWALAuthorityRecordState>
    {
        if (!leaf)
            return std::nullopt;
        return DatabaseSchemaWALAuthorityRecordState{
            .object_revision = leaf->object_revision,
            .canonical_record_hash = leaf->canonical_record_hash,
        };
    };
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas;
    if (before.expectation_leaf || after.expectation_leaf)
    {
        authority_deltas.push_back({
            .key = before.expectation_leaf ? before.expectation_leaf->key : after.expectation_leaf->key,
            .before = authority_state_from_leaf(before.expectation_leaf),
            .after = authority_state_from_leaf(after.expectation_leaf),
        });
    }

    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts;
    staged_artifacts.reserve(before.artifacts.size() + after.artifacts.size());
    for (auto & artifact : before.artifacts)
        staged_artifacts.push_back(std::move(artifact));
    for (auto & artifact : after.artifacts)
        staged_artifacts.push_back(std::move(artifact));

    auto transition = DatabaseSchemaWALTransitionBuilder::build(
        102,
        base,
        after_state,
        std::move(authority_deltas),
        {DatabaseSchemaWALDependentObjectDelta{
            .object = fixture.dependent_object,
            .before = before.state,
            .after = after.state,
        }},
        graph_delta,
        std::move(staged_artifacts),
        limits);
    return {
        .transition = std::move(transition),
        .base = std::move(base),
        .after_inventory = std::move(after_inventory),
        .after_graph = std::move(after_graph),
    };
}

DefinitionInput physicalizationDefinitionInput(UUID database_uuid, UUID type_uuid, String qualified_name, String local_name)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 1};
    input.normalized_name = std::move(qualified_name);
    input.normalized_local_name = std::move(local_name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return input;
}

Record physicalizationDefinitionRecord(const Definition & definition, UInt8 tag)
{
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = "CREATE TYPE " + definition.getNormalizedName() + " AS UInt64",
            .canonical_physical_template_sql = "UInt64",
            .owner_uuid = uuid(0x9000, tag),
            .owner_display_name = "owner",
            .comment = "physicalization-" + std::to_string(tag),
            .creation_time_us_utc = tag,
        });
}

struct PhysicalizationTransitionCase
{
    AuthorityRoot::Ptr before_root;
    AuthorityRoot::Ptr after_root;
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_record_deltas;
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_object_deltas;
    SchemaObjectDependencyGraphMutation graph_delta;
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts;
};

PhysicalizationTransitionCase physicalizationTransitionCase()
{
    const UUID database_uuid = uuid(0x7000, 1);
    auto definitions = TemplateChecker::checkAll({
        physicalizationDefinitionInput(database_uuid, uuid(0x7100, 1), "db.Alpha", "Alpha"),
        physicalizationDefinitionInput(database_uuid, uuid(0x7200, 1), "db.Beta", "Beta"),
    });

    std::vector<Record> records;
    records.reserve(definitions.size());
    for (size_t index = 0; index < definitions.size(); ++index)
        records.push_back(physicalizationDefinitionRecord(*definitions[index], static_cast<UInt8>(index + 1)));

    std::vector<SchemaObjectID> nodes;
    std::vector<DefinitionIdentity> definition_removals;
    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_record_deltas;
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts;
    std::vector<AuthorityInventoryLeaf> leaves;
    nodes.reserve(records.size());
    definition_removals.reserve(records.size());
    authority_record_deltas.reserve(records.size());
    staged_artifacts.reserve(records.size());
    leaves.reserve(records.size());
    for (const auto & record : records)
    {
        const SchemaObjectID node{
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = database_uuid,
            .object_uuid = record.identity.type_uuid,
        };
        const auto leaf = definitionLeaf(record);
        nodes.push_back(node);
        definition_removals.push_back(record.identity);
        authority_record_deltas.push_back({
            .key = leaf.key,
            .before = DatabaseSchemaWALAuthorityRecordState{
                .object_revision = leaf.object_revision,
                .canonical_record_hash = leaf.canonical_record_hash,
            },
            .after = std::nullopt,
        });
        staged_artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = node,
            .revision = record.identity.revision,
            .canonical_bytes = encodeRecord(record),
        });
        leaves.push_back(leaf);
    }
    std::sort(nodes.begin(), nodes.end());
    std::sort(
        definition_removals.begin(),
        definition_removals.end(),
        [](const DefinitionIdentity & lhs, const DefinitionIdentity & rhs)
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
        });

    auto graph = SchemaObjectDependencyGraph::build(database_uuid, nodes, {});
    auto authority_inventory = inventory(std::move(leaves));
    auto before_state = state(database_uuid, 20, dependent_object_authority_capability_mask, authority_inventory, graph);
    const std::vector<SidecarExpectationRecord> expectations;
    auto before_root = AuthorityRootBuilder::build(before_state, 1, definitions, records, expectations, graph);
    SchemaObjectDependencyGraphMutation graph_delta{
        .node_additions = {},
        .node_removals = nodes,
        .edge_additions = {},
        .edge_removals = {},
    };
    auto after_root
        = AuthorityRootBuilder::buildPhysicalizationDelta(*before_root, 21, definition_removals, {}, graph_delta);
    return {
        .before_root = std::move(before_root),
        .after_root = std::move(after_root),
        .authority_record_deltas = std::move(authority_record_deltas),
        .dependent_object_deltas = {},
        .graph_delta = std::move(graph_delta),
        .staged_artifacts = std::move(staged_artifacts),
    };
}

PhysicalizationTransitionCase tablePhysicalizationTransitionCase(bool retain_physical_graph_node)
{
    constexpr UInt64 before_revision = 8;
    constexpr UInt64 after_revision = 9;
    const UUID database_uuid = uuid(0x7300, 1);
    const SchemaObjectID table = objectID(SchemaObjectKind::Table, database_uuid, uuid(0x7400, 1));
    auto definitions = TemplateChecker::checkAll({
        physicalizationDefinitionInput(database_uuid, uuid(0x7500, 1), "db.Alpha", "Alpha"),
    });
    std::vector<Record> records{physicalizationDefinitionRecord(*definitions.front(), 1)};
    const SchemaObjectID definition
        = objectID(SchemaObjectKind::TypeDefinition, database_uuid, records.front().identity.type_uuid);

    const String before_metadata = "CREATE TABLE db.events (id UInt64) ENGINE = Memory /* mapped */";
    const String after_metadata = "CREATE TABLE db.events (id UInt64) ENGINE = Memory";
    const Digest before_metadata_hash
        = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, before_metadata);
    const Digest after_metadata_hash
        = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, after_metadata);
    const Digest physical_fingerprint = digest(0xd0);
    const auto sidecar = references(table, before_revision, records.front(), physical_fingerprint);
    const String sidecar_bytes = encodePersistedTypeReferences(sidecar);
    const Digest sidecar_hash = computePersistedTypeReferencesSidecarHash(sidecar);
    const DependentObjectMetadataInstallationRecord installation{
        .object = table,
        .object_schema_revision = before_revision,
        .object_name = "events",
        .metadata_artifact_hash = before_metadata_hash,
    };
    const SidecarExpectationRecord expectation{
        .object = table,
        .object_schema_revision = before_revision,
        .sidecar_hash = sidecar_hash,
        .physical_schema_fingerprint = physical_fingerprint,
        .installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installation),
    };
    std::vector<SidecarExpectationRecord> expectations{expectation};
    const auto expectation_leaf = expectationLeaf(expectation);
    auto before_inventory = inventory({definitionLeaf(records.front()), expectation_leaf});
    const SchemaObjectDependencyEdge definition_edge{
        .dependent = table,
        .dependency = definition,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const std::array graph_nodes{definition, table};
    const std::array graph_edges{definition_edge};
    auto before_graph = SchemaObjectDependencyGraph::build(database_uuid, graph_nodes, graph_edges);
    const auto before_state = state(
        database_uuid, 20, dependent_object_authority_capability_mask, before_inventory, before_graph);
    const Test::DependentObjectResourceImageBatch dependent_objects(
        expectations,
        {{
            .canonical_metadata_bytes = before_metadata,
            .references = sidecar,
            .canonical_installation_record_bytes = encodeDependentObjectMetadataInstallationRecord(installation),
        }});
    auto before_root
        = AuthorityRootBuilder::build(before_state, 3, definitions, records, expectations, before_graph, {}, dependent_objects.get());

    SchemaObjectDependencyGraphMutation graph_delta{
        .node_additions = {},
        .node_removals = retain_physical_graph_node ? std::vector<SchemaObjectID>{} : std::vector<SchemaObjectID>{table},
        .edge_additions = {},
        .edge_removals = {definition_edge},
    };
    auto after_root = AuthorityRootBuilder::buildPhysicalizationDelta(
        *before_root,
        21,
        std::span<const DefinitionIdentity>{},
        std::span<const SchemaObjectID>(&table, 1),
        graph_delta);

    return {
        .before_root = std::move(before_root),
        .after_root = std::move(after_root),
        .authority_record_deltas = {{
            .key = expectation_leaf.key,
            .before = DatabaseSchemaWALAuthorityRecordState{
                .object_revision = expectation_leaf.object_revision,
                .canonical_record_hash = expectation_leaf.canonical_record_hash,
            },
            .after = std::nullopt,
        }},
        .dependent_object_deltas = {{
            .object = table,
            .before = DatabaseSchemaWALDependentObjectState{
                .object_schema_revision = before_revision,
                .metadata_hash = before_metadata_hash,
                .sidecar_record_hash = sidecar_hash,
                .expectation_record_hash = expectation_leaf.canonical_record_hash,
            },
            .after = DatabaseSchemaWALDependentObjectState{
                .object_schema_revision = after_revision,
                .metadata_hash = after_metadata_hash,
                .sidecar_record_hash = std::nullopt,
                .expectation_record_hash = std::nullopt,
            },
        }},
        .graph_delta = std::move(graph_delta),
        .staged_artifacts = {
            {
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
                .image = DatabaseSchemaWALStagedArtifactImage::Before,
                .object = table,
                .revision = before_revision,
                .canonical_bytes = before_metadata,
            },
            {
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = table,
                .revision = after_revision,
                .canonical_bytes = after_metadata,
            },
            {
                .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
                .image = DatabaseSchemaWALStagedArtifactImage::Before,
                .object = table,
                .revision = before_revision,
                .canonical_bytes = sidecar_bytes,
            },
            {
                .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::Before,
                .object = table,
                .revision = before_revision,
                .canonical_bytes = encodeSidecarExpectationRecord(expectation),
            },
            {
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::Before,
                .object = table,
                .revision = before_revision,
                .canonical_bytes = encodeDependentObjectMetadataInstallationRecord(installation),
            },
        },
    };
}

DatabaseSchemaWALValidatedTransition
buildPhysicalizationTransition(const PhysicalizationTransitionCase & value, const DatabaseSchemaWALLimits & limits = {})
{
    return DatabaseSchemaWALTransitionBuilder::buildPhysicalization(
        105,
        *value.before_root,
        *value.after_root,
        value.authority_record_deltas,
        value.dependent_object_deltas,
        value.graph_delta,
        value.staged_artifacts,
        limits);
}

}

TEST(DatabaseSchemaWAL, FirstEnablementAcceptsImportedNonzeroDefinitionRevision)
{
    const Fixture fixture;
    const auto transition = fixture.firstTransition();
    EXPECT_EQ(transition.getPrepare().after_authority_state, fixture.definition_only_state);
    EXPECT_EQ(transition.getAfterInventory().getSummary(), fixture.definition_inventory->getSummary());
    EXPECT_TRUE(transition.getAfterGraph().containsNode(fixture.type_object));
    EXPECT_EQ(transition.getAfterGraph().computeRoot(), fixture.definition_graph->computeRoot());
    ASSERT_EQ(transition.getPrepare().staged_artifacts.size(), 1);
    EXPECT_EQ(transition.getPrepare().staged_artifacts.front().revision, 7);

    const auto encoded = encodeDatabaseSchemaWALPrepare(transition.getPrepare());
    const auto decoded = decodeDatabaseSchemaWALPrepare(encoded);
    EXPECT_EQ(decoded, transition.getPrepare());
    auto validated = DatabaseSchemaWALTransitionBuilder::validateDecoded(decoded, fixture.firstBase(), copyArtifactBytes(transition));
    EXPECT_EQ(validated.getPrepare(), transition.getPrepare());
}

TEST(DatabaseSchemaWAL, DefinitionOnlyGraphAcceptsOnlyDefinitionContentAndStartsFromEmptySnapshots)
{
    const Fixture fixture;
    const auto authority_delta = DatabaseSchemaWALAuthorityRecordDelta{
        .key = fixture.definition_leaf.key,
        .before = std::nullopt,
        .after = DatabaseSchemaWALAuthorityRecordState{
            .object_revision = fixture.definition_leaf.object_revision,
            .canonical_record_hash = fixture.definition_leaf.canonical_record_hash,
        },
    };
    const auto definition_artifact = DatabaseSchemaWALStagedArtifact{
        .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
        .image = DatabaseSchemaWALStagedArtifactImage::After,
        .object = fixture.type_object,
        .revision = fixture.definition.identity.revision,
        .canonical_bytes = fixture.definition_bytes,
    };

    const std::array rogue_nodes{fixture.type_object, fixture.dependent_object};
    const auto rogue_graph = SchemaObjectDependencyGraph::build(fixture.database_uuid, rogue_nodes, {});
    const auto rogue_state
        = state(fixture.database_uuid, 1, definition_authority_capability_mask, fixture.definition_inventory, rogue_graph);
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(DatabaseSchemaWALTransitionBuilder::build(
                100,
                fixture.firstBase(),
                rogue_state,
                {authority_delta},
                {},
                SchemaObjectDependencyGraphMutation{
                    .node_additions = {fixture.type_object, fixture.dependent_object},
                    .node_removals = {},
                    .edge_additions = {},
                    .edge_removals = {},
                },
                {definition_artifact}));
        });

    auto nonempty_base = fixture.firstBase();
    nonempty_base.schema_graph = fixture.definition_graph;
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(DatabaseSchemaWALTransitionBuilder::build(
                100, nonempty_base, fixture.definition_only_state, {authority_delta}, {}, {}, {definition_artifact}));
        });
}

TEST(DatabaseSchemaWAL, CommitAndRecoveryRequireAValidatedTransition)
{
    const Fixture fixture;
    const auto transition = fixture.firstTransition();
    const auto commit = makeDatabaseSchemaWALCommit(transition);
    EXPECT_EQ(decodeDatabaseSchemaWALCommit(encodeDatabaseSchemaWALCommit(commit)), commit);
    EXPECT_NO_THROW(validateDatabaseSchemaWALCommit(transition, commit));
    EXPECT_EQ(decideDatabaseSchemaWALRecovery(transition, std::nullopt), DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
    EXPECT_EQ(decideDatabaseSchemaWALRecovery(transition, commit), DatabaseSchemaWALRecoveryDecision::CompleteCommitted);

    auto wrong = commit;
    wrong.inventory_root[0] ^= 1;
    wrong.commit_hash = computeDatabaseSchemaWALCommitHash(wrong);
    expectError(DatabaseSchemaWALError::Code::TransitionMismatch, [&] { validateDatabaseSchemaWALCommit(transition, wrong); });
}

TEST(DatabaseSchemaWAL, DependentObjectActivationLeavesInventoryAndGraphUnchanged)
{
    const Fixture fixture;
    const auto transition = fixture.activationTransition();
    const auto & prepare = transition.getPrepare();
    ASSERT_TRUE(prepare.before_authority_state);
    EXPECT_EQ(prepare.before_authority_state->leaf_count, prepare.after_authority_state.leaf_count);
    EXPECT_EQ(prepare.before_authority_state->inventory_root, prepare.after_authority_state.inventory_root);
    EXPECT_EQ(prepare.before_authority_state->schema_graph_root, prepare.after_authority_state.schema_graph_root);
    EXPECT_EQ(transition.getAfterInventory().getSummary(), fixture.definition_inventory->getSummary());
    EXPECT_EQ(transition.getAfterGraph().computeRoot(), fixture.definition_graph->computeRoot());
    EXPECT_TRUE(prepare.staged_artifacts.empty());
}

TEST(DatabaseSchemaWAL, DependentObjectCrossLinksObjectSidecarExpectationAndGraph)
{
    const Fixture fixture;
    auto value = objectTransition(fixture);
    const auto encoded = encodeDatabaseSchemaWALPrepare(value.transition.getPrepare());
    const auto decoded = decodeDatabaseSchemaWALPrepare(encoded);
    EXPECT_EQ(decoded, value.transition.getPrepare());
    ASSERT_EQ(decoded.staged_artifacts.size(), 4);
    for (size_t index = 1; index < decoded.staged_artifacts.size(); ++index)
        EXPECT_TRUE(decoded.staged_artifacts[index - 1].kind <= decoded.staged_artifacts[index].kind);

    auto validated = DatabaseSchemaWALTransitionBuilder::validateDecoded(decoded, value.base, copyArtifactBytes(value.transition));
    EXPECT_EQ(validated.getAfterInventory().getSummary(), value.after_inventory->getSummary());
    EXPECT_EQ(validated.getAfterGraph().computeRoot(), value.after_graph->computeRoot());
}

TEST(DatabaseSchemaWAL, TableAbsentToMappedTransitionIsAccepted)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    const auto value = objectTransition(fixture, {}, true, false, DependentImageKind::Absent, DependentImageKind::Logical);

    ASSERT_EQ(value.transition.getPrepare().dependent_object_deltas.size(), 1);
    const auto & delta = value.transition.getPrepare().dependent_object_deltas.front();
    EXPECT_FALSE(delta.before);
    ASSERT_TRUE(delta.after);
    EXPECT_TRUE(delta.after->sidecar_record_hash);
}

TEST(DatabaseSchemaWAL, TablePhysicalToMappedInstallationRecordRoundTripsAndCrossLinksMetadata)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    auto value = objectTransition(fixture, {}, true);
    const auto decoded = decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(value.transition.getPrepare()));
    ASSERT_EQ(decoded.staged_artifacts.size(), 5);
    ASSERT_TRUE(decoded.dependent_object_deltas.front().before);
    EXPECT_FALSE(decoded.dependent_object_deltas.front().before->sidecar_record_hash);
    ASSERT_TRUE(decoded.dependent_object_deltas.front().after);
    EXPECT_TRUE(decoded.dependent_object_deltas.front().after->sidecar_record_hash);

    const auto installation_it = std::find_if(
        decoded.staged_artifacts.begin(),
        decoded.staged_artifacts.end(),
        [](const auto & artifact)
        { return artifact.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord; });
    ASSERT_NE(installation_it, decoded.staged_artifacts.end());
    const auto installation_index = static_cast<size_t>(std::distance(decoded.staged_artifacts.begin(), installation_it));
    const auto installation
        = decodeDependentObjectMetadataInstallationRecord(value.transition.getStagedArtifactBytes()[installation_index]);
    EXPECT_EQ(installation.object, fixture.dependent_object);
    EXPECT_EQ(installation.object_schema_revision, 9);
    EXPECT_EQ(installation.object_name, "events");
    ASSERT_TRUE(decoded.dependent_object_deltas.front().after);
    EXPECT_EQ(installation.metadata_artifact_hash, decoded.dependent_object_deltas.front().after->metadata_hash);

    EXPECT_NO_THROW(
        static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(decoded, value.base, copyArtifactBytes(value.transition))));
}

TEST(DatabaseSchemaWAL, TableMappedToPhysicalTransitionIsAccepted)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    const auto value = objectTransition(fixture, {}, true, false, DependentImageKind::Logical, DependentImageKind::PhysicalOnly);

    ASSERT_EQ(value.transition.getPrepare().dependent_object_deltas.size(), 1);
    const auto & delta = value.transition.getPrepare().dependent_object_deltas.front();
    ASSERT_TRUE(delta.before);
    EXPECT_TRUE(delta.before->sidecar_record_hash);
    ASSERT_TRUE(delta.after);
    EXPECT_FALSE(delta.after->sidecar_record_hash);
}

TEST(DatabaseSchemaWAL, TablePhysicalImageCannotRetainDependencyGraphNode)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(objectTransition(
                fixture, {}, true, false, DependentImageKind::Logical, DependentImageKind::PhysicalOnly, "events", "events", true));
        });
}

TEST(DatabaseSchemaWAL, SpecializedPhysicalizationRejectsRetainedTableGraphNode)
{
    const auto value = tablePhysicalizationTransitionCase(true);
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&] { static_cast<void>(buildPhysicalizationTransition(value)); });
}

TEST(DatabaseSchemaWAL, SpecializedPhysicalizationRemovesTableGraphNodeAndReplays)
{
    const auto value = tablePhysicalizationTransitionCase(false);
    auto transition = buildPhysicalizationTransition(value);
    EXPECT_FALSE(transition.getAfterGraph().containsNode(value.dependent_object_deltas.front().object));

    const auto decoded = decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(transition.getPrepare()));
    const DatabaseSchemaWALTransitionBase base{
        .authority_state = value.before_root->getAuthorityState(),
        .authority_inventory = value.before_root->pinAuthorityInventory(),
        .schema_graph = value.before_root->pinSchemaObjectDependencyGraph(),
    };
    auto replayed = DatabaseSchemaWALTransitionBuilder::validateDecoded(decoded, base, copyArtifactBytes(transition));
    EXPECT_EQ(replayed.getPrepare(), transition.getPrepare());
    EXPECT_EQ(replayed.getAfterInventory().getSummary(), transition.getAfterInventory().getSummary());
    EXPECT_EQ(replayed.getAfterGraph().computeRoot(), transition.getAfterGraph().computeRoot());
}

TEST(DatabaseSchemaWAL, TableDeltaWithOnlyPhysicalImagesIsRejected)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(
                objectTransition(fixture, {}, true, false, DependentImageKind::PhysicalOnly, DependentImageKind::PhysicalOnly));
        });
}

TEST(DatabaseSchemaWAL, TableMappedReplacementKeepsTheSameRawName)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    const auto value
        = objectTransition(fixture, {}, true, false, DependentImageKind::Logical, DependentImageKind::Logical, "events", "events");

    ASSERT_EQ(value.transition.getPrepare().dependent_object_deltas.size(), 1);
    const auto & delta = value.transition.getPrepare().dependent_object_deltas.front();
    ASSERT_TRUE(delta.before);
    ASSERT_TRUE(delta.after);
    EXPECT_TRUE(delta.before->sidecar_record_hash);
    EXPECT_TRUE(delta.after->sidecar_record_hash);
}

TEST(DatabaseSchemaWAL, TableMappedRenameCarriesDistinctExactInstallationRecords)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    auto value = objectTransition(
        fixture, {}, true, false, DependentImageKind::Logical, DependentImageKind::Logical, "events", "renamed_events");

    const auto & prepare = value.transition.getPrepare();
    ASSERT_EQ(prepare.dependent_object_deltas.size(), 1);
    const auto & delta = prepare.dependent_object_deltas.front();
    ASSERT_TRUE(delta.before);
    ASSERT_TRUE(delta.after);
    EXPECT_EQ(delta.object, fixture.dependent_object);
    EXPECT_EQ(delta.before->object_schema_revision, 8);
    EXPECT_EQ(delta.after->object_schema_revision, 9);
    EXPECT_TRUE(delta.before->sidecar_record_hash);
    EXPECT_TRUE(delta.before->expectation_record_hash);
    EXPECT_TRUE(delta.after->sidecar_record_hash);
    EXPECT_TRUE(delta.after->expectation_record_hash);

    const auto artifact_bytes = value.transition.getStagedArtifactBytes();
    ASSERT_EQ(prepare.staged_artifacts.size(), artifact_bytes.size());
    std::optional<DependentObjectMetadataInstallationRecord> before_installation;
    std::optional<DependentObjectMetadataInstallationRecord> after_installation;
    size_t installation_count = 0;
    for (size_t index = 0; index < prepare.staged_artifacts.size(); ++index)
    {
        const auto & artifact = prepare.staged_artifacts[index];
        if (artifact.kind != DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord)
            continue;
        ++installation_count;
        auto installation = decodeDependentObjectMetadataInstallationRecord(artifact_bytes[index]);
        if (artifact.image == DatabaseSchemaWALStagedArtifactImage::Before)
        {
            ASSERT_FALSE(before_installation);
            before_installation.emplace(std::move(installation));
        }
        else
        {
            ASSERT_EQ(artifact.image, DatabaseSchemaWALStagedArtifactImage::After);
            ASSERT_FALSE(after_installation);
            after_installation.emplace(std::move(installation));
        }
    }

    EXPECT_EQ(installation_count, 2);
    ASSERT_TRUE(before_installation);
    ASSERT_TRUE(after_installation);
    EXPECT_EQ(before_installation->object, fixture.dependent_object);
    EXPECT_EQ(before_installation->object_schema_revision, 8);
    EXPECT_EQ(before_installation->object_name, "events");
    EXPECT_EQ(before_installation->metadata_artifact_hash, delta.before->metadata_hash);
    EXPECT_EQ(after_installation->object, fixture.dependent_object);
    EXPECT_EQ(after_installation->object_schema_revision, 9);
    EXPECT_EQ(after_installation->object_name, "renamed_events");
    EXPECT_EQ(after_installation->metadata_artifact_hash, delta.after->metadata_hash);
    EXPECT_NE(before_installation->object_name, after_installation->object_name);

    EXPECT_NO_THROW(static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(
        decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(prepare)),
        value.base,
        copyArtifactBytes(value.transition))));
}

TEST(DatabaseSchemaWAL, SyntheticPhysicalOnlyTransitionRemainsAccepted)
{
    const Fixture fixture;
    const auto value = objectTransition(fixture, {}, false, false, DependentImageKind::PhysicalOnly, DependentImageKind::PhysicalOnly);
    EXPECT_TRUE(value.transition.getPrepare().authority_record_deltas.empty());
    EXPECT_EQ(value.transition.getPrepare().staged_artifacts.size(), 2);
}

TEST(DatabaseSchemaWAL, MetadataInstallationRecordIsRequiredExactlyWhenExpectationLinksIt)
{
    const Fixture synthetic_fixture;
    expectError(DatabaseSchemaWALError::Code::ArtifactMismatch, [&] { static_cast<void>(objectTransition(synthetic_fixture, {}, true)); });

    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    expectError(DatabaseSchemaWALError::Code::MissingArtifact, [&] { static_cast<void>(objectTransition(fixture)); });

    auto mapped = objectTransition(fixture, {}, true);
    auto missing = mapped.transition.getPrepare();
    auto missing_bytes = copyArtifactBytes(mapped.transition);
    const auto missing_it = std::find_if(
        missing.staged_artifacts.begin(),
        missing.staged_artifacts.end(),
        [](const auto & artifact)
        { return artifact.kind == DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord; });
    ASSERT_NE(missing_it, missing.staged_artifacts.end());
    const auto missing_index = static_cast<size_t>(std::distance(missing.staged_artifacts.begin(), missing_it));
    missing.staged_artifacts.erase(missing_it);
    missing_bytes.erase(missing_bytes.begin() + missing_index);
    missing.prepare_hash = computeDatabaseSchemaWALPrepareHash(missing);
    expectError(
        DatabaseSchemaWALError::Code::MissingArtifact,
        [&] { static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(missing, mapped.base, std::move(missing_bytes))); });

    Fixture ordinary_fixture;
    auto ordinary = objectTransition(ordinary_fixture);
    auto unexpected = ordinary.transition.getPrepare();
    auto unexpected_bytes = copyArtifactBytes(ordinary.transition);
    const auto metadata_hash
        = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, "metadata-after");
    const String installation_bytes = encodeDependentObjectMetadataInstallationRecord({
        .object = ordinary_fixture.dependent_object,
        .object_schema_revision = 9,
        .object_name = "events",
        .metadata_artifact_hash = metadata_hash,
    });
    unexpected.staged_artifacts.push_back({
        .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
        .image = DatabaseSchemaWALStagedArtifactImage::After,
        .object = ordinary_fixture.dependent_object,
        .revision = 9,
        .byte_size = installation_bytes.size(),
        .content_hash = computeDatabaseSchemaWALStagedArtifactHash(
            DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, installation_bytes),
    });
    unexpected_bytes.push_back(installation_bytes);
    unexpected.prepare_hash = computeDatabaseSchemaWALPrepareHash(unexpected);
    expectError(
        DatabaseSchemaWALError::Code::UnexpectedArtifact,
        [&]
        {
            static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(unexpected, ordinary.base, std::move(unexpected_bytes)));
        });
}

TEST(DatabaseSchemaWAL, MetadataInstallationRecordRejectsAnotherMetadataArtifact)
{
    Fixture fixture;
    fixture.dependent_object.kind = SchemaObjectKind::Table;
    expectError(DatabaseSchemaWALError::Code::ArtifactMismatch, [&] { static_cast<void>(objectTransition(fixture, {}, true, true)); });
}

TEST(DatabaseSchemaWAL, RawDecodeDoesNotSubstituteForBaseAndArtifactValidation)
{
    const Fixture fixture;
    auto value = objectTransition(fixture);
    const auto decoded = decodeDatabaseSchemaWALPrepare(encodeDatabaseSchemaWALPrepare(value.transition.getPrepare()));

    auto wrong_base = value.base;
    wrong_base.authority_inventory = fixture.empty_inventory;
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(
                DatabaseSchemaWALTransitionBuilder::validateDecoded(decoded, wrong_base, copyArtifactBytes(value.transition)));
        });

    auto corrupt_bytes = copyArtifactBytes(value.transition);
    ASSERT_FALSE(corrupt_bytes.front().empty());
    corrupt_bytes.front().front() ^= 1;
    expectError(
        DatabaseSchemaWALError::Code::ArtifactMismatch,
        [&] { static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(decoded, value.base, std::move(corrupt_bytes))); });

    auto missing_bytes = copyArtifactBytes(value.transition);
    missing_bytes.pop_back();
    expectError(
        DatabaseSchemaWALError::Code::MissingArtifact,
        [&] { static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(decoded, value.base, std::move(missing_bytes))); });
}

TEST(DatabaseSchemaWAL, RejectsBrokenCrossLinksUnexpectedArtifactsAndRevisionJumps)
{
    const Fixture fixture;
    auto value = objectTransition(fixture);

    auto broken_cross_link = value.transition.getPrepare();
    ASSERT_TRUE(broken_cross_link.dependent_object_deltas.front().after);
    ASSERT_TRUE(broken_cross_link.dependent_object_deltas.front().after->sidecar_record_hash);
    broken_cross_link.dependent_object_deltas.front().after->sidecar_record_hash->front() ^= 1;
    broken_cross_link.prepare_hash = computeDatabaseSchemaWALPrepareHash(broken_cross_link);
    expectError(
        DatabaseSchemaWALError::Code::ArtifactMismatch,
        [&]
        {
            static_cast<void>(
                DatabaseSchemaWALTransitionBuilder::validateDecoded(broken_cross_link, value.base, copyArtifactBytes(value.transition)));
        });

    auto revision_jump = value.transition.getPrepare();
    revision_jump.dependent_object_deltas.front().after->object_schema_revision += 1;
    revision_jump.prepare_hash = computeDatabaseSchemaWALPrepareHash(revision_jump);
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(
                DatabaseSchemaWALTransitionBuilder::validateDecoded(revision_jump, value.base, copyArtifactBytes(value.transition)));
        });

    auto unexpected = value.transition.getPrepare();
    const String extra_bytes = "unclaimed-sidecar";
    unexpected.staged_artifacts.push_back(
        DatabaseSchemaWALStagedArtifactRef{
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = fixture.dependent_object,
            .revision = 10,
            .byte_size = extra_bytes.size(),
            .content_hash = computeDatabaseSchemaWALStagedArtifactHash(
                DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, extra_bytes),
        });
    unexpected.prepare_hash = computeDatabaseSchemaWALPrepareHash(unexpected);
    auto unexpected_bytes = copyArtifactBytes(value.transition);
    unexpected_bytes.push_back(extra_bytes);
    expectError(
        DatabaseSchemaWALError::Code::UnexpectedArtifact,
        [&]
        { static_cast<void>(DatabaseSchemaWALTransitionBuilder::validateDecoded(unexpected, value.base, std::move(unexpected_bytes))); });
}

TEST(DatabaseSchemaWAL, SameRevisionRenamePresentationRewritePreservesImmutableSemantics)
{
    const Fixture fixture;
    auto renamed = fixture.definition;
    renamed.normalized_name = "db.Renamed";
    renamed.normalized_local_name = "Renamed";
    renamed.canonical_definition_sql = "CREATE TYPE db.Renamed AS Tuple(db.Renamed)";
    renamed.canonical_physical_template_sql = "Tuple(db.Renamed)";
    renamed.comment = "renamed";
    const String renamed_bytes = encodeRecord(renamed);
    const auto renamed_leaf = definitionLeaf(renamed);
    auto after_inventory = inventory({renamed_leaf});
    const auto after_state
        = state(fixture.database_uuid, 2, definition_authority_capability_mask, after_inventory, fixture.definition_graph);

    EXPECT_NO_THROW(static_cast<void>(DatabaseSchemaWALTransitionBuilder::build(
        103,
        fixture.definition_onlyBase(),
        after_state,
        {DatabaseSchemaWALAuthorityRecordDelta{
            .key = fixture.definition_leaf.key,
            .before = DatabaseSchemaWALAuthorityRecordState{
                .object_revision = fixture.definition_leaf.object_revision,
                .canonical_record_hash = fixture.definition_leaf.canonical_record_hash,
            },
            .after = DatabaseSchemaWALAuthorityRecordState{
                .object_revision = renamed_leaf.object_revision,
                .canonical_record_hash = renamed_leaf.canonical_record_hash,
            },
        }},
        {},
        {},
        {
            DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::Before,
                .object = fixture.type_object,
                .revision = fixture.definition.identity.revision,
                .canonical_bytes = fixture.definition_bytes,
            },
            DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = fixture.type_object,
                .revision = renamed.identity.revision,
                .canonical_bytes = renamed_bytes,
            },
        })));
}

TEST(DatabaseSchemaWAL, SameRevisionDefinitionCannotChangeImmutableSemantics)
{
    const Fixture fixture;
    auto changed = fixture.definition;
    changed.normalized_name = "db.Renamed";
    changed.normalized_local_name = "Renamed";
    changed.canonical_definition_sql = "CREATE TYPE db.Renamed AS Tuple(db.Renamed)";
    changed.canonical_physical_template_sql = "Tuple(db.Renamed)";
    changed.comment = "renamed";
    changed.canonical_template_ir += ":semantic-drift";
    const String changed_bytes = encodeRecord(changed);
    const auto changed_leaf = definitionLeaf(changed);
    auto after_inventory = inventory({changed_leaf});
    const auto after_state
        = state(fixture.database_uuid, 2, definition_authority_capability_mask, after_inventory, fixture.definition_graph);
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(DatabaseSchemaWALTransitionBuilder::build(
                104,
                fixture.definition_onlyBase(),
                after_state,
                {DatabaseSchemaWALAuthorityRecordDelta{
                    .key = fixture.definition_leaf.key,
                    .before = DatabaseSchemaWALAuthorityRecordState{
                        .object_revision = fixture.definition_leaf.object_revision,
                        .canonical_record_hash = fixture.definition_leaf.canonical_record_hash,
                    },
                    .after = DatabaseSchemaWALAuthorityRecordState{
                        .object_revision = changed_leaf.object_revision,
                        .canonical_record_hash = changed_leaf.canonical_record_hash,
                    },
                }},
                {},
                {},
                {
                    DatabaseSchemaWALStagedArtifact{
                        .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                        .image = DatabaseSchemaWALStagedArtifactImage::Before,
                        .object = fixture.type_object,
                        .revision = fixture.definition.identity.revision,
                        .canonical_bytes = fixture.definition_bytes,
                    },
                    DatabaseSchemaWALStagedArtifact{
                        .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                        .image = DatabaseSchemaWALStagedArtifactImage::After,
                        .object = fixture.type_object,
                        .revision = changed.identity.revision,
                        .canonical_bytes = changed_bytes,
                    },
                }));
        });
}

TEST(DatabaseSchemaWAL, BoundsApplyToTotalGraphDeltaAndReserveTrailingDigest)
{
    const Fixture fixture;
    DatabaseSchemaWALLimits graph_limits;
    graph_limits.maximum_graph_node_deltas = 1;
    auto oversized_graph_delta = fixture.firstTransition().getPrepare();
    oversized_graph_delta.graph_delta.node_removals = {fixture.type_object};
    expectError(
        DatabaseSchemaWALError::Code::LimitExceeded,
        [&] { static_cast<void>(computeDatabaseSchemaWALPrepareHash(oversized_graph_delta, graph_limits)); });

    const auto transition = fixture.firstTransition();
    const auto encoded = encodeDatabaseSchemaWALPrepare(transition.getPrepare());
    DatabaseSchemaWALLimits byte_limits;
    byte_limits.maximum_encoded_bytes = encoded.size() - 1;
    expectError(
        DatabaseSchemaWALError::Code::LimitExceeded,
        [&] { static_cast<void>(encodeDatabaseSchemaWALPrepare(transition.getPrepare(), byte_limits)); });
}

TEST(DatabaseSchemaWAL, WriterEntryPointsPreflightExactArtifactBudgetsBeforeMaterialization)
{
    const Fixture fixture;
    auto generic_reference = objectTransition(fixture);
    const UInt64 generic_count = generic_reference.transition.getPrepare().staged_artifacts.size();
    const auto [generic_maximum, generic_total] = artifactBudget(generic_reference.transition.getStagedArtifactBytes());
    ASSERT_GT(generic_count, 1u);
    ASSERT_GT(generic_maximum, 1u);
    ASSERT_GT(generic_total, generic_maximum);

    DatabaseSchemaWALLimits generic_exact;
    generic_exact.maximum_staged_artifacts = generic_count;
    generic_exact.maximum_staged_artifact_bytes = generic_maximum;
    generic_exact.maximum_total_staged_artifact_bytes = generic_total;
    EXPECT_NO_THROW(static_cast<void>(objectTransition(fixture, generic_exact)));

    auto generic_count_too_small = generic_exact;
    generic_count_too_small.maximum_staged_artifacts = generic_count - 1;
    expectError(
        DatabaseSchemaWALError::Code::LimitExceeded, [&] { static_cast<void>(objectTransition(fixture, generic_count_too_small)); });

    auto generic_item_too_small = generic_exact;
    generic_item_too_small.maximum_staged_artifact_bytes = generic_maximum - 1;
    expectError(DatabaseSchemaWALError::Code::LimitExceeded, [&] { static_cast<void>(objectTransition(fixture, generic_item_too_small)); });

    auto generic_total_too_small = generic_exact;
    generic_total_too_small.maximum_total_staged_artifact_bytes = generic_total - 1;
    expectError(
        DatabaseSchemaWALError::Code::LimitExceeded, [&] { static_cast<void>(objectTransition(fixture, generic_total_too_small)); });

    auto physicalization = physicalizationTransitionCase();
    auto physicalization_reference = buildPhysicalizationTransition(physicalization);
    const UInt64 physicalization_count = physicalization_reference.getPrepare().staged_artifacts.size();
    const auto [physicalization_maximum, physicalization_total] = artifactBudget(physicalization_reference.getStagedArtifactBytes());
    ASSERT_GT(physicalization_count, 1u);
    ASSERT_GT(physicalization_maximum, 1u);
    ASSERT_GT(physicalization_total, physicalization_maximum);

    DatabaseSchemaWALLimits physicalization_exact;
    physicalization_exact.maximum_staged_artifacts = physicalization_count;
    physicalization_exact.maximum_staged_artifact_bytes = physicalization_maximum;
    physicalization_exact.maximum_total_staged_artifact_bytes = physicalization_total;
    EXPECT_NO_THROW(static_cast<void>(buildPhysicalizationTransition(physicalization, physicalization_exact)));

    auto physicalization_count_too_small = physicalization_exact;
    physicalization_count_too_small.maximum_staged_artifacts = physicalization_count - 1;
    expectError(
        DatabaseSchemaWALError::Code::LimitExceeded,
        [&] { static_cast<void>(buildPhysicalizationTransition(physicalization, physicalization_count_too_small)); });

    auto physicalization_item_too_small = physicalization_exact;
    physicalization_item_too_small.maximum_staged_artifact_bytes = physicalization_maximum - 1;
    expectError(
        DatabaseSchemaWALError::Code::LimitExceeded,
        [&] { static_cast<void>(buildPhysicalizationTransition(physicalization, physicalization_item_too_small)); });

    auto physicalization_total_too_small = physicalization_exact;
    physicalization_total_too_small.maximum_total_staged_artifact_bytes = physicalization_total - 1;
    expectError(
        DatabaseSchemaWALError::Code::LimitExceeded,
        [&] { static_cast<void>(buildPhysicalizationTransition(physicalization, physicalization_total_too_small)); });
}

TEST(DatabaseSchemaWAL, CheckpointBindsFullCommitAndVerifiedSnapshots)
{
    const Fixture fixture;
    auto value = objectTransition(fixture);
    const auto commit = makeDatabaseSchemaWALCommit(value.transition);
    auto checkpoint = DatabaseSchemaWALCheckpointBuilder::build(
        500, commit, value.transition.getPrepare().after_authority_state, value.after_inventory, value.after_graph);
    const auto encoded = encodeDatabaseSchemaWALCheckpoint(checkpoint.getCheckpoint());
    auto decoded = decodeDatabaseSchemaWALCheckpoint(encoded);
    EXPECT_EQ(decoded, checkpoint.getCheckpoint());
    EXPECT_EQ(decoded.covered_commit, commit);
    EXPECT_NO_THROW(
        static_cast<void>(DatabaseSchemaWALCheckpointBuilder::validateDecoded(
            decoded, checkpoint.getInventorySnapshotBytes(), checkpoint.getSchemaGraphSnapshotBytes())));

    const String wrong_graph_snapshot = fixture.empty_graph->encodeSnapshot();
    expectError(
        DatabaseSchemaWALError::Code::DigestMismatch,
        [&]
        {
            static_cast<void>(DatabaseSchemaWALCheckpointBuilder::validateDecoded(
                decoded, checkpoint.getInventorySnapshotBytes(), wrong_graph_snapshot));
        });

    decoded.covered_commit.inventory_root[0] ^= 1;
    decoded.covered_commit.commit_hash = computeDatabaseSchemaWALCommitHash(decoded.covered_commit);
    decoded.checkpoint_hash = computeDatabaseSchemaWALCheckpointHash(decoded);
    expectError(
        DatabaseSchemaWALError::Code::TransitionMismatch,
        [&]
        {
            static_cast<void>(DatabaseSchemaWALCheckpointBuilder::validateDecoded(
                decoded, checkpoint.getInventorySnapshotBytes(), checkpoint.getSchemaGraphSnapshotBytes()));
        });
}

TEST(DatabaseSchemaWAL, RejectsDigestMismatchTrailingDataAndNoncanonicalManifest)
{
    const Fixture fixture;
    const auto transition = fixture.firstTransition();
    auto prepare_bytes = encodeDatabaseSchemaWALPrepare(transition.getPrepare());
    prepare_bytes.back() ^= 1;
    expectError(DatabaseSchemaWALError::Code::DigestMismatch, [&] { static_cast<void>(decodeDatabaseSchemaWALPrepare(prepare_bytes)); });

    auto commit_bytes = encodeDatabaseSchemaWALCommit(makeDatabaseSchemaWALCommit(transition));
    commit_bytes.push_back('\0');
    expectError(DatabaseSchemaWALError::Code::TrailingData, [&] { static_cast<void>(decodeDatabaseSchemaWALCommit(commit_bytes)); });

    auto duplicate = transition.getPrepare();
    duplicate.staged_artifacts.push_back(duplicate.staged_artifacts.front());
    expectError(DatabaseSchemaWALError::Code::DuplicateDelta, [&] { static_cast<void>(computeDatabaseSchemaWALPrepareHash(duplicate)); });
}

TEST(DatabaseSchemaWAL, StagedArtifactEnvelopeAndDerivedLocatorAreStable)
{
    EXPECT_EQ(
        toHex(computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, "abc")),
        "753f6e06f8e719b7a2433213d7a26c640a393ab02bb3f088e9199257fbd3c8bd");
    EXPECT_EQ(
        toHex(computeDatabaseSchemaWALStagedArtifactHash(
            DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, "abc")),
        "efacc331cd2446b0a5ee48effec35b40e62ced6319786f143e96ce11b13d338a");
    const UUID database_uuid = uuid(1, 2);
    EXPECT_EQ(
        makeDatabaseSchemaWALStagedArtifactLocator(database_uuid, 7, 9),
        (DatabaseSchemaWALStagedArtifactLocator{.database_uuid = database_uuid, .transaction_id = 7, .ordinal = 9}));
    expectError(
        DatabaseSchemaWALError::Code::InvalidValue,
        [&] { static_cast<void>(makeDatabaseSchemaWALStagedArtifactLocator(UUIDHelpers::Nil, 7, 9)); });
}

}
