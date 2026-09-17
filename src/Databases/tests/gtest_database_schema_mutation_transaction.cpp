#include <Databases/DatabaseSchemaMutationTransaction.h>

#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

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

SchemaObjectID objectID(SchemaObjectKind kind, UUID database_uuid, UUID object_uuid)
{
    return {.kind = kind, .database_uuid = database_uuid, .object_uuid = object_uuid};
}

Record definitionRecord(UUID database_uuid, UUID type_uuid)
{
    Record result;
    result.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 7};
    result.normalized_name = "db.Alpha";
    result.normalized_local_name = "Alpha";
    result.policy_semantic_hash = CheckerProof::empty_policy_semantic_hash;
    result.canonical_definition_sql = "CREATE TYPE db.Alpha AS UInt64";
    result.canonical_physical_template_sql = "UInt64";
    result.canonical_template_ir = "ir:UInt64";
    result.semantic_definition_digest = hashFramedDomainSeparated("test semantic definition", "UInt64");
    result.definition_hash = hashFramedDomainSeparated("test definition", "UInt64");
    result.compositional_dependency_closure_digest = hashFramedDomainSeparated("test dependency closure", "UInt64");
    result.encoded_checker_certificate = "certificate:UInt64";
    result.checker_certificate_digest = hashDomainSeparated(CheckerProof::checker_proof_domain, result.encoded_checker_certificate);
    result.charged_work = 1;
    result.logical_node_count = 1;
    result.maximum_template_depth = 0;
    result.owner_uuid = uuid(0x9000, 1);
    result.owner_display_name = "owner";
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

Digest testDigest(UInt8 first)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(first + index);
    return result;
}

String noTypeArguments()
{
    constexpr std::array<char, 3> bytes{1, 0, 0};
    return {bytes.data(), bytes.size()};
}

PersistedTypeDescriptor persistedDescriptor(const Record & definition)
{
    const String canonical_arguments = noTypeArguments();
    const String canonical_physical_type = "UInt64";
    const Digest storage_fingerprint = testDigest(0x40);
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
persistedReferences(const SchemaObjectID & object, UInt64 revision, const Record & definition, Digest physical_fingerprint)
{
    PersistedTypePathSection path_section;
    switch (object.kind)
    {
        case SchemaObjectKind::Table: path_section = PersistedTypePathSection::ColumnType; break;
        case SchemaObjectKind::View: path_section = PersistedTypePathSection::ViewExpression; break;
        case SchemaObjectKind::Dictionary: path_section = PersistedTypePathSection::DictionaryAttribute; break;
        case SchemaObjectKind::SyntheticTestObject: path_section = PersistedTypePathSection::SyntheticPayload; break;
        case SchemaObjectKind::TypeDefinition: throw std::logic_error("type definition cannot own persisted references");
    }

    PersistedTypeReferences result;
    result.object = object;
    result.object_schema_revision = revision;
    result.physical_schema_fingerprint = physical_fingerprint;
    result.descriptors = {persistedDescriptor(definition)};
    result.occurrence_paths = {{
        .section = path_section,
        .object_ordinal = 0,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    }};
    result.uses = {{.path_id = 0, .descriptor_id = 0}};
    return result;
}

AuthorityInventory::Ptr inventory(std::vector<AuthorityInventoryLeaf> leaves)
{
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    return AuthorityInventory::create(buildAuthorityInventorySummary(leaves), std::move(leaves));
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

struct Fixture
{
    UUID database_uuid = uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL);
    UUID type_uuid = uuid(0x1021324354657687ULL, 0x98a9bacbdcedfe01ULL);
    SchemaObjectID type_object = objectID(SchemaObjectKind::TypeDefinition, database_uuid, type_uuid);
    Record record = definitionRecord(database_uuid, type_uuid);
    String record_bytes = encodeRecord(record);
    AuthorityInventoryLeaf leaf = definitionLeaf(record);
    AuthorityInventory::Ptr empty_inventory = inventory({});
    AuthorityInventory::Ptr definition_inventory = inventory({leaf});
    SchemaObjectDependencyGraph::Ptr empty_graph = SchemaObjectDependencyGraph::createEmpty(database_uuid);
    SchemaObjectDependencyGraph::Ptr definition_graph = SchemaObjectDependencyGraph::build(database_uuid, {&type_object, 1}, {});
    AuthorityState definition_only_state = state(database_uuid, 1, definition_authority_capability_mask, definition_inventory, definition_graph);
    AuthorityState dependent_object_state = activateDependentObjectAuthority(definition_only_state);

    DatabaseSchemaWALValidatedTransition firstTransition() const
    {
        return DatabaseSchemaWALTransitionBuilder::build(
            100,
            DatabaseSchemaWALTransitionBase{
                .authority_state = std::nullopt,
                .authority_inventory = empty_inventory,
                .schema_graph = empty_graph,
            },
            definition_only_state,
            {DatabaseSchemaWALAuthorityRecordDelta{
                .key = leaf.key,
                .before = std::nullopt,
                .after = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = leaf.object_revision,
                    .canonical_record_hash = leaf.canonical_record_hash,
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
                .revision = record.identity.revision,
                .canonical_bytes = record_bytes,
            }});
    }

    DatabaseSchemaWALValidatedTransition replacementTransition() const
    {
        auto replacement = record;
        replacement.normalized_name = "db.Renamed";
        replacement.normalized_local_name = "Renamed";
        replacement.canonical_definition_sql = "CREATE TYPE db.Renamed AS UInt64";
        replacement.comment = "new presentation";
        const String replacement_bytes = encodeRecord(replacement);
        const auto replacement_leaf = definitionLeaf(replacement);
        const auto replacement_inventory = inventory({replacement_leaf});
        const auto after_state = state(database_uuid, 2, definition_authority_capability_mask, replacement_inventory, definition_graph);

        return DatabaseSchemaWALTransitionBuilder::build(
            101,
            DatabaseSchemaWALTransitionBase{
                .authority_state = definition_only_state,
                .authority_inventory = definition_inventory,
                .schema_graph = definition_graph,
            },
            after_state,
            {DatabaseSchemaWALAuthorityRecordDelta{
                .key = leaf.key,
                .before = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = leaf.object_revision,
                    .canonical_record_hash = leaf.canonical_record_hash,
                },
                .after = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = replacement_leaf.object_revision,
                    .canonical_record_hash = replacement_leaf.canonical_record_hash,
                },
            }},
            {},
            {},
            {
                DatabaseSchemaWALStagedArtifact{
                    .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                    .image = DatabaseSchemaWALStagedArtifactImage::Before,
                    .object = type_object,
                    .revision = record.identity.revision,
                    .canonical_bytes = record_bytes,
                },
                DatabaseSchemaWALStagedArtifact{
                    .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                    .image = DatabaseSchemaWALStagedArtifactImage::After,
                    .object = type_object,
                    .revision = replacement.identity.revision,
                    .canonical_bytes = replacement_bytes,
                },
            });
    }
};

DatabaseSchemaWALValidatedTransition
graphlessPhysicalTransition(const Fixture & fixture, SchemaObjectKind object_kind, bool graphless_after)
{
    constexpr UInt64 before_revision = 8;
    constexpr UInt64 after_revision = 9;
    const SchemaObjectID object = objectID(object_kind, fixture.database_uuid, uuid(0x8100, static_cast<UInt64>(object_kind)));
    const String before_metadata = graphless_after ? "metadata-before-mapped" : "metadata-before-physical";
    const String after_metadata = graphless_after ? "metadata-after-physical" : "metadata-after-mapped";
    const Digest before_metadata_hash
        = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, before_metadata);
    const Digest after_metadata_hash
        = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, after_metadata);
    const UInt64 logical_revision = graphless_after ? before_revision : after_revision;
    const Digest logical_metadata_hash = graphless_after ? before_metadata_hash : after_metadata_hash;
    const Digest physical_fingerprint = testDigest(0x70);
    const auto references = persistedReferences(object, logical_revision, fixture.record, physical_fingerprint);
    const String sidecar_bytes = encodePersistedTypeReferences(references);
    const Digest sidecar_hash = computePersistedTypeReferencesSidecarHash(references);
    const bool has_installation_record
        = object_kind == SchemaObjectKind::Table || object_kind == SchemaObjectKind::View || object_kind == SchemaObjectKind::Dictionary;
    const auto installation = [&]() -> std::optional<DependentObjectMetadataInstallationRecord>
    {
        if (!has_installation_record)
            return std::nullopt;
        return DependentObjectMetadataInstallationRecord{
            .object = object,
            .object_schema_revision = logical_revision,
            .object_name = "object",
            .metadata_artifact_hash = logical_metadata_hash,
        };
    }();
    const SidecarExpectationRecord expectation{
        .object = object,
        .object_schema_revision = logical_revision,
        .sidecar_hash = sidecar_hash,
        .physical_schema_fingerprint = physical_fingerprint,
        .installation_record_hash
        = installation ? std::optional<Digest>{computeDependentObjectMetadataInstallationRecordHash(*installation)} : std::nullopt,
    };
    const auto expectation_leaf = expectationLeaf(expectation);
    auto logical_inventory = inventory({fixture.leaf, expectation_leaf});
    const auto physical_inventory = fixture.definition_inventory;

    const SchemaObjectDependencyEdge definition_edge{
        .dependent = object,
        .dependency = fixture.type_object,
        .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
    };
    const std::array logical_nodes{fixture.type_object, object};
    const std::array logical_edges{definition_edge};
    auto logical_graph = SchemaObjectDependencyGraph::build(fixture.database_uuid, logical_nodes, logical_edges);
    const auto physical_graph = fixture.definition_graph;
    const auto before_inventory = graphless_after ? logical_inventory : physical_inventory;
    const auto after_inventory = graphless_after ? physical_inventory : logical_inventory;
    const auto before_graph = graphless_after ? logical_graph : physical_graph;
    const auto after_graph = graphless_after ? physical_graph : logical_graph;
    const auto before_state = state(fixture.database_uuid, 20, dependent_object_authority_capability_mask, before_inventory, before_graph);
    const auto after_state = state(fixture.database_uuid, 21, dependent_object_authority_capability_mask, after_inventory, after_graph);

    const auto object_state = [&](UInt64 revision, Digest metadata_hash, bool mapped)
    {
        return DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = revision,
            .metadata_hash = metadata_hash,
            .sidecar_record_hash = mapped ? std::optional<Digest>{sidecar_hash} : std::optional<Digest>{},
            .expectation_record_hash = mapped ? std::optional<Digest>{expectation_leaf.canonical_record_hash} : std::optional<Digest>{},
        };
    };
    const DatabaseSchemaWALAuthorityRecordState expectation_state{
        .object_revision = expectation_leaf.object_revision,
        .canonical_record_hash = expectation_leaf.canonical_record_hash,
    };
    const SchemaObjectDependencyGraphMutation graph_delta = graphless_after
        ? SchemaObjectDependencyGraphMutation{
              .node_additions = {},
              .node_removals = {object},
              .edge_additions = {},
              .edge_removals = {definition_edge},
          }
        : SchemaObjectDependencyGraphMutation{
              .node_additions = {object},
              .node_removals = {},
              .edge_additions = {definition_edge},
              .edge_removals = {},
          };

    const auto logical_image = graphless_after ? DatabaseSchemaWALStagedArtifactImage::Before : DatabaseSchemaWALStagedArtifactImage::After;
    std::vector<DatabaseSchemaWALStagedArtifact> artifacts{
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::Before,
            .object = object,
            .revision = before_revision,
            .canonical_bytes = before_metadata,
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = object,
            .revision = after_revision,
            .canonical_bytes = after_metadata,
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = logical_image,
            .object = object,
            .revision = logical_revision,
            .canonical_bytes = sidecar_bytes,
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = logical_image,
            .object = object,
            .revision = logical_revision,
            .canonical_bytes = encodeSidecarExpectationRecord(expectation),
        },
    };
    if (installation)
    {
        artifacts.push_back({
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
            .image = logical_image,
            .object = object,
            .revision = logical_revision,
            .canonical_bytes = encodeDependentObjectMetadataInstallationRecord(*installation),
        });
    }

    return DatabaseSchemaWALTransitionBuilder::build(
        110 + static_cast<UInt64>(object_kind) + (graphless_after ? 0 : 10),
        {
            .authority_state = before_state,
            .authority_inventory = before_inventory,
            .schema_graph = before_graph,
        },
        after_state,
        {DatabaseSchemaWALAuthorityRecordDelta{
            .key = expectation_leaf.key,
            .before = graphless_after ? std::optional<DatabaseSchemaWALAuthorityRecordState>{expectation_state}
                                      : std::optional<DatabaseSchemaWALAuthorityRecordState>{},
            .after = graphless_after ? std::optional<DatabaseSchemaWALAuthorityRecordState>{}
                                     : std::optional<DatabaseSchemaWALAuthorityRecordState>{expectation_state},
        }},
        {DatabaseSchemaWALDependentObjectDelta{
            .object = object,
            .before = object_state(before_revision, before_metadata_hash, graphless_after),
            .after = object_state(after_revision, after_metadata_hash, !graphless_after),
        }},
        graph_delta,
        std::move(artifacts));
}

struct ArtifactKey
{
    DatabaseSchemaWALStagedArtifactKind kind{};
    SchemaObjectID object;

    bool operator<(const ArtifactKey & other) const noexcept { return std::tie(kind, object) < std::tie(other.kind, other.object); }
};

struct LocatorKey
{
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 transaction_id = 0;
    UInt64 ordinal = 0;

    bool operator<(const LocatorKey & other) const noexcept
    {
        return std::tuple{UUIDHelpers::getHighBytes(database_uuid), UUIDHelpers::getLowBytes(database_uuid), transaction_id, ordinal}
        < std::tuple{
            UUIDHelpers::getHighBytes(other.database_uuid),
            UUIDHelpers::getLowBytes(other.database_uuid),
            other.transaction_id,
            other.ordinal};
    }
};

LocatorKey locatorKey(const DatabaseSchemaWALStagedArtifactLocator & locator)
{
    return {.database_uuid = locator.database_uuid, .transaction_id = locator.transaction_id, .ordinal = locator.ordinal};
}

struct ArtifactValue
{
    DatabaseSchemaWALStagedArtifactRef reference;
    String bytes;

    bool operator==(const ArtifactValue &) const = default;
};

struct CheckpointValue
{
    DatabaseSchemaWALCheckpoint record;
    String canonical_record;
    String inventory_snapshot;
    String graph_snapshot;

    bool operator==(const CheckpointValue &) const = default;
};

enum class FailurePosition : UInt8
{
    Before = 1,
    After = 2,
};

class FakeDurableStorage final : public IDatabaseSchemaMutationDurableStorage
{
public:
    explicit FakeDurableStorage(
        UUID database_uuid_,
        UInt64 durable_predecessor_transaction_id_ = 0,
        std::optional<AuthorityState> authority_state_ = std::nullopt)
        : database_uuid(database_uuid_)
        , durable_predecessor_transaction_id(durable_predecessor_transaction_id_)
        , authority_state(std::move(authority_state_))
    {
    }

    DatabaseSchemaMutationGuard issueGuard()
    {
        ++guard_identity;
        return DatabaseSchemaMutationGuard::issue(database_uuid, guard_identity, durable_predecessor_transaction_id);
    }

    void seedInstalled(const DatabaseSchemaWALStagedArtifactRef & reference, String bytes)
    {
        installed[{reference.kind, reference.object}] = ArtifactValue{.reference = reference, .bytes = std::move(bytes)};
    }

    void validateMutationGuardAndDurablePredecessor(
        const DatabaseSchemaMutationGuard & guard,
        const std::optional<AuthorityState> & expected_preceding_authority_state,
        UInt64 transaction_id) override
    {
        validateGuard(guard);
        ++validation_calls;
        if (recovery_required_transaction)
            conflict("database is fail-stopped pending schema-mutation recovery");
        if (guard.getDurablePredecessorTransactionID() != durable_predecessor_transaction_id)
            conflict("schema-mutation guard captured a stale durable predecessor");
        if (transaction_id <= durable_predecessor_transaction_id)
            conflict("schema-mutation transaction ID is not monotonic");
        if (expected_preceding_authority_state != authority_state)
            conflict("schema-mutation preceding authority state is stale");
        current_transaction_id = transaction_id;
    }

    void markMutationRecoveryRequired(
        const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id, DatabaseSchemaMutationDurabilityPhase phase) noexcept override
    {
        if (guard.getDatabaseUUID() == database_uuid && guard.getOpaqueIdentity() == guard_identity)
        {
            recovery_required_transaction = transaction_id;
            recovery_required_phase = phase;
        }
    }

    void validateRecoveryGuard(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) override
    {
        validateGuard(guard);
        ++validation_calls;
        if (!prepares.contains(transaction_id))
            conflict("schema-mutation recovery has no durable Prepare marker");
        if (transaction_id > durable_predecessor_transaction_id)
            conflict("schema-mutation recovery transaction exceeds the durable high-water mark");
        current_transaction_id = transaction_id;
    }

    void stageArtifact(
        const DatabaseSchemaWALStagedArtifactLocator & locator,
        const DatabaseSchemaWALStagedArtifactRef & artifact,
        std::string_view canonical_bytes) override
    {
        durableEvent(
            "stage",
            [&]
            {
                if (locator.database_uuid != database_uuid || locator.transaction_id != current_transaction_id)
                    conflict("staged artifact locator belongs to another transaction");
                insertExact(
                    staged,
                    locatorKey(locator),
                    ArtifactValue{.reference = artifact, .bytes = String(canonical_bytes)},
                    "staged artifact locator replay differs");
            });
    }

    void finishStaging(UUID requested_database_uuid, UInt64 transaction_id) override
    {
        durableEvent(
            "finish-staging",
            [&]
            {
                if (requested_database_uuid != database_uuid || transaction_id != current_transaction_id)
                    conflict("staging barrier belongs to another transaction");
            });
    }

    void persistPrepare(UInt64 transaction_id, std::string_view canonical_prepare) override
    {
        const auto decoded = decodeDatabaseSchemaWALPrepare(canonical_prepare);
        durableEvent(
            "prepare",
            [&]
            {
                if (transaction_id != current_transaction_id || decoded.transaction_id != transaction_id)
                    conflict("Prepare marker transaction identity differs");
                insertExact(prepares, transaction_id, String(canonical_prepare), "Prepare marker replay differs");
                durable_predecessor_transaction_id = std::max(durable_predecessor_transaction_id, transaction_id);
            });
    }

    void installArtifact(const DatabaseSchemaWALStagedArtifactRef & artifact, std::string_view canonical_bytes) override
    {
        durableEvent(
            "install",
            [&]
            {
                const ArtifactKey key{artifact.kind, artifact.object};
                const ArtifactValue desired{.reference = artifact, .bytes = String(canonical_bytes)};
                const auto existing = installed.find(key);
                if (existing != installed.end() && existing->second == desired)
                    return;
                const auto opposite = findOppositeImage(artifact);
                if (existing == installed.end())
                {
                    if (opposite)
                        conflict("canonical artifact disappeared instead of matching the opposite image");
                    installed.emplace(key, desired);
                }
                else
                {
                    if (!opposite || existing->second != *opposite)
                        conflict("canonical artifact matches neither durable transition image");
                    existing->second = desired;
                }
                installed_order.push_back(artifact.object);
            });
    }

    void removeArtifact(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) override
    {
        durableEvent(
            "remove",
            [&]
            {
                const ArtifactKey key{kind, object};
                const auto existing = installed.find(key);
                if (existing == installed.end())
                    return;
                const auto selected = findOnlyImage(kind, object);
                if (!selected || existing->second != *selected)
                    conflict("canonical artifact removal does not match the durable transition image");
                installed.erase(existing);
                removed_order.push_back(object);
            });
    }

    void finishInstallation(UUID requested_database_uuid, UInt64 transaction_id) override
    {
        durableEvent(
            "finish-installation",
            [&]
            {
                if (requested_database_uuid != database_uuid || transaction_id != current_transaction_id)
                    conflict("installation barrier belongs to another transaction");
            });
    }

    void persistCommit(UInt64 transaction_id, std::string_view canonical_commit) override
    {
        const auto decoded = decodeDatabaseSchemaWALCommit(canonical_commit);
        durableEvent(
            "commit",
            [&]
            {
                if (transaction_id != current_transaction_id || decoded.transaction_id != transaction_id)
                    conflict("Commit marker transaction identity differs");
                const auto prepare_it = prepares.find(transaction_id);
                if (prepare_it == prepares.end())
                    conflict("Commit marker has no durable Prepare marker");
                const auto prepare = decodeDatabaseSchemaWALPrepare(prepare_it->second);
                if (decoded.prepare_hash != prepare.prepare_hash)
                    conflict("Commit marker does not bind the durable Prepare marker");
                insertExact(commits, transaction_id, String(canonical_commit), "Commit marker replay differs");
                authority_state = prepare.after_authority_state;
            });
    }

    void finishRecovery(UUID requested_database_uuid, UInt64 transaction_id, DatabaseSchemaWALRecoveryDecision decision) override
    {
        durableEvent(
            "finish-recovery",
            [&]
            {
                if (requested_database_uuid != database_uuid || transaction_id != current_transaction_id)
                    conflict("recovery marker belongs to another transaction");
                insertExact(recovery_decisions, transaction_id, decision, "recovery decision replay differs");
                if (decision == DatabaseSchemaWALRecoveryDecision::CompleteCommitted)
                {
                    if (!commits.contains(transaction_id))
                        conflict("commit completion has no durable Commit marker");
                    authority_state = decodeDatabaseSchemaWALPrepare(prepares.at(transaction_id)).after_authority_state;
                }
                if (recovery_required_transaction == transaction_id)
                {
                    recovery_required_transaction.reset();
                    recovery_required_phase.reset();
                }
            });
    }

    void discardUnpreparedStaging(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) override
    {
        ++discard_calls;
        validateGuard(guard);
        if (prepares.contains(transaction_id))
            conflict("cannot discard staging for a durable prepared transaction");
        eraseStagingThrough(transaction_id, transaction_id);
        if (recovery_required_transaction == transaction_id)
        {
            recovery_required_transaction.reset();
            recovery_required_phase.reset();
        }
    }

    void retireRolledBackTransaction(const DatabaseSchemaMutationGuard & guard, UInt64 transaction_id) override
    {
        validateGuard(guard);
        if (retired_rollbacks.contains(transaction_id))
            return;
        const auto decision = recovery_decisions.find(transaction_id);
        if (decision == recovery_decisions.end() || decision->second != DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
            conflict("cannot retire a transaction without a durable rollback decision");
        retired_rollbacks.insert(transaction_id);
        prepares.erase(transaction_id);
        commits.erase(transaction_id);
        recovery_decisions.erase(transaction_id);
        eraseStagingThrough(transaction_id, transaction_id);
    }

    void persistValidatedCheckpoint(
        const DatabaseSchemaMutationGuard & guard,
        const DatabaseSchemaWALCheckpoint & checkpoint,
        std::string_view canonical_checkpoint,
        std::string_view canonical_inventory_snapshot,
        std::string_view canonical_schema_graph_snapshot) override
    {
        validateGuard(guard);
        if (guard.getDurablePredecessorTransactionID() != durable_predecessor_transaction_id)
            conflict("checkpoint guard captured a stale durable predecessor");
        const auto commit_it = commits.find(checkpoint.covered_commit.transaction_id);
        if (commit_it == commits.end() || decodeDatabaseSchemaWALCommit(commit_it->second) != checkpoint.covered_commit)
            conflict("checkpoint does not cover an exact durable commit");
        if (authority_state != checkpoint.authority_state)
            conflict("checkpoint authority state is not current");
        const CheckpointValue value{
            .record = checkpoint,
            .canonical_record = String(canonical_checkpoint),
            .inventory_snapshot = String(canonical_inventory_snapshot),
            .graph_snapshot = String(canonical_schema_graph_snapshot),
        };
        durableEvent("checkpoint", [&] { insertExact(checkpoints, checkpoint.checkpoint_id, value, "checkpoint replay differs"); });
    }

    void compactThroughValidatedCheckpoint(
        const DatabaseSchemaMutationGuard & guard, const DatabaseSchemaWALCheckpoint & checkpoint) override
    {
        validateGuard(guard);
        const auto durable_checkpoint = checkpoints.find(checkpoint.checkpoint_id);
        if (durable_checkpoint == checkpoints.end() || durable_checkpoint->second.record != checkpoint)
            conflict("WAL compaction has no exact durable checkpoint");
        durableEvent(
            "compact",
            [&]
            {
                const UInt64 covered = checkpoint.covered_commit.transaction_id;
                eraseMapThrough(prepares, covered);
                eraseMapThrough(commits, covered);
                eraseMapThrough(recovery_decisions, covered);
                eraseStagingThrough(0, covered);
                compacted_through = std::max(compacted_through, covered);
            });
    }

    void configureFailure(size_t event, FailurePosition position)
    {
        failure_event = event;
        failure_position = position;
    }

    void disableFailure()
    {
        failure_event = std::numeric_limits<size_t>::max();
        failure_position = FailurePosition::Before;
    }

    UUID database_uuid;
    UInt64 durable_predecessor_transaction_id = 0;
    std::optional<AuthorityState> authority_state;
    UInt64 guard_identity = 0;
    UInt64 current_transaction_id = 0;
    size_t validation_calls = 0;
    size_t discard_calls = 0;
    std::optional<UInt64> recovery_required_transaction;
    std::optional<DatabaseSchemaMutationDurabilityPhase> recovery_required_phase;
    std::vector<String> events;
    std::vector<SchemaObjectID> installed_order;
    std::vector<SchemaObjectID> removed_order;
    std::map<LocatorKey, ArtifactValue> staged;
    std::map<ArtifactKey, ArtifactValue> installed;
    std::map<UInt64, String> prepares;
    std::map<UInt64, String> commits;
    std::map<UInt64, DatabaseSchemaWALRecoveryDecision> recovery_decisions;
    std::set<UInt64> retired_rollbacks;
    std::map<UInt64, CheckpointValue> checkpoints;
    UInt64 compacted_through = 0;

private:
    [[noreturn]] static void conflict(std::string_view message) { throw DatabaseSchemaMutationReplayConflictError(message); }

    void validateGuard(const DatabaseSchemaMutationGuard & guard) const
    {
        if (guard.getDatabaseUUID() != database_uuid || guard.getOpaqueIdentity() != guard_identity)
            conflict("schema-mutation guard token is stale or foreign");
    }

    template <typename Map, typename Key, typename Value>
    static void insertExact(Map & map, Key key, Value value, std::string_view message)
    {
        const auto existing = map.find(key);
        if (existing != map.end())
        {
            if (existing->second != value)
                conflict(message);
            return;
        }
        const bool inserted = map.emplace(std::move(key), std::move(value)).second;
        if (!inserted)
            conflict(message);
    }

    template <typename Function>
    void durableEvent(std::string_view name, Function && function)
    {
        events.emplace_back(name);
        const size_t event = events.size();
        if (event == failure_event && failure_position == FailurePosition::Before)
            throw std::runtime_error("injected failure before durable event");
        function();
        if (event == failure_event && failure_position == FailurePosition::After)
            throw std::runtime_error("injected failure after durable event");
    }

    std::optional<ArtifactValue> stagedValue(UInt64 transaction_id, size_t ordinal) const
    {
        const auto it = staged.find(LocatorKey{.database_uuid = database_uuid, .transaction_id = transaction_id, .ordinal = ordinal});
        if (it == staged.end())
            return std::nullopt;
        return it->second;
    }

    std::optional<ArtifactValue> findOppositeImage(const DatabaseSchemaWALStagedArtifactRef & selected) const
    {
        const auto prepare_it = prepares.find(current_transaction_id);
        if (prepare_it == prepares.end())
            conflict("artifact installation has no durable Prepare marker");
        const auto prepare = decodeDatabaseSchemaWALPrepare(prepare_it->second);
        for (size_t index = 0; index < prepare.staged_artifacts.size(); ++index)
        {
            const auto & candidate = prepare.staged_artifacts[index];
            if (candidate.kind == selected.kind && candidate.object == selected.object && candidate.image != selected.image)
            {
                auto result = stagedValue(current_transaction_id, index);
                if (!result)
                    conflict("opposite artifact image is missing from durable staging");
                return result;
            }
        }
        return std::nullopt;
    }

    std::optional<ArtifactValue> findOnlyImage(DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object) const
    {
        const auto prepare_it = prepares.find(current_transaction_id);
        if (prepare_it == prepares.end())
            conflict("artifact removal has no durable Prepare marker");
        const auto prepare = decodeDatabaseSchemaWALPrepare(prepare_it->second);
        std::optional<ArtifactValue> result;
        for (size_t index = 0; index < prepare.staged_artifacts.size(); ++index)
        {
            const auto & candidate = prepare.staged_artifacts[index];
            if (candidate.kind != kind || candidate.object != object)
                continue;
            if (result)
                conflict("artifact removal selected a pair having two staged images");
            result = stagedValue(current_transaction_id, index);
            if (!result)
                conflict("removed artifact image is missing from durable staging");
        }
        return result;
    }

    template <typename Map>
    static void eraseMapThrough(Map & map, UInt64 transaction_id)
    {
        for (auto it = map.begin(); it != map.end() && it->first <= transaction_id;)
            it = map.erase(it);
    }

    void eraseStagingThrough(UInt64 minimum_transaction_id, UInt64 maximum_transaction_id)
    {
        for (auto it = staged.begin(); it != staged.end();)
        {
            if (it->first.transaction_id >= minimum_transaction_id && it->first.transaction_id <= maximum_transaction_id)
                it = staged.erase(it);
            else
                ++it;
        }
    }

    size_t failure_event = std::numeric_limits<size_t>::max();
    FailurePosition failure_position = FailurePosition::Before;
};

template <typename Function>
DatabaseSchemaMutationIndeterminateDurabilityError expectIndeterminate(Function && function)
{
    try
    {
        function();
        ADD_FAILURE() << "expected indeterminate durability error";
    }
    catch (const DatabaseSchemaMutationIndeterminateDurabilityError & error)
    {
        return error;
    }
    throw std::logic_error("indeterminate durability assertion did not catch the expected error");
}

TEST(DatabaseSchemaMutationTransaction, ReplacementStagesDistinctCanonicalOrdinalsAndCommitsInOrder)
{
    Fixture fixture;
    auto transition = fixture.replacementTransition();
    FakeDurableStorage storage(fixture.database_uuid, 100, fixture.definition_only_state);
    const auto & artifacts = transition.getPrepare().staged_artifacts;
    ASSERT_EQ(artifacts.size(), 2);
    storage.seedInstalled(artifacts.front(), fixture.record_bytes);
    auto guard = storage.issueGuard();

    const auto commit = executeDatabaseSchemaMutation(storage, guard, transition);

    EXPECT_EQ(
        storage.events, (std::vector<String>{"stage", "stage", "finish-staging", "prepare", "install", "finish-installation", "commit"}));
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
    ASSERT_EQ(storage.staged.size(), 2);
    const auto first = storage.staged.find({.database_uuid = fixture.database_uuid, .transaction_id = 101, .ordinal = 0});
    const auto second = storage.staged.find({.database_uuid = fixture.database_uuid, .transaction_id = 101, .ordinal = 1});
    ASSERT_NE(first, storage.staged.end());
    ASSERT_NE(second, storage.staged.end());
    EXPECT_EQ(first->second.reference, artifacts[0]);
    EXPECT_EQ(second->second.reference, artifacts[1]);
    EXPECT_NE(first->second.bytes, second->second.bytes);
    EXPECT_EQ(decodeDatabaseSchemaWALCommit(storage.commits.at(101)), commit);
}

TEST(DatabaseSchemaMutationTransaction, EncodingAndPlanningFailBeforeTheFirstStorageCall)
{
    Fixture fixture;
    auto transition = fixture.firstTransition();
    FakeDurableStorage storage(fixture.database_uuid);
    auto guard = storage.issueGuard();
    DatabaseSchemaWALLimits limits;
    limits.maximum_encoded_bytes = 1;

    EXPECT_THROW(static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition, limits)), DatabaseSchemaWALError);
    EXPECT_EQ(storage.validation_calls, 0);
    EXPECT_TRUE(storage.events.empty());
}

TEST(DatabaseSchemaMutationTransaction, MoveLeavesOneUsableGuardAndRejectsMovedFromLifecycleCallsBeforeStorage)
{
    Fixture fixture;

    {
        auto transition = fixture.firstTransition();
        FakeDurableStorage storage(fixture.database_uuid);
        auto source = storage.issueGuard();
        auto guard = std::move(source);

        EXPECT_EQ(source.getDatabaseUUID(), UUIDHelpers::Nil);
        EXPECT_EQ(source.getOpaqueIdentity(), 0);
        EXPECT_EQ(source.getDurablePredecessorTransactionID(), 0);
        EXPECT_EQ(source.getState(), DatabaseSchemaMutationGuard::State::Finished);
        EXPECT_THROW(static_cast<void>(executeDatabaseSchemaMutation(storage, source, transition)), std::logic_error);
        EXPECT_EQ(storage.validation_calls, 0);
        EXPECT_TRUE(storage.events.empty());

        EXPECT_NO_THROW(static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition)));
    }

    {
        auto transition = fixture.firstTransition();
        FakeDurableStorage storage(fixture.database_uuid);
        auto source = storage.issueGuard();
        auto guard = std::move(source);

        EXPECT_THROW(static_cast<void>(recoverDatabaseSchemaMutation(storage, source, transition, std::nullopt)), std::logic_error);
        EXPECT_EQ(storage.validation_calls, 0);
        EXPECT_TRUE(storage.events.empty());
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
    }

    {
        FakeDurableStorage storage(fixture.database_uuid);
        auto source = storage.issueGuard();
        auto guard = std::move(source);

        EXPECT_THROW(discardUnpreparedDatabaseSchemaMutationStaging(storage, source, 100), std::logic_error);
        EXPECT_EQ(storage.discard_calls, 0);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
    }
}

static_assert(std::is_move_constructible_v<DatabaseSchemaMutationGuard>);
static_assert(!std::is_move_assignable_v<DatabaseSchemaMutationGuard>);

TEST(DatabaseSchemaMutationTransaction, PreparedExecutionMoveTransfersOneStorageBoundCapability)
{
    Fixture fixture;
    auto transition = fixture.firstTransition();
    FakeDurableStorage storage(fixture.database_uuid);
    auto guard = storage.issueGuard();
    auto source = prepareDatabaseSchemaMutationExecution(transition);
    validatePreparedDatabaseSchemaMutationExecution(storage, guard, source);
    auto prepared = std::move(source);

    EXPECT_THROW(validatePreparedDatabaseSchemaMutationExecution(storage, guard, source), std::logic_error);
    EXPECT_DEATH(static_cast<void>(executePreparedDatabaseSchemaMutation(storage, guard, std::move(source))), "");
    EXPECT_NO_THROW(static_cast<void>(executePreparedDatabaseSchemaMutation(storage, guard, std::move(prepared))));
}

TEST(DatabaseSchemaMutationTransaction, PreparedExecutionCannotBeAppliedThroughAnotherStorageInstance)
{
    Fixture fixture;
    auto transition = fixture.firstTransition();
    FakeDurableStorage validated_storage(fixture.database_uuid);
    FakeDurableStorage foreign_storage(fixture.database_uuid);
    auto guard = validated_storage.issueGuard();
    auto prepared = prepareDatabaseSchemaMutationExecution(transition);
    validatePreparedDatabaseSchemaMutationExecution(validated_storage, guard, prepared);

    EXPECT_DEATH(static_cast<void>(executePreparedDatabaseSchemaMutation(foreign_storage, guard, std::move(prepared))), "");
    EXPECT_TRUE(foreign_storage.events.empty());
}

static_assert(std::is_move_constructible_v<PreparedDatabaseSchemaMutationExecution>);
static_assert(!std::is_move_assignable_v<PreparedDatabaseSchemaMutationExecution>);

TEST(DatabaseSchemaMutationTransaction, PrePrepareFailureLeavesOnlyProvablyDiscardableStaging)
{
    Fixture fixture;
    auto transition = fixture.firstTransition();
    FakeDurableStorage storage(fixture.database_uuid);
    auto guard = storage.issueGuard();
    storage.configureFailure(1, FailurePosition::After);

    EXPECT_THROW(static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition)), std::runtime_error);
    EXPECT_FALSE(storage.staged.empty());
    EXPECT_TRUE(storage.prepares.empty());
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);

    storage.disableFailure();
    discardUnpreparedDatabaseSchemaMutationStaging(storage, guard, 100);
    EXPECT_TRUE(storage.staged.empty());
    EXPECT_EQ(storage.durable_predecessor_transaction_id, 0);
    EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Finished);
}

TEST(DatabaseSchemaMutationTransaction, PrepareAttemptIsTypedFailStoppedAndResolvedFromDurableTruth)
{
    for (const auto position : {FailurePosition::Before, FailurePosition::After})
    {
        Fixture fixture;
        auto transition = fixture.firstTransition();
        FakeDurableStorage storage(fixture.database_uuid);
        auto guard = storage.issueGuard();
        storage.configureFailure(3, position);

        const auto error = expectIndeterminate([&] { static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition)); });
        EXPECT_EQ(error.transaction_id, 100);
        EXPECT_EQ(error.phase, DatabaseSchemaMutationDurabilityPhase::PrepareMarker);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);
        EXPECT_EQ(storage.recovery_required_transaction, 100);

        storage.disableFailure();
        auto recovery_guard = storage.issueGuard();
        EXPECT_THROW(
            static_cast<void>(executeDatabaseSchemaMutation(storage, recovery_guard, transition)),
            DatabaseSchemaMutationReplayConflictError);
        if (position == FailurePosition::Before)
        {
            EXPECT_TRUE(storage.prepares.empty());
            discardUnpreparedDatabaseSchemaMutationStaging(storage, recovery_guard, 100);
            EXPECT_EQ(storage.durable_predecessor_transaction_id, 0);
        }
        else
        {
            ASSERT_TRUE(storage.prepares.contains(100));
            EXPECT_EQ(
                recoverDatabaseSchemaMutation(storage, recovery_guard, transition, std::nullopt),
                DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
            retireRolledBackDatabaseSchemaMutation(storage, recovery_guard, 100);
            EXPECT_EQ(storage.durable_predecessor_transaction_id, 100);
            EXPECT_TRUE(storage.prepares.empty());
        }
        EXPECT_FALSE(storage.recovery_required_transaction);
    }
}

TEST(DatabaseSchemaMutationTransaction, EveryPostPrepareFailureReportsItsExactPhaseAndRecovers)
{
    const std::vector<std::pair<size_t, DatabaseSchemaMutationDurabilityPhase>> cases{
        {4, DatabaseSchemaMutationDurabilityPhase::AfterImage},
        {5, DatabaseSchemaMutationDurabilityPhase::InstallationBarrier},
        {6, DatabaseSchemaMutationDurabilityPhase::CommitMarker},
    };
    for (const auto & [event, phase] : cases)
    {
        for (const auto position : {FailurePosition::Before, FailurePosition::After})
        {
            Fixture fixture;
            auto transition = fixture.firstTransition();
            FakeDurableStorage storage(fixture.database_uuid);
            auto guard = storage.issueGuard();
            storage.configureFailure(event, position);

            const auto error = expectIndeterminate([&] { static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition)); });
            EXPECT_EQ(error.phase, phase);
            storage.disableFailure();
            std::optional<DatabaseSchemaWALCommit> commit;
            if (storage.commits.contains(100))
                commit = decodeDatabaseSchemaWALCommit(storage.commits.at(100));
            const auto decision = recoverDatabaseSchemaMutation(storage, guard, transition, commit);
            EXPECT_EQ(
                decision,
                commit ? DatabaseSchemaWALRecoveryDecision::CompleteCommitted : DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
            EXPECT_FALSE(storage.recovery_required_transaction);
            if (!commit)
                retireRolledBackDatabaseSchemaMutation(storage, guard, 100);
        }
    }
}

TEST(DatabaseSchemaMutationTransaction, RecoveryFailuresRemainFailStoppedAndReplayConverges)
{
    const std::vector<std::pair<size_t, DatabaseSchemaMutationDurabilityPhase>> cases{
        {1, DatabaseSchemaMutationDurabilityPhase::RecoveryImage},
        {2, DatabaseSchemaMutationDurabilityPhase::RecoveryBarrier},
        {3, DatabaseSchemaMutationDurabilityPhase::RecoveryMarker},
    };
    for (const auto & [recovery_event, phase] : cases)
    {
        Fixture fixture;
        auto transition = fixture.firstTransition();
        FakeDurableStorage storage(fixture.database_uuid);
        auto guard = storage.issueGuard();
        storage.configureFailure(5, FailurePosition::Before);
        static_cast<void>(expectIndeterminate([&] { static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition)); }));
        storage.disableFailure();
        const size_t event = storage.events.size() + recovery_event;
        storage.configureFailure(event, FailurePosition::After);

        const auto error
            = expectIndeterminate([&] { static_cast<void>(recoverDatabaseSchemaMutation(storage, guard, transition, std::nullopt)); });
        EXPECT_EQ(error.phase, phase);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::RecoveryRequired);

        storage.disableFailure();
        EXPECT_EQ(
            recoverDatabaseSchemaMutation(storage, guard, transition, std::nullopt),
            DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
        EXPECT_TRUE(storage.installed.empty());
    }
}

TEST(DatabaseSchemaMutationTransaction, ReplayIsIdempotentAndConflictingLocatorContentIsRejected)
{
    Fixture fixture;
    auto transition = fixture.firstTransition();
    const auto & artifact = transition.getPrepare().staged_artifacts.front();
    const auto locator = makeDatabaseSchemaWALStagedArtifactLocator(fixture.database_uuid, 100, 0);
    FakeDurableStorage storage(fixture.database_uuid);
    auto guard = storage.issueGuard();
    storage.validateMutationGuardAndDurablePredecessor(guard, std::nullopt, 100);

    storage.stageArtifact(locator, artifact, fixture.record_bytes);
    storage.stageArtifact(locator, artifact, fixture.record_bytes);
    EXPECT_EQ(storage.staged.size(), 1);

    auto conflicting = artifact;
    conflicting.content_hash[0] ^= 1;
    EXPECT_THROW(storage.stageArtifact(locator, conflicting, fixture.record_bytes), DatabaseSchemaMutationReplayConflictError);
    EXPECT_EQ(storage.staged.at(locatorKey(locator)).reference, artifact);
}

TEST(DatabaseSchemaMutationTransaction, StalePredecessorAndNonmonotonicTransactionAreRejectedBeforeStaging)
{
    Fixture fixture;
    auto transition = fixture.replacementTransition();
    FakeDurableStorage storage(fixture.database_uuid, 100, fixture.definition_only_state);
    auto guard = storage.issueGuard();
    storage.durable_predecessor_transaction_id = 101;

    EXPECT_THROW(
        static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition)), DatabaseSchemaMutationReplayConflictError);
    EXPECT_TRUE(storage.events.empty());
    EXPECT_TRUE(storage.staged.empty());

    auto nonmonotonic_guard = storage.issueGuard();
    EXPECT_THROW(
        static_cast<void>(executeDatabaseSchemaMutation(storage, nonmonotonic_guard, transition)),
        DatabaseSchemaMutationReplayConflictError);
    EXPECT_TRUE(storage.events.empty());

    FakeDurableStorage wrong_state_storage(fixture.database_uuid, 100, std::nullopt);
    auto wrong_state_guard = wrong_state_storage.issueGuard();
    EXPECT_THROW(
        static_cast<void>(executeDatabaseSchemaMutation(wrong_state_storage, wrong_state_guard, transition)),
        DatabaseSchemaMutationReplayConflictError);
    EXPECT_TRUE(wrong_state_storage.events.empty());
}

TEST(DatabaseSchemaMutationTransaction, ValidatedCheckpointRetainsStagingUntilExplicitCompaction)
{
    Fixture fixture;
    auto transition = fixture.firstTransition();
    FakeDurableStorage storage(fixture.database_uuid);
    auto mutation_guard = storage.issueGuard();
    const auto commit = executeDatabaseSchemaMutation(storage, mutation_guard, transition);
    auto checkpoint = DatabaseSchemaWALCheckpointBuilder::build(
        1, commit, fixture.definition_only_state, fixture.definition_inventory, fixture.definition_graph);
    auto checkpoint_guard = storage.issueGuard();

    persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, checkpoint);
    persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, checkpoint);
    EXPECT_FALSE(storage.staged.empty());
    EXPECT_TRUE(storage.prepares.contains(100));
    EXPECT_TRUE(storage.commits.contains(100));

    String conflicting_checkpoint_bytes = encodeDatabaseSchemaWALCheckpoint(checkpoint.getCheckpoint());
    conflicting_checkpoint_bytes.push_back('x');
    EXPECT_THROW(
        storage.persistValidatedCheckpoint(
            checkpoint_guard,
            checkpoint.getCheckpoint(),
            conflicting_checkpoint_bytes,
            checkpoint.getInventorySnapshotBytes(),
            checkpoint.getSchemaGraphSnapshotBytes()),
        DatabaseSchemaMutationReplayConflictError);

    const auto & newer_reference = transition.getPrepare().staged_artifacts.front();
    const auto newer_locator = makeDatabaseSchemaWALStagedArtifactLocator(fixture.database_uuid, 101, 0);
    storage.current_transaction_id = 101;
    storage.stageArtifact(newer_locator, newer_reference, fixture.record_bytes);

    compactDatabaseSchemaWALThroughValidatedCheckpoint(storage, checkpoint_guard, checkpoint);
    ASSERT_EQ(storage.staged.size(), 1);
    EXPECT_TRUE(storage.staged.contains(locatorKey(newer_locator)));
    EXPECT_TRUE(storage.prepares.empty());
    EXPECT_TRUE(storage.commits.empty());
    EXPECT_EQ(storage.compacted_through, 100);
    EXPECT_NO_THROW(compactDatabaseSchemaWALThroughValidatedCheckpoint(storage, checkpoint_guard, checkpoint));
}

DatabaseSchemaWALValidatedTransition
diamondTransition(const Fixture & fixture, SchemaObjectID & base, SchemaObjectID & left, SchemaObjectID & right, SchemaObjectID & top)
{
    base = objectID(SchemaObjectKind::SyntheticTestObject, fixture.database_uuid, uuid(0x7000, 1));
    left = objectID(SchemaObjectKind::SyntheticTestObject, fixture.database_uuid, uuid(0x7000, 2));
    right = objectID(SchemaObjectKind::SyntheticTestObject, fixture.database_uuid, uuid(0x7000, 3));
    top = objectID(SchemaObjectKind::SyntheticTestObject, fixture.database_uuid, uuid(0x7000, 4));
    const std::vector<SchemaObjectID> nodes{base, left, right, top};
    const std::vector<SchemaObjectDependencyEdge> edges{
        {.dependent = left, .dependency = base, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
        {.dependent = right, .dependency = base, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
        {.dependent = top, .dependency = left, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
        {.dependent = top, .dependency = right, .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnObject},
    };
    SchemaObjectDependencyGraphMutation graph_delta{
        .node_additions = nodes,
        .node_removals = {},
        .edge_additions = edges,
        .edge_removals = {},
    };
    const auto after_graph = SchemaObjectDependencyGraph::applyMutation(fixture.definition_graph, graph_delta);
    const auto after_state = state(
        fixture.database_uuid,
        fixture.dependent_object_state.database_catalog_epoch + 1,
        dependent_object_authority_capability_mask,
        fixture.definition_inventory,
        after_graph);

    std::vector<DatabaseSchemaWALDependentObjectDelta> deltas;
    std::vector<DatabaseSchemaWALStagedArtifact> artifacts;
    deltas.reserve(nodes.size());
    artifacts.reserve(nodes.size());
    for (size_t index = 0; index < nodes.size(); ++index)
    {
        const String bytes = "metadata-" + std::to_string(index);
        deltas.push_back(
            DatabaseSchemaWALDependentObjectDelta{
                .object = nodes[index],
                .before = std::nullopt,
                .after = DatabaseSchemaWALDependentObjectState{
                    .object_schema_revision = 1,
                    .metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
                        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, bytes),
                    .sidecar_record_hash = std::nullopt,
                    .expectation_record_hash = std::nullopt,
                },
            });
        artifacts.push_back(
            DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = nodes[index],
                .revision = 1,
                .canonical_bytes = bytes,
            });
    }

    return DatabaseSchemaWALTransitionBuilder::build(
        102,
        DatabaseSchemaWALTransitionBase{
            .authority_state = fixture.dependent_object_state,
            .authority_inventory = fixture.definition_inventory,
            .schema_graph = fixture.definition_graph,
        },
        after_state,
        {},
        std::move(deltas),
        std::move(graph_delta),
        std::move(artifacts));
}

size_t positionOf(std::span<const SchemaObjectID> values, const SchemaObjectID & object)
{
    const auto it = std::find(values.begin(), values.end(), object);
    if (it == values.end())
        throw std::logic_error("expected object is absent from action order");
    return static_cast<size_t>(it - values.begin());
}

TEST(DatabaseSchemaMutationTransaction, GraphlessPhysicalSuccessorInstallsExactMetadataForEveryStoredObjectKind)
{
    const Fixture fixture;
    for (const auto object_kind : {SchemaObjectKind::Table, SchemaObjectKind::View, SchemaObjectKind::Dictionary})
    {
        SCOPED_TRACE(static_cast<UInt64>(object_kind));
        const auto transition = graphlessPhysicalTransition(fixture, object_kind, true);
        const auto actions = planValidatedDatabaseSchemaArtifactInstallation(transition, DatabaseSchemaWALStagedArtifactImage::After);
        ASSERT_FALSE(actions.empty());
        const auto metadata_install = std::find_if(
            actions.begin(),
            actions.end(),
            [](const auto & action) { return action.action == DatabaseSchemaMutationArtifactActionKind::Install; });
        ASSERT_NE(metadata_install, actions.end());
        EXPECT_EQ(metadata_install->kind, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata);
        EXPECT_EQ(metadata_install->object.kind, object_kind);
        EXPECT_EQ(
            std::count_if(
                actions.begin(),
                actions.end(),
                [](const auto & action) { return action.action == DatabaseSchemaMutationArtifactActionKind::Install; }),
            1);
        EXPECT_NO_THROW(static_cast<void>(prepareDatabaseSchemaMutationExecution(transition)));
    }
}

TEST(DatabaseSchemaMutationTransaction, GraphlessPhysicalMetadataExceptionRemainsDirectionAndKindBound)
{
    const Fixture fixture;

    const auto table_admission = graphlessPhysicalTransition(fixture, SchemaObjectKind::Table, false);
    EXPECT_NO_THROW(
        static_cast<void>(planValidatedDatabaseSchemaArtifactInstallation(table_admission, DatabaseSchemaWALStagedArtifactImage::Before)));

    for (const auto object_kind : {SchemaObjectKind::View, SchemaObjectKind::Dictionary})
    {
        SCOPED_TRACE(static_cast<UInt64>(object_kind));
        const auto unsupported_admission = graphlessPhysicalTransition(fixture, object_kind, false);
        EXPECT_THROW(
            static_cast<void>(
                planValidatedDatabaseSchemaArtifactInstallation(unsupported_admission, DatabaseSchemaWALStagedArtifactImage::Before)),
            std::logic_error);
    }

    const auto synthetic_physicalization = graphlessPhysicalTransition(fixture, SchemaObjectKind::SyntheticTestObject, true);
    EXPECT_THROW(
        static_cast<void>(
            planValidatedDatabaseSchemaArtifactInstallation(synthetic_physicalization, DatabaseSchemaWALStagedArtifactImage::After)),
        std::logic_error);
}

TEST(DatabaseSchemaMutationTransaction, SyntheticDependentObjectDiamondUsesDependencySafeInstallAndRollbackOrder)
{
    Fixture fixture;
    SchemaObjectID base;
    SchemaObjectID left;
    SchemaObjectID right;
    SchemaObjectID top;
    auto transition = diamondTransition(fixture, base, left, right, top);

    const auto after = planValidatedDatabaseSchemaArtifactInstallation(transition, DatabaseSchemaWALStagedArtifactImage::After);
    std::vector<SchemaObjectID> install_order;
    for (const auto & action : after)
    {
        EXPECT_EQ(action.action, DatabaseSchemaMutationArtifactActionKind::Install);
        install_order.push_back(action.object);
    }
    EXPECT_LT(positionOf(install_order, base), positionOf(install_order, left));
    EXPECT_LT(positionOf(install_order, base), positionOf(install_order, right));
    EXPECT_LT(positionOf(install_order, left), positionOf(install_order, top));
    EXPECT_LT(positionOf(install_order, right), positionOf(install_order, top));

    const auto before = planValidatedDatabaseSchemaArtifactInstallation(transition, DatabaseSchemaWALStagedArtifactImage::Before);
    std::vector<SchemaObjectID> remove_order;
    for (const auto & action : before)
    {
        EXPECT_EQ(action.action, DatabaseSchemaMutationArtifactActionKind::Remove);
        remove_order.push_back(action.object);
    }
    EXPECT_LT(positionOf(remove_order, top), positionOf(remove_order, left));
    EXPECT_LT(positionOf(remove_order, top), positionOf(remove_order, right));
    EXPECT_LT(positionOf(remove_order, left), positionOf(remove_order, base));
    EXPECT_LT(positionOf(remove_order, right), positionOf(remove_order, base));
}

}
}
