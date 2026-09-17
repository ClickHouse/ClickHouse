#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AtomicAuthorityStartup.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AtomicTableMetadataValidator.h>
#include <Databases/UDT/AuthorityStorageOperationGate.h>
#include <Databases/UDT/AuthorityVerificationBatchExecutor.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>
#include <Databases/UDT/AuthorityVerificationScheduler.h>
#include <Databases/UDT/ILifecycleAdapter.h>
#include <Databases/UDT/SyntheticObjectMetadata.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <Disks/tests/gtest_disk.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/UDT/QueryResultCacheStorageDependencies.h>

#include <Core/BackgroundSchedulePool.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Storages/ConstraintsDescription.h>
#include <Storages/MemorySettings.h>
#include <Storages/StorageMemory.h>

#include <Common/AsyncLoader.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/tests/gtest_global_context.h>

#include <base/scope_guard.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int NOT_IMPLEMENTED;
}

namespace DB::FailPoints
{
extern const char udt_authority_runtime_pause_after_publication_waiter_registration[];
}

namespace DB::UDT
{
namespace
{

using StorageError = AtomicDatabaseSchemaMutationStorageError;

template <typename Function>
void expectMappedTableMutationRejected(std::string_view operation, Function && function)
{
    try
    {
        function();
        FAIL() << operation << " unexpectedly accepted a mapped Table";
    }
    catch (const Exception & error)
    {
        String expected_message = "Cannot ";
        expected_message += operation;
        expected_message += " mapped table";
        EXPECT_EQ(error.code(), ErrorCodes::NOT_IMPLEMENTED) << error.message();
        EXPECT_NE(error.message().find(expected_message), String::npos) << error.message();
    }
    catch (...)
    {
        FAIL() << operation << " returned an unexpected exception type";
    }
}

template <typename Function>
void expectMappedTableExactTemporaryDetachRequired(Function && function)
{
    try
    {
        function();
        FAIL() << "ATTACH unexpectedly accepted a mapped Table without an exact temporary-detach image";
    }
    catch (const Exception & error)
    {
        EXPECT_EQ(error.code(), ErrorCodes::ABORTED) << error.message();
        EXPECT_NE(error.message().find("not the exact temporarily detached object requested by ATTACH"), String::npos) << error.message();
    }
    catch (...)
    {
        FAIL() << "ATTACH returned an unexpected exception type";
    }
}

template <typename Function>
void expectMappedTableAuthorityMismatchRejected(std::string_view operation, Function && function)
{
    try
    {
        function();
        FAIL() << operation << " unexpectedly accepted a split-brain Atomic authority";
    }
    catch (const DatabaseSchemaMutationReplayConflictError & error)
    {
        const String message = error.what();
        EXPECT_TRUE(
            message.contains("different published and durable user-defined type authority states")
            || message.contains("published dependent-object-capable authority differs from the durable WAL head"))
            << message;
    }
    catch (...)
    {
        FAIL() << operation << " returned an unexpected exception type";
    }
}

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

Digest digest(UInt8 first)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(first + index);
    return result;
}

template <typename Predicate>
bool waitForDatabaseTestCondition(Predicate && predicate, std::chrono::steady_clock::duration timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!predicate())
    {
        if (std::chrono::steady_clock::now() >= deadline)
            return predicate();
        std::this_thread::yield();
    }
    return true;
}

bool runDatabaseTestSchedulePoolSentinel(const ContextPtr & context)
{
    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    auto task = context->getSchedulePool()->createTask(
        StorageID::createEmpty(),
        "AtomicUDTVerificationTestSentinel",
        [&]
        {
            {
                std::lock_guard lock(mutex);
                completed = true;
            }
            cv.notify_one();
        });
    if (!task->activateAndSchedule())
        return false;
    std::unique_lock lock(mutex);
    const bool result = cv.wait_for(lock, std::chrono::seconds(5), [&] { return completed; });
    lock.unlock();
    task->deactivate();
    return result;
}

bool waitForDatabaseTestFailPointPause(const String & fail_point_name, std::chrono::milliseconds timeout)
{
    std::packaged_task<void()> wait_task([&] { FailPointInjection::waitForPause(fail_point_name); });
    auto wait_result = wait_task.get_future();
    std::thread wait_thread(std::move(wait_task));
    SCOPE_EXIT({
        if (wait_thread.joinable())
            wait_thread.join();
    });

    if (wait_result.wait_for(timeout) != std::future_status::ready)
    {
        /// Wake both the waiter and a target that may have reached the
        /// failpoint concurrently with the timeout.
        FailPointInjection::disableFailPoint(fail_point_name);
        return false;
    }

    wait_thread.join();
    wait_result.get();
    return true;
}

String toHex(std::string_view bytes)
{
    static constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    String result;
    result.reserve(2 * bytes.size());
    for (const unsigned char byte : bytes)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

String toHex(const Digest & value)
{
    return toHex({reinterpret_cast<const char *>(value.data()), value.size()});
}

String canonicalAttachTypeSQL(
    std::string_view normalized_name,
    const DefinitionIdentity & identity,
    const Digest & definition_hash,
    std::string_view canonical_physical_template_sql,
    std::string_view comment = {})
{
    String result = "ATTACH TYPE " + String(normalized_name) + " UUID '" + toString(identity.type_uuid) + "' REVISION "
        + std::to_string(identity.revision) + " AS " + String(canonical_physical_template_sql) + " DEFINITION HASH '"
        + toHex(definition_hash) + "'";
    if (!comment.empty())
        result += " COMMENT '" + String(comment) + "'";
    return result;
}

String rewriteInternalChecksum(String bytes, std::string_view domain)
{
    EXPECT_GE(bytes.size(), sizeof(Digest));
    const auto prefix = std::string_view(bytes).substr(0, bytes.size() - sizeof(Digest));
    const Digest checksum = hashFramedDomainSeparated(domain, prefix);
    std::copy(checksum.begin(), checksum.end(), bytes.end() - sizeof(Digest));
    return bytes;
}

String readFile(const DiskPtr & disk, const String & path)
{
    const size_t size = disk->getFileSize(path);
    auto input = disk->readFile(path, ReadSettings{}, size);
    String result;
    readStringUntilEOF(result, *input);
    return result;
}

void writeFile(const DiskPtr & disk, const String & path, std::string_view bytes)
{
    disk->createDirectories(std::filesystem::path(path).parent_path().generic_string());
    auto output = disk->writeFile(path, std::max<size_t>(bytes.size(), 1), WriteMode::Rewrite, WriteSettings{});
    writeString(bytes, *output);
    output->finalize();
    output->sync();
}

Record definitionRecord(UUID database_uuid, UUID type_uuid)
{
    DefinitionInput input;
    input.identity = {.database_uuid = database_uuid, .type_uuid = type_uuid, .revision = 7};
    input.normalized_name = "db.Alpha";
    input.normalized_local_name = "Alpha";
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    const auto definitions = TemplateChecker::checkAll({std::move(input)});
    if (definitions.size() != 1)
        throw std::logic_error("test definition checker returned an unexpected batch");
    const auto & definition = *definitions.front();
    return makeRecord(
        definition,
        {
            .canonical_definition_sql
            = canonicalAttachTypeSQL(definition.getNormalizedName(), definition.getIdentity(), definition.getDefinitionHash(), "UInt64"),
            .canonical_physical_template_sql = "UInt64",
            .owner_uuid = uuid(0x9000, 1),
            .owner_display_name = "owner",
            .comment = {},
            .creation_time_us_utc = 100,
        });
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

String noArguments()
{
    constexpr std::array<char, 3> bytes{1, 0, 0};
    return {bytes.data(), bytes.size()};
}

PersistedTypeDescriptor descriptor(const Record & definition, Digest storage_fingerprint = digest(0x40))
{
    const String canonical_arguments = noArguments();
    const String canonical_physical_type = "UInt64";
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

PersistedTypeReferences references(
    const SchemaObjectID & object,
    UInt64 revision,
    const Record & definition,
    Digest physical_fingerprint,
    Digest descriptor_storage_fingerprint = digest(0x40))
{
    PersistedTypeReferences result;
    result.object = object;
    result.object_schema_revision = revision;
    result.physical_schema_fingerprint = physical_fingerprint;
    result.descriptors = {descriptor(definition, descriptor_storage_fingerprint)};
    result.occurrence_paths = {{
        .section
        = object.kind == SchemaObjectKind::Table ? PersistedTypePathSection::ColumnType : PersistedTypePathSection::SyntheticPayload,
        .object_ordinal = 0,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    }};
    result.uses = {{.path_id = 0, .descriptor_id = 0}};
    return result;
}

struct Model
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
    AuthorityState definition_only_state
        = state(database_uuid, 1, definition_authority_capability_mask, definition_inventory, definition_graph);
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

    DatabaseSchemaWALValidatedTransition dependentObjectActivationTransition() const
    {
        return DatabaseSchemaWALTransitionBuilder::build(
            101,
            DatabaseSchemaWALTransitionBase{
                .authority_state = definition_only_state,
                .authority_inventory = definition_inventory,
                .schema_graph = definition_graph,
            },
            dependent_object_state,
            {},
            {},
            {},
            {});
    }

    DatabaseSchemaWALValidatedTransition definitionPresentationTransition(
        UInt64 transaction_id,
        const Record & before_record,
        const AuthorityState & before_state,
        AuthorityInventory::Ptr before_inventory,
        const Record & after_record) const
    {
        const auto before_leaf = definitionLeaf(before_record);
        const auto after_leaf = definitionLeaf(after_record);
        const auto after_inventory = inventory({after_leaf});
        const auto after_state = state(
            database_uuid,
            before_state.database_catalog_epoch + 1,
            before_state.persistent_capability_mask,
            after_inventory,
            definition_graph);
        return DatabaseSchemaWALTransitionBuilder::build(
            transaction_id,
            DatabaseSchemaWALTransitionBase{
                .authority_state = before_state,
                .authority_inventory = std::move(before_inventory),
                .schema_graph = definition_graph,
            },
            after_state,
            {DatabaseSchemaWALAuthorityRecordDelta{
                .key = before_leaf.key,
                .before = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = before_leaf.object_revision,
                    .canonical_record_hash = before_leaf.canonical_record_hash,
                },
                .after = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = after_leaf.object_revision,
                    .canonical_record_hash = after_leaf.canonical_record_hash,
                },
            }},
            {},
            {},
            {
                DatabaseSchemaWALStagedArtifact{
                    .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                    .image = DatabaseSchemaWALStagedArtifactImage::Before,
                    .object = type_object,
                    .revision = before_record.identity.revision,
                    .canonical_bytes = encodeRecord(before_record),
                },
                DatabaseSchemaWALStagedArtifact{
                    .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
                    .image = DatabaseSchemaWALStagedArtifactImage::After,
                    .object = type_object,
                    .revision = after_record.identity.revision,
                    .canonical_bytes = encodeRecord(after_record),
                },
            });
    }

    DatabaseSchemaWALValidatedTransition syntheticTransition(SchemaObjectID synthetic) const
    {
        const SchemaObjectDependencyGraphMutation graph_delta{
            .node_additions = {synthetic},
            .node_removals = {},
            .edge_additions = {{
                .dependent = synthetic,
                .dependency = type_object,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            }},
            .edge_removals = {},
        };
        const auto after_graph = SchemaObjectDependencyGraph::applyMutation(definition_graph, graph_delta);
        const auto after_state = state(
            database_uuid,
            dependent_object_state.database_catalog_epoch + 1,
            dependent_object_authority_capability_mask,
            definition_inventory,
            after_graph);
        const String metadata = "metadata-0";
        return DatabaseSchemaWALTransitionBuilder::build(
            102,
            DatabaseSchemaWALTransitionBase{
                .authority_state = dependent_object_state,
                .authority_inventory = definition_inventory,
                .schema_graph = definition_graph,
            },
            after_state,
            {},
            {DatabaseSchemaWALDependentObjectDelta{
                .object = synthetic,
                .before = std::nullopt,
                .after = DatabaseSchemaWALDependentObjectState{
                    .object_schema_revision = 1,
                    .metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
                        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata),
                    .sidecar_record_hash = std::nullopt,
                    .expectation_record_hash = std::nullopt,
                },
            }},
            graph_delta,
            {DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = synthetic,
                .revision = 1,
                .canonical_bytes = metadata,
            }});
    }

    DatabaseSchemaWALValidatedTransition syntheticSidecarTransition(SchemaObjectID synthetic) const
    {
        constexpr UInt64 revision = 2;
        const auto metadata_record = makeSyntheticObjectMetadata(
            synthetic,
            revision,
            "synthetic.with-references",
            {SyntheticObjectPhysicalOccurrence{
                .path = {
                    .section = PersistedTypePathSection::SyntheticPayload,
                    .object_ordinal = 0,
                    .occurrence_ordinal = 0,
                    .type_child_ordinals = {},
                },
                .canonical_physical_type = "UInt64",
                .storage_fingerprint = digest(0x40),
                .selected_semantic_capabilities = record.semantic_capabilities,
            }});
        const Digest physical_fingerprint = metadata_record.physical_schema_fingerprint;
        const String metadata = encodeSyntheticObjectMetadata(metadata_record);
        const auto sidecar = references(synthetic, revision, record, physical_fingerprint);
        const String sidecar_bytes = encodePersistedTypeReferences(sidecar);
        const Digest sidecar_hash = computePersistedTypeReferencesSidecarHash(sidecar);
        const SidecarExpectationRecord expectation{
            .object = synthetic,
            .object_schema_revision = revision,
            .sidecar_hash = sidecar_hash,
            .physical_schema_fingerprint = physical_fingerprint,
        };
        const String expectation_bytes = encodeSidecarExpectationRecord(expectation);
        const auto expectation_leaf = expectationLeaf(expectation);
        const auto after_inventory = inventory({leaf, expectation_leaf});
        const SchemaObjectDependencyGraphMutation graph_delta{
            .node_additions = {synthetic},
            .node_removals = {},
            .edge_additions = {{
                .dependent = synthetic,
                .dependency = type_object,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            }},
            .edge_removals = {},
        };
        const auto after_graph = SchemaObjectDependencyGraph::applyMutation(definition_graph, graph_delta);
        const auto after_state = state(
            database_uuid,
            dependent_object_state.database_catalog_epoch + 1,
            dependent_object_authority_capability_mask,
            after_inventory,
            after_graph);
        return DatabaseSchemaWALTransitionBuilder::build(
            102,
            DatabaseSchemaWALTransitionBase{
                .authority_state = dependent_object_state,
                .authority_inventory = definition_inventory,
                .schema_graph = definition_graph,
            },
            after_state,
            {DatabaseSchemaWALAuthorityRecordDelta{
                .key = expectation_leaf.key,
                .before = std::nullopt,
                .after = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = expectation_leaf.object_revision,
                    .canonical_record_hash = expectation_leaf.canonical_record_hash,
                },
            }},
            {DatabaseSchemaWALDependentObjectDelta{
                .object = synthetic,
                .before = std::nullopt,
                .after = DatabaseSchemaWALDependentObjectState{
                    .object_schema_revision = revision,
                    .metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
                        DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata),
                    .sidecar_record_hash = sidecar_hash,
                    .expectation_record_hash = expectation_leaf.canonical_record_hash,
                },
            }},
            graph_delta,
            {
                DatabaseSchemaWALStagedArtifact{
                    .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
                    .image = DatabaseSchemaWALStagedArtifactImage::After,
                    .object = synthetic,
                    .revision = revision,
                    .canonical_bytes = sidecar_bytes,
                },
                DatabaseSchemaWALStagedArtifact{
                    .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
                    .image = DatabaseSchemaWALStagedArtifactImage::After,
                    .object = synthetic,
                    .revision = revision,
                    .canonical_bytes = metadata,
                },
                DatabaseSchemaWALStagedArtifact{
                    .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
                    .image = DatabaseSchemaWALStagedArtifactImage::After,
                    .object = synthetic,
                    .revision = revision,
                    .canonical_bytes = expectation_bytes,
                },
            });
    }

    DatabaseSchemaWALValidatedTransition tableSidecarTransition(
        SchemaObjectID table,
        const String & object_name = "events/2026",
        const String & metadata = "table-metadata",
        const std::optional<String> & physical_before = std::nullopt,
        const std::optional<PersistedTypeReferences> & persisted_references = std::nullopt) const
    {
        constexpr UInt64 revision = 3;
        const auto sidecar = persisted_references ? *persisted_references : references(table, revision, record, digest(0xd0));
        const Digest physical_fingerprint = sidecar.physical_schema_fingerprint;
        const String sidecar_bytes = encodePersistedTypeReferences(sidecar);
        const Digest sidecar_hash = computePersistedTypeReferencesSidecarHash(sidecar);
        const Digest metadata_hash
            = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata);
        const DependentObjectMetadataInstallationRecord installation{
            .object = table,
            .object_schema_revision = revision,
            .object_name = object_name,
            .metadata_artifact_hash = metadata_hash,
        };
        const String installation_bytes = encodeDependentObjectMetadataInstallationRecord(installation);
        const SidecarExpectationRecord expectation{
            .object = table,
            .object_schema_revision = revision,
            .sidecar_hash = sidecar_hash,
            .physical_schema_fingerprint = physical_fingerprint,
            .installation_record_hash = computeDependentObjectMetadataInstallationRecordHash(installation),
        };
        const String expectation_bytes = encodeSidecarExpectationRecord(expectation);
        const auto expectation_leaf = expectationLeaf(expectation);
        const auto after_inventory = inventory({leaf, expectation_leaf});
        const SchemaObjectDependencyGraphMutation graph_delta{
            .node_additions = {table},
            .node_removals = {},
            .edge_additions = {{
                .dependent = table,
                .dependency = type_object,
                .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
            }},
            .edge_removals = {},
        };
        const auto after_graph = SchemaObjectDependencyGraph::applyMutation(definition_graph, graph_delta);
        const auto after_state = state(
            database_uuid,
            dependent_object_state.database_catalog_epoch + 1,
            dependent_object_authority_capability_mask,
            after_inventory,
            after_graph);
        std::optional<DatabaseSchemaWALDependentObjectState> before_state;
        if (physical_before)
        {
            before_state = DatabaseSchemaWALDependentObjectState{
                .object_schema_revision = revision - 1,
                .metadata_hash = computeDatabaseSchemaWALStagedArtifactHash(
                    DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, *physical_before),
                .sidecar_record_hash = std::nullopt,
                .expectation_record_hash = std::nullopt,
            };
        }
        std::vector<DatabaseSchemaWALStagedArtifact> artifacts{
            DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = table,
                .revision = revision,
                .canonical_bytes = sidecar_bytes,
            },
            DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = table,
                .revision = revision,
                .canonical_bytes = metadata,
            },
            DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = table,
                .revision = revision,
                .canonical_bytes = expectation_bytes,
            },
            DatabaseSchemaWALStagedArtifact{
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = table,
                .revision = revision,
                .canonical_bytes = installation_bytes,
            },
        };
        if (physical_before)
        {
            artifacts.push_back({
                .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
                .image = DatabaseSchemaWALStagedArtifactImage::Before,
                .object = table,
                .revision = revision - 1,
                .canonical_bytes = *physical_before,
            });
        }
        return DatabaseSchemaWALTransitionBuilder::build(
            102,
            DatabaseSchemaWALTransitionBase{
                .authority_state = dependent_object_state,
                .authority_inventory = definition_inventory,
                .schema_graph = definition_graph,
            },
            after_state,
            {DatabaseSchemaWALAuthorityRecordDelta{
                .key = expectation_leaf.key,
                .before = std::nullopt,
                .after = DatabaseSchemaWALAuthorityRecordState{
                    .object_revision = expectation_leaf.object_revision,
                    .canonical_record_hash = expectation_leaf.canonical_record_hash,
                },
            }},
            {DatabaseSchemaWALDependentObjectDelta{
                .object = table,
                .before = before_state,
                .after = DatabaseSchemaWALDependentObjectState{
                    .object_schema_revision = revision,
                    .metadata_hash = metadata_hash,
                    .sidecar_record_hash = sidecar_hash,
                    .expectation_record_hash = expectation_leaf.canonical_record_hash,
                },
            }},
            graph_delta,
            std::move(artifacts));
    }
};

class AtomicDatabaseSchemaMutationStorageTest : public testing::Test
{
public:
    void SetUp() override { disk = createDisk("atomic_schema_mutation_storage"); }
    void TearDown() override { destroyDisk(disk); }

    DiskPtr disk;
    Model model;
};

class PendingTableStartupTestDatabase final : public DatabaseAtomic
{
public:
    struct VerificationExecutionForTest
    {
        UDT::AuthorityVerificationBatchPlan::Ptr plan;
        UDT::AuthorityVerificationBatchReceipt::Ptr prefix;
        UDT::AuthorityVerificationBatchReceipt::Ptr complete;
        UDT::AuthorityVerificationScheduleCursor cursor_before;
        UDT::AuthorityVerificationScheduleCursor cursor_after_prefix;
        UDT::AuthorityVerificationScheduleCursor cursor_after_complete;
        std::optional<UDT::AuthorityVerificationScheduleCursor> durable_cursor;
    };

    PendingTableStartupTestDatabase(String name, String metadata_root, UUID database_uuid, ContextPtr context_, DiskPtr disk)
        : DatabaseAtomic(std::move(name), std::move(metadata_root), database_uuid, "PendingTableStartupTestDatabase", std::move(context_))
    {
        metadata_disk_ptr = std::move(disk);
    }

    bool hasPendingTableStartupForTest() const
    {
        std::lock_guard lock(udt_authority_mutex);
        return static_cast<bool>(udt_table_startup_state);
    }

    bool hasDegradedUDTStartupStatusForTest() const
    {
        std::lock_guard lock(udt_authority_mutex);
        return static_cast<bool>(udt_degraded_startup_status);
    }

    bool forceEagerTableLoadForTest(const ASTCreateQuery & query) const { return forceEagerTableLoadAtStartup(query); }

    void shutdownVerificationSchedulerForTest()
    {
        UDT::AuthorityVerificationScheduler * scheduler = nullptr;
        {
            std::lock_guard lock(udt_authority_mutex);
            scheduler = udt_verification_scheduler.get();
        }
        if (!scheduler)
            throw std::logic_error("test Atomic verification scheduler is absent");
        scheduler->shutdownAndDrain();
    }

    void shutdownVerificationRuntimeForTest()
    {
        UDT::AuthorityVerificationRuntimeState * runtime = nullptr;
        {
            std::lock_guard lock(udt_authority_mutex);
            runtime = udt_verification_runtime.get();
        }
        if (!runtime)
            throw std::logic_error("test Atomic verification runtime is absent");
        runtime->shutdownAndDrain();
    }

    void validateDetachedTableForTest(const ASTCreateQuery & query) const { validateTableMetadataForLoading(query, true); }

    void validateAutomaticRewriteForTest(const ASTCreateQuery & query) const { validateTableMetadataRewriteBeforeLoading(query); }

    void activatePendingTablesForTest()
    {
        std::lock_guard lock(udt_schema_mutation_mutex);
        activateUDTAuthorityAfterPendingTableStartup();
    }

    AuthorityState advanceDurableAuthorityWithoutPublicationForTest()
    {
        std::lock_guard schema_lock(udt_schema_mutation_mutex);
        std::optional<AtomicAuthority::RootSnapshot> snapshot;
        AtomicDatabaseSchemaMutationStorage * storage = nullptr;
        {
            std::lock_guard authority_lock(udt_authority_mutex);
            if (!udt_authority || !udt_mutation_storage)
                throw std::logic_error("test Atomic authority components are absent");
            snapshot.emplace(udt_authority->acquireCurrentRoot());
            storage = udt_mutation_storage.get();
        }
        if (!*snapshot)
            throw std::logic_error("test Atomic authority root is absent");

        const auto inventory = snapshot->get().pinAuthorityInventory();
        const auto graph = snapshot->get().pinSchemaObjectDependencyGraph();
        const auto before = snapshot->get().getAuthorityState();
        const auto after
            = state(before.database_uuid, before.database_catalog_epoch + 1, before.persistent_capability_mask, inventory, graph);
        auto transition = DatabaseSchemaWALTransitionBuilder::build(
            storage->getDurableHighWaterMark() + 1,
            DatabaseSchemaWALTransitionBase{
                .authority_state = before,
                .authority_inventory = inventory,
                .schema_graph = graph,
            },
            after,
            {},
            {},
            {},
            {});
        auto guard = storage->issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(*storage, guard, transition));
        const auto durable_state = storage->getCurrentAuthorityState();
        if (!durable_state || *durable_state != after)
            throw std::logic_error("test failed to advance only the durable Atomic authority");
        return *durable_state;
    }

    VerificationExecutionForTest executeVerificationPrefixAndResumeForTest()
    {
        std::optional<UDT::AtomicAuthority::RootSnapshot> snapshot;
        {
            std::lock_guard authority_lock(udt_authority_mutex);
            if (!udt_authority || !udt_mutation_storage || !udt_verification_runtime)
                throw std::logic_error("test Atomic verification components are absent");
            snapshot.emplace(udt_authority->acquireCurrentRoot());
        }
        if (!*snapshot)
            throw std::logic_error("test Atomic verification root is absent");

        const auto inventory = snapshot->get().pinAuthorityInventory();
        if (!inventory || inventory->getSummary().leaf_count < 2)
            throw std::logic_error("test Atomic verification root needs at least two targets");

        std::vector<UDT::AuthorityVerificationTarget> targets;
        targets.reserve(inventory->getLeaves().size());
        for (const auto & leaf : inventory->getLeaves())
        {
            targets.push_back({
                .leaf = leaf,
                .last_changed_catalog_epoch = snapshot->get().getDatabaseCatalogEpoch(),
                .last_periodic_verification_sequence = 0,
                .reverse_dependency_count = 0,
                .cost = {
                    .canonical_bytes = 1ULL << 20,
                    .work_units = 100'000,
                    .transient_bytes = UDT::AuthorityIntegrityVerifierLimits{}.maximum_transient_bytes,
                    .io_bytes = 1ULL << 20,
                },
            });
        }

        VerificationExecutionForTest result;
        result.cursor_before = udt_verification_runtime->getCursor();
        UDT::AuthorityVerificationSchedulePolicy policy;
        policy.bucket_count = result.cursor_before.bucket_count;
        policy.bucket_seed = result.cursor_before.bucket_seed;
        policy.maximum_recent_targets_per_batch = static_cast<UInt64>(targets.size());
        UDT::AuthorityVerificationScheduleLimits schedule_limits;
        schedule_limits.maximum_targets_per_batch = static_cast<UInt64>(targets.size());
        result.plan = UDT::planPeriodicAuthorityVerification(snapshot->get(), targets, result.cursor_before, policy, schedule_limits);
        if (!result.plan || result.plan->getTargets().size() < 2)
            throw std::logic_error("test Atomic verification plan did not select a resumable batch");

        UDT::AuthorityVerificationBatchExecutorLimits prefix_limits;
        prefix_limits.maximum_terminal_targets = 1;
        result.prefix = UDT::AuthorityVerificationBatchExecutor::execute(*this, *result.plan, prefix_limits);
        result.cursor_after_prefix = udt_verification_runtime->getCursor();

        result.complete = UDT::AuthorityVerificationBatchExecutor::execute(*this, *result.plan, {}, result.prefix.get());
        result.cursor_after_complete = udt_verification_runtime->getCursor();
        result.durable_cursor = udt_mutation_storage->loadAuthorityVerificationCursor();
        return result;
    }
};

ASTPtr parseTableMetadata(const String & text)
{
    ParserCreateQuery parser;
    return parseQuery(parser, text.data(), text.data() + text.size(), "Atomic pending Table startup test", 16ULL << 20, 256, 100'000);
}

StoragePtr makeMemoryTable(const SchemaObjectID & object, const DataTypePtr & column_type)
{
    MemorySettings settings;
    return std::make_shared<StorageMemory>(
        StorageID("app", "events", object.object_uuid),
        ColumnsDescription(NamesAndTypesList{{"id", column_type}}),
        ConstraintsDescription{},
        String{},
        settings);
}

class DecoratingMetadataStorage final : public IStorage
{
public:
    explicit DecoratingMetadataStorage(StorageID storage_id)
        : IStorage(std::move(storage_id))
    {
    }

    String getName() const override { return "DecoratingMetadataStorage"; }

    StorageMetadataHandle getInMemoryMetadataPtr(ContextPtr context, bool bypass_metadata_cache) const override
    {
        ++decorated_metadata_reads;
        return IStorage::getInMemoryMetadataPtr(context, bypass_metadata_cache);
    }

    size_t getDecoratedMetadataReads() const { return decorated_metadata_reads; }

private:
    mutable size_t decorated_metadata_reads = 0;
};

class DirectorySyncUnavailableDisk final : public DiskLocal
{
public:
    using DiskLocal::DiskLocal;

    SyncGuardPtr getDirectorySyncGuard(const String &) const override { return {}; }
};

class InjectedDirectorySyncError final : public std::runtime_error
{
public:
    InjectedDirectorySyncError()
        : std::runtime_error("injected directory synchronization failure")
    {
    }
};

class RetirementDirectorySyncDisk final : public DiskLocal
{
public:
    using DiskLocal::DiskLocal;

    void failNextDirectorySync(String path)
    {
        failure_path = std::move(path);
        synchronized_paths.clear();
    }

    const std::vector<String> & getSynchronizedPaths() const noexcept { return synchronized_paths; }

    SyncGuardPtr getDirectorySyncGuard(const String & path) const override
    {
        synchronized_paths.push_back(path);
        if (failure_path && path == *failure_path)
        {
            failure_path.reset();
            throw InjectedDirectorySyncError();
        }
        return DiskLocal::getDirectorySyncGuard(path);
    }

private:
    mutable std::optional<String> failure_path;
    mutable std::vector<String> synchronized_paths;
};

void copyDirectory(const DiskPtr & disk, const String & source, const String & destination)
{
    const auto source_path = std::filesystem::path(disk->getPath()) / source;
    const auto destination_path = std::filesystem::path(disk->getPath()) / destination;
    std::filesystem::create_directories(destination_path.parent_path());
    std::filesystem::copy(source_path, destination_path, std::filesystem::copy_options::recursive);
}

void stageTransition(
    AtomicDatabaseSchemaMutationStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedTransition & transition)
{
    const auto & prepare = transition.getPrepare();
    storage.validateMutationGuardAndDurablePredecessor(guard, prepare.before_authority_state, prepare.transaction_id);
    const auto bytes = transition.getStagedArtifactBytes();
    ASSERT_EQ(bytes.size(), prepare.staged_artifacts.size());
    for (size_t ordinal = 0; ordinal < bytes.size(); ++ordinal)
    {
        storage.stageArtifact(
            makeDatabaseSchemaWALStagedArtifactLocator(prepare.after_authority_state.database_uuid, prepare.transaction_id, ordinal),
            prepare.staged_artifacts[ordinal],
            bytes[ordinal]);
    }
}

void persistPrepare(
    AtomicDatabaseSchemaMutationStorage & storage,
    const DatabaseSchemaMutationGuard & guard,
    const DatabaseSchemaWALValidatedTransition & transition)
{
    stageTransition(storage, guard, transition);
    const auto & prepare = transition.getPrepare();
    storage.finishStaging(prepare.after_authority_state.database_uuid, prepare.transaction_id);
    storage.persistPrepare(prepare.transaction_id, encodeDatabaseSchemaWALPrepare(prepare));
}

DatabaseSchemaWALCommit
executeFirstActivation(AtomicDatabaseSchemaMutationStorage & storage, const DatabaseSchemaWALValidatedTransition & transition)
{
    auto prepared_configuration = storage.prepareUDTConfigurationForFirstActivationV2({});
    auto guard = storage.issueMutationGuard();
    auto commit = executeDatabaseSchemaMutation(storage, guard, transition);
    prepared_configuration.disarmAfterDurableActivation();
    return commit;
}

TEST(AtomicDatabaseSchemaMutationStoragePaths, GoldenPathsAreIdentityDerived)
{
    const Model model;
    const AtomicDatabaseSchemaMutationPaths paths("metadata/app", model.database_uuid);
    const AtomicDatabaseSchemaMutationPaths directory_path("metadata/app/", model.database_uuid);
    const String base = "metadata/app/types/.authority/databases/00112233-4455-6677-8899-aabbccddeeff";

    EXPECT_EQ(directory_path.getMetadataRoot(), "metadata/app");
    EXPECT_EQ(directory_path.typesDirectory(), "metadata/app/types");
    EXPECT_EQ(paths.typesDirectory(), "metadata/app/types");
    EXPECT_EQ(paths.authorityDirectory(), base);
    EXPECT_EQ(paths.stagingDirectory(), base + "/staging");
    EXPECT_EQ(paths.stagingTransactionDirectory(100), base + "/staging/00000000000000000100");
    EXPECT_EQ(paths.stagedArtifactPath(100, 0), base + "/staging/00000000000000000100/artifact-00000000000000000000.bin");
    EXPECT_EQ(paths.walDirectory(), base + "/wal");
    EXPECT_EQ(paths.walTransactionDirectory(100), base + "/wal/00000000000000000100");
    EXPECT_EQ(paths.preparePath(100), base + "/wal/00000000000000000100/prepare.wal");
    EXPECT_EQ(paths.commitPath(100), base + "/wal/00000000000000000100/commit.wal");
    EXPECT_EQ(paths.recoveryDecisionPath(100), base + "/wal/00000000000000000100/recovery.bin");
    EXPECT_EQ(paths.retiredDirectory(), base + "/retired");
    EXPECT_EQ(paths.retiredRollbackDirectory(), base + "/retired/rollback");
    EXPECT_EQ(paths.retiredRollbackTransactionDirectory(100), base + "/retired/rollback/00000000000000000100");
    EXPECT_EQ(paths.retiredCheckpointDirectory(), base + "/retired/checkpoint");
    EXPECT_EQ(paths.retiredCheckpointTransactionDirectory(7, 100), base + "/retired/checkpoint/00000000000000000007/00000000000000000100");
    EXPECT_EQ(paths.checkpointsDirectory(), base + "/checkpoints");
    EXPECT_EQ(paths.checkpointDirectory(7), base + "/checkpoints/00000000000000000007");
    EXPECT_EQ(paths.checkpointRecordPath(7), base + "/checkpoints/00000000000000000007/checkpoint.wal");
    EXPECT_EQ(paths.checkpointInventorySnapshotPath(7), base + "/checkpoints/00000000000000000007/inventory.snapshot");
    EXPECT_EQ(paths.checkpointSchemaGraphSnapshotPath(7), base + "/checkpoints/00000000000000000007/schema_graph.snapshot");
    EXPECT_EQ(paths.activationMarkerPath(), "metadata/app/.udt_activation.bin");
    EXPECT_EQ(paths.activationMarkerTemporaryPath(), "metadata/app/.udt_activation.bin.activation.tmp");
    EXPECT_EQ(paths.highWaterMarkPath(), base + "/transaction_high_water.bin");
    EXPECT_EQ(paths.authorityRecordPath(model.leaf.key), "metadata/app/types/10213243-5465-7687-98a9-bacbdcedfe01.sql");
    const AuthorityInventoryKey expectation_key{
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = uuid(0x7000, 1),
    };
    EXPECT_EQ(paths.authorityRecordPath(expectation_key), base + "/expectations/00000000-0000-7000-0000-000000000001.bin");
    EXPECT_EQ(
        paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord, model.type_object),
        "metadata/app/types/10213243-5465-7687-98a9-bacbdcedfe01.sql");
    const SchemaObjectID synthetic = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x7000, 1));
    EXPECT_EQ(
        paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, synthetic),
        base + "/synthetic/00000000-0000-7000-0000-000000000001.metadata");
    EXPECT_EQ(
        paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, synthetic),
        base + "/synthetic/00000000-0000-7000-0000-000000000001.references");
    const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 1));
    EXPECT_EQ(paths.tableMetadataPath("events/2026"), "metadata/app/events%2F2026.sql");
    EXPECT_EQ(paths.tableReferencesPath(table), base + "/expectations/00000000-0000-8000-0000-000000000001.references");
    EXPECT_EQ(paths.metadataInstallationRecordPath(table), base + "/expectations/00000000-0000-8000-0000-000000000001.installation");
    EXPECT_EQ(
        paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, table),
        paths.tableReferencesPath(table));
    EXPECT_EQ(
        paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, table),
        paths.metadataInstallationRecordPath(table));
    EXPECT_THROW(
        static_cast<void>(paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, table)),
        DatabaseSchemaMutationReplayConflictError);
    for (const auto kind : {SchemaObjectKind::View, SchemaObjectKind::Dictionary})
    {
        const SchemaObjectID stored_object = objectID(kind, model.database_uuid, uuid(0x9000, static_cast<UInt64>(kind)));
        EXPECT_EQ(
            paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, stored_object),
            paths.tableReferencesPath(stored_object));
        EXPECT_EQ(
            paths.canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, stored_object),
            paths.metadataInstallationRecordPath(stored_object));
    }
    EXPECT_THROW(static_cast<void>(AtomicDatabaseSchemaMutationPaths("/", model.database_uuid)), StorageError);
    EXPECT_THROW(static_cast<void>(AtomicDatabaseSchemaMutationPaths("../escape", model.database_uuid)), StorageError);
}

TEST(AtomicDatabaseSchemaMutationStorageCodec, HighWaterV1HasExactPermanentBytesAndRejectsDamage)
{
    const Model model;
    const String encoded = encodeAtomicDatabaseSchemaMutationHighWaterMark(model.database_uuid, 100);
    EXPECT_EQ(
        toHex(encoded),
        "434855445448573100112233445566778899aabbccddeeff6400000000000000"
        "138250b797873f1ea0758d8f03a050840fc140dd92e52d1e4cdea5aaa6798e25");
    EXPECT_EQ(decodeAtomicDatabaseSchemaMutationHighWaterMark(encoded, model.database_uuid), 100);

    for (const auto & damaged :
         {encoded.substr(0, encoded.size() - 1),
          encoded + "x",
          String("UNKNOWN1") + encoded.substr(8),
          encoded.substr(0, 20) + String(1, static_cast<char>(encoded[20] ^ 1)) + encoded.substr(21)})
        EXPECT_THROW(static_cast<void>(decodeAtomicDatabaseSchemaMutationHighWaterMark(damaged, model.database_uuid)), StorageError);
    EXPECT_THROW(static_cast<void>(decodeAtomicDatabaseSchemaMutationHighWaterMark(encoded, uuid(0x11, 0x22))), StorageError);
}

TEST(AtomicDatabaseSchemaMutationStorageCodec, ActivationMarkerV1HasExactPermanentBytesAndRejectsDamage)
{
    const Model model;
    const String encoded = encodeAuthorityActivationMarker(model.database_uuid, 100);
    EXPECT_EQ(
        toHex(encoded),
        "4348554454414d3100112233445566778899aabbccddeeff6400000000000000"
        "6303d3c09ae2653b9a7b119958921b117c8706c9fd4c9a4ba30853ffdf0f025d");
    EXPECT_EQ(decodeAuthorityActivationMarker(encoded, model.database_uuid), 100);

    for (const auto & damaged :
         {encoded.substr(0, encoded.size() - 1),
          encoded + "x",
          String("UNKNOWN1") + encoded.substr(8),
          encoded.substr(0, 20) + String(1, static_cast<char>(encoded[20] ^ 1)) + encoded.substr(21)})
        EXPECT_THROW(static_cast<void>(decodeAuthorityActivationMarker(damaged, model.database_uuid)), StorageError);
    EXPECT_THROW(static_cast<void>(decodeAuthorityActivationMarker(encoded, uuid(0x11, 0x22))), StorageError);
}

TEST(AtomicDatabaseSchemaMutationStorageCodec, RecoveryDecisionV1HasExactPermanentBytesAndRejectsDamage)
{
    const Model model;
    Digest prepare_hash{};
    for (size_t index = 0; index < prepare_hash.size(); ++index)
        prepare_hash[index] = static_cast<UInt8>(index);
    const String encoded = encodeAtomicDatabaseSchemaMutationRecoveryDecision(
        model.database_uuid, 100, DatabaseSchemaWALRecoveryDecision::RollBackPrepared, prepare_hash);
    EXPECT_EQ(
        toHex(encoded),
        "434855445452433100112233445566778899aabbccddeeff640000000000000001"
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        "f0758dc4781e569648b44067d4520f358843da58c0ab56f725347d45f7b92c02");
    EXPECT_EQ(
        decodeAtomicDatabaseSchemaMutationRecoveryDecision(encoded, model.database_uuid, 100, prepare_hash),
        DatabaseSchemaWALRecoveryDecision::RollBackPrepared);

    Digest other_hash = prepare_hash;
    other_hash.front() ^= 1;
    EXPECT_THROW(
        static_cast<void>(decodeAtomicDatabaseSchemaMutationRecoveryDecision(encoded, model.database_uuid, 100, other_hash)), StorageError);
    EXPECT_THROW(
        static_cast<void>(decodeAtomicDatabaseSchemaMutationRecoveryDecision(
            encoded.substr(0, encoded.size() - 1), model.database_uuid, 100, prepare_hash)),
        StorageError);
    EXPECT_THROW(
        static_cast<void>(decodeAtomicDatabaseSchemaMutationRecoveryDecision(encoded + "x", model.database_uuid, 100, prepare_hash)),
        StorageError);
    String tampered = encoded;
    tampered[40] ^= 1;
    EXPECT_THROW(
        static_cast<void>(decodeAtomicDatabaseSchemaMutationRecoveryDecision(tampered, model.database_uuid, 100, prepare_hash)),
        StorageError);
    String unknown_decision = encoded;
    unknown_decision[32] = static_cast<char>(0x7f);
    unknown_decision = rewriteInternalChecksum(std::move(unknown_decision), atomic_database_schema_mutation_recovery_decision_hash_domain);
    EXPECT_THROW(
        static_cast<void>(decodeAtomicDatabaseSchemaMutationRecoveryDecision(unknown_decision, model.database_uuid, 100, prepare_hash)),
        StorageError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, ConstructorProbeAndEmptyCleanupDoNotCreateTypes)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    EXPECT_FALSE(disk->existsFileOrDirectory("metadata/app/types"));
    EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().activationMarkerPath()));
    EXPECT_FALSE(storage.hasDurableAuthorityMarker());
    EXPECT_FALSE(storage.hasBoundedDurableAuthorityHead());
    EXPECT_FALSE(storage.cleanupNeverEnabledScaffold());
    EXPECT_FALSE(disk->existsFileOrDirectory("metadata/app/types"));

    disk->createDirectories("metadata/app/types");
    EXPECT_FALSE(storage.hasDurableAuthorityMarker());
    EXPECT_TRUE(storage.cleanupNeverEnabledScaffold());
    EXPECT_FALSE(disk->existsFileOrDirectory("metadata/app/types"));
    EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().activationMarkerPath()));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, MetadataRootSymbolicLinksAndUnavailableDirectorySyncFailClosed)
{
    disk->createDirectories("outside");
    std::filesystem::create_directory_symlink(
        std::filesystem::path(disk->getPath()) / "outside", std::filesystem::path(disk->getPath()) / "linked");
    AtomicDatabaseSchemaMutationStorage linked(disk, model.database_uuid, "linked/app");
    EXPECT_THROW(static_cast<void>(linked.hasDurableAuthorityMarker()), StorageError);
    EXPECT_THROW(static_cast<void>(linked.cleanupNeverEnabledScaffold()), StorageError);

    DiskPtr seed = createDisk("atomic_schema_mutation_no_directory_sync");
    const String no_sync_path = seed->getPath();
    destroyDisk(seed);
    std::filesystem::create_directory(no_sync_path);
    {
        auto no_sync_disk = std::make_shared<DirectorySyncUnavailableDisk>("no_sync", no_sync_path);
        AtomicDatabaseSchemaMutationStorage no_sync_storage(no_sync_disk, model.database_uuid, "metadata/app");
        try
        {
            static_cast<void>(no_sync_storage.issueMutationGuard());
            FAIL() << "expected unavailable directory synchronization to fail";
        }
        catch (const StorageError & error)
        {
            EXPECT_EQ(error.code, StorageError::Code::DirectorySyncUnavailable);
        }
    }
    std::filesystem::remove_all(no_sync_path);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StagedEnvelopeV1HasExactPermanentBytesAndRejectsDamage)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto guard = storage.issueMutationGuard();
    storage.validateMutationGuardAndDurablePredecessor(guard, std::nullopt, 100);
    const SchemaObjectID object = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, model.type_uuid);
    const String bytes = "metadata-0";
    const DatabaseSchemaWALStagedArtifactRef artifact{
        .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
        .image = DatabaseSchemaWALStagedArtifactImage::After,
        .object = object,
        .revision = 1,
        .byte_size = bytes.size(),
        .content_hash = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, bytes),
    };
    storage.stageArtifact(makeDatabaseSchemaWALStagedArtifactLocator(model.database_uuid, 100, 0), artifact, bytes);
    const String path = storage.getPaths().stagedArtifactPath(100, 0);
    const String encoded = readFile(disk, path);
    EXPECT_EQ(
        toHex(encoded),
        "434855445453413100112233445566778899aabbccddeeff6400000000000000"
        "00000000000000000302fe00112233445566778899aabbccddeeff102132435465768798a9bacbdcedfe01"
        "01000000000000000a00000000000000a6da303bff2ccf84d25751a8e8221d9cefecb8f2346de5dbcb757925dd1faa85"
        "6d657461646174612d30f1492443566eb48a8e579d7faa06dbb4c96d818506e74183161d89786eef2254");
    EXPECT_TRUE(storage.hasDurableAuthorityMarker());

    for (String damaged :
         {encoded.substr(0, encoded.size() - 1),
          encoded + "x",
          String("UNKNOWN1") + encoded.substr(8),
          encoded.substr(0, 70) + String(1, static_cast<char>(encoded[70] ^ 1)) + encoded.substr(71)})
    {
        writeFile(disk, path, damaged);
        EXPECT_THROW(static_cast<void>(storage.hasDurableAuthorityMarker()), StorageError);
    }
    for (const size_t tag_offset : {size_t{40}, size_t{41}, size_t{42}})
    {
        String unknown_tag = encoded;
        unknown_tag[tag_offset] = static_cast<char>(0x7f);
        unknown_tag = rewriteInternalChecksum(std::move(unknown_tag), atomic_database_schema_mutation_staged_artifact_hash_domain);
        writeFile(disk, path, unknown_tag);
        try
        {
            static_cast<void>(storage.hasDurableAuthorityMarker());
            FAIL() << "expected an unknown durable tag to be rejected";
        }
        catch (const StorageError & error)
        {
            EXPECT_EQ(error.code, StorageError::Code::CorruptDurableState);
        }
    }
    String content_tamper = encoded;
    content_tamper[content_tamper.size() - sizeof(Digest) - 1] ^= 1;
    content_tamper = rewriteInternalChecksum(std::move(content_tamper), atomic_database_schema_mutation_staged_artifact_hash_domain);
    writeFile(disk, path, content_tamper);
    EXPECT_THROW(static_cast<void>(storage.hasDurableAuthorityMarker()), StorageError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, UnpreparedStagingEnforcesAggregateArtifactByteLimit)
{
    AtomicDatabaseSchemaMutationStorageLimits limits;
    limits.wal.maximum_staged_artifact_bytes = 8;
    limits.wal.maximum_total_staged_artifact_bytes = 10;
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app", limits);
    auto guard = storage.issueMutationGuard();
    storage.validateMutationGuardAndDurablePredecessor(guard, std::nullopt, 100);

    for (UInt64 ordinal = 0; ordinal < 2; ++ordinal)
    {
        const SchemaObjectID object = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x6000, ordinal + 1));
        const String bytes = ordinal == 0 ? "first!" : "second";
        const DatabaseSchemaWALStagedArtifactRef artifact{
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = object,
            .revision = 1,
            .byte_size = bytes.size(),
            .content_hash = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, bytes),
        };
        storage.stageArtifact(makeDatabaseSchemaWALStagedArtifactLocator(model.database_uuid, 100, ordinal), artifact, bytes);
    }

    const auto expect_limit_exceeded = [&](const auto & operation)
    {
        try
        {
            operation();
            FAIL() << "expected aggregate staged bytes to be rejected";
        }
        catch (const StorageError & error)
        {
            EXPECT_EQ(error.code, StorageError::Code::LimitExceeded);
        }
    };
    expect_limit_exceeded([&] { storage.finishStaging(model.database_uuid, 100); });
    expect_limit_exceeded([&] { static_cast<void>(storage.hasDurableAuthorityMarker()); });
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, MatchingTemporaryAndDurableStagedImagesCountOneUniqueOrdinal)
{
    AtomicDatabaseSchemaMutationStorageLimits limits;
    limits.wal.maximum_staged_artifact_bytes = 6;
    limits.wal.maximum_total_staged_artifact_bytes = 6;
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app", limits);
    auto guard = storage.issueMutationGuard();
    storage.validateMutationGuardAndDurablePredecessor(guard, std::nullopt, 100);
    const SchemaObjectID object = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x6000, 3));
    const String bytes = "single";
    const DatabaseSchemaWALStagedArtifactRef artifact{
        .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
        .image = DatabaseSchemaWALStagedArtifactImage::After,
        .object = object,
        .revision = 1,
        .byte_size = bytes.size(),
        .content_hash = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, bytes),
    };
    storage.stageArtifact(makeDatabaseSchemaWALStagedArtifactLocator(model.database_uuid, 100, 0), artifact, bytes);
    const String durable_path = storage.getPaths().stagedArtifactPath(100, 0);
    writeFile(disk, durable_path + ".stage.tmp", readFile(disk, durable_path));

    EXPECT_NO_THROW(storage.finishStaging(model.database_uuid, 100));
    EXPECT_TRUE(storage.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, PrePrepareRestartSweepRestoresExactNeverEnabledState)
{
    for (const bool finish_staging : {false, true})
    {
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
            auto transition = model.firstTransition();
            auto guard = storage.issueMutationGuard();
            stageTransition(storage, guard, transition);
            if (finish_staging)
                storage.finishStaging(model.database_uuid, transition.getPrepare().transaction_id);
        }
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
            EXPECT_TRUE(storage.hasDurableAuthorityMarker());
            auto guard = storage.issueMutationGuard();
            EXPECT_EQ(storage.sweepUnpreparedStaging(guard), 1);
            EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().typesDirectory()));
        }
        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
        EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
        EXPECT_FALSE(restarted.cleanupNeverEnabledScaffold());
    }
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, CurrentGuardExecutesAndEveryEarlierGuardIsRejected)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto prepared_configuration = storage.prepareUDTConfigurationForFirstActivationV2({});
    auto stale_guard = storage.issueMutationGuard();
    auto current_guard = storage.issueMutationGuard();
    EXPECT_THROW(
        storage.validateMutationGuardAndDurablePredecessor(stale_guard, std::nullopt, 100), DatabaseSchemaMutationReplayConflictError);

    auto transition = model.firstTransition();
    static_cast<void>(executeDatabaseSchemaMutation(storage, current_guard, transition));
    prepared_configuration.disarmAfterDurableActivation();
    EXPECT_EQ(storage.getDurableHighWaterMark(), 100);
    EXPECT_EQ(storage.getCurrentAuthorityState(), model.definition_only_state);
    EXPECT_TRUE(storage.hasBoundedDurableAuthorityHead());

    auto next_guard = storage.issueMutationGuard();
    EXPECT_THROW(
        storage.validateMutationGuardAndDurablePredecessor(next_guard, model.definition_only_state, 100),
        DatabaseSchemaMutationReplayConflictError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, TemporaryPrepareOrphanIsRecoveryEvidenceAndSweptWithOrWithoutStaging)
{
    for (const bool retain_staging : {false, true})
    {
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
            auto transition = model.firstTransition();
            auto guard = storage.issueMutationGuard();
            stageTransition(storage, guard, transition);
            storage.finishStaging(model.database_uuid, transition.getPrepare().transaction_id);
            writeFile(disk, storage.getPaths().preparePath(100) + ".prepare.tmp", encodeDatabaseSchemaWALPrepare(transition.getPrepare()));
            if (!retain_staging)
                disk->removeRecursive(storage.getPaths().stagingTransactionDirectory(100));
        }

        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
        EXPECT_TRUE(restarted.hasDurableAuthorityMarker());
        auto guard = restarted.issueMutationGuard();
        EXPECT_EQ(restarted.sweepUnpreparedStaging(guard), 1);
        EXPECT_FALSE(disk->existsFileOrDirectory(restarted.getPaths().typesDirectory()));
        EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
    }
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, PrepareRejectsStagedArtifactOutsideItsExactManifest)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto transition = model.firstTransition();
    auto guard = storage.issueMutationGuard();
    stageTransition(storage, guard, transition);
    EXPECT_THROW(storage.persistPrepare(100, "invalid"), DatabaseSchemaMutationReplayConflictError);
    const auto & artifact = transition.getPrepare().staged_artifacts.front();
    storage.stageArtifact(
        makeDatabaseSchemaWALStagedArtifactLocator(model.database_uuid, 100, 1), artifact, transition.getStagedArtifactBytes().front());
    storage.finishStaging(model.database_uuid, 100);
    EXPECT_THROW(
        storage.persistPrepare(100, encodeDatabaseSchemaWALPrepare(transition.getPrepare())), DatabaseSchemaMutationReplayConflictError);
    storage.discardUnpreparedStaging(guard, 100);
    EXPECT_FALSE(storage.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StagingBarrierRejectsLateArtifactsBeforePrepare)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto transition = model.firstTransition();
    auto guard = storage.issueMutationGuard();
    stageTransition(storage, guard, transition);
    storage.finishStaging(model.database_uuid, transition.getPrepare().transaction_id);

    const auto & artifact = transition.getPrepare().staged_artifacts.front();
    EXPECT_THROW(
        storage.stageArtifact(
            makeDatabaseSchemaWALStagedArtifactLocator(model.database_uuid, transition.getPrepare().transaction_id, 1),
            artifact,
            transition.getStagedArtifactBytes().front()),
        DatabaseSchemaMutationReplayConflictError);
    EXPECT_FALSE(disk->existsFile(storage.getPaths().preparePath(transition.getPrepare().transaction_id)));
    discardUnpreparedDatabaseSchemaMutationStaging(storage, guard, transition.getPrepare().transaction_id);
    EXPECT_FALSE(storage.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, RecoveryGuardKeepsDurablePrepareStagingManifestSealed)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto transition = model.firstTransition();
    auto guard = storage.issueMutationGuard();
    persistPrepare(storage, guard, transition);
    storage.validateRecoveryGuard(guard, transition.getPrepare().transaction_id);

    const auto & artifact = transition.getPrepare().staged_artifacts.front();
    EXPECT_THROW(
        storage.stageArtifact(
            makeDatabaseSchemaWALStagedArtifactLocator(model.database_uuid, transition.getPrepare().transaction_id, 1),
            artifact,
            transition.getStagedArtifactBytes().front()),
        DatabaseSchemaMutationReplayConflictError);
    EXPECT_EQ(recoverDatabaseSchemaMutation(storage, guard, transition, std::nullopt), DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
    retireRolledBackDatabaseSchemaMutation(storage, guard, transition.getPrepare().transaction_id);
    EXPECT_FALSE(storage.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, FirstActivationRollbackRetirementIsRestartSafeAtEveryCleanupBoundary)
{
    for (size_t cleanup_boundary = 0; cleanup_boundary != 5; ++cleanup_boundary)
    {
        String retired;
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
            auto transition = model.firstTransition();
            auto guard = storage.issueMutationGuard();
            persistPrepare(storage, guard, transition);
            writeFile(disk, storage.getPaths().activationMarkerPath(), encodeAuthorityActivationMarker(model.database_uuid, 100));
            EXPECT_TRUE(storage.hasDurableAuthorityMarker());
            EXPECT_FALSE(storage.getCurrentAuthorityState());
            storage.finishRecovery(model.database_uuid, 100, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
            retired = storage.getPaths().retiredRollbackTransactionDirectory(100);
            disk->createDirectories(std::filesystem::path(retired).parent_path().generic_string());
            disk->moveDirectory(storage.getPaths().walTransactionDirectory(100), retired);
            if (cleanup_boundary >= 1)
                disk->removeRecursive(storage.getPaths().stagingTransactionDirectory(100));
            if (cleanup_boundary >= 2)
                disk->removeFile(storage.getPaths().highWaterMarkPath());
            if (cleanup_boundary >= 3)
                disk->removeFile(retired + "/recovery.bin");
            if (cleanup_boundary >= 4)
                disk->removeFile(retired + "/prepare.wal");
        }
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
            EXPECT_TRUE(storage.hasDurableAuthorityMarker());
            auto guard = storage.issueMutationGuard();
            EXPECT_EQ(storage.sweepRetiredTransactions(guard), 1);
            EXPECT_FALSE(disk->existsFileOrDirectory(retired));
            EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().typesDirectory()));
            EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().activationMarkerPath()));
        }
        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
        EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
    }
}

TEST(AtomicDatabaseSchemaMutationStorageRetirement, DestinationBarrierPrecedesSourceBarrierAndRestartCompletes)
{
    const Model model;
    for (const bool fail_destination_barrier : {true, false})
    {
        DiskPtr seed = createDisk("atomic_schema_mutation_retirement_sync");
        const String disk_path = seed->getPath();
        destroyDisk(seed);
        std::filesystem::create_directory(disk_path);
        auto traced_disk = std::make_shared<RetirementDirectorySyncDisk>("retirement_sync", disk_path);

        String retired_parent;
        String wal_parent;
        {
            AtomicDatabaseSchemaMutationStorage storage(traced_disk, model.database_uuid, "metadata/app");
            auto transition = model.firstTransition();
            auto guard = storage.issueMutationGuard();
            persistPrepare(storage, guard, transition);
            storage.finishRecovery(model.database_uuid, 100, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
            retired_parent = storage.getPaths().retiredRollbackDirectory();
            wal_parent = storage.getPaths().walDirectory();
            traced_disk->failNextDirectorySync(fail_destination_barrier ? retired_parent : wal_parent);
            EXPECT_THROW(storage.retireRolledBackTransaction(guard, 100), InjectedDirectorySyncError);
            EXPECT_TRUE(traced_disk->existsDirectory(storage.getPaths().retiredRollbackTransactionDirectory(100)));
            EXPECT_FALSE(traced_disk->existsFileOrDirectory(storage.getPaths().walTransactionDirectory(100)));
        }

        const auto & synchronized_paths = traced_disk->getSynchronizedPaths();
        const auto destination_sync = std::find(synchronized_paths.begin(), synchronized_paths.end(), retired_parent);
        const auto source_sync = std::find(synchronized_paths.begin(), synchronized_paths.end(), wal_parent);
        ASSERT_NE(destination_sync, synchronized_paths.end());
        if (fail_destination_barrier)
            EXPECT_EQ(source_sync, synchronized_paths.end());
        else
        {
            ASSERT_NE(source_sync, synchronized_paths.end());
            EXPECT_LT(destination_sync, source_sync);
        }

        {
            AtomicDatabaseSchemaMutationStorage restarted(traced_disk, model.database_uuid, "metadata/app");
            const auto result = recoverAtomicAuthorityAtStartup(restarted);
            EXPECT_FALSE(result.authority_root);
            EXPECT_EQ(result.swept_retired_transactions, 1);
            EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
        }
        traced_disk.reset();
        std::filesystem::remove_all(disk_path);
    }
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, IdenticalOrDeletionProgressLiveAndRetiredRollbackCopiesAreReconciled)
{
    for (size_t source_cleanup_boundary = 0; source_cleanup_boundary != 3; ++source_cleanup_boundary)
    {
        const String metadata_root = "metadata/app" + std::to_string(source_cleanup_boundary);
        String source;
        String destination;
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, metadata_root);
            auto transition = model.firstTransition();
            auto guard = storage.issueMutationGuard();
            persistPrepare(storage, guard, transition);
            storage.finishRecovery(model.database_uuid, 100, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
            source = storage.getPaths().walTransactionDirectory(100);
            destination = storage.getPaths().retiredRollbackTransactionDirectory(100);
            copyDirectory(disk, source, destination);
            if (source_cleanup_boundary >= 1)
                disk->removeFile(source + "/recovery.bin");
            if (source_cleanup_boundary >= 2)
                disk->removeFile(source + "/prepare.wal");
        }

        ASSERT_TRUE(disk->existsDirectory(source));
        ASSERT_TRUE(disk->existsDirectory(destination));
        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, metadata_root);
        const auto result = recoverAtomicAuthorityAtStartup(restarted);
        EXPECT_FALSE(result.authority_root);
        EXPECT_EQ(result.swept_retired_transactions, 1);
        EXPECT_FALSE(disk->existsFileOrDirectory(source));
        EXPECT_FALSE(disk->existsFileOrDirectory(destination));
        EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
    }
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, DivergentLiveAndRetiredRollbackCopiesFailClosed)
{
    String source;
    String destination;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        auto guard = storage.issueMutationGuard();
        persistPrepare(storage, guard, transition);
        storage.finishRecovery(model.database_uuid, 100, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
        source = storage.getPaths().walTransactionDirectory(100);
        destination = storage.getPaths().retiredRollbackTransactionDirectory(100);
        copyDirectory(disk, source, destination);
        disk->removeFile(destination + "/recovery.bin");
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    EXPECT_TRUE(restarted.hasDurableAuthorityMarker());
    auto guard = restarted.issueMutationGuard();
    try
    {
        static_cast<void>(restarted.sweepRetiredTransactions(guard));
        FAIL() << "expected divergent live and retired copies to fail closed";
    }
    catch (const StorageError & error)
    {
        EXPECT_EQ(error.code, StorageError::Code::CorruptDurableState);
    }
    EXPECT_TRUE(disk->existsDirectory(source));
    EXPECT_TRUE(disk->existsDirectory(destination));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, CommitRestartAndAnchoredReconciliationAreExact)
{
    DatabaseSchemaWALCommit commit;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        commit = executeFirstActivation(storage, transition);
        EXPECT_EQ(storage.getDurableHighWaterMark(), 100);
        EXPECT_EQ(storage.getCurrentAuthorityState(), model.definition_only_state);
        EXPECT_TRUE(storage.hasDurableAuthorityMarker());
        ASSERT_TRUE(disk->existsFile(storage.getPaths().activationMarkerPath()));
        EXPECT_EQ(decodeAuthorityActivationMarker(readFile(disk, storage.getPaths().activationMarkerPath()), model.database_uuid), 100);
        const auto reconciliation = storage.readAndReconcileAuthorityRecords(*model.definition_inventory, *model.definition_graph);
        ASSERT_EQ(reconciliation.authority_records.size(), 1);
        EXPECT_TRUE(reconciliation.dependent_objects.empty());
        EXPECT_EQ(reconciliation.authority_records.front().key, model.leaf.key);
        EXPECT_EQ(reconciliation.authority_records.front().canonical_bytes, model.record_bytes);
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    EXPECT_TRUE(restarted.hasDurableAuthorityMarker());
    ASSERT_EQ(restarted.listDurableTransactionIDs(), std::vector<UInt64>{100});
    const auto recovered = restarted.loadTransactionForRecovery(100);
    EXPECT_EQ(recovered.commit, commit);
    EXPECT_FALSE(recovered.recovery_decision);
    ASSERT_EQ(recovered.staged_artifact_bytes.size(), 1);
    EXPECT_EQ(recovered.staged_artifact_bytes.front(), model.record_bytes);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, PermanentActivationMarkerRejectsWholeTypesTreeDeletion)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto transition = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, transition));

    const String activation_marker_path = storage.getPaths().activationMarkerPath();
    ASSERT_TRUE(disk->existsFile(activation_marker_path));
    disk->removeRecursive(storage.getPaths().typesDirectory());
    ASSERT_TRUE(disk->existsFile(activation_marker_path));

    try
    {
        static_cast<void>(storage.hasDurableAuthorityMarker());
        FAIL() << "expected deletion of the complete types namespace to fail closed";
    }
    catch (const StorageError & error)
    {
        EXPECT_EQ(error.code, StorageError::Code::CorruptDurableState);
    }
    auto degraded = recoverAtomicAuthorityAtStartup(storage);
    EXPECT_FALSE(degraded.authority_root);
    EXPECT_TRUE(degraded.degraded_status);
    EXPECT_TRUE(disk->existsFile(activation_marker_path));
    EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().typesDirectory()));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, CommittedAuthorityRejectsMissingActivationMarker)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto transition = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, transition));

    const String activation_marker_path = storage.getPaths().activationMarkerPath();
    ASSERT_TRUE(disk->existsFile(activation_marker_path));
    disk->removeFile(activation_marker_path);
    EXPECT_THROW(static_cast<void>(storage.hasDurableAuthorityMarker()), StorageError);
    auto degraded = recoverAtomicAuthorityAtStartup(storage);
    EXPECT_FALSE(degraded.authority_root);
    EXPECT_TRUE(degraded.degraded_status);
    EXPECT_TRUE(disk->existsDirectory(storage.getPaths().typesDirectory()));
    EXPECT_FALSE(disk->existsFileOrDirectory(activation_marker_path));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, ReconciliationRejectsExtraMissingAndSymbolicLinkDefinitions)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto transition = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, transition));
    const String record_path = storage.getPaths().authorityRecordPath(model.leaf.key);
    const String extra_path = storage.getPaths().typesDirectory() + "/ffffffff-ffff-ffff-ffff-ffffffffffff.sql";

    writeFile(disk, extra_path, model.record_bytes);
    EXPECT_THROW(
        static_cast<void>(storage.readAndReconcileAuthorityRecords(*model.definition_inventory, *model.definition_graph)), StorageError);
    disk->removeFile(extra_path);
    disk->removeFile(record_path);
    EXPECT_THROW(
        static_cast<void>(storage.readAndReconcileAuthorityRecords(*model.definition_inventory, *model.definition_graph)), StorageError);

    const String target = storage.getPaths().typesDirectory() + "/target";
    writeFile(disk, target, model.record_bytes);
    std::filesystem::create_symlink(std::filesystem::path(disk->getPath()) / target, std::filesystem::path(disk->getPath()) / record_path);
    EXPECT_THROW(
        static_cast<void>(storage.readAndReconcileAuthorityRecords(*model.definition_inventory, *model.definition_graph)), StorageError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, SyntheticMetadataIsGraphAnchoredAndRealAdaptersAreRejected)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto first = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, first));
    auto activation = model.dependentObjectActivationTransition();
    auto activation_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
    const SchemaObjectID synthetic = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x7000, 1));
    auto synthetic_transition = model.syntheticTransition(synthetic);
    auto synthetic_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, synthetic_guard, synthetic_transition));

    const auto reconciliation
        = storage.readAndReconcileAuthorityRecords(synthetic_transition.getAfterInventory(), synthetic_transition.getAfterGraph());
    ASSERT_EQ(reconciliation.authority_records.size(), 1);
    EXPECT_TRUE(reconciliation.dependent_objects.empty());
    EXPECT_TRUE(disk->existsFile(
        storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, synthetic)));

    const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 1));
    EXPECT_THROW(
        static_cast<void>(storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, table)),
        DatabaseSchemaMutationReplayConflictError);
    const auto graph_with_table = SchemaObjectDependencyGraph::build(model.database_uuid, {&table, 1}, {});
    EXPECT_THROW(static_cast<void>(storage.readAndReconcileAuthorityRecords(*model.definition_inventory, *graph_with_table)), StorageError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, ExpectedSyntheticSidecarsAreReconciledByInventoryGraphAndUUID)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto first = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, first));
    auto activation = model.dependentObjectActivationTransition();
    auto activation_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
    const SchemaObjectID synthetic = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x7000, 2));
    auto sidecar_transition = model.syntheticSidecarTransition(synthetic);
    auto sidecar_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, sidecar_guard, sidecar_transition));

    const auto & anchored_inventory = sidecar_transition.getAfterInventory();
    const auto & anchored_graph = sidecar_transition.getAfterGraph();
    const auto reconciliation = storage.readAndReconcileAuthorityRecords(anchored_inventory, anchored_graph);
    ASSERT_EQ(reconciliation.authority_records.size(), 2);
    ASSERT_EQ(reconciliation.dependent_objects.size(), 1);
    EXPECT_EQ(reconciliation.dependent_objects.front().expectation.object, synthetic);

    const String sidecar_path
        = storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, synthetic);
    const String metadata_path
        = storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, synthetic);
    const String sidecar_bytes = readFile(disk, sidecar_path);
    const String metadata_bytes = readFile(disk, metadata_path);

    disk->removeFile(sidecar_path);
    EXPECT_THROW(static_cast<void>(storage.readAndReconcileAuthorityRecords(anchored_inventory, anchored_graph)), StorageError);
    writeFile(disk, sidecar_path, sidecar_bytes);

    const SchemaObjectID swapped = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x7000, 3));
    writeFile(disk, sidecar_path, encodePersistedTypeReferences(references(swapped, 2, model.record, digest(0xc0))));
    EXPECT_THROW(static_cast<void>(storage.readAndReconcileAuthorityRecords(anchored_inventory, anchored_graph)), StorageError);
    writeFile(disk, sidecar_path, sidecar_bytes);

    disk->removeFile(metadata_path);
    EXPECT_THROW(static_cast<void>(storage.readAndReconcileAuthorityRecords(anchored_inventory, anchored_graph)), StorageError);
    writeFile(disk, metadata_path, metadata_bytes);

    const String extra_metadata_path
        = storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, swapped);
    writeFile(disk, extra_metadata_path, "extra-metadata");
    EXPECT_THROW(static_cast<void>(storage.readAndReconcileAuthorityRecords(anchored_inventory, anchored_graph)), StorageError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, TableMetadataUsesDurableMappedNameAndReconcilesExactCompanions)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto first = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, first));
    auto activation = model.dependentObjectActivationTransition();
    auto activation_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));

    const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 2));
    const String object_name = "events/2026";
    const String metadata = "table-metadata";
    auto transition = model.tableSidecarTransition(table, object_name, metadata);
    auto guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition));

    const auto & paths = storage.getPaths();
    EXPECT_EQ(readFile(disk, paths.tableMetadataPath(object_name)), metadata);
    EXPECT_TRUE(disk->existsFile(paths.tableReferencesPath(table)));
    EXPECT_TRUE(disk->existsFile(paths.metadataInstallationRecordPath(table)));

    const auto reconciled = storage.readAndReconcileAuthorityRecords(transition.getAfterInventory(), transition.getAfterGraph());
    ASSERT_EQ(reconciled.dependent_objects.size(), 1);
    EXPECT_EQ(reconciled.dependent_objects.front().expectation.object, table);
    EXPECT_EQ(reconciled.dependent_objects.front().object_name, object_name);
    EXPECT_EQ(reconciled.dependent_objects.front().canonical_metadata_bytes, metadata);

    /// Physical-only ordinary metadata is outside the sparse UDT inventory and
    /// must remain loadable without acquiring logical provenance.
    writeFile(disk, "metadata/app/ordinary.sql", "ordinary-physical-metadata");
    EXPECT_NO_THROW(
        static_cast<void>(storage.readAndReconcileAuthorityRecords(transition.getAfterInventory(), transition.getAfterGraph())));

    const String metadata_path = paths.tableMetadataPath(object_name);
    writeFile(disk, metadata_path, "tampered-table-metadata");
    EXPECT_THROW(
        static_cast<void>(storage.readAndReconcileAuthorityRecords(transition.getAfterInventory(), transition.getAfterGraph())),
        StorageError);
    writeFile(disk, metadata_path, metadata);

    const String installation_path = paths.metadataInstallationRecordPath(table);
    const String installation_bytes = readFile(disk, installation_path);
    disk->removeFile(installation_path);
    EXPECT_THROW(
        static_cast<void>(storage.readAndReconcileAuthorityRecords(transition.getAfterInventory(), transition.getAfterGraph())),
        StorageError);
    writeFile(disk, installation_path, installation_bytes);

    auto installation = decodeDependentObjectMetadataInstallationRecord(installation_bytes);
    installation.object_name = "other-events";
    writeFile(disk, installation_path, encodeDependentObjectMetadataInstallationRecord(installation));
    EXPECT_THROW(
        static_cast<void>(storage.readAndReconcileAuthorityRecords(transition.getAfterInventory(), transition.getAfterGraph())),
        StorageError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, PhysicalOnlyTableCanEnterMappedAuthorityAtItsExistingName)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto first = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, first));
    auto activation = model.dependentObjectActivationTransition();
    auto activation_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));

    const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 4));
    const String object_name = "existing-events";
    const String physical_before = "physical-before";
    const String logical_after = "logical-after";
    const String target = storage.getPaths().tableMetadataPath(object_name);
    writeFile(disk, target, physical_before);
    auto transition = model.tableSidecarTransition(table, object_name, logical_after, physical_before);
    auto guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition));

    EXPECT_EQ(readFile(disk, target), logical_after);
    const auto reconciled = storage.readAndReconcileAuthorityRecords(transition.getAfterInventory(), transition.getAfterGraph());
    ASSERT_EQ(reconciled.dependent_objects.size(), 1);
    EXPECT_EQ(reconciled.dependent_objects.front().object_name, object_name);
    EXPECT_EQ(reconciled.dependent_objects.front().canonical_metadata_bytes, logical_after);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, PhysicalOnlyBeforeImageRollsBackThroughOppositeMappedName)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto first = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, first));
    auto activation = model.dependentObjectActivationTransition();
    auto activation_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));

    const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 5));
    const String object_name = "rollback-events";
    const String physical_before = "physical-before";
    const String target = storage.getPaths().tableMetadataPath(object_name);
    writeFile(disk, target, physical_before);
    auto transition = model.tableSidecarTransition(table, object_name, "logical-after", physical_before);
    auto guard = storage.issueMutationGuard();
    persistPrepare(storage, guard, transition);

    EXPECT_EQ(recoverDatabaseSchemaMutation(storage, guard, transition, std::nullopt), DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
    EXPECT_EQ(readFile(disk, target), physical_before);
    EXPECT_FALSE(disk->existsFile(storage.getPaths().tableReferencesPath(table)));
    EXPECT_FALSE(disk->existsFile(storage.getPaths().metadataInstallationRecordPath(table)));
    retireRolledBackDatabaseSchemaMutation(storage, guard, transition.getPrepare().transaction_id);
    EXPECT_EQ(storage.getCurrentAuthorityState(), model.dependent_object_state);
    EXPECT_NO_THROW(static_cast<void>(storage.readAndReconcileAuthorityRecords(*model.definition_inventory, *model.definition_graph)));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, TableMetadataCollisionIsRejectedBeforePrepareAndRestartPreservesForeignFile)
{
    String target;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, first));
        auto activation = model.dependentObjectActivationTransition();
        auto activation_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));

        const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 3));
        target = storage.getPaths().tableMetadataPath("events/2026");
        writeFile(disk, target, "foreign-metadata");
        auto transition = model.tableSidecarTransition(table);
        auto guard = storage.issueMutationGuard();
        EXPECT_THROW(
            static_cast<void>(executeDatabaseSchemaMutation(storage, guard, transition)), DatabaseSchemaMutationReplayConflictError);
        EXPECT_EQ(guard.getState(), DatabaseSchemaMutationGuard::State::Ready);
        EXPECT_FALSE(disk->existsFile(storage.getPaths().preparePath(transition.getPrepare().transaction_id)));
        EXPECT_EQ(readFile(disk, target), "foreign-metadata");
        discardUnpreparedDatabaseSchemaMutationStaging(storage, guard, transition.getPrepare().transaction_id);
        EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().stagingTransactionDirectory(transition.getPrepare().transaction_id)));
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    EXPECT_FALSE(restarted.getRecoveryRequiredTransactionID());
    EXPECT_EQ(restarted.listDurableTransactionIDs(), (std::vector<UInt64>{100, 101}));
    EXPECT_EQ(restarted.getCurrentAuthorityState(), model.dependent_object_state);
    EXPECT_EQ(readFile(disk, target), "foreign-metadata");
    EXPECT_NO_THROW(static_cast<void>(restarted.readAndReconcileAuthorityRecords(*model.definition_inventory, *model.definition_graph)));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, DuplicateMappedTableTargetsAreRejectedBeforePrepare)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto first = model.firstTransition();
    static_cast<void>(executeFirstActivation(storage, first));
    auto activation = model.dependentObjectActivationTransition();
    auto activation_guard = storage.issueMutationGuard();
    static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));

    constexpr UInt64 transaction_id = 102;
    constexpr UInt64 revision = 3;
    auto guard = storage.issueMutationGuard();
    storage.validateMutationGuardAndDurablePredecessor(guard, model.dependent_object_state, transaction_id);
    const auto stage = [&](UInt64 ordinal, DatabaseSchemaWALStagedArtifactKind kind, const SchemaObjectID & object, const String & bytes)
    {
        storage.stageArtifact(
            makeDatabaseSchemaWALStagedArtifactLocator(model.database_uuid, transaction_id, ordinal),
            {
                .kind = kind,
                .image = DatabaseSchemaWALStagedArtifactImage::After,
                .object = object,
                .revision = revision,
                .byte_size = bytes.size(),
                .content_hash = computeDatabaseSchemaWALStagedArtifactHash(kind, bytes),
            },
            bytes);
    };

    for (UInt64 index = 0; index < 2; ++index)
    {
        const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8100, index + 1));
        const String metadata = "table-metadata-" + std::to_string(index);
        const String installation = encodeDependentObjectMetadataInstallationRecord({
            .object = table,
            .object_schema_revision = revision,
            .object_name = "shared-target",
            .metadata_artifact_hash
            = computeDatabaseSchemaWALStagedArtifactHash(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, metadata),
        });
        stage(2 * index, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, table, metadata);
        stage(2 * index + 1, DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord, table, installation);
    }

    EXPECT_THROW(storage.finishStaging(model.database_uuid, transaction_id), DatabaseSchemaMutationReplayConflictError);
    EXPECT_FALSE(disk->existsFile(storage.getPaths().preparePath(transaction_id)));
    discardUnpreparedDatabaseSchemaMutationStaging(storage, guard, transaction_id);
    EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().stagingTransactionDirectory(transaction_id)));
    EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().tableMetadataPath("shared-target")));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, CheckpointRetirementSurvivesRenameAndDeletesOnlyCoveredData)
{
    for (size_t cleanup_boundary = 0; cleanup_boundary != 4; ++cleanup_boundary)
    {
        const String metadata_root = "metadata/app" + std::to_string(cleanup_boundary);
        auto transition = model.firstTransition();
        DatabaseSchemaWALValidatedCheckpoint checkpoint = [&]
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, metadata_root);
            const auto commit = executeFirstActivation(storage, transition);
            auto result = DatabaseSchemaWALCheckpointBuilder::build(
                1, commit, model.definition_only_state, model.definition_inventory, model.definition_graph);
            auto checkpoint_guard = storage.issueMutationGuard();
            writeFile(disk, storage.getPaths().checkpointInventorySnapshotPath(1) + ".inventory.tmp", "partial");
            writeFile(disk, storage.getPaths().checkpointSchemaGraphSnapshotPath(1) + ".graph.tmp", "partial");
            writeFile(disk, storage.getPaths().checkpointRecordPath(1) + ".checkpoint.tmp", "partial");
            persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, result);
            const String destination = storage.getPaths().retiredCheckpointTransactionDirectory(1, 100);
            disk->createDirectories(std::filesystem::path(destination).parent_path().generic_string());
            disk->moveDirectory(storage.getPaths().walTransactionDirectory(100), destination);
            if (cleanup_boundary >= 1)
                disk->removeRecursive(storage.getPaths().stagingTransactionDirectory(100));
            if (cleanup_boundary >= 2)
                disk->removeFile(destination + "/prepare.wal");
            if (cleanup_boundary >= 3)
                disk->removeFile(destination + "/commit.wal");
            return result;
        }();

        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, metadata_root);
        EXPECT_TRUE(restarted.hasDurableAuthorityMarker());
        auto sweep_guard = restarted.issueMutationGuard();
        EXPECT_EQ(restarted.sweepRetiredTransactions(sweep_guard), 1);
        EXPECT_FALSE(disk->existsFileOrDirectory(restarted.getPaths().stagingTransactionDirectory(100)));
        ASSERT_TRUE(restarted.loadLatestCheckpoint());
        EXPECT_EQ(restarted.loadLatestCheckpoint()->checkpoint, checkpoint.getCheckpoint());
        EXPECT_EQ(restarted.getCurrentAuthorityState(), model.definition_only_state);
        EXPECT_TRUE(restarted.hasDurableAuthorityMarker());
    }
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, CheckpointRetirementReconcilesLiveSourceDeletionProgress)
{
    String source;
    String destination;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        const auto commit = executeFirstActivation(storage, transition);
        const auto checkpoint = DatabaseSchemaWALCheckpointBuilder::build(
            1, commit, model.definition_only_state, model.definition_inventory, model.definition_graph);
        auto checkpoint_guard = storage.issueMutationGuard();
        persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, checkpoint);

        source = storage.getPaths().walTransactionDirectory(100);
        destination = storage.getPaths().retiredCheckpointTransactionDirectory(1, 100);
        copyDirectory(disk, source, destination);
        disk->removeFile(source + "/commit.wal");
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    const auto result = recoverAtomicAuthorityAtStartup(restarted);
    ASSERT_TRUE(result.authority_root);
    EXPECT_EQ(result.authority_root->getAuthorityState(), model.definition_only_state);
    EXPECT_EQ(result.swept_retired_transactions, 1);
    EXPECT_FALSE(disk->existsFileOrDirectory(source));
    EXPECT_FALSE(disk->existsFileOrDirectory(destination));
    EXPECT_FALSE(disk->existsFileOrDirectory(restarted.getPaths().stagingTransactionDirectory(100)));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, RetiredCleanupRejectsStatesOutsideItsExactDeletionOrder)
{
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/rollback");
        auto transition = model.firstTransition();
        auto guard = storage.issueMutationGuard();
        persistPrepare(storage, guard, transition);
        storage.finishRecovery(model.database_uuid, 100, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
        const String retired = storage.getPaths().retiredRollbackTransactionDirectory(100);
        disk->createDirectories(std::filesystem::path(retired).parent_path().generic_string());
        disk->moveDirectory(storage.getPaths().walTransactionDirectory(100), retired);
        disk->removeFile(retired + "/prepare.wal");
    }
    AtomicDatabaseSchemaMutationStorage rollback_restarted(disk, model.database_uuid, "metadata/rollback");
    EXPECT_THROW(static_cast<void>(rollback_restarted.hasDurableAuthorityMarker()), StorageError);

    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/checkpoint");
        auto transition = model.firstTransition();
        const auto commit = executeFirstActivation(storage, transition);
        const auto checkpoint = DatabaseSchemaWALCheckpointBuilder::build(
            1, commit, model.definition_only_state, model.definition_inventory, model.definition_graph);
        auto checkpoint_guard = storage.issueMutationGuard();
        persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, checkpoint);
        const String retired = storage.getPaths().retiredCheckpointTransactionDirectory(1, 100);
        disk->createDirectories(std::filesystem::path(retired).parent_path().generic_string());
        disk->moveDirectory(storage.getPaths().walTransactionDirectory(100), retired);
        disk->removeFile(retired + "/commit.wal");
    }
    AtomicDatabaseSchemaMutationStorage checkpoint_restarted(disk, model.database_uuid, "metadata/checkpoint");
    EXPECT_THROW(static_cast<void>(checkpoint_restarted.hasDurableAuthorityMarker()), StorageError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupNeverEnabledProbeAndTypesOnlyCleanupStayUnmanaged)
{
    AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
    auto absent = recoverAtomicAuthorityAtStartup(storage);
    EXPECT_FALSE(absent.authority_root);
    EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().typesDirectory()));

    disk->createDirectories(storage.getPaths().typesDirectory());
    auto types_only = recoverAtomicAuthorityAtStartup(storage);
    EXPECT_FALSE(types_only.authority_root);
    EXPECT_FALSE(disk->existsFileOrDirectory(storage.getPaths().typesDirectory()));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupRollsBackFirstPrepareWithPreCommitActivationMarkerToExactPreActivationState)
{
    String activation_marker_path;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        auto guard = storage.issueMutationGuard();
        persistPrepare(storage, guard, transition);
        activation_marker_path = storage.getPaths().activationMarkerPath();
        ASSERT_FALSE(disk->existsFileOrDirectory(activation_marker_path));
        writeFile(disk, activation_marker_path, encodeAuthorityActivationMarker(model.database_uuid, 100));
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto result = recoverAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(result.authority_root);
    EXPECT_EQ(result.rolled_back_transactions, 1);
    EXPECT_FALSE(disk->existsFileOrDirectory(restarted.getPaths().typesDirectory()));
    EXPECT_FALSE(disk->existsFileOrDirectory(activation_marker_path));
    EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupRepairsInterruptedHighWaterMaterializationFromDurablePrepare)
{
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        auto guard = storage.issueMutationGuard();
        persistPrepare(storage, guard, transition);
        const String high_water = storage.getPaths().highWaterMarkPath();
        const String temporary = high_water + ".00000000000000000100.tmp";
        disk->moveFile(high_water, temporary);
        writeFile(disk, temporary, "partial");
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    EXPECT_TRUE(restarted.hasDurableAuthorityMarker());
    EXPECT_EQ(restarted.getDurableHighWaterMark(), 100);
    auto result = recoverAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(result.authority_root);
    EXPECT_EQ(result.rolled_back_transactions, 1);
    EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupRepairsPartialInstallAndRecoveryTemporaries)
{
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        auto guard = storage.issueMutationGuard();
        persistPrepare(storage, guard, transition);
        const String target = storage.getPaths().authorityRecordPath(model.leaf.key);
        writeFile(disk, target + ".install-00000000000000000100.tmp", "partial-install");
        writeFile(disk, storage.getPaths().commitPath(100) + ".commit.tmp", "partial-commit");
        writeFile(disk, storage.getPaths().recoveryDecisionPath(100) + ".recovery.tmp", "partial-recovery");
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto result = recoverAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(result.authority_root);
    EXPECT_EQ(result.rolled_back_transactions, 1);
    EXPECT_FALSE(disk->existsFileOrDirectory(restarted.getPaths().typesDirectory()));
    EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupRetiresLiveRollbackDecisionAfterStagingWasAlreadyDeleted)
{
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        auto guard = storage.issueMutationGuard();
        persistPrepare(storage, guard, transition);
        storage.finishRecovery(model.database_uuid, 100, DatabaseSchemaWALRecoveryDecision::RollBackPrepared);
        disk->removeRecursive(storage.getPaths().stagingTransactionDirectory(100));
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto result = recoverAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(result.authority_root);
    EXPECT_EQ(result.rolled_back_transactions, 1);
    EXPECT_FALSE(disk->existsFileOrDirectory(restarted.getPaths().typesDirectory()));
    EXPECT_FALSE(restarted.hasDurableAuthorityMarker());
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupReconstructsCommittedFirstAuthorityRoot)
{
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, transition));
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto result = recoverAtomicAuthorityAtStartup(restarted);
    ASSERT_TRUE(result.authority_root);
    EXPECT_EQ(result.authority_root->getAuthorityState(), model.definition_only_state);
    EXPECT_EQ(result.completed_transactions, 1);
    ASSERT_EQ(result.authority_root->getDefinitionRecords().size(), 1);
    EXPECT_EQ(result.authority_root->getDefinitionRecords().front(), model.record);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupRepairsMalformedAndForeignDefinitionImagesFromCommittedLocalWAL)
{
    std::vector<std::pair<String, String>> damaged_images;
    damaged_images.emplace_back("malformed", String(1, '\x01'));
    damaged_images.emplace_back(
        "foreign", encodeRecord(definitionRecord(uuid(0x1111222233334444ULL, 0x5555666677778888ULL), uuid(0x9000, 2))));

    for (const auto & [label, damaged_bytes] : damaged_images)
    {
        SCOPED_TRACE(label);
        const String metadata_root = "metadata/definition-repair-" + label;
        String record_path;
        {
            AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, metadata_root);
            auto transition = model.firstTransition();
            static_cast<void>(executeFirstActivation(storage, transition));
            record_path = storage.getPaths().authorityRecordPath(model.leaf.key);
        }

        ASSERT_NE(damaged_bytes, model.record_bytes);
        writeFile(disk, record_path, damaged_bytes);

        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, metadata_root);
        auto result = recoverAtomicAuthorityAtStartup(restarted);
        ASSERT_TRUE(result.authority_root);
        EXPECT_FALSE(result.degraded_status);
        EXPECT_EQ(result.completed_transactions, 2);
        EXPECT_EQ(readFile(disk, record_path), model.record_bytes);
        ASSERT_EQ(result.authority_root->getDefinitionRecords().size(), 1);
        EXPECT_EQ(result.authority_root->getDefinitionRecords().front(), model.record);
        const auto & repaired_state = result.authority_root->getAuthorityState();
        EXPECT_EQ(repaired_state.database_catalog_epoch, model.definition_only_state.database_catalog_epoch + 1);
        EXPECT_EQ(repaired_state.inventory_root, model.definition_only_state.inventory_root);
        EXPECT_EQ(repaired_state.schema_graph_root, model.definition_only_state.schema_graph_root);

        const auto provenance = restarted.loadLatestExactRepairProvenance();
        ASSERT_TRUE(provenance);
        EXPECT_EQ(provenance->transaction_id, 101);
        EXPECT_EQ(provenance->damaged_artifact_count, 1);
        EXPECT_EQ(provenance->local_wal_sources, 1);
        EXPECT_EQ(provenance->replicated_authority_sources, 0);
        EXPECT_EQ(provenance->verified_backup_sources, 0);
        EXPECT_EQ(provenance->previous_catalog_epoch, model.definition_only_state.database_catalog_epoch);
        EXPECT_EQ(provenance->repaired_catalog_epoch, repaired_state.database_catalog_epoch);
    }
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupSidecarRepairCountsOnlyTheDamagedTargetInProvenance)
{
    const SchemaObjectID synthetic = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x7000, 6));
    const String metadata_root = "metadata/sidecar-repair-provenance";
    String sidecar_path;
    String expectation_path;
    String sidecar_bytes;
    String expectation_bytes;
    AuthorityState before_repair;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, metadata_root);
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, first));
        auto activation = model.dependentObjectActivationTransition();
        auto activation_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
        auto sidecar = model.syntheticSidecarTransition(synthetic);
        before_repair = sidecar.getPrepare().after_authority_state;
        auto sidecar_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, sidecar_guard, sidecar));

        sidecar_path
            = storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar, synthetic);
        expectation_path
            = storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord, synthetic);
        sidecar_bytes = readFile(disk, sidecar_path);
        expectation_bytes = readFile(disk, expectation_path);
    }

    const String damaged_bytes(1, '\x01');
    ASSERT_NE(damaged_bytes, sidecar_bytes);
    writeFile(disk, sidecar_path, damaged_bytes);

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, metadata_root);
    auto result = recoverAtomicAuthorityAtStartup(
        restarted,
        {},
        [](const SidecarExpectationRecord & expectation,
           std::string_view canonical_metadata_bytes,
           std::string_view canonical_sidecar_bytes)
        { return validateSyntheticDependentObjectMetadata(expectation, canonical_metadata_bytes, canonical_sidecar_bytes); });
    ASSERT_TRUE(result.authority_root);
    EXPECT_FALSE(result.degraded_status);
    EXPECT_EQ(result.completed_transactions, 4);
    EXPECT_EQ(readFile(disk, sidecar_path), sidecar_bytes);
    EXPECT_EQ(readFile(disk, expectation_path), expectation_bytes);

    const auto & repaired_state = result.authority_root->getAuthorityState();
    EXPECT_EQ(repaired_state.database_catalog_epoch, before_repair.database_catalog_epoch + 1);
    EXPECT_EQ(repaired_state.inventory_root, before_repair.inventory_root);
    EXPECT_EQ(repaired_state.schema_graph_root, before_repair.schema_graph_root);

    const auto provenance = restarted.loadLatestExactRepairProvenance();
    ASSERT_TRUE(provenance);
    EXPECT_EQ(provenance->transaction_id, 103);
    EXPECT_EQ(provenance->damaged_artifact_count, 1);
    EXPECT_EQ(provenance->local_wal_sources, 1);
    EXPECT_EQ(provenance->replicated_authority_sources, 0);
    EXPECT_EQ(provenance->verified_backup_sources, 0);
    EXPECT_EQ(provenance->previous_catalog_epoch, before_repair.database_catalog_epoch);
    EXPECT_EQ(provenance->repaired_catalog_epoch, provenance->previous_catalog_epoch + 1);
    const std::vector<DatabaseSchemaWALStagedArtifact> damaged_manifest{{
        .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
        .image = DatabaseSchemaWALStagedArtifactImage::After,
        .object = synthetic,
        .revision = 2,
        .canonical_bytes = sidecar_bytes,
    }};
    EXPECT_EQ(provenance->damaged_artifact_manifest_digest, computeDatabaseSchemaWALExactRepairArtifactManifestDigest(damaged_manifest));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupKeepsKeyedInvalidFallbackWhenDefinitionRepairCandidateIsUnavailable)
{
    const String metadata_root = "metadata/definition-repair-unavailable";
    String record_path;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, metadata_root);
        auto transition = model.firstTransition();
        const auto commit = executeFirstActivation(storage, transition);
        const auto checkpoint = DatabaseSchemaWALCheckpointBuilder::build(
            1, commit, model.definition_only_state, model.definition_inventory, model.definition_graph);
        auto checkpoint_guard = storage.issueMutationGuard();
        persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, checkpoint);
        auto compaction_guard = storage.issueMutationGuard();
        compactDatabaseSchemaWALThroughValidatedCheckpoint(storage, compaction_guard, checkpoint);
        EXPECT_TRUE(storage.listDurableTransactionIDs().empty());
        record_path = storage.getPaths().authorityRecordPath(model.leaf.key);
    }

    const String damaged_bytes(1, '\x01');
    writeFile(disk, record_path, damaged_bytes);

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, metadata_root);
    auto result = recoverAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(result.authority_root);
    ASSERT_TRUE(result.degraded_status);
    EXPECT_EQ(result.completed_transactions, 0);
    EXPECT_EQ(readFile(disk, record_path), damaged_bytes);
    EXPECT_EQ(restarted.getCurrentAuthorityState(), model.definition_only_state);
    EXPECT_FALSE(restarted.loadLatestExactRepairProvenance());
    EXPECT_EQ(result.degraded_status->getGlobalStatus(), AuthorityDefinitionStatus::Invalid);
    EXPECT_EQ(
        result.degraded_status->getGlobalLastError(), "durable definition record does not match its anchored identity or canonical bytes");
    const auto diagnostics = result.degraded_status->getDefinitionDiagnostics();
    ASSERT_EQ(diagnostics.size(), 1);
    EXPECT_EQ(diagnostics.front().key, model.leaf.key);
    EXPECT_EQ(diagnostics.front().status, AuthorityDefinitionStatus::Invalid);
    EXPECT_FALSE(diagnostics.front().record);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupValidatesCommittedPrefixWithoutReinstallingOlderArtifactImages)
{
    Record second_record = model.record;
    second_record.normalized_name = "db.Beta";
    second_record.normalized_local_name = "Beta";
    second_record.comment = "second presentation";
    second_record.canonical_definition_sql = canonicalAttachTypeSQL(
        second_record.normalized_name,
        second_record.identity,
        second_record.definition_hash,
        second_record.canonical_physical_template_sql,
        second_record.comment);
    static_cast<void>(encodeRecord(second_record));

    Record third_record = second_record;
    third_record.normalized_name = "db.Gamma";
    third_record.normalized_local_name = "Gamma";
    third_record.comment = "third presentation";
    third_record.canonical_definition_sql = canonicalAttachTypeSQL(
        third_record.normalized_name,
        third_record.identity,
        third_record.definition_hash,
        third_record.canonical_physical_template_sql,
        third_record.comment);
    static_cast<void>(encodeRecord(third_record));

    AuthorityState expected_state;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, first));

        auto second = model.definitionPresentationTransition(
            101, model.record, model.definition_only_state, model.definition_inventory, second_record);
        auto second_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, second_guard, second));

        auto third = model.definitionPresentationTransition(
            102, second_record, second.getPrepare().after_authority_state, second.pinAfterInventory(), third_record);
        expected_state = third.getPrepare().after_authority_state;
        auto third_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, third_guard, third));
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    const auto result = recoverAtomicAuthorityAtStartup(restarted);
    ASSERT_TRUE(result.authority_root);
    EXPECT_EQ(result.completed_transactions, 3);
    EXPECT_EQ(result.authority_root->getAuthorityState(), expected_state);
    ASSERT_EQ(result.authority_root->getDefinitionRecords().size(), 1);
    EXPECT_EQ(result.authority_root->getDefinitionRecords().front(), third_record);
    for (const UInt64 transaction_id : std::array<UInt64, 3>{100, 101, 102})
        EXPECT_FALSE(disk->existsFile(restarted.getPaths().recoveryDecisionPath(transaction_id)));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupRejectsNonterminalUnresolvedPrepareBeforePhysicalRecovery)
{
    Record later_record = model.record;
    later_record.normalized_name = "db.Later";
    later_record.normalized_local_name = "Later";
    later_record.comment = "later committed transaction";
    later_record.canonical_definition_sql = canonicalAttachTypeSQL(
        later_record.normalized_name,
        later_record.identity,
        later_record.definition_hash,
        later_record.canonical_physical_template_sql,
        later_record.comment);
    static_cast<void>(encodeRecord(later_record));

    AtomicDatabaseSchemaMutationPaths target_paths("metadata/target", model.database_uuid);
    {
        AtomicDatabaseSchemaMutationStorage target(disk, model.database_uuid, "metadata/target");
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(target, first));
        auto unresolved = model.dependentObjectActivationTransition();
        auto unresolved_guard = target.issueMutationGuard();
        persistPrepare(target, unresolved_guard, unresolved);
    }

    {
        AtomicDatabaseSchemaMutationStorage generator(disk, model.database_uuid, "metadata/generator");
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(generator, first));
        auto later = model.definitionPresentationTransition(
            102, model.record, model.definition_only_state, model.definition_inventory, later_record);
        auto later_guard = generator.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(generator, later_guard, later));
        copyDirectory(disk, generator.getPaths().stagingTransactionDirectory(102), target_paths.stagingTransactionDirectory(102));
        copyDirectory(disk, generator.getPaths().walTransactionDirectory(102), target_paths.walTransactionDirectory(102));
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/target");
    auto degraded = recoverAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(degraded.authority_root);
    EXPECT_TRUE(degraded.degraded_status);
    EXPECT_EQ(readFile(disk, target_paths.authorityRecordPath(model.leaf.key)), model.record_bytes);
    EXPECT_FALSE(disk->existsFile(target_paths.recoveryDecisionPath(101)));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupChainsCheckpointAndCommittedTailAfterCompaction)
{
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto first = model.firstTransition();
        const auto first_commit = executeFirstActivation(storage, first);
        const auto checkpoint = DatabaseSchemaWALCheckpointBuilder::build(
            1, first_commit, model.definition_only_state, model.definition_inventory, model.definition_graph);
        auto checkpoint_guard = storage.issueMutationGuard();
        persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, checkpoint);

        auto activation = model.dependentObjectActivationTransition();
        auto activation_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
        auto replay_guard = storage.issueMutationGuard();
        persistValidatedDatabaseSchemaCheckpoint(storage, replay_guard, checkpoint);
        EXPECT_THROW(
            storage.persistValidatedCheckpoint(
                replay_guard,
                checkpoint.getCheckpoint(),
                encodeDatabaseSchemaWALCheckpoint(checkpoint.getCheckpoint()),
                checkpoint.getInventorySnapshotBytes() + "x",
                checkpoint.getSchemaGraphSnapshotBytes()),
            DatabaseSchemaMutationReplayConflictError);
        auto compaction_guard = storage.issueMutationGuard();
        storage.compactThroughValidatedCheckpoint(compaction_guard, checkpoint.getCheckpoint());
        EXPECT_EQ(storage.listDurableTransactionIDs(), std::vector<UInt64>{101});
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto result = recoverAtomicAuthorityAtStartup(restarted);
    ASSERT_TRUE(result.authority_root);
    EXPECT_EQ(result.authority_root->getAuthorityState(), model.dependent_object_state);
    EXPECT_EQ(result.completed_transactions, 1);
    EXPECT_EQ(restarted.listDurableTransactionIDs(), std::vector<UInt64>{101});
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, ProductionStartupReturnsMappedTableAsExactPendingImage)
{
    const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 6));
    const String object_name = "events/pending";
    const String metadata = "CREATE TABLE pending (value UInt64) ENGINE = Memory";
    AtomicDatabaseSchemaMutationDependentObjectImage expected_pending;
    AuthorityState expected_state;

    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, first));
        auto activation = model.dependentObjectActivationTransition();
        auto activation_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
        auto table_transition = model.tableSidecarTransition(table, object_name, metadata);
        auto table_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, table_guard, table_transition));
        expected_state = table_transition.getPrepare().after_authority_state;

        const auto reconciled
            = storage.readAndReconcileAuthorityRecords(*table_transition.pinAfterInventory(), *table_transition.pinAfterGraph());
        ASSERT_EQ(reconciled.dependent_objects.size(), 1);
        expected_pending = reconciled.dependent_objects.front();
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    const auto recovered = recoverAndActivateAtomicAuthorityAtStartup(restarted);
    ASSERT_TRUE(recovered.authority_root);
    EXPECT_EQ(recovered.authority_root->getAuthorityState(), expected_state);
    EXPECT_EQ(recovered.completed_transactions, 3);
    ASSERT_EQ(recovered.pending_tables.size(), 1);
    const auto & pending = recovered.pending_tables.front();
    EXPECT_EQ(pending.expectation, expected_pending.expectation);
    EXPECT_EQ(pending.object_name, object_name);
    EXPECT_EQ(pending.object_name, expected_pending.object_name);
    EXPECT_EQ(pending.canonical_metadata_bytes, metadata);
    EXPECT_EQ(pending.canonical_metadata_bytes, expected_pending.canonical_metadata_bytes);
    EXPECT_EQ(pending.canonical_sidecar_bytes, expected_pending.canonical_sidecar_bytes);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, OrdinaryMutationCheckReadsOnlyOuterStorageMetadata)
{
    auto context = Context::createCopy(getContext().context);
    auto database = std::make_shared<PendingTableStartupTestDatabase>(
        "app", DatabaseCatalog::getStoreDirPath(model.database_uuid).string(), model.database_uuid, context, disk);
    SCOPE_EXIT({
        try
        {
            database->shutdown();
        }
        catch (...)
        {
            ADD_FAILURE() << "Failed to shut down the outer-metadata fixture";
        }
    });

    auto table = std::make_shared<DecoratingMetadataStorage>(StorageID("app", "ordinary_view", uuid(0x8000, 0x61)));
    EXPECT_NO_THROW(database->assertUDTTableAllowsOrdinaryMetadataMutation(table, context, "ATTACH"));
    EXPECT_EQ(table->getDecoratedMetadataReads(), 0);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, VerificationSchedulerRemainsDormantUntilDatabaseStartupActivation)
{
    auto context = Context::createCopy(getContext().context);
    auto database = std::make_shared<PendingTableStartupTestDatabase>(
        "scheduler_dormant", "metadata/scheduler_dormant", model.database_uuid, context, disk);
    std::atomic<UInt64> callbacks = 0;
    AuthorityVerificationSchedulerLimits limits;
    limits.load_throttle_retry_interval = std::chrono::hours(1);
    limits.load_probe = [&]
    {
        callbacks.fetch_add(1, std::memory_order_relaxed);
        return AuthorityVerificationSchedulerLoadSnapshot{.foreground_queries = 1};
    };

    {
        AuthorityVerificationScheduler scheduler(*database, limits);
        EXPECT_EQ(scheduler.getStatus().state, AuthorityVerificationSchedulerState::Dormant);
        ASSERT_TRUE(runDatabaseTestSchedulePoolSentinel(context));
        EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 0);
        EXPECT_EQ(scheduler.getStatus().runs, 0);

        scheduler.activateAfterDatabaseStartup();
        ASSERT_TRUE(waitForDatabaseTestCondition(
            [&] { return callbacks.load(std::memory_order_acquire) != 0; }, std::chrono::seconds(5)));
        scheduler.shutdownAndDrain();
        const auto status = scheduler.getStatus();
        EXPECT_EQ(status.state, AuthorityVerificationSchedulerState::Shutdown);
        EXPECT_EQ(status.runs, 1);
        EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 1);
    }
    database->shutdown();
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, VerificationSchedulerShutdownBeforeActivationIsSticky)
{
    auto context = Context::createCopy(getContext().context);
    auto database = std::make_shared<PendingTableStartupTestDatabase>(
        "scheduler_pre_stopped", "metadata/scheduler_pre_stopped", model.database_uuid, context, disk);
    std::atomic<UInt64> callbacks = 0;
    AuthorityVerificationSchedulerLimits limits;
    limits.load_probe = [&]
    {
        callbacks.fetch_add(1, std::memory_order_relaxed);
        return AuthorityVerificationSchedulerLoadSnapshot{.foreground_queries = 1};
    };

    {
        AuthorityVerificationScheduler scheduler(*database, limits);
        scheduler.shutdownAndDrain();
        scheduler.activateAfterDatabaseStartup();
        ASSERT_TRUE(runDatabaseTestSchedulePoolSentinel(context));
        const auto status = scheduler.getStatus();
        EXPECT_EQ(status.state, AuthorityVerificationSchedulerState::Shutdown);
        EXPECT_EQ(status.runs, 0);
        EXPECT_EQ(callbacks.load(std::memory_order_relaxed), 0);
    }
    database->shutdown();
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, VerificationSchedulerConcurrentActivationAndShutdownAlwaysDrains)
{
    auto context = Context::createCopy(getContext().context);
    auto database = std::make_shared<PendingTableStartupTestDatabase>(
        "scheduler_activation_race", "metadata/scheduler_activation_race", model.database_uuid, context, disk);

    for (size_t iteration = 0; iteration < 32; ++iteration)
    {
        std::atomic<UInt64> callbacks = 0;
        std::atomic<UInt64> failures = 0;
        AuthorityVerificationSchedulerLimits limits;
        limits.load_throttle_retry_interval = std::chrono::hours(1);
        limits.load_probe = [&]
        {
            callbacks.fetch_add(1, std::memory_order_relaxed);
            return AuthorityVerificationSchedulerLoadSnapshot{.foreground_queries = 1};
        };
        AuthorityVerificationScheduler scheduler(*database, limits);
        std::barrier<> start(3);
        std::thread activation(
            [&]
            {
                start.arrive_and_wait();
                try
                {
                    scheduler.activateAfterDatabaseStartup();
                }
                catch (...)
                {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            });
        std::thread shutdown(
            [&]
            {
                start.arrive_and_wait();
                try
                {
                    scheduler.shutdownAndDrain();
                }
                catch (...)
                {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
            });
        start.arrive_and_wait();
        activation.join();
        shutdown.join();

        EXPECT_EQ(failures.load(std::memory_order_relaxed), 0);
        EXPECT_EQ(scheduler.getStatus().state, AuthorityVerificationSchedulerState::Shutdown);
        const UInt64 completed_callbacks = callbacks.load(std::memory_order_acquire);
        ASSERT_TRUE(runDatabaseTestSchedulePoolSentinel(context));
        EXPECT_EQ(callbacks.load(std::memory_order_relaxed), completed_callbacks);
    }
    database->shutdown();
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, VerificationSchedulerShutdownWaitsForExecutingCallback)
{
    auto context = Context::createCopy(getContext().context);
    auto database = std::make_shared<PendingTableStartupTestDatabase>(
        "scheduler_running_shutdown", "metadata/scheduler_running_shutdown", model.database_uuid, context, disk);
    std::mutex probe_mutex;
    std::condition_variable probe_cv;
    bool probe_entered = false;
    bool release_probe = false;
    AuthorityVerificationSchedulerLimits limits;
    limits.load_probe = [&]
    {
        std::unique_lock lock(probe_mutex);
        probe_entered = true;
        probe_cv.notify_all();
        probe_cv.wait(lock, [&] { return release_probe; });
        return AuthorityVerificationSchedulerLoadSnapshot{.foreground_queries = 1};
    };

    AuthorityVerificationScheduler scheduler(*database, limits);
    SCOPE_EXIT({
        {
            std::lock_guard lock(probe_mutex);
            release_probe = true;
        }
        probe_cv.notify_all();
        scheduler.shutdownAndDrain();
        database->shutdown();
    });
    scheduler.activateAfterDatabaseStartup();
    {
        std::unique_lock lock(probe_mutex);
        ASSERT_TRUE(probe_cv.wait_for(lock, std::chrono::seconds(5), [&] { return probe_entered; }));
    }

    auto shutdown = std::async(std::launch::async, [&] { scheduler.shutdownAndDrain(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    {
        std::lock_guard lock(probe_mutex);
        release_probe = true;
    }
    probe_cv.notify_all();
    ASSERT_EQ(shutdown.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_NO_THROW(shutdown.get());
    EXPECT_EQ(scheduler.getStatus().state, AuthorityVerificationSchedulerState::Shutdown);
    ASSERT_TRUE(runDatabaseTestSchedulePoolSentinel(context));
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, DatabaseAtomicPublishesRecoveredAuthorityOnlyAfterExactMappedTableBinding)
{
    constexpr UInt64 object_schema_revision = 3;
    const String atomic_metadata_root = DatabaseCatalog::getStoreDirPath(model.database_uuid).string();
    const SchemaObjectID table = objectID(SchemaObjectKind::Table, model.database_uuid, uuid(0x8000, 7));
    const auto uint64_type = std::make_shared<DataTypeUInt64>();
    const Digest physical_schema_fingerprint = computeTableColumnPhysicalSchemaFingerprint(NamesAndTypesList{{"id", uint64_type}});
    const auto persisted_references
        = references(table, object_schema_revision, model.record, physical_schema_fingerprint, physicalTypeFingerprint(uint64_type));
    const String canonical_sidecar = encodePersistedTypeReferences(persisted_references);
    const SidecarExpectationRecord validation_expectation{
        .object = table,
        .object_schema_revision = object_schema_revision,
        .sidecar_hash = computePersistedTypeReferencesSidecarHash(persisted_references),
        .physical_schema_fingerprint = physical_schema_fingerprint,
    };

    const String metadata = "ATTACH TABLE app.events UUID '" + toString(table.object_uuid) + "' (id UInt64) ENGINE = Memory";
    const ASTPtr trusted_query = parseTableMetadata(metadata);
    const StoragePtr trusted_table = makeMemoryTable(table, uint64_type);
    AtomicTableMetadataValidator metadata_validator(model.database_uuid, trusted_query, trusted_table);
    auto validated_metadata = metadata_validator.validateAndCanonicalize(validation_expectation, metadata, canonical_sidecar);
    const String canonical_metadata = validated_metadata.releaseCanonicalMetadataBytes();

    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, atomic_metadata_root);
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, first));
        auto activation = model.dependentObjectActivationTransition();
        auto activation_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
        auto table_transition = model.tableSidecarTransition(table, "events", canonical_metadata, std::nullopt, persisted_references);
        auto table_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, table_guard, table_transition));
    }

    auto context = Context::createCopy(getContext().context);
    auto & catalog = DatabaseCatalog::instance();
    ASSERT_FALSE(catalog.hasUUIDMapping(table.object_uuid));
    catalog.addUUIDMapping(table.object_uuid);
    std::shared_ptr<PendingTableStartupTestDatabase> database;
    SCOPE_EXIT({
        if (database)
        {
            try
            {
                database->shutdown();
            }
            catch (...)
            {
                ADD_FAILURE() << "Failed to shut down the Atomic pending Table startup fixture";
            }
            database.reset();
        }
        const auto [mapped_database, mapped_table] = catalog.tryGetByUUID(table.object_uuid);
        EXPECT_EQ(mapped_database, nullptr);
        EXPECT_EQ(mapped_table, nullptr);
        if (!mapped_database && !mapped_table && catalog.hasUUIDMapping(table.object_uuid))
            catalog.removeUUIDMappingFinally(table.object_uuid);
    });
    database = std::make_shared<PendingTableStartupTestDatabase>("app", atomic_metadata_root, model.database_uuid, context, disk);
    database->beforeLoadingMetadata(context, LoadingStrictnessLevel::ATTACH);
    EXPECT_NO_THROW(database->beforeLoadingMetadata(context, LoadingStrictnessLevel::ATTACH));

    EXPECT_TRUE(database->hasPendingTableStartupForTest());
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_THROW(database->getUDTLifecycleAdapter().acquireSnapshot(), Exception);
    EXPECT_THROW(database->activatePendingTablesForTest(), Exception);

    const auto & exact_query = trusted_query->as<ASTCreateQuery &>();
    EXPECT_TRUE(database->forceEagerTableLoadForTest(exact_query));
    EXPECT_THROW(database->validateDetachedTableForTest(exact_query), Exception);
    EXPECT_THROW(database->validateAutomaticRewriteForTest(exact_query), Exception);

    ASTPtr mismatched_name_ast = trusted_query->clone();
    auto & mismatched_name = mismatched_name_ast->as<ASTCreateQuery &>();
    mismatched_name.setTable("other");
    EXPECT_THROW(database->forceEagerTableLoadForTest(mismatched_name), Exception);

    ASTPtr unrelated_ast = trusted_query->clone();
    auto & unrelated = unrelated_ast->as<ASTCreateQuery &>();
    unrelated.setTable("other");
    unrelated.uuid = uuid(0x8000, 8);
    EXPECT_FALSE(database->forceEagerTableLoadForTest(unrelated));

    const StoragePtr wrong_table = makeMemoryTable(table, std::make_shared<DataTypeUInt32>());
    EXPECT_THROW(database->attachTable(context, "events", wrong_table, "store/wrong"), std::exception);
    EXPECT_FALSE(database->tryGetTable("events", context));
    EXPECT_THROW(database->getTableDataPath("events"), Exception);
    EXPECT_TRUE(catalog.hasUUIDMapping(table.object_uuid));
    {
        const auto [mapped_database, mapped_table] = catalog.tryGetByUUID(table.object_uuid);
        EXPECT_EQ(mapped_database, nullptr);
        EXPECT_EQ(mapped_table, nullptr);
    }
    const auto wrong_metadata = wrong_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(wrong_metadata);
    EXPECT_FALSE(wrong_metadata->getBoundUDTReferences());
    EXPECT_FALSE(wrong_metadata->getBoundUDTExpectation());
    EXPECT_FALSE(database->hasPendingTableStartupForTest());
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_TRUE(database->hasDegradedUDTStartupStatusForTest());
    auto degraded_snapshot = database->getUDTLifecycleAdapter().acquireSnapshot();
    EXPECT_EQ(degraded_snapshot->getDatabaseCatalogEpoch(), 0);
    EXPECT_TRUE(degraded_snapshot->getDefinitionRecords().empty());
    EXPECT_NO_THROW(database->activatePendingTablesForTest());

    /// Exact binding corruption is process-fatal for the private recovered
    /// root, but it does not mutate durable authority bytes. A fresh database
    /// image must therefore reconstruct the pending entry and may bind the
    /// canonical storage successfully.
    database->shutdown();
    database.reset();
    database = std::make_shared<PendingTableStartupTestDatabase>("app", atomic_metadata_root, model.database_uuid, context, disk);
    database->beforeLoadingMetadata(context, LoadingStrictnessLevel::ATTACH);
    EXPECT_TRUE(database->hasPendingTableStartupForTest());
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_FALSE(database->hasDegradedUDTStartupStatusForTest());
    EXPECT_THROW(database->getUDTLifecycleAdapter().acquireSnapshot(), Exception);

    database->attachTable(context, "events", trusted_table, "store/events");
    auto bound_metadata = trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(bound_metadata);
    ASSERT_TRUE(bound_metadata->getBoundUDTReferences());
    ASSERT_TRUE(bound_metadata->getBoundUDTExpectation());
    EXPECT_EQ(bound_metadata->getBoundUDTReferences()->getObject(), table);
    EXPECT_EQ(bound_metadata->getBoundUDTExpectation()->object, table);

    EXPECT_TRUE(database->hasPendingTableStartupForTest());
    EXPECT_FALSE(database->hasActiveUDTAuthority());
    EXPECT_THROW(database->getUDTLifecycleAdapter().acquireSnapshot(), Exception);
    EXPECT_THROW(database->activatePendingTablesForTest(), Exception);

    auto & async_loader = context->getAsyncLoader();
    auto table_startup = database->startupTableAsync(
        async_loader, {}, QualifiedTableName{.database = "app", .table = "events"}, LoadingStrictnessLevel::ATTACH);
    auto database_startup = database->startupDatabaseAsync(async_loader, table_startup->goals(), LoadingStrictnessLevel::ATTACH);
    EXPECT_NO_THROW(waitLoad(database_startup));
    database->shutdownVerificationSchedulerForTest();
    EXPECT_FALSE(database->hasPendingTableStartupForTest());
    EXPECT_TRUE(database->hasActiveUDTAuthority());
    auto lifecycle_snapshot = database->getUDTLifecycleAdapter().acquireSnapshot();
    EXPECT_EQ(lifecycle_snapshot->getDatabaseCatalogEpoch(), 3);
    ASSERT_EQ(lifecycle_snapshot->getDefinitionRecords().size(), 1);
    EXPECT_EQ(lifecycle_snapshot->getDefinitionRecords().front(), model.record);
    EXPECT_FALSE(database->forceEagerTableLoadForTest(exact_query));
    EXPECT_NO_THROW(database->validateDetachedTableForTest(exact_query));
    EXPECT_NO_THROW(database->validateAutomaticRewriteForTest(exact_query));

    const auto exact_read_evidence = database->acquireUDTStorageReadContinuationEvidence(bound_metadata);
    ASSERT_TRUE(exact_read_evidence);
    int cache_resolution_owner = 0;
    QueryResultCacheStorageDependencyCollector exact_cache_dependencies(true);
    ASSERT_TRUE(exact_cache_dependencies.tryBeginResolution(&cache_resolution_owner));
    exact_cache_dependencies.record(
        trusted_table->getStorageID(),
        trusted_table->getName(),
        QueryResultCacheStorageKind::Storage,
        bound_metadata->getBoundUDTReferences(),
        exact_read_evidence);
    exact_cache_dependencies.markResolutionComplete(&cache_resolution_owner);
    const auto exact_cache_proof = exact_cache_dependencies.snapshotIfComplete();
    ASSERT_TRUE(exact_cache_proof);
    ASSERT_EQ(exact_cache_proof->dependencies.size(), 1);
    ASSERT_TRUE(exact_cache_proof->dependencies.front().udt_binding);
    EXPECT_EQ(exact_cache_proof->dependencies.front().udt_binding->object, table);
    EXPECT_EQ(exact_cache_proof->dependencies.front().udt_binding->authority_root, exact_read_evidence->getPinnedRoot());

    QueryResultCacheStorageDependencyCollector evidence_without_bindings(true);
    ASSERT_TRUE(evidence_without_bindings.tryBeginResolution(&cache_resolution_owner));
    evidence_without_bindings.record(
        trusted_table->getStorageID(), trusted_table->getName(), QueryResultCacheStorageKind::Storage, {}, exact_read_evidence);
    evidence_without_bindings.markResolutionComplete(&cache_resolution_owner);
    EXPECT_FALSE(evidence_without_bindings.snapshotIfComplete());

    auto mismatched_references = persisted_references;
    ++mismatched_references.object_schema_revision;
    BoundObjectPhysicalSchema mismatched_physical_schema{
        .object = mismatched_references.object,
        .object_schema_revision = mismatched_references.object_schema_revision,
        .physical_schema_fingerprint = mismatched_references.physical_schema_fingerprint,
        .occurrences = {{
            .path = mismatched_references.occurrence_paths.front(),
            .physical_type = uint64_type,
            .runtime_owner_key = "id",
            .selected_semantic_capabilities = bound_metadata->getBoundUDTReferences()->getUses().front().getSemanticCapabilities(),
        }},
    };
    const auto mismatched_bound_references
        = BoundObjectTypeReferences::bind(mismatched_references, std::move(mismatched_physical_schema), database->getUDTAuthorityAdapter());
    QueryResultCacheStorageDependencyCollector mismatched_object_image(true);
    ASSERT_TRUE(mismatched_object_image.tryBeginResolution(&cache_resolution_owner));
    mismatched_object_image.record(
        trusted_table->getStorageID(),
        trusted_table->getName(),
        QueryResultCacheStorageKind::Storage,
        mismatched_bound_references,
        exact_read_evidence);
    mismatched_object_image.markResolutionComplete(&cache_resolution_owner);
    EXPECT_FALSE(mismatched_object_image.snapshotIfComplete());

    QueryResultCacheStorageDependencyCollector conflicting_same_uuid(true);
    ASSERT_TRUE(conflicting_same_uuid.tryBeginResolution(&cache_resolution_owner));
    conflicting_same_uuid.record(
        trusted_table->getStorageID(),
        trusted_table->getName(),
        QueryResultCacheStorageKind::Storage,
        bound_metadata->getBoundUDTReferences(),
        exact_read_evidence);
    conflicting_same_uuid.record(trusted_table->getStorageID(), trusted_table->getName(), QueryResultCacheStorageKind::Storage, {}, {});
    conflicting_same_uuid.markResolutionComplete(&cache_resolution_owner);
    EXPECT_FALSE(conflicting_same_uuid.snapshotIfComplete());

    const auto verification = database->executeVerificationPrefixAndResumeForTest();
    ASSERT_TRUE(verification.plan);
    ASSERT_TRUE(verification.prefix);
    ASSERT_TRUE(verification.complete);
    EXPECT_EQ(verification.plan->getTargets().size(), 2);
    EXPECT_EQ(verification.prefix->getTerminalCompletions().size(), 1);
    EXPECT_EQ(verification.complete->getTerminalCompletions().size(), 2);
    EXPECT_TRUE(
        std::ranges::all_of(
            verification.complete->getTerminalCompletions(),
            [](const auto & completion) { return completion.disposition == AuthorityVerificationTargetDisposition::Verified; }));
    EXPECT_EQ(verification.cursor_after_prefix, verification.cursor_before);
    EXPECT_NE(verification.cursor_after_complete, verification.cursor_before);
    EXPECT_EQ(verification.cursor_after_complete.planned_batches, verification.cursor_before.planned_batches + 1);
    ASSERT_TRUE(verification.durable_cursor);
    EXPECT_EQ(*verification.durable_cursor, verification.cursor_after_complete);
    const auto verified_metadata = trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(verified_metadata);
    ASSERT_TRUE(verified_metadata->getBoundUDTVerificationStamp());
    EXPECT_EQ(verified_metadata->getBoundUDTVerificationStamp()->getVerifiedObject().object, table);
    EXPECT_EQ(verified_metadata->getBoundUDTVerificationStamp()->getVerifiedObject().object_schema_revision, object_schema_revision);
    EXPECT_EQ(verified_metadata->getBoundUDTVerificationStamp()->getVerifiedRoot().database_catalog_epoch, 3);
    const StorageMetadataPtr verified_metadata_ptr = verified_metadata;

    /// Clean cursor publication deliberately does not take the exclusive
    /// operation fence. Run it in a sibling thread with repeated final-commit
    /// acquisitions so race detectors exercise the atomic current-state check
    /// rather than an accidentally writer-mutex-protected owner pointer.
    std::atomic<UInt64> concurrent_publication_failures = 0;
    std::barrier<> concurrent_publication_start(3);
    std::thread cursor_publication(
        [&]
        {
            concurrent_publication_start.arrive_and_wait();
            try
            {
                for (size_t iteration = 0; iteration < 4; ++iteration)
                {
                    const auto execution = database->executeVerificationPrefixAndResumeForTest();
                    if (!execution.complete || execution.complete->getTerminalCompletions().size() != 2
                        || !std::ranges::all_of(
                            execution.complete->getTerminalCompletions(),
                            [](const auto & completion)
                            { return completion.disposition == AuthorityVerificationTargetDisposition::Verified; }))
                    {
                        concurrent_publication_failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
            catch (...)
            {
                concurrent_publication_failures.fetch_add(1, std::memory_order_relaxed);
            }
        });
    std::thread commit_fences(
        [&]
        {
            concurrent_publication_start.arrive_and_wait();
            try
            {
                for (size_t iteration = 0; iteration < 256; ++iteration)
                {
                    auto guard = database->acquireUDTNewStorageOperationCommitGuard(
                        verified_metadata_ptr, AuthorityQuarantineOperationKind::Write);
                    if (!guard)
                        concurrent_publication_failures.fetch_add(1, std::memory_order_relaxed);
                }
            }
            catch (...)
            {
                concurrent_publication_failures.fetch_add(1, std::memory_order_relaxed);
            }
        });
    concurrent_publication_start.arrive_and_wait();
    cursor_publication.join();
    commit_fences.join();
    EXPECT_EQ(concurrent_publication_failures.load(std::memory_order_relaxed), 0);
    const auto metadata_before_rejected_mutations = trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(metadata_before_rejected_mutations);
    const StorageMetadataPtr metadata_before_rejected_mutations_ptr = metadata_before_rejected_mutations;

    const StoragePtr replacement_table = makeMemoryTable(table, std::make_shared<DataTypeUInt64>());
    expectMappedTableExactTemporaryDetachRequired([&]
                                                  { database->attachTable(context, "events", replacement_table, "store/replacement"); });
    expectMappedTableMutationRejected("DETACH", [&] { static_cast<void>(database->detachTable(context, "events")); });
    EXPECT_EQ(database->tryGetTable("events", context), trusted_table);
    EXPECT_FALSE(database->tryGetTable("renamed_events", context));
    EXPECT_EQ(database->getTableDataPath("events"), "store/events");
    EXPECT_THROW(database->getTableDataPath("renamed_events"), Exception);
    const auto current_table_id = trusted_table->getStorageID();
    EXPECT_EQ(current_table_id.database_name, "app");
    EXPECT_EQ(current_table_id.table_name, "events");
    EXPECT_EQ(current_table_id.uuid, table.object_uuid);
    const auto unchanged_metadata = trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(unchanged_metadata);
    const StorageMetadataPtr unchanged_metadata_ptr = unchanged_metadata;
    EXPECT_EQ(unchanged_metadata_ptr, metadata_before_rejected_mutations_ptr);
    ASSERT_TRUE(unchanged_metadata->getBoundUDTReferences());
    ASSERT_TRUE(unchanged_metadata->getBoundUDTExpectation());
    EXPECT_EQ(unchanged_metadata->getBoundUDTReferences()->getObject(), table);
    EXPECT_EQ(unchanged_metadata->getBoundUDTExpectation()->object, table);
    const auto [mapped_database, mapped_table] = catalog.tryGetByUUID(table.object_uuid);
    EXPECT_EQ(mapped_database.get(), database.get());
    EXPECT_EQ(mapped_table, trusted_table);
    EXPECT_TRUE(database->hasActiveUDTAuthority());
    auto unchanged_snapshot = database->getUDTLifecycleAdapter().acquireSnapshot();
    EXPECT_EQ(unchanged_snapshot->getDatabaseCatalogEpoch(), 3);
    ASSERT_EQ(unchanged_snapshot->getDefinitionRecords().size(), 1);
    EXPECT_EQ(unchanged_snapshot->getDefinitionRecords().front(), model.record);

    const auto divergent_durable_state = database->advanceDurableAuthorityWithoutPublicationForTest();
    EXPECT_EQ(divergent_durable_state.database_catalog_epoch, 4);
    auto still_published_snapshot = database->getUDTLifecycleAdapter().acquireSnapshot();
    EXPECT_EQ(still_published_snapshot->getDatabaseCatalogEpoch(), 3);
    ASSERT_EQ(still_published_snapshot->getDefinitionRecords().size(), 1);
    EXPECT_EQ(still_published_snapshot->getDefinitionRecords().front(), model.record);

    expectMappedTableAuthorityMismatchRejected(
        "ATTACH", [&] { database->attachTable(context, "events", replacement_table, "store/replacement"); });
    StorageInMemoryMetadata split_brain_alter_metadata(*bound_metadata);
    split_brain_alter_metadata.prepareUDTAlterPublication();
    expectMappedTableAuthorityMismatchRejected(
        "ALTER", [&] { database->alterTable(context, trusted_table->getStorageID(), split_brain_alter_metadata, true); });
    expectMappedTableAuthorityMismatchRejected("DETACH", [&] { static_cast<void>(database->detachTable(context, "events")); });
    expectMappedTableAuthorityMismatchRejected(
        "RENAME", [&] { database->renameTable(context, "events", *database, "renamed_events", false, false); });
    expectMappedTableAuthorityMismatchRejected("DROP", [&] { database->dropTable(context, "events", true); });
    DatabaseOrdinary ordinary_target("ordinary_target", "metadata/ordinary_target", context);
    expectMappedTableAuthorityMismatchRejected(
        "CROSS DATABASE RENAME", [&] { database->renameTable(context, "events", ordinary_target, "moved_events", false, false); });

    EXPECT_EQ(database->tryGetTable("events", context), trusted_table);
    EXPECT_FALSE(database->tryGetTable("renamed_events", context));
    EXPECT_EQ(database->getTableDataPath("events"), "store/events");
    EXPECT_THROW(database->getTableDataPath("renamed_events"), Exception);
    EXPECT_EQ(trusted_table->getStorageID(), current_table_id);
    const auto final_metadata = trusted_table->getInMemoryMetadataPtr(nullptr, false);
    ASSERT_TRUE(final_metadata);
    const StorageMetadataPtr final_metadata_ptr = final_metadata;
    EXPECT_EQ(final_metadata_ptr, unchanged_metadata_ptr);
    const auto [split_brain_mapped_database, split_brain_mapped_table] = catalog.tryGetByUUID(table.object_uuid);
    EXPECT_EQ(split_brain_mapped_database.get(), database.get());
    EXPECT_EQ(split_brain_mapped_table, trusted_table);
    auto final_published_snapshot = database->getUDTLifecycleAdapter().acquireSnapshot();
    EXPECT_EQ(final_published_snapshot->getDatabaseUUID(), unchanged_snapshot->getDatabaseUUID());
    EXPECT_EQ(final_published_snapshot->getDatabaseCatalogEpoch(), unchanged_snapshot->getDatabaseCatalogEpoch());
    ASSERT_EQ(final_published_snapshot->getDefinitionRecords().size(), unchanged_snapshot->getDefinitionRecords().size());
    EXPECT_EQ(final_published_snapshot->getDefinitionRecords().front(), unchanged_snapshot->getDefinitionRecords().front());
    EXPECT_FALSE(ordinary_target.tryGetTable("moved_events", context));

    std::optional<AuthorityStorageNewOperationCommitGuard> outer_commit_fence;
    std::optional<AuthorityStorageNewOperationCommitGuard> nested_commit_fence;
    outer_commit_fence.emplace(database->acquireUDTNewStorageOperationCommitGuard(
        final_metadata_ptr, AuthorityQuarantineOperationKind::Write));
    nested_commit_fence.emplace(database->acquireUDTNewStorageOperationCommitGuard(
        final_metadata_ptr, AuthorityQuarantineOperationKind::Mutation));
    std::future<void> runtime_shutdown;
    std::future<bool> late_commit_fence;
    bool publication_failpoint_enabled = false;
    SCOPE_EXIT({
        if (publication_failpoint_enabled)
            FailPointInjection::disableFailPoint(FailPoints::udt_authority_runtime_pause_after_publication_waiter_registration);
        nested_commit_fence.reset();
        outer_commit_fence.reset();
    });

    FailPointInjection::enableFailPoint(FailPoints::udt_authority_runtime_pause_after_publication_waiter_registration);
    publication_failpoint_enabled = true;
    runtime_shutdown = std::async(std::launch::async, [&] { database->shutdownVerificationRuntimeForTest(); });
    ASSERT_TRUE(waitForDatabaseTestFailPointPause(
        FailPoints::udt_authority_runtime_pause_after_publication_waiter_registration, std::chrono::seconds(5)));

    late_commit_fence = std::async(
        std::launch::async,
        [&]
        {
            auto guard = database->acquireUDTNewStorageOperationCommitGuard(
                final_metadata_ptr, AuthorityQuarantineOperationKind::Write);
            return static_cast<bool>(guard);
        });
    EXPECT_EQ(late_commit_fence.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    FailPointInjection::disableFailPoint(FailPoints::udt_authority_runtime_pause_after_publication_waiter_registration);
    publication_failpoint_enabled = false;

    EXPECT_EQ(runtime_shutdown.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    nested_commit_fence.reset();
    EXPECT_EQ(runtime_shutdown.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    EXPECT_EQ(late_commit_fence.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
    outer_commit_fence.reset();
    EXPECT_EQ(runtime_shutdown.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_NO_THROW(runtime_shutdown.get());
    ASSERT_EQ(late_commit_fence.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_THROW(static_cast<void>(late_commit_fence.get()), AuthorityVerificationRuntimeStateError);
    EXPECT_THROW(
        static_cast<void>(database->acquireUDTNewStorageOperationCommitGuard(
            final_metadata_ptr, AuthorityQuarantineOperationKind::Write)),
        AuthorityVerificationRuntimeStateError);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, GenericStartupRejectsPendingValidationForNonTable)
{
    const SchemaObjectID synthetic = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x7000, 5));
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, first));
        auto activation = model.dependentObjectActivationTransition();
        auto activation_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
        auto sidecar = model.syntheticSidecarTransition(synthetic);
        auto sidecar_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, sidecar_guard, sidecar));
    }

    {
        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
        const auto recovered = recoverAtomicAuthorityAtStartup(
            restarted,
            {},
            [](const SidecarExpectationRecord & expectation,
               std::string_view canonical_metadata_bytes,
               std::string_view canonical_sidecar_bytes)
            { return validateSyntheticDependentObjectMetadata(expectation, canonical_metadata_bytes, canonical_sidecar_bytes); });
        ASSERT_TRUE(recovered.authority_root);
        EXPECT_TRUE(recovered.pending_tables.empty());
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto degraded = recoverAtomicAuthorityAtStartup(
        restarted,
        {},
        [](const SidecarExpectationRecord & expectation, std::string_view, std::string_view)
        {
            return AtomicAuthorityValidatedDependentObject{
                .object = expectation.object,
                .object_schema_revision = expectation.object_schema_revision,
                .physical_schema_fingerprint = expectation.physical_schema_fingerprint,
                .state = AtomicAuthorityDependentObjectValidationState::PendingTable,
            };
        });
    EXPECT_FALSE(degraded.authority_root);
    EXPECT_TRUE(degraded.degraded_status);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, ProductionStartupValidatesCanonicalDependentMetadataBeforeRootPublication)
{
    const SchemaObjectID synthetic = objectID(SchemaObjectKind::SyntheticTestObject, model.database_uuid, uuid(0x7000, 4));

    String metadata_path;
    AuthorityState expected_state;
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto first = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, first));
        auto activation = model.dependentObjectActivationTransition();
        auto activation_guard = storage.issueMutationGuard();
        static_cast<void>(executeDatabaseSchemaMutation(storage, activation_guard, activation));
        auto sidecar = model.syntheticSidecarTransition(synthetic);
        auto sidecar_guard = storage.issueMutationGuard();
        const auto sidecar_commit = executeDatabaseSchemaMutation(storage, sidecar_guard, sidecar);
        expected_state = sidecar.getPrepare().after_authority_state;

        const auto checkpoint = DatabaseSchemaWALCheckpointBuilder::build(
            1, sidecar_commit, expected_state, sidecar.pinAfterInventory(), sidecar.pinAfterGraph());
        auto checkpoint_guard = storage.issueMutationGuard();
        persistValidatedDatabaseSchemaCheckpoint(storage, checkpoint_guard, checkpoint);
        auto compaction_guard = storage.issueMutationGuard();
        compactDatabaseSchemaWALThroughValidatedCheckpoint(storage, compaction_guard, checkpoint);
        EXPECT_TRUE(storage.listDurableTransactionIDs().empty());
        metadata_path = storage.getPaths().canonicalArtifactPath(DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, synthetic);
    }

    {
        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
        try
        {
            static_cast<void>(recoverAtomicAuthorityAtStartup(restarted));
            FAIL() << "expected dependent metadata without a registered validator to fail startup";
        }
        catch (const AtomicAuthorityStartupError & error)
        {
            EXPECT_EQ(error.code, AtomicAuthorityStartupError::Code::IncompleteRecovery);
        }
    }
    {
        AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
        const auto recovered = recoverAndActivateAtomicAuthorityAtStartup(restarted);
        ASSERT_TRUE(recovered.authority_root);
        EXPECT_EQ(recovered.authority_root->getAuthorityState(), expected_state);
        EXPECT_EQ(recovered.completed_transactions, 0);
        EXPECT_TRUE(recovered.pending_tables.empty());
    }

    const auto tampered_metadata = makeSyntheticObjectMetadata(
        synthetic,
        2,
        "synthetic.with-references",
        {SyntheticObjectPhysicalOccurrence{
            .path = {
                .section = PersistedTypePathSection::SyntheticPayload,
                .object_ordinal = 0,
                .occurrence_ordinal = 0,
                .type_child_ordinals = {},
            },
            .canonical_physical_type = "UInt64",
            .storage_fingerprint = digest(0x40),
            .selected_semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input),
        }});
    writeFile(disk, metadata_path, encodeSyntheticObjectMetadata(tampered_metadata));

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto degraded = recoverAndActivateAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(degraded.authority_root);
    EXPECT_TRUE(degraded.degraded_status);
}

TEST_F(AtomicDatabaseSchemaMutationStorageTest, StartupRejectsExtraCanonicalDefinitionFile)
{
    {
        AtomicDatabaseSchemaMutationStorage storage(disk, model.database_uuid, "metadata/app");
        auto transition = model.firstTransition();
        static_cast<void>(executeFirstActivation(storage, transition));
        writeFile(disk, storage.getPaths().typesDirectory() + "/ffffffff-ffff-ffff-ffff-ffffffffffff.sql", model.record_bytes);
    }

    AtomicDatabaseSchemaMutationStorage restarted(disk, model.database_uuid, "metadata/app");
    auto degraded = recoverAtomicAuthorityAtStartup(restarted);
    EXPECT_FALSE(degraded.authority_root);
    EXPECT_TRUE(degraded.degraded_status);
}

}
}
