#include <Interpreters/UDT/QueryResultCacheStorageDependencies.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID testUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Digest digest(UInt8 seed)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(seed + index);
    return result;
}

TypeAuthorityCapabilities authorityCapabilities()
{
    return {
        .adapter_abi = 1,
        .mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates),
        .limits = {
            .maximum_definitions = 32,
            .maximum_definition_bytes = 1ULL << 20,
            .maximum_template_nodes = 4'096,
            .maximum_direct_dependencies = 256,
            .maximum_transitive_dependencies = 32,
            .maximum_checker_work = 65'536,
        },
    };
}

Definition::Ptr checkedDefinition()
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = testUUID(0x550e8400e29b41d4ULL, 0xa716446655440000ULL),
        .type_uuid = testUUID(0x323456789abcdef0ULL, 1),
        .revision = 7,
    };
    input.normalized_name = "query_cache.CacheType";
    input.normalized_local_name = "CacheType";
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    input.semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input) | semanticCapabilityBit(SemanticCapability::ValueChecks)
        | semanticCapabilityBit(SemanticCapability::Default);
    input.policy_bearing = true;
    input.policy_semantic_hash = hashDomainSeparated("ClickHouse UDT query-cache dependency test V1", input.normalized_name);
    return TemplateChecker::checkAll({std::move(input)}).front();
}

struct BoundVariant
{
    UInt64 object_schema_revision = 10;
    UInt64 object_ordinal = 0;
    UInt8 fingerprint_seed = 0x90;
    SemanticCapabilityMask selected_capabilities = semanticCapabilityBit(SemanticCapability::Input);
};

BoundObjectTypeReferences::Ptr boundReferences(const BoundVariant & variant = {})
{
    const auto definition = checkedDefinition();
    const auto descriptor
        = InstantiatedTypeDescriptor::create(definition, CanonicalTypeArguments::validate({}, {}), std::make_shared<DataTypeUInt64>());

    PersistedTypeReferences sidecar;
    sidecar.object = {
        .kind = SchemaObjectKind::SyntheticTestObject,
        .database_uuid = definition->getIdentity().database_uuid,
        .object_uuid = testUUID(0x423456789abcdef0ULL, 1),
    };
    sidecar.object_schema_revision = variant.object_schema_revision;
    sidecar.physical_schema_fingerprint = digest(variant.fingerprint_seed);
    sidecar.descriptors = {descriptor->getPersistedDescriptor()};
    sidecar.occurrence_paths = {{
        .section = PersistedTypePathSection::SyntheticPayload,
        .object_ordinal = variant.object_ordinal,
        .occurrence_ordinal = 0,
        .type_child_ordinals = {},
    }};
    sidecar.uses = {{.path_id = 0, .descriptor_id = 0}};

    BoundObjectPhysicalSchema physical_schema;
    physical_schema.object = sidecar.object;
    physical_schema.object_schema_revision = sidecar.object_schema_revision;
    physical_schema.physical_schema_fingerprint = sidecar.physical_schema_fingerprint;
    physical_schema.occurrences = {{
        .path = sidecar.occurrence_paths.front(),
        .physical_type = descriptor->getPhysicalType(),
        .runtime_owner_key = "payload_" + std::to_string(variant.object_ordinal),
        .selected_semantic_capabilities = variant.selected_capabilities,
    }};

    const auto authority = makeTransientAuthorityAdapter(definition->getIdentity().database_uuid, authorityCapabilities(), {definition});
    return BoundObjectTypeReferences::bind(sidecar, std::move(physical_schema), *authority);
}

StorageID storageID(UInt64 low, String database = "db", String table = "table")
{
    return {database, table, testUUID(0x523456789abcdef0ULL, low)};
}

QueryResultCacheUDTBindingIdentity bindingIdentity(const BoundObjectTypeReferences & references)
{
    return {
        .object = references.getObject(),
        .object_schema_revision = references.getObjectSchemaRevision(),
        .sidecar_hash = references.getSidecarHash(),
        .physical_schema_fingerprint = references.getPhysicalSchemaFingerprint(),
        .semantic_capabilities = references.getSemanticCapabilities(),
        .authority_root = {
            .authority_root = {
                .database_uuid = references.getObject().database_uuid,
                .database_catalog_epoch = 1,
                .authority_anchor = digest(0xb0),
            },
            .schema_graph_root = digest(0xc0),
        },
    };
}

void complete(QueryResultCacheStorageDependencyCollector & collector, const void * owner)
{
    ASSERT_TRUE(collector.tryBeginResolution(owner));
    collector.markResolutionComplete(owner);
}

}

TEST(UDTQueryCacheDependencies, EmptyBoundaryAndUnknownContextualCandidatesCannotMintProof)
{
    int owner = 0;
    QueryResultCacheStorageDependencyCollector no_storage(/*boundary_saw_storage_reference_=*/false);
    complete(no_storage, &owner);
    auto empty_proof = no_storage.snapshotIfComplete();
    ASSERT_TRUE(empty_proof);
    EXPECT_TRUE(empty_proof->dependencies.empty());
    EXPECT_EQ(empty_proof->contextual_sink_candidates, 0);

    QueryResultCacheStorageDependencyCollector missing_storage(/*boundary_saw_storage_reference_=*/true);
    complete(missing_storage, &owner);
    EXPECT_FALSE(missing_storage.snapshotIfComplete().has_value());

    const auto invalid_mask
        = static_cast<QueryResultCacheContextualSinkCandidateMask>(all_query_result_cache_contextual_sink_candidates | (1U << 7));
    QueryResultCacheStorageDependencyCollector invalid_candidates(false, invalid_mask);
    EXPECT_EQ(invalid_candidates.getContextualSinkCandidates(), invalid_mask);
    complete(invalid_candidates, &owner);
    EXPECT_FALSE(invalid_candidates.snapshotIfComplete().has_value());
}

TEST(UDTQueryCacheDependencies, ContextualCandidateMaskIsPartOfThePublishedProof)
{
    const auto mask = queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::Equality)
        | queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::Has);
    QueryResultCacheStorageDependencyCollector collector(false, mask);
    int owner = 0;
    complete(collector, &owner);
    auto proof = collector.snapshotIfComplete();
    ASSERT_TRUE(proof);
    EXPECT_EQ(proof->contextual_sink_candidates, mask);
}

TEST(UDTQueryCacheDependencies, ResolvedTransientTableFunctionCompletesAnEmptyPhysicalProof)
{
    int owner = 0;
    QueryResultCacheStorageDependencyCollector collector(/*boundary_saw_storage_reference_=*/true);
    ASSERT_TRUE(collector.tryBeginResolution(&owner));
    collector.recordResolvedTransientTableFunction(StorageID{"", "numbers"}, "SystemNumbers", {});
    collector.markResolutionComplete(&owner);
    auto proof = collector.snapshotIfComplete();
    ASSERT_TRUE(proof);
    EXPECT_TRUE(proof->dependencies.empty());

    QueryResultCacheStorageDependencyCollector mapped_transient(true);
    ASSERT_TRUE(mapped_transient.tryBeginResolution(&owner));
    mapped_transient.recordResolvedTransientTableFunction(StorageID{"", "eval"}, "View", boundReferences());
    mapped_transient.markResolutionComplete(&owner);
    EXPECT_FALSE(mapped_transient.snapshotIfComplete().has_value());

    QueryResultCacheStorageDependencyCollector persistent_storage(true);
    ASSERT_TRUE(persistent_storage.tryBeginResolution(&owner));
    persistent_storage.recordResolvedTransientTableFunction(storageID(42), "MergeTree", {});
    persistent_storage.markResolutionComplete(&owner);
    EXPECT_FALSE(persistent_storage.snapshotIfComplete().has_value());
}

TEST(UDTQueryCacheDependencies, ResolutionOwnerExceptionAndLateStorageInvalidateProof)
{
    QueryResultCacheStorageDependencyCollector collector(true);
    int owner = 0;
    int stranger = 0;
    EXPECT_FALSE(collector.tryBeginResolution(nullptr));
    ASSERT_TRUE(collector.tryBeginResolution(&owner));
    EXPECT_FALSE(collector.tryBeginResolution(&stranger));
    collector.record(storageID(1), "MergeTree", QueryResultCacheStorageKind::Storage, {});
    collector.markResolutionComplete(&stranger);
    EXPECT_FALSE(collector.snapshotIfComplete().has_value());
    collector.markResolutionComplete(&owner);
    ASSERT_TRUE(collector.snapshotIfComplete());

    collector.record(storageID(2), "MergeTree", QueryResultCacheStorageKind::Storage, {});
    EXPECT_FALSE(collector.snapshotIfComplete().has_value());
    ASSERT_TRUE(collector.tryBeginResolution(&owner));
    collector.markResolutionComplete(&owner);
    ASSERT_EQ(collector.snapshotIfComplete()->dependencies.size(), 2);

    QueryResultCacheStorageDependencyCollector abandoned(true);
    ASSERT_TRUE(abandoned.tryBeginResolution(&owner));
    abandoned.record(storageID(3), "MergeTree", QueryResultCacheStorageKind::Storage, {});
    abandoned.abandonResolution(&owner);
    abandoned.markResolutionComplete(&owner);
    EXPECT_FALSE(abandoned.snapshotIfComplete().has_value());
    EXPECT_TRUE(abandoned.tryBeginResolution(&owner));
    abandoned.markResolutionComplete(&owner);
    EXPECT_FALSE(abandoned.snapshotIfComplete().has_value());
}

TEST(UDTQueryCacheDependencies, UUIDImageDeduplicatesSortsAndRejectsRenameEngineAndKindDrift)
{
    int owner = 0;
    QueryResultCacheStorageDependencyCollector exact(true);
    ASSERT_TRUE(exact.tryBeginResolution(&owner));
    exact.record(storageID(2), "MergeTree", QueryResultCacheStorageKind::Storage, {});
    exact.record(storageID(1), "Memory", QueryResultCacheStorageKind::Storage, {});
    exact.record(storageID(2), "MergeTree", QueryResultCacheStorageKind::Storage, {});
    exact.markResolutionComplete(&owner);
    auto proof = exact.snapshotIfComplete();
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->dependencies.size(), 2);
    EXPECT_LT(proof->dependencies[0].storage_id.uuid, proof->dependencies[1].storage_id.uuid);

    const auto expect_conflict = [&](const StorageID & second_id, String engine, QueryResultCacheStorageKind kind)
    {
        QueryResultCacheStorageDependencyCollector collector(true);
        ASSERT_TRUE(collector.tryBeginResolution(&owner));
        collector.record(storageID(7), "MergeTree", QueryResultCacheStorageKind::Storage, {});
        collector.record(second_id, std::move(engine), kind, {});
        collector.markResolutionComplete(&owner);
        EXPECT_FALSE(collector.snapshotIfComplete().has_value());
    };
    expect_conflict(storageID(7, "renamed_db", "table"), "MergeTree", QueryResultCacheStorageKind::Storage);
    expect_conflict(storageID(7, "db", "renamed_table"), "MergeTree", QueryResultCacheStorageKind::Storage);
    expect_conflict(storageID(7), "ReplacingMergeTree", QueryResultCacheStorageKind::Storage);
    expect_conflict(storageID(7), "MergeTree", QueryResultCacheStorageKind::View);
}

TEST(UDTQueryCacheDependencies, ViewAndMappedStorageWithoutExactRootEvidenceAreUncacheable)
{
    int owner = 0;
    QueryResultCacheStorageDependencyCollector view(true);
    ASSERT_TRUE(view.tryBeginResolution(&owner));
    view.record(storageID(8), "View", QueryResultCacheStorageKind::View, {});
    view.markResolutionComplete(&owner);
    EXPECT_FALSE(view.snapshotIfComplete().has_value());

    QueryResultCacheStorageDependencyCollector mapped_without_evidence(true);
    ASSERT_TRUE(mapped_without_evidence.tryBeginResolution(&owner));
    mapped_without_evidence.record(storageID(9), "MergeTree", QueryResultCacheStorageKind::Storage, boundReferences());
    mapped_without_evidence.markResolutionComplete(&owner);
    EXPECT_FALSE(mapped_without_evidence.snapshotIfComplete().has_value());
}

TEST(UDTQueryCacheDependencies, BindingDriftConflictsUnderOneStorageUUIDKey)
{
    const auto baseline = boundReferences();
    const std::vector variants{
        boundReferences(BoundVariant{.object_schema_revision = 11}),
        boundReferences(BoundVariant{.object_ordinal = 1}),
        boundReferences(BoundVariant{.fingerprint_seed = 0xa0}),
        boundReferences(BoundVariant{.selected_capabilities = semanticCapabilityBit(SemanticCapability::Default)}),
    };
    ASSERT_NE(baseline->getObjectSchemaRevision(), variants[0]->getObjectSchemaRevision());
    ASSERT_NE(baseline->getSidecarHash(), variants[1]->getSidecarHash());
    ASSERT_NE(baseline->getPhysicalSchemaFingerprint(), variants[2]->getPhysicalSchemaFingerprint());
    ASSERT_NE(baseline->getSemanticCapabilities(), variants[3]->getSemanticCapabilities());

    const QueryResultCacheStorageDependency baseline_dependency{
        storageID(9), "MergeTree", QueryResultCacheStorageKind::Storage, bindingIdentity(*baseline)};
    /// `record()` keys by this ordered-set equivalence and then requires exact
    /// value equality. Exercise both parts without bypassing the production
    /// exact-root-evidence constructor boundary.
    std::set<QueryResultCacheStorageDependency, QueryResultCacheStorageDependencyLess> dependency_set;
    ASSERT_TRUE(dependency_set.insert(baseline_dependency).second);
    ASSERT_EQ(dependency_set.size(), 1);

    for (const auto & variant : variants)
    {
        const QueryResultCacheStorageDependency variant_dependency{
            storageID(9), "MergeTree", QueryResultCacheStorageKind::Storage, bindingIdentity(*variant)};
        EXPECT_FALSE(QueryResultCacheStorageDependencyLess{}(baseline_dependency, variant_dependency));
        EXPECT_FALSE(QueryResultCacheStorageDependencyLess{}(variant_dependency, baseline_dependency));
        EXPECT_NE(baseline_dependency, variant_dependency);
        EXPECT_FALSE(dependency_set.insert(variant_dependency).second);
    }

    const QueryResultCacheStorageDependency absent_binding{storageID(9), "MergeTree", QueryResultCacheStorageKind::Storage, std::nullopt};
    EXPECT_FALSE(QueryResultCacheStorageDependencyLess{}(baseline_dependency, absent_binding));
    EXPECT_FALSE(QueryResultCacheStorageDependencyLess{}(absent_binding, baseline_dependency));
    EXPECT_NE(baseline_dependency, absent_binding);
    EXPECT_FALSE(dependency_set.insert(absent_binding).second);
}

TEST(UDTQueryCacheDependencies, ConcurrentWritersPublishOneSortedCompleteClosure)
{
    constexpr size_t dependency_count = 64;
    QueryResultCacheStorageDependencyCollector collector(true);
    int owner = 0;
    ASSERT_TRUE(collector.tryBeginResolution(&owner));

    std::vector<std::thread> writers;
    writers.reserve(dependency_count);
    for (size_t index = 0; index < dependency_count; ++index)
    {
        writers.emplace_back(
            [&, index]
            {
                collector.record(
                    storageID(static_cast<UInt64>(dependency_count - index)),
                    index % 2 ? "Memory" : "MergeTree",
                    QueryResultCacheStorageKind::Storage,
                    {});
            });
    }
    for (auto & writer : writers)
        writer.join();
    collector.markResolutionComplete(&owner);

    auto proof = collector.snapshotIfComplete();
    ASSERT_TRUE(proof);
    ASSERT_EQ(proof->dependencies.size(), dependency_count);
    EXPECT_TRUE(std::is_sorted(proof->dependencies.begin(), proof->dependencies.end(), QueryResultCacheStorageDependencyLess{}));
}

TEST(UDTQueryCacheDependencies, ConcurrentConflictingUUIDImagesCannotPublishRaceWinner)
{
    QueryResultCacheStorageDependencyCollector collector(true);
    int owner = 0;
    ASSERT_TRUE(collector.tryBeginResolution(&owner));
    const auto id = storageID(77);
    std::thread first([&] { collector.record(id, "MergeTree", QueryResultCacheStorageKind::Storage, {}); });
    std::thread second([&] { collector.record(id, "View", QueryResultCacheStorageKind::View, {}); });
    first.join();
    second.join();
    collector.markResolutionComplete(&owner);
    EXPECT_FALSE(collector.snapshotIfComplete().has_value());
}

TEST(UDTQueryCacheDependencies, InvalidIdentityAndRetainedBytesBoundsAreUncacheable)
{
    int owner = 0;
    QueryResultCacheStorageDependencyCollector name_only(true);
    ASSERT_TRUE(name_only.tryBeginResolution(&owner));
    name_only.record(StorageID{"db", "table"}, "MergeTree", QueryResultCacheStorageKind::Storage, {});
    name_only.markResolutionComplete(&owner);
    EXPECT_FALSE(name_only.snapshotIfComplete().has_value());

    QueryResultCacheStorageDependencyCollector empty_engine(true);
    ASSERT_TRUE(empty_engine.tryBeginResolution(&owner));
    empty_engine.record(storageID(1), "", QueryResultCacheStorageKind::Storage, {});
    empty_engine.markResolutionComplete(&owner);
    EXPECT_FALSE(empty_engine.snapshotIfComplete().has_value());

    QueryResultCacheStorageDependencyCollector oversized(true);
    ASSERT_TRUE(oversized.tryBeginResolution(&owner));
    oversized.record(
        storageID(2),
        String(QueryResultCacheStorageDependencyCollector::maximum_retained_bytes, 'x'),
        QueryResultCacheStorageKind::Storage,
        {});
    oversized.markResolutionComplete(&owner);
    EXPECT_FALSE(oversized.snapshotIfComplete().has_value());
}

}
