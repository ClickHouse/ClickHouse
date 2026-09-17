#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/TableColumnTypeAlterBindings.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageLoop.h>

#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
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

UUID databaseUUID()
{
    return uuid(0x550e8400e29b41d4ULL, 0xa716446655440000ULL);
}

SchemaObjectID tableObject()
{
    return {
        .kind = SchemaObjectKind::Table,
        .database_uuid = databaseUUID(),
        .object_uuid = uuid(0x123456789abcdef0ULL, 0x0102030405060708ULL),
    };
}

TypeAuthorityCapabilities capabilities()
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

Definition::Ptr
checkedAlias(String name, UInt64 type_id, UUID database_uuid = databaseUUID(), String physical_type_name = "UInt64", UInt64 revision = 1)
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = database_uuid,
        .type_uuid = uuid(0xabcdef0123456789ULL, type_id),
        .revision = revision,
    };
    input.normalized_name = "app." + name;
    input.normalized_local_name = std::move(name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = std::move(physical_type_name);
    input.nodes.push_back(std::move(root));
    return TemplateChecker::checkAll({std::move(input)}).front();
}

InstantiatedTypeDescriptor::Ptr descriptor(const Definition::Ptr & definition, DataTypePtr physical_type = {})
{
    if (!physical_type)
        physical_type = std::make_shared<DataTypeUInt64>();
    return InstantiatedTypeDescriptor::create(definition, CanonicalTypeArguments::validate({}, {}), std::move(physical_type));
}

BoundDeclaredTypeResult rootAlias(const InstantiatedTypeDescriptor::Ptr & logical_descriptor)
{
    const auto & physical_type = logical_descriptor->getPhysicalType();
    auto tree = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = physical_type}},
        {{.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
        {});
    return BoundDeclaredTypeResult::withLogicalTree(std::move(tree));
}

BoundDeclaredTypeResult nestedArrayAlias(const InstantiatedTypeDescriptor::Ptr & logical_descriptor)
{
    const auto & nested_type = logical_descriptor->getPhysicalType();
    auto physical_type = std::make_shared<DataTypeArray>(nested_type);
    auto tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = physical_type},
            {.type_child_ordinals = {0}, .physical_type = nested_type},
        },
        {{.type_child_ordinals = {0}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
        {});
    return BoundDeclaredTypeResult::withLogicalTree(std::move(tree));
}

template <typename Callback>
void expectBindingError(TableColumnTypeBindingError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a table column binding error";
    }
    catch (const TableColumnTypeBindingError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

class MetadataPublishingStorage final : public IStorage
{
public:
    explicit MetadataPublishingStorage(
        UUID table_uuid = tableObject().object_uuid, std::unique_ptr<StorageInMemoryMetadata> metadata = nullptr)
        : IStorage(StorageID("test", "table", table_uuid), std::move(metadata))
    {
    }

    std::string getName() const override { return "MetadataPublishingStorage"; }
};

}

TEST(UDTTableColumnBindings, PhysicalOnlyColumnsProduceNoLogicalState)
{
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"id", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    columns.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});

    const auto result = prepareTableColumnTypeBindings(tableObject(), 1, columns);
    ASSERT_EQ(result.physical_columns.size(), 2);
    auto column = result.physical_columns.begin();
    EXPECT_EQ(column->name, "id");
    EXPECT_EQ(column->type->getName(), "UInt64");
    ++column;
    EXPECT_EQ(column->name, "label");
    EXPECT_EQ(column->type->getName(), "String");
    EXPECT_FALSE(result.persisted_references);
    EXPECT_FALSE(result.bound_physical_schema);
    EXPECT_FALSE(result.sidecar_expectation);
    EXPECT_TRUE(result.dependency_edges.empty());

    std::vector<TableColumnTypeBindingInput> reordered;
    reordered.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});
    reordered.push_back({"id", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    EXPECT_NE(
        result.physical_schema_fingerprint, prepareTableColumnTypeBindings(tableObject(), 1, reordered).physical_schema_fingerprint);
}

TEST(UDTTableColumnBindings, MaximumWideSchemaCoversSparseAndDenseLogicalState)
{
    constexpr size_t column_count = 10'000;
    constexpr size_t definition_count = 32;
    const auto definition = checkedAlias("UserId", 41);
    const auto logical_descriptor = descriptor(definition);

    std::vector<InstantiatedTypeDescriptor::Ptr> dense_descriptors;
    dense_descriptors.reserve(definition_count);
    for (size_t index = 0; index < definition_count; ++index)
        dense_descriptors.push_back(descriptor(checkedAlias("Wide" + std::to_string(index), 100 + index)));

    std::vector<TableColumnTypeBindingInput> sparse_columns;
    std::vector<TableColumnTypeBindingInput> dense_columns;
    std::vector<TableColumnTypeBindingInput> physical_columns;
    sparse_columns.reserve(column_count);
    dense_columns.reserve(column_count);
    physical_columns.reserve(column_count);
    for (size_t index = 0; index < column_count; ++index)
    {
        const String name = "c" + std::to_string(index);
        sparse_columns.push_back({
            name,
            index == column_count - 1 ? rootAlias(logical_descriptor)
                                      : BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>()),
        });
        dense_columns.push_back({name, rootAlias(dense_descriptors[index % definition_count])});
        physical_columns.push_back({name, BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    }

    const auto sparse = prepareTableColumnTypeBindings(tableObject(), 1, sparse_columns);
    const auto dense = prepareTableColumnTypeBindings(tableObject(), 1, dense_columns);
    const auto physical = prepareTableColumnTypeBindings(tableObject(), 1, physical_columns);
    ASSERT_EQ(sparse.physical_columns.size(), column_count);
    ASSERT_TRUE(sparse.persisted_references);
    ASSERT_TRUE(sparse.bound_physical_schema);
    ASSERT_TRUE(sparse.sidecar_expectation);
    EXPECT_EQ(sparse.persisted_references->descriptors.size(), 1);
    EXPECT_EQ(sparse.persisted_references->occurrence_paths.size(), 1);
    EXPECT_EQ(sparse.persisted_references->occurrence_paths.front().object_ordinal, column_count - 1);
    EXPECT_EQ(sparse.dependency_edges.size(), 1);
    EXPECT_EQ(sparse.physical_schema_fingerprint, physical.physical_schema_fingerprint);

    ASSERT_TRUE(dense.persisted_references);
    ASSERT_TRUE(dense.bound_physical_schema);
    EXPECT_EQ(dense.persisted_references->descriptors.size(), definition_count);
    EXPECT_EQ(dense.persisted_references->occurrence_paths.size(), column_count);
    EXPECT_EQ(dense.persisted_references->uses.size(), column_count);
    EXPECT_EQ(dense.bound_physical_schema->occurrences.size(), column_count);
    EXPECT_EQ(dense.dependency_edges.size(), definition_count);
    EXPECT_EQ(dense.physical_schema_fingerprint, physical.physical_schema_fingerprint);

    EXPECT_FALSE(physical.persisted_references);
    EXPECT_FALSE(physical.bound_physical_schema);
    EXPECT_FALSE(physical.sidecar_expectation);
    EXPECT_TRUE(physical.dependency_edges.empty());
}

TEST(UDTTableColumnBindings, ComposesNestedAndRepeatedAliasIntoOneCanonicalDescriptorAndEdge)
{
    const auto definition = checkedAlias("UserId", 1);
    const auto logical_descriptor = descriptor(definition);
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"id", rootAlias(logical_descriptor)});
    columns.push_back({"ids", nestedArrayAlias(logical_descriptor)});
    columns.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});

    const auto result = prepareTableColumnTypeBindings(tableObject(), 7, columns);
    ASSERT_TRUE(result.persisted_references);
    ASSERT_TRUE(result.bound_physical_schema);
    ASSERT_TRUE(result.sidecar_expectation);
    const auto & references = *result.persisted_references;
    EXPECT_EQ(references.object, tableObject());
    EXPECT_EQ(references.object_schema_revision, 7);
    EXPECT_EQ(references.physical_schema_fingerprint, result.physical_schema_fingerprint);
    ASSERT_EQ(references.descriptors.size(), 1);
    ASSERT_EQ(references.occurrence_paths.size(), 2);
    ASSERT_EQ(references.uses.size(), 2);
    EXPECT_EQ(references.occurrence_paths[0].section, PersistedTypePathSection::ColumnType);
    EXPECT_EQ(references.occurrence_paths[0].object_ordinal, 0);
    EXPECT_TRUE(references.occurrence_paths[0].type_child_ordinals.empty());
    EXPECT_EQ(references.occurrence_paths[1].object_ordinal, 1);
    EXPECT_EQ(references.occurrence_paths[1].type_child_ordinals, std::vector<UInt64>({0}));
    EXPECT_EQ(references.uses[0].descriptor_id, 0);
    EXPECT_EQ(references.uses[1].descriptor_id, 0);
    EXPECT_EQ(decodePersistedTypeReferences(encodePersistedTypeReferences(references)), references);
    EXPECT_EQ(result.sidecar_expectation->object, tableObject());
    EXPECT_EQ(result.sidecar_expectation->object_schema_revision, 7);
    EXPECT_EQ(result.sidecar_expectation->sidecar_hash, computePersistedTypeReferencesSidecarHash(references));
    EXPECT_EQ(result.sidecar_expectation->physical_schema_fingerprint, result.physical_schema_fingerprint);

    ASSERT_EQ(result.bound_physical_schema->occurrences.size(), 2);
    EXPECT_EQ(result.bound_physical_schema->occurrences[0].physical_type->getName(), "UInt64");
    EXPECT_EQ(result.bound_physical_schema->occurrences[1].physical_type->getName(), "UInt64");
    EXPECT_EQ(result.bound_physical_schema->occurrences[0].selected_semantic_capabilities, 0);

    ASSERT_EQ(result.dependency_edges.size(), 1);
    EXPECT_EQ(result.dependency_edges[0].dependent, tableObject());
    EXPECT_EQ(result.dependency_edges[0].dependency.kind, SchemaObjectKind::TypeDefinition);
    EXPECT_EQ(result.dependency_edges[0].dependency.object_uuid, definition->getIdentity().type_uuid);
    EXPECT_EQ(result.dependency_edges[0].kind, SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition);

    auto authority = makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition});
    const auto rebound = BoundObjectTypeReferences::bind(references, *result.bound_physical_schema, *authority);
    ASSERT_TRUE(rebound);
    EXPECT_EQ(rebound->getUses().size(), 2);
    EXPECT_EQ(rebound->getObject(), tableObject());
    EXPECT_EQ(rebound->getObjectSchemaRevision(), 7);
    EXPECT_EQ(rebound->getSidecarHash(), result.sidecar_expectation->sidecar_hash);
    EXPECT_EQ(rebound->getPhysicalSchemaFingerprint(), result.physical_schema_fingerprint);
    EXPECT_EQ(computeTableColumnPhysicalSchemaFingerprint(result.physical_columns), result.physical_schema_fingerprint);

    // Binding is the only authority boundary. The immutable table metadata
    // must remain completely usable after the catalog adapter is gone; an
    // ordinary consumer has no root or name/UUID lookup path to fall back to.
    authority.reset();
    EXPECT_EQ(rebound->getUses()[0].getPhysicalType()->getName(), "UInt64");
    EXPECT_EQ(rebound->getUses()[1].getPhysicalType()->getName(), "UInt64");

    StorageInMemoryMetadata metadata;
    metadata.setColumnsAndBoundUDTReferences(ColumnsDescription(result.physical_columns), rebound, *result.sidecar_expectation);
    StorageInMemoryMetadata copied(metadata);
    StorageInMemoryMetadata assigned;
    assigned = metadata;
    EXPECT_EQ(copied.getBoundUDTReferences(), rebound);
    EXPECT_EQ(assigned.getBoundUDTReferences(), rebound);
    ASSERT_TRUE(copied.getBoundUDTExpectation());
    ASSERT_TRUE(assigned.getBoundUDTExpectation());
    EXPECT_EQ(*copied.getBoundUDTExpectation(), *result.sidecar_expectation);
    EXPECT_EQ(*assigned.getBoundUDTExpectation(), *result.sidecar_expectation);
    EXPECT_NO_THROW(copied.validateBoundUDTReferences());
    EXPECT_NO_THROW(assigned.validateBoundUDTReferences());
    copied.setColumns(ColumnsDescription(result.physical_columns));
    EXPECT_EQ(copied.getBoundUDTReferences(), nullptr);
    EXPECT_NO_THROW(copied.validateBoundUDTReferences());
    copied.setColumnsAndBoundUDTReferences(ColumnsDescription(result.physical_columns), rebound, *result.sidecar_expectation);
    EXPECT_EQ(copied.getBoundUDTReferences(), rebound);
    EXPECT_NO_THROW(copied.validateBoundUDTReferences());
}

TEST(UDTTableColumnBindings, MetadataPublicationRejectsStaleBoundReferences)
{
    const auto definition = checkedAlias("UserId", 40);
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"id", rootAlias(descriptor(definition))});
    columns.push_back({"label", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeString>())});
    const auto prepared = prepareTableColumnTypeBindings(tableObject(), 11, columns);
    ASSERT_TRUE(prepared.persisted_references);
    ASSERT_TRUE(prepared.bound_physical_schema);
    ASSERT_TRUE(prepared.sidecar_expectation);

    auto authority = makeTransientAuthorityAdapter(databaseUUID(), capabilities(), {definition});
    const auto rebound = BoundObjectTypeReferences::bind(*prepared.persisted_references, *prepared.bound_physical_schema, *authority);

    StorageInMemoryMetadata metadata;
    metadata.setColumnsAndBoundUDTReferences(
        ColumnsDescription(prepared.physical_columns), rebound, *prepared.sidecar_expectation);
    EXPECT_NO_THROW(metadata.validateBoundUDTReferences());

    metadata.setComment("retained physical metadata");
    const auto physical_only = metadata.cloneAsPhysicalOnlyForIndependentStorage();
    EXPECT_EQ(physical_only.getBoundUDTReferences(), nullptr);
    EXPECT_EQ(physical_only.getBoundUDTExpectation(), nullptr);
    EXPECT_EQ(physical_only.getColumns().getAllPhysical(), metadata.getColumns().getAllPhysical());
    EXPECT_EQ(physical_only.comment, metadata.comment);
    EXPECT_EQ(metadata.getBoundUDTReferences(), rebound);

    auto inner_storage = std::make_shared<MetadataPublishingStorage>();
    inner_storage->setInMemoryMetadata(metadata);
    StorageLoop loop(StorageID("_table_function", "loop"), inner_storage);
    const auto loop_metadata = loop.getInMemoryMetadataPtr(nullptr, false);
    EXPECT_EQ(loop_metadata->getBoundUDTReferences(), nullptr);
    EXPECT_EQ(loop_metadata->getBoundUDTExpectation(), nullptr);
    EXPECT_EQ(loop_metadata->getColumns().getAllPhysical(), metadata.getColumns().getAllPhysical());
    EXPECT_EQ(loop_metadata->comment, metadata.comment);

    auto other_table = tableObject();
    other_table.object_uuid = uuid(0x9988776655443322ULL, 0x1100ffeeddccbbaaULL);
    const auto wrong_owner_prepared = prepareTableColumnTypeBindings(other_table, 11, columns);
    ASSERT_TRUE(wrong_owner_prepared.persisted_references);
    ASSERT_TRUE(wrong_owner_prepared.bound_physical_schema);
    const auto wrong_owner_rebound = BoundObjectTypeReferences::bind(
        *wrong_owner_prepared.persisted_references, *wrong_owner_prepared.bound_physical_schema, *authority);
    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&]
        {
            metadata.setColumnsAndBoundUDTReferences(
                ColumnsDescription(prepared.physical_columns), wrong_owner_rebound, *prepared.sidecar_expectation);
        });
    EXPECT_EQ(metadata.getBoundUDTReferences(), rebound);
    EXPECT_NO_THROW(metadata.validateBoundUDTReferences());

    const auto stale_revision_prepared = prepareTableColumnTypeBindings(tableObject(), 10, columns);
    ASSERT_TRUE(stale_revision_prepared.persisted_references);
    ASSERT_TRUE(stale_revision_prepared.bound_physical_schema);
    const auto stale_revision_rebound = BoundObjectTypeReferences::bind(
        *stale_revision_prepared.persisted_references, *stale_revision_prepared.bound_physical_schema, *authority);
    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&]
        {
            metadata.setColumnsAndBoundUDTReferences(
                ColumnsDescription(prepared.physical_columns), stale_revision_rebound, *prepared.sidecar_expectation);
        });
    EXPECT_EQ(metadata.getBoundUDTReferences(), rebound);
    EXPECT_NO_THROW(metadata.validateBoundUDTReferences());

    auto wrong_hash_expectation = *prepared.sidecar_expectation;
    wrong_hash_expectation.sidecar_hash.front() ^= 0xff;
    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&]
        {
            metadata.setColumnsAndBoundUDTReferences(
                ColumnsDescription(prepared.physical_columns), rebound, wrong_hash_expectation);
        });
    EXPECT_EQ(metadata.getBoundUDTReferences(), rebound);
    EXPECT_NO_THROW(metadata.validateBoundUDTReferences());

    NamesAndTypesList renamed_columns;
    renamed_columns.emplace_back("renamed_id", std::make_shared<DataTypeUInt64>());
    renamed_columns.emplace_back("label", std::make_shared<DataTypeString>());
    expectBindingError(
        TableColumnTypeBindingError::Code::PhysicalSchemaMismatch,
        [&]
        {
            metadata.setColumnsAndBoundUDTReferences(
                ColumnsDescription(renamed_columns), rebound, *prepared.sidecar_expectation);
        });
    EXPECT_EQ(metadata.getColumns().getAllPhysical().front().name, "id");
    EXPECT_EQ(metadata.getBoundUDTReferences(), rebound);

    NamesAndTypesList reordered_columns;
    reordered_columns.emplace_back("label", std::make_shared<DataTypeString>());
    reordered_columns.emplace_back("id", std::make_shared<DataTypeUInt64>());
    expectBindingError(
        TableColumnTypeBindingError::Code::PhysicalSchemaMismatch,
        [&]
        {
            metadata.setColumnsAndBoundUDTReferences(
                ColumnsDescription(reordered_columns), rebound, *prepared.sidecar_expectation);
        });

    AlterCommands ordinary_alter;
    AlterCommand ordinary_modify;
    ordinary_modify.type = AlterCommand::MODIFY_COLUMN;
    ordinary_modify.column_name = "id";
    ordinary_modify.data_type = std::make_shared<DataTypeUInt64>();
    ordinary_alter.push_back(std::move(ordinary_modify));
    ordinary_alter.prepare(metadata);
    const auto context = getContext().context;
    auto pending_metadata = metadata;
    EXPECT_NO_THROW(ordinary_alter.apply(pending_metadata, context));
    const auto & pending_alter = pending_metadata.getPendingUDTColumnAlter();
    ASSERT_TRUE(pending_alter);
    EXPECT_EQ(pending_alter->getObject(), tableObject());
    EXPECT_EQ(pending_alter->getBeforeObjectSchemaRevision(), 11);
    EXPECT_FALSE(pending_alter->getDesiredReferences());
    EXPECT_EQ(pending_alter->getAfterPhysicalColumns(), prepared.physical_columns);
    EXPECT_FALSE(pending_alter->getCompletedPublication());
    EXPECT_NO_THROW(static_cast<void>(ordinary_alter.getMutationCommands(metadata, false, context)));
    EXPECT_NO_THROW(pending_metadata.validateBoundUDTReferences());
    EXPECT_NO_THROW(metadata.validateBoundUDTReferences());
    EXPECT_EQ(metadata.getBoundUDTReferences(), rebound);
    ASSERT_TRUE(metadata.getBoundUDTExpectation());
    EXPECT_EQ(*metadata.getBoundUDTExpectation(), *prepared.sidecar_expectation);
    EXPECT_FALSE(metadata.getPendingUDTColumnAlter());

    MetadataPublishingStorage unpublished_storage;
    EXPECT_THROW(unpublished_storage.setInMemoryMetadata(pending_metadata), std::logic_error);

    MetadataPublishingStorage storage;
    EXPECT_NO_THROW(storage.setInMemoryMetadata(metadata));

    const auto wrong_storage_uuid = uuid(0xfedcba9876543210ULL, 0x8877665544332211ULL);
    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&] { storage.renameInMemory(StorageID("test", "renamed_table", wrong_storage_uuid)); });
    EXPECT_EQ(storage.getStorageID().uuid, tableObject().object_uuid);

    MetadataPublishingStorage wrong_storage(wrong_storage_uuid);
    expectBindingError(TableColumnTypeBindingError::Code::SidecarMismatch, [&] { wrong_storage.setInMemoryMetadata(metadata); });
    const auto wrong_storage_metadata = wrong_storage.getInMemoryMetadataPtr(nullptr, false);
    EXPECT_EQ(wrong_storage_metadata->getBoundUDTReferences(), nullptr);

    MetadataPublishingStorage non_atomic_storage(UUIDHelpers::Nil);
    expectBindingError(TableColumnTypeBindingError::Code::SidecarMismatch, [&] { non_atomic_storage.setInMemoryMetadata(metadata); });

    EXPECT_NO_THROW({
        auto valid_constructor_metadata = std::make_unique<StorageInMemoryMetadata>(metadata);
        static_cast<void>(MetadataPublishingStorage(tableObject().object_uuid, std::move(valid_constructor_metadata)));
    });
    auto wrong_owner_constructor_metadata = std::make_unique<StorageInMemoryMetadata>(metadata);
    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&] { static_cast<void>(MetadataPublishingStorage(wrong_storage_uuid, std::move(wrong_owner_constructor_metadata))); });
    auto non_atomic_constructor_metadata = std::make_unique<StorageInMemoryMetadata>(metadata);
    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&] { static_cast<void>(MetadataPublishingStorage(UUIDHelpers::Nil, std::move(non_atomic_constructor_metadata))); });

    metadata.columns.modify("id", [](ColumnDescription & column) { column.type = std::make_shared<DataTypeString>(); });
    expectBindingError(
        TableColumnTypeBindingError::Code::PhysicalSchemaMismatch, [&] { metadata.validateBoundUDTReferences(); });
    expectBindingError(
        TableColumnTypeBindingError::Code::PhysicalSchemaMismatch,
        [&] { static_cast<void>(metadata.cloneAsPhysicalOnlyForIndependentStorage()); });

    expectBindingError(TableColumnTypeBindingError::Code::PhysicalSchemaMismatch, [&] { storage.setInMemoryMetadata(metadata); });

    auto stale_constructor_metadata = std::make_unique<StorageInMemoryMetadata>(metadata);
    expectBindingError(
        TableColumnTypeBindingError::Code::PhysicalSchemaMismatch,
        [&] { static_cast<void>(MetadataPublishingStorage(tableObject().object_uuid, std::move(stale_constructor_metadata))); });
}

TEST(UDTTableColumnBindings, PreservesStackedLogicalOccurrencesAtOnePhysicalNode)
{
    const auto outer_definition = checkedAlias("Outer", 1);
    const auto inner_definition = checkedAlias("Inner", 2);
    const auto outer = descriptor(outer_definition);
    const auto inner = descriptor(inner_definition);
    auto physical_type = std::make_shared<DataTypeUInt64>();
    auto tree = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = physical_type}},
        {
            {.type_child_ordinals = {}, .logical_descriptor = outer, .logical_preorder = 0},
            {.type_child_ordinals = {}, .logical_descriptor = inner, .logical_preorder = 1},
        },
        {});
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"value", BoundDeclaredTypeResult::withLogicalTree(std::move(tree))});

    const auto result = prepareTableColumnTypeBindings(tableObject(), 1, columns);
    ASSERT_TRUE(result.persisted_references);
    const auto & references = *result.persisted_references;
    ASSERT_EQ(references.descriptors.size(), 2);
    ASSERT_EQ(references.occurrence_paths.size(), 2);
    EXPECT_EQ(references.occurrence_paths[0].object_ordinal, 0);
    EXPECT_EQ(references.occurrence_paths[0].occurrence_ordinal, 0);
    EXPECT_EQ(references.occurrence_paths[1].object_ordinal, 0);
    EXPECT_EQ(references.occurrence_paths[1].occurrence_ordinal, 1);
    EXPECT_NE(references.uses[0].descriptor_id, references.uses[1].descriptor_id);
    EXPECT_EQ(result.dependency_edges.size(), 2);
}

TEST(UDTTableColumnBindings, ReconstructsCanonicalPhysicalOccurrencesWithoutAuthorityLookup)
{
    const auto definition = checkedAlias("UserId", 20);
    const auto logical_descriptor = descriptor(definition);
    auto nested_type = std::make_shared<DataTypeUInt64>();
    auto physical_type = std::make_shared<DataTypeArray>(nested_type);
    auto tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = physical_type},
            {.type_child_ordinals = {0}, .physical_type = nested_type},
        },
        {
            {.type_child_ordinals = {0}, .logical_descriptor = logical_descriptor, .logical_preorder = 0},
            {.type_child_ordinals = {0}, .logical_descriptor = logical_descriptor, .logical_preorder = 1},
        },
        {});
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"ids", BoundDeclaredTypeResult::withLogicalTree(std::move(tree))});

    const auto prepared = prepareTableColumnTypeBindings(tableObject(), 9, columns);
    ASSERT_TRUE(prepared.persisted_references);
    const auto reconstructed
        = reconstructTableColumnPhysicalSchema(tableObject(), 9, prepared.physical_columns, *prepared.persisted_references);

    EXPECT_EQ(reconstructed.object, tableObject());
    EXPECT_EQ(reconstructed.object_schema_revision, 9);
    EXPECT_EQ(reconstructed.physical_schema_fingerprint, prepared.physical_schema_fingerprint);
    ASSERT_EQ(reconstructed.occurrences.size(), 2);
    EXPECT_EQ(reconstructed.occurrences[0].path.type_child_ordinals, std::vector<UInt64>({0}));
    EXPECT_EQ(reconstructed.occurrences[0].path.occurrence_ordinal, 0);
    EXPECT_EQ(reconstructed.occurrences[1].path.occurrence_ordinal, 1);
    EXPECT_EQ(reconstructed.occurrences[0].physical_type->getName(), "UInt64");
    EXPECT_EQ(reconstructed.occurrences[1].physical_type->getName(), "UInt64");
    EXPECT_EQ(reconstructed.occurrences[0].selected_semantic_capabilities, 0);
}

TEST(UDTTableColumnBindings, ReconstructionRejectsSnapshotAndPhysicalFingerprintMismatch)
{
    const auto definition = checkedAlias("UserId", 21);
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"id", rootAlias(descriptor(definition))});
    const auto prepared = prepareTableColumnTypeBindings(tableObject(), 4, columns);
    ASSERT_TRUE(prepared.persisted_references);

    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&]
        {
            static_cast<void>(
                reconstructTableColumnPhysicalSchema(tableObject(), 5, prepared.physical_columns, *prepared.persisted_references));
        });

    auto another_table = tableObject();
    another_table.object_uuid = uuid(7, 8);
    expectBindingError(
        TableColumnTypeBindingError::Code::SidecarMismatch,
        [&]
        {
            static_cast<void>(
                reconstructTableColumnPhysicalSchema(another_table, 4, prepared.physical_columns, *prepared.persisted_references));
        });

    NamesAndTypesList changed_columns;
    changed_columns.emplace_back("id", std::make_shared<DataTypeString>());
    expectBindingError(
        TableColumnTypeBindingError::Code::PhysicalSchemaMismatch,
        [&]
        { static_cast<void>(reconstructTableColumnPhysicalSchema(tableObject(), 4, changed_columns, *prepared.persisted_references)); });
}

TEST(UDTTableColumnBindings, ReconstructionRejectsInvalidColumnAndOccurrenceTopology)
{
    const auto definition = checkedAlias("UserId", 22);
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"ids", nestedArrayAlias(descriptor(definition))});
    const auto prepared = prepareTableColumnTypeBindings(tableObject(), 1, columns);
    ASSERT_TRUE(prepared.persisted_references);

    auto invalid_column = *prepared.persisted_references;
    invalid_column.occurrence_paths[0].object_ordinal = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::PathMismatch,
        [&] { static_cast<void>(reconstructTableColumnPhysicalSchema(tableObject(), 1, prepared.physical_columns, invalid_column)); });

    auto invalid_child = *prepared.persisted_references;
    invalid_child.occurrence_paths[0].type_child_ordinals[0] = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::PathMismatch,
        [&] { static_cast<void>(reconstructTableColumnPhysicalSchema(tableObject(), 1, prepared.physical_columns, invalid_child)); });

    auto oversized_child = *prepared.persisted_references;
    oversized_child.occurrence_paths[0].type_child_ordinals[0] = std::numeric_limits<UInt64>::max();
    expectBindingError(
        TableColumnTypeBindingError::Code::PathMismatch,
        [&] { static_cast<void>(reconstructTableColumnPhysicalSchema(tableObject(), 1, prepared.physical_columns, oversized_child)); });

    auto nonzero_first_occurrence = *prepared.persisted_references;
    nonzero_first_occurrence.occurrence_paths[0].occurrence_ordinal = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::PathMismatch,
        [&]
        {
            static_cast<void>(
                reconstructTableColumnPhysicalSchema(tableObject(), 1, prepared.physical_columns, nonzero_first_occurrence));
        });
}

TEST(UDTTableColumnBindings, ReconstructionEnforcesApplicableWorkLimits)
{
    const auto definition = checkedAlias("UserId", 23);
    const auto logical_descriptor = descriptor(definition);
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"first", nestedArrayAlias(logical_descriptor)});
    columns.push_back({"second", nestedArrayAlias(logical_descriptor)});
    const auto prepared = prepareTableColumnTypeBindings(tableObject(), 1, columns);
    ASSERT_TRUE(prepared.persisted_references);

    TableColumnTypeBindingLimits column_limits;
    column_limits.maximum_columns = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(reconstructTableColumnPhysicalSchema(
                tableObject(), 1, prepared.physical_columns, *prepared.persisted_references, column_limits));
        });

    TableColumnTypeBindingLimits occurrence_limits;
    occurrence_limits.maximum_descriptor_occurrences = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(reconstructTableColumnPhysicalSchema(
                tableObject(), 1, prepared.physical_columns, *prepared.persisted_references, occurrence_limits));
        });

    TableColumnTypeBindingLimits path_limits;
    path_limits.maximum_retained_path_components = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(reconstructTableColumnPhysicalSchema(
                tableObject(), 1, prepared.physical_columns, *prepared.persisted_references, path_limits));
        });
}

TEST(UDTTableColumnBindings, RejectsBoundNodeThatDiffersFromNormalizedPhysicalSubtree)
{
    const auto definition = checkedAlias("TextValue", 24, databaseUUID(), "String");
    const auto logical_descriptor = descriptor(definition, std::make_shared<DataTypeString>());
    auto actual_nested_type = std::make_shared<DataTypeUInt64>();
    auto root_type = std::make_shared<DataTypeArray>(actual_nested_type);
    auto inconsistent_nested_type = std::make_shared<DataTypeString>();
    auto tree = BoundDeclaredTypeTree::build(
        {
            {.type_child_ordinals = {}, .physical_type = root_type},
            {.type_child_ordinals = {0}, .physical_type = inconsistent_nested_type},
        },
        {{.type_child_ordinals = {0}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
        {definition});
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"values", BoundDeclaredTypeResult::withLogicalTree(std::move(tree))});

    expectBindingError(
        TableColumnTypeBindingError::Code::ConflictingDescriptor,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns)); });
}

TEST(UDTTableColumnBindings, RejectsMultipleRevisionsOfOneStableTypeIdentity)
{
    const auto old_definition = checkedAlias("Value", 25, databaseUUID(), "UInt64", 1);
    const auto new_definition = checkedAlias("Value", 25, databaseUUID(), "UInt64", 2);
    ASSERT_EQ(old_definition->getIdentity().database_uuid, new_definition->getIdentity().database_uuid);
    ASSERT_EQ(old_definition->getIdentity().type_uuid, new_definition->getIdentity().type_uuid);
    ASSERT_NE(old_definition->getIdentity().revision, new_definition->getIdentity().revision);

    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"old_value", rootAlias(descriptor(old_definition))});
    columns.push_back({"new_value", rootAlias(descriptor(new_definition))});
    expectBindingError(
        TableColumnTypeBindingError::Code::ConflictingDescriptor,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns)); });
}

TEST(UDTTableColumnBindings, CanonicalizesRenamedDescriptorDiagnosticsIndependentOfInputHandles)
{
    const auto old_definition = checkedAlias("ZuluName", 10);
    const auto new_definition = checkedAlias("AlphaName", 10);
    ASSERT_EQ(old_definition->getIdentity(), new_definition->getIdentity());
    ASSERT_TRUE(old_definition->hasSameCheckedSemantics(*new_definition));
    const auto old_descriptor = descriptor(old_definition);
    const auto new_descriptor = descriptor(new_definition);
    ASSERT_TRUE(old_descriptor->getPersistedDescriptor().hasSameInstantiation(new_descriptor->getPersistedDescriptor()));

    std::vector<TableColumnTypeBindingInput> old_then_new;
    old_then_new.push_back({"first", rootAlias(old_descriptor)});
    old_then_new.push_back({"second", rootAlias(new_descriptor)});
    std::vector<TableColumnTypeBindingInput> new_then_old;
    new_then_old.push_back({"first", rootAlias(new_descriptor)});
    new_then_old.push_back({"second", rootAlias(old_descriptor)});

    const auto first = prepareTableColumnTypeBindings(tableObject(), 1, old_then_new);
    const auto second = prepareTableColumnTypeBindings(tableObject(), 1, new_then_old);
    ASSERT_TRUE(first.persisted_references);
    ASSERT_TRUE(second.persisted_references);
    ASSERT_EQ(first.persisted_references->descriptors.size(), 1);
    EXPECT_EQ(first.persisted_references->descriptors[0].getLastKnownQualifiedName(), "app.AlphaName");
    EXPECT_EQ(encodePersistedTypeReferences(*first.persisted_references), encodePersistedTypeReferences(*second.persisted_references));
}

TEST(UDTTableColumnBindings, RejectsConflictingDefinitionBodiesAcrossColumnTrees)
{
    const auto first_definition = checkedAlias("Value", 11);
    const auto conflicting_definition = checkedAlias("Value", 11, databaseUUID(), "UInt32");
    ASSERT_EQ(first_definition->getIdentity(), conflicting_definition->getIdentity());
    ASSERT_FALSE(first_definition->hasSameCheckedSemantics(*conflicting_definition));

    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"first", rootAlias(descriptor(first_definition))});
    columns.push_back({"second", rootAlias(descriptor(conflicting_definition, std::make_shared<DataTypeUInt32>()))});
    expectBindingError(
        TableColumnTypeBindingError::Code::ConflictingDescriptor,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns)); });
}

TEST(UDTTableColumnBindings, EnforcesExactSidecarAndDescriptorFieldLimits)
{
    const auto definition = checkedAlias("UserId", 12);
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"id", rootAlias(descriptor(definition))});

    const auto baseline = prepareTableColumnTypeBindings(tableObject(), 1, columns);
    ASSERT_TRUE(baseline.persisted_references);
    const String encoded = encodePersistedTypeReferences(*baseline.persisted_references);
    ASSERT_GT(encoded.size(), 1);

    TableColumnTypeBindingLimits exact_limits;
    exact_limits.persisted.maximum_sidecar_bytes = static_cast<UInt64>(encoded.size());
    const auto exact = prepareTableColumnTypeBindings(tableObject(), 1, columns, exact_limits);
    ASSERT_TRUE(exact.persisted_references);
    EXPECT_EQ(encodePersistedTypeReferences(*exact.persisted_references, exact_limits.persisted), encoded);

    --exact_limits.persisted.maximum_sidecar_bytes;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, exact_limits)); });

    TableColumnTypeBindingLimits field_limits;
    field_limits.persisted.maximum_qualified_name_bytes = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, field_limits)); });
}

TEST(UDTTableColumnBindings, ValidatesFullConfigurationBeforePhysicalOnlyFastPath)
{
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"id", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});

    TableColumnTypeBindingLimits limits;
    limits.persisted.maximum_sidecar_bytes = 0;
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidConfiguration,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits)); });

    limits = {};
    limits.persisted.maximum_path_depth = PersistedTypeReferencesLimits{}.maximum_path_depth + 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidConfiguration,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits)); });

    limits = {};
    limits.persisted.maximum_text_bytes = 0;
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidConfiguration,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits)); });

    limits = {};
    limits.maximum_columns = TableColumnTypeBindingLimits{}.maximum_columns + 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidConfiguration,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits)); });

    const BoundObjectTypeReferencesLimits reload_limits;
    EXPECT_EQ(TableColumnTypeBindingLimits{}.persisted.maximum_descriptors, reload_limits.persisted.maximum_descriptors);
    EXPECT_EQ(TableColumnTypeBindingLimits{}.maximum_distinct_definition_handles, reload_limits.specializer.maximum_definition_handles);

    limits = {};
    limits.persisted.maximum_descriptors = reload_limits.persisted.maximum_descriptors + 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidConfiguration,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits)); });

    limits = {};
    limits.maximum_distinct_definition_handles = reload_limits.specializer.maximum_definition_handles + 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidConfiguration,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits)); });
}

TEST(UDTTableColumnBindings, EnforcesRawAndDistinctDefinitionHandleLimitsIndependently)
{
    const auto first_definition = checkedAlias("First", 30);
    const auto second_definition = checkedAlias("Second", 31);
    const auto first_descriptor = descriptor(first_definition);
    const auto second_descriptor = descriptor(second_definition);

    const auto with_definition_handle
        = [](const InstantiatedTypeDescriptor::Ptr & logical_descriptor, const Definition::Ptr & definition)
    {
        const auto & physical_type = logical_descriptor->getPhysicalType();
        return BoundDeclaredTypeResult::withLogicalTree(
            BoundDeclaredTypeTree::build(
                {{.type_child_ordinals = {}, .physical_type = physical_type}},
                {{.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0}},
                {definition}));
    };

    std::vector<TableColumnTypeBindingInput> repeated_definition;
    repeated_definition.push_back({"first", with_definition_handle(first_descriptor, first_definition)});
    repeated_definition.push_back({"second", with_definition_handle(first_descriptor, first_definition)});

    TableColumnTypeBindingLimits limits;
    limits.maximum_definition_handles = 2;
    limits.maximum_distinct_definition_handles = 1;
    EXPECT_TRUE(prepareTableColumnTypeBindings(tableObject(), 1, repeated_definition, limits).persisted_references);

    limits.maximum_definition_handles = 1;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, repeated_definition, limits)); });

    std::vector<TableColumnTypeBindingInput> distinct_definitions;
    distinct_definitions.push_back({"first", with_definition_handle(first_descriptor, first_definition)});
    distinct_definitions.push_back({"second", with_definition_handle(second_descriptor, second_definition)});
    limits.maximum_definition_handles = 2;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, distinct_definitions, limits)); });
}

TEST(UDTTableColumnBindings, EnforcesAggregateColumnNameBytesAtTheExactBoundary)
{
    std::vector<TableColumnTypeBindingInput> columns;
    columns.push_back({"aa", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    columns.push_back({"bbb", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});

    TableColumnTypeBindingLimits limits;
    limits.maximum_total_column_name_bytes = 5;
    EXPECT_EQ(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits).physical_columns.size(), 2);

    --limits.maximum_total_column_name_bytes;
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, columns, limits)); });
}

TEST(UDTTableColumnBindings, RejectsInvalidIdentityCrossDatabaseDuplicateColumnsAndAggregateLimit)
{
    const auto definition = checkedAlias("UserId", 1);
    const auto logical_descriptor = descriptor(definition);
    std::vector<TableColumnTypeBindingInput> one_column;
    one_column.push_back({"id", rootAlias(logical_descriptor)});

    auto wrong_kind = tableObject();
    wrong_kind.kind = SchemaObjectKind::View;
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidObject,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(wrong_kind, 1, one_column)); });

    const auto foreign_definition = checkedAlias("Foreign", 2, uuid(9, 9));
    std::vector<TableColumnTypeBindingInput> foreign;
    foreign.push_back({"id", rootAlias(descriptor(foreign_definition))});
    expectBindingError(
        TableColumnTypeBindingError::Code::CrossDatabaseReference,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, foreign)); });

    std::vector<TableColumnTypeBindingInput> duplicates;
    duplicates.push_back({"id", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    duplicates.push_back({"id", BoundDeclaredTypeResult::physicalOnly(std::make_shared<DataTypeUInt64>())});
    expectBindingError(
        TableColumnTypeBindingError::Code::InvalidColumn,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, duplicates)); });

    TableColumnTypeBindingLimits limits;
    limits.persisted.maximum_occurrence_paths = 1;
    auto physical_type = std::make_shared<DataTypeUInt64>();
    auto tree = BoundDeclaredTypeTree::build(
        {{.type_child_ordinals = {}, .physical_type = physical_type}},
        {
            {.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 0},
            {.type_child_ordinals = {}, .logical_descriptor = logical_descriptor, .logical_preorder = 1},
        },
        {});
    std::vector<TableColumnTypeBindingInput> over_limit;
    over_limit.push_back({"id", BoundDeclaredTypeResult::withLogicalTree(std::move(tree))});
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, over_limit, limits)); });

    TableColumnTypeBindingLimits root_only_limits;
    root_only_limits.persisted.maximum_path_depth = 0;
    EXPECT_TRUE(prepareTableColumnTypeBindings(tableObject(), 1, one_column, root_only_limits).persisted_references.has_value());

    std::vector<TableColumnTypeBindingInput> nested;
    nested.push_back({"ids", nestedArrayAlias(logical_descriptor)});
    expectBindingError(
        TableColumnTypeBindingError::Code::LimitExceeded,
        [&] { static_cast<void>(prepareTableColumnTypeBindings(tableObject(), 1, nested, root_only_limits)); });
}

}
