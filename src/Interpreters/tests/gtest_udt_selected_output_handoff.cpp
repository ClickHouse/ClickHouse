#include <Analyzer/UDT/SelectedOutputTypeBindings.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UDT/CanonicalTypeArguments.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Interpreters/Context.h>
#include <Interpreters/UDT/StoredObjectTypeBindingPreparation.h>
#include <Interpreters/UDT/StoredObjectTypeSupport.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
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

UUID viewUUID()
{
    return uuid(0x123456789abcdef0ULL, 0x0102030405060708ULL);
}

SchemaObjectID viewObject(UUID database_uuid = databaseUUID(), UUID object_uuid = viewUUID())
{
    return {
        .kind = SchemaObjectKind::View,
        .database_uuid = database_uuid,
        .object_uuid = object_uuid,
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

Definition::Ptr checkedAlias(String name, UUID owning_database_uuid = databaseUUID())
{
    DefinitionInput input;
    input.identity = {
        .database_uuid = owning_database_uuid,
        .type_uuid = uuid(0xabcdef0123456789ULL, 1),
        .revision = 1,
    };
    input.normalized_name = "app." + name;
    input.normalized_local_name = std::move(name);
    TemplateNode root;
    root.kind = TemplateNodeKind::BuiltIn;
    root.atom = "UInt64";
    input.nodes.push_back(std::move(root));
    return TemplateChecker::checkAll({std::move(input)}).front();
}

AuthorityAdapterPtr authority(UUID owning_database_uuid = databaseUUID())
{
    return makeTransientAuthorityAdapter(owning_database_uuid, capabilities(), {checkedAlias("UserId", owning_database_uuid)});
}

ASTPtr parseCreate(const String & sql)
{
    ParserCreateQuery parser;
    return parseQuery(parser, sql, "Selected-output handoff test", 0, 512, 1'000'000);
}

ASTCreateQuery & asCreate(const ASTPtr & ast)
{
    auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    if (!create)
        throw std::logic_error("test input is not a CREATE query");
    return *create;
}

ASTLiteral * findStringLiteral(IAST * root, std::string_view value)
{
    if (!root)
        return nullptr;
    std::vector<IAST *> pending{root};
    while (!pending.empty())
    {
        IAST * node = pending.back();
        pending.pop_back();
        if (auto * literal = node->as<ASTLiteral>();
            literal && literal->value.getType() == Field::Types::String && literal->value.safeGet<String>() == value)
            return literal;
        for (const auto & child : node->children)
            if (child)
                pending.push_back(child.get());
    }
    return nullptr;
}

ASTLiteral * findStringLiteral(const ASTPtr & root, std::string_view value)
{
    return findStringLiteral(root.get(), value);
}

struct ASTOwner
{
    IAST * parent = nullptr;
    size_t child_ordinal = 0;
};

ASTOwner findOwner(IAST * root, const IAST * target)
{
    if (!root || !target)
        return {};
    std::vector<IAST *> pending{root};
    while (!pending.empty())
    {
        IAST * node = pending.back();
        pending.pop_back();
        for (size_t ordinal = 0; ordinal < node->children.size(); ++ordinal)
        {
            if (node->children[ordinal].get() == target)
                return {.parent = node, .child_ordinal = ordinal};
            if (node->children[ordinal])
                pending.push_back(node->children[ordinal].get());
        }
    }
    return {};
}

SelectedOutputTypeBinding physicalSelectedOutput(String name, String physical_type)
{
    SelectedOutputTypeBinding result;
    result.output_name = std::move(name);
    result.physical_type = DataTypeFactory::instance().get(physical_type);
    return result;
}

SelectedOutputTypeBindings selectedOutputs()
{
    return {
        physicalSelectedOutput("id", "UInt64"),
        physicalSelectedOutput("schema", "String"),
    };
}

SelectedOutputTypeBindings oneStringOutput()
{
    return {
        physicalSelectedOutput("schema", "String"),
    };
}

InstantiatedTypeDescriptor::Ptr instantiatedAlias()
{
    auto definition = checkedAlias("UserId");
    return InstantiatedTypeDescriptor::create(
        std::move(definition), CanonicalTypeArguments::validate({}, {}), DataTypeFactory::instance().get("UInt64"));
}

BoundDeclaredTypeTree::Ptr directLogicalTree()
{
    const auto descriptor = instantiatedAlias();
    std::vector<BoundDeclaredTypeNodeInput> nodes{
        {.type_child_ordinals = {}, .physical_type = descriptor->getPhysicalType()},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {}, .logical_descriptor = descriptor, .logical_preorder = 0},
    };
    return BoundDeclaredTypeTree::build(std::move(nodes), std::move(occurrences), {descriptor->getDefinition()});
}

BoundDeclaredTypeTree::Ptr nestedStackedLogicalTree()
{
    const auto descriptor = instantiatedAlias();
    const auto root = DataTypeFactory::instance().get("Tuple(String, Array(UInt64))");
    const auto array = DataTypeFactory::instance().get("Array(UInt64)");
    const auto leaf = DataTypeFactory::instance().get("UInt64");
    std::vector<BoundDeclaredTypeNodeInput> nodes{
        {.type_child_ordinals = {}, .physical_type = root},
        {.type_child_ordinals = {1}, .physical_type = array},
        {.type_child_ordinals = {1, 0}, .physical_type = leaf},
    };
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences{
        {.type_child_ordinals = {1, 0}, .logical_descriptor = descriptor, .logical_preorder = 7},
        {.type_child_ordinals = {1, 0}, .logical_descriptor = descriptor, .logical_preorder = 8},
    };
    return BoundDeclaredTypeTree::build(std::move(nodes), std::move(occurrences), {descriptor->getDefinition()});
}

SelectedOutputTypeBinding
logicalSelectedOutput(String output_name, DataTypePtr physical_type, BoundDeclaredTypeTree::Ptr tree, std::vector<UInt64> prefix = {})
{
    SelectedOutputTypeBinding result;
    result.output_name = std::move(output_name);
    result.physical_type = std::move(physical_type);
    result.explicit_logical_tree = std::move(tree);
    result.explicit_type_child_prefix = std::move(prefix);
    return result;
}

std::vector<ASTFunction *> findFunctions(IAST * root, std::string_view name)
{
    std::vector<ASTFunction *> result;
    if (!root)
        return result;
    std::vector<IAST *> pending{root};
    while (!pending.empty())
    {
        IAST * node = pending.back();
        pending.pop_back();
        if (auto * function = node->as<ASTFunction>(); function && function->name == name)
            result.push_back(function);
        for (const auto & child : node->children)
            if (child)
                pending.push_back(child.get());
    }
    return result;
}

template <typename Callback>
void expectPreparationError(StoredObjectTypeBindingPreparationError::Code code, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "expected a selected-output binding preparation error";
    }
    catch (const StoredObjectTypeBindingPreparationError & error)
    {
        EXPECT_EQ(error.code, code) << error.what();
    }
}

String multiEndpointCreateSQL()
{
    return "CREATE VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' AS "
           "SELECT CAST(1 AS app.UserId) AS id, "
           "structureToProtobufSchema('copy app.UserId') AS schema "
           "FROM null('source app.UserId') "
           "SETTINGS schema_inference_hints = 'hint app.UserId'";
}

ContextMutablePtr testContext()
{
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{1}});
    return context;
}

}

TEST(UDTSelectedOutputHandoff, PhysicalCloneDoesNotMutateOriginalAndFinalConsumeIsCanonical)
{
    const auto ast = parseCreate(multiEndpointCreateSQL());
    auto & create = asCreate(ast);
    const auto classification = classifyStoredObjectCreateQuery(create);
    ASSERT_EQ(classification.object_kind, StoredObjectKind::View);
    ASSERT_TRUE(classification.hasQualifiedTypeReferenceCandidate(StoredObjectOccurrenceSite::TableFunctionSchemaString));
    ASSERT_TRUE(classification.hasQualifiedTypeReferenceCandidate(StoredObjectOccurrenceSite::FormatSchemaString));
    ASSERT_TRUE(classification.hasStructuredUDTReference(StoredObjectOccurrenceSite::ViewStoredCast));

    const auto context = testContext();
    const auto adapter = authority();
    auto schema_handoff = prepareStoredObjectSelectedOutputSchemaStringBindings(
        create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter);

    ASSERT_NE(findStringLiteral(create.select, "source app.UserId"), nullptr);
    ASSERT_NE(findStringLiteral(create.select, "copy app.UserId"), nullptr);
    auto analysis_select = schema_handoff.clonePhysicalizedSelectForAnalysis();
    ASSERT_TRUE(schema_handoff.hasPreparedPhysicalizedAnalysisAST());
    EXPECT_NE(findStringLiteral(analysis_select, "`source` UInt64"), nullptr);
    EXPECT_NE(findStringLiteral(analysis_select, "`copy` UInt64"), nullptr);
    EXPECT_NE(findStringLiteral(create.select, "source app.UserId"), nullptr);
    EXPECT_NE(findStringLiteral(create.select, "copy app.UserId"), nullptr);
    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::InvalidState,
        [&] { static_cast<void>(schema_handoff.clonePhysicalizedSelectForAnalysis()); });

    auto outputs = selectedOutputs();
    auto prepared = prepareStoredObjectSelectedOutputBindings(
        create, classification, StoredObjectKind::View, viewObject(), 1, "app", context, *adapter, outputs, &schema_handoff);
    EXPECT_FALSE(schema_handoff.hasPreparedPhysicalizedAnalysisAST());
    const auto * bindings = prepared.tryGetViewBindings();
    ASSERT_NE(bindings, nullptr);
    ASSERT_TRUE(bindings->persisted_references);
    ASSERT_TRUE(bindings->bound_physical_schema);
    ASSERT_TRUE(bindings->sidecar_expectation);
    EXPECT_EQ(bindings->persisted_references->occurrence_paths.size(), 4u);
    EXPECT_EQ(bindings->dependency_edges.size(), 1u);
    EXPECT_FALSE(prepared.hasAppliedPhysicalTypeASTs());

    prepared.applyPhysicalTypeASTs();
    EXPECT_TRUE(prepared.hasAppliedPhysicalTypeASTs());
    EXPECT_NE(findStringLiteral(create.select, "`source` UInt64"), nullptr);
    EXPECT_NE(findStringLiteral(create.select, "`copy` UInt64"), nullptr);
    EXPECT_NE(create.select->formatWithSecretsOneLine().find("`hint` UInt64"), String::npos);
    EXPECT_EQ(create.select->formatWithSecretsOneLine().find("app.UserId"), String::npos);

    const auto post = classifyStoredObjectCreateQuery(create);
    EXPECT_EQ(post.structured_udt_occurrence_sites, 0u);
    EXPECT_EQ(post.qualified_type_reference_candidate_sites, 0u);
    EXPECT_EQ(post.unresolved_type_string_occurrence_sites, 0u);
    EXPECT_EQ(classifyStoredObjectCreatePreparation(create, post, true).route, StoredObjectCreatePreparationRoute::PhysicalOnly);
}

TEST(UDTSelectedOutputHandoff, FinalApplyPreflightIsAllOrNothingAndRetryable)
{
    const auto ast = parseCreate(multiEndpointCreateSQL());
    auto & create = asCreate(ast);
    const auto classification = classifyStoredObjectCreateQuery(create);
    const auto context = testContext();
    const auto adapter = authority();
    auto schema_handoff = prepareStoredObjectSelectedOutputSchemaStringBindings(
        create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter);
    static_cast<void>(schema_handoff.clonePhysicalizedSelectForAnalysis());
    auto outputs = selectedOutputs();
    auto prepared = prepareStoredObjectSelectedOutputBindings(
        create, classification, StoredObjectKind::View, viewObject(), 1, "app", context, *adapter, outputs, &schema_handoff);

    auto * source = findStringLiteral(create.select, "source app.UserId");
    auto * copy = findStringLiteral(create.select, "copy app.UserId");
    ASSERT_NE(source, nullptr);
    ASSERT_NE(copy, nullptr);
    copy->value = "copy String";
    expectPreparationError(StoredObjectTypeBindingPreparationError::Code::QueryChanged, [&] { prepared.applyPhysicalTypeASTs(); });
    EXPECT_EQ(source->value.safeGet<String>(), "source app.UserId");
    EXPECT_NE(create.select->formatWithSecretsOneLine().find("app.UserId"), String::npos);

    copy->value = "copy app.UserId";
    EXPECT_NO_THROW(prepared.applyPhysicalTypeASTs());
    EXPECT_EQ(source->value.safeGet<String>(), "`source` UInt64");
    EXPECT_EQ(copy->value.safeGet<String>(), "`copy` UInt64");
}

TEST(UDTSelectedOutputHandoff, CloneRejectsBytesPathAndRootGenerationDriftWithoutMutation)
{
    const auto context = testContext();
    const auto adapter = authority();

    {
        const auto ast = parseCreate(
            "CREATE VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' AS "
            "SELECT structureToProtobufSchema('id app.UserId') AS schema");
        auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        auto handoff = prepareStoredObjectSelectedOutputSchemaStringBindings(
            create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter);
        auto * literal = findStringLiteral(create.select, "id app.UserId");
        ASSERT_NE(literal, nullptr);
        literal->value = "id String";
        expectPreparationError(
            StoredObjectTypeBindingPreparationError::Code::QueryChanged,
            [&] { static_cast<void>(handoff.clonePhysicalizedSelectForAnalysis()); });
        EXPECT_EQ(literal->value.safeGet<String>(), "id String");
    }

    {
        const auto ast = parseCreate(
            "CREATE VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' AS "
            "SELECT structureToProtobufSchema('id app.UserId') AS schema");
        auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        auto handoff = prepareStoredObjectSelectedOutputSchemaStringBindings(
            create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter);
        auto * literal = findStringLiteral(create.select, "id app.UserId");
        const auto owner = findOwner(create.select, literal);
        ASSERT_NE(owner.parent, nullptr);
        ASTPtr original = owner.parent->children[owner.child_ordinal];
        owner.parent->children[owner.child_ordinal] = original->clone();
        expectPreparationError(
            StoredObjectTypeBindingPreparationError::Code::QueryChanged,
            [&] { static_cast<void>(handoff.clonePhysicalizedSelectForAnalysis()); });
        EXPECT_NE(findStringLiteral(create.select, "id app.UserId"), nullptr);
        owner.parent->children[owner.child_ordinal] = std::move(original);
    }

    {
        const auto ast = parseCreate(
            "CREATE VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' AS "
            "SELECT structureToProtobufSchema('id app.UserId') AS schema");
        auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        auto handoff = prepareStoredObjectSelectedOutputSchemaStringBindings(
            create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter);
        auto * original_select = create.select;
        ASTPtr changed_select = create.select->clone();
        create.select = changed_select->as<ASTSelectWithUnionQuery>();
        ASSERT_NE(create.select, nullptr);
        expectPreparationError(
            StoredObjectTypeBindingPreparationError::Code::InvalidState,
            [&] { static_cast<void>(handoff.clonePhysicalizedSelectForAnalysis()); });
        create.select = original_select;
    }
}

TEST(UDTSelectedOutputHandoff, FinalBindingRevalidatesObjectDatabaseAndAuthority)
{
    const auto ast = parseCreate(
        "CREATE VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' AS "
        "SELECT structureToProtobufSchema('id app.UserId') AS schema");
    auto & create = asCreate(ast);
    const auto classification = classifyStoredObjectCreateQuery(create);
    const auto context = testContext();
    const auto adapter = authority();
    auto handoff = prepareStoredObjectSelectedOutputSchemaStringBindings(
        create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter);
    static_cast<void>(handoff.clonePhysicalizedSelectForAnalysis());
    auto outputs = oneStringOutput();

    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::InvalidObject,
        [&]
        {
            static_cast<void>(prepareStoredObjectSelectedOutputBindings(
                create,
                classification,
                StoredObjectKind::View,
                viewObject(databaseUUID(), uuid(9, 9)),
                1,
                "app",
                context,
                *adapter,
                outputs,
                &handoff));
        });

    const UUID other_database = uuid(0x9988776655443322ULL, 0x1100ffeeddccbbaaULL);
    const auto other_authority = authority(other_database);
    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::InvalidObject,
        [&]
        {
            static_cast<void>(prepareStoredObjectSelectedOutputBindings(
                create, classification, StoredObjectKind::View, viewObject(), 1, "app", context, *other_authority, outputs, &handoff));
        });

    EXPECT_NO_THROW(
        static_cast<void>(prepareStoredObjectSelectedOutputBindings(
            create, classification, StoredObjectKind::View, viewObject(), 1, "app", context, *adapter, outputs, &handoff)));
}

TEST(UDTSelectedOutputHandoff, FailureAfterMovePoisonsHandoffAndCannotBeRetried)
{
    const auto ast = parseCreate(
        "CREATE VIEW app.v UUID '12345678-9abc-def0-0102-030405060708' AS "
        "SELECT structureToProtobufSchema('id app.UserId') AS schema");
    auto & create = asCreate(ast);
    const auto classification = classifyStoredObjectCreateQuery(create);
    const auto context = testContext();
    const auto adapter = authority();
    auto handoff = prepareStoredObjectSelectedOutputSchemaStringBindings(
        create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter);
    static_cast<void>(handoff.clonePhysicalizedSelectForAnalysis());
    auto outputs = selectedOutputs();
    ViewOutputTypeBindingLimits limits;
    limits.maximum_outputs = 1;

    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(prepareStoredObjectSelectedOutputBindings(
                create, classification, StoredObjectKind::View, viewObject(), 1, "app", context, *adapter, outputs, &handoff, limits));
        });
    EXPECT_FALSE(handoff.hasPreparedPhysicalizedAnalysisAST());
    EXPECT_NE(findStringLiteral(create.select, "id app.UserId"), nullptr);

    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::QueryChanged,
        [&]
        {
            static_cast<void>(prepareStoredObjectSelectedOutputBindings(
                create, classification, StoredObjectKind::View, viewObject(), 1, "app", context, *adapter, outputs, &handoff));
        });
}

TEST(UDTSelectedOutputHandoff, ContextOwnedLayoutsCannotCreatePreAnalysisHandoff)
{
    static constexpr std::array queries{
        std::string_view{"CREATE VIEW app.v AS SELECT * FROM values('id app.UserId', (1))"},
        std::string_view{"CREATE VIEW app.v AS SELECT * FROM url('http://127.0.0.1/data', 'CSV', 'id app.UserId')"},
        std::string_view{"CREATE VIEW app.v AS SELECT * FROM s3('http://127.0.0.1/data', 'CSV', 'id app.UserId')"},
        std::string_view{"CREATE VIEW app.v AS SELECT * FROM mongodb('host', 'db', 'collection', 'user', 'password', 'id app.UserId')"},
    };
    const auto context = testContext();
    const auto adapter = authority();
    for (const auto query : queries)
    {
        SCOPED_TRACE(query);
        const auto ast = parseCreate(String(query));
        auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        expectPreparationError(
            StoredObjectTypeBindingPreparationError::Code::InvalidDecision,
            [&]
            {
                static_cast<void>(prepareStoredObjectSelectedOutputSchemaStringBindings(
                    create, classification, StoredObjectKind::View, databaseUUID(), "app", context, *adapter));
            });
        EXPECT_NE(create.select->formatWithSecretsOneLine().find("app.UserId"), String::npos);
    }
}

TEST(UDTSelectedOutputHandoff, MaterializedViewAlterCanRebindOrRemoveTheLastLogicalOccurrence)
{
    const auto context = testContext();
    const auto adapter = authority();

    {
        const auto ast = parseCreate(
            "CREATE MATERIALIZED VIEW app.mv TO app.dst AS "
            "SELECT CAST(1 AS app.UserId) AS id");
        auto & create = asCreate(ast);
        ASTPtr stored_select = create.select->ptr();
        SelectedOutputTypeBindings outputs{
            logicalSelectedOutput("id", DataTypeFactory::instance().get("UInt64"), directLogicalTree()),
        };

        auto prepared = prepareStoredObjectSelectedOutputAlterBindings(
            stored_select, StoredObjectKind::MaterializedView, viewObject(), 2, "app", context, *adapter, outputs);
        EXPECT_EQ(prepared.getObjectKind(), StoredObjectKind::MaterializedView);
        EXPECT_EQ(prepared.getSourceMode(), StoredObjectSourceMode::AsSelect);
        EXPECT_TRUE(prepared.usesSelectedOutputClassification());
        const auto * bindings = prepared.tryGetViewBindings();
        ASSERT_NE(bindings, nullptr);
        ASSERT_TRUE(bindings->persisted_references);
        ASSERT_TRUE(bindings->bound_physical_schema);
        ASSERT_TRUE(bindings->sidecar_expectation);
        ASSERT_EQ(bindings->persisted_references->occurrence_paths.size(), 2u);
        EXPECT_EQ(
            std::count_if(
                bindings->persisted_references->occurrence_paths.begin(),
                bindings->persisted_references->occurrence_paths.end(),
                [](const auto & path) { return path.section == PersistedTypePathSection::ViewExpression; }),
            2);
        EXPECT_EQ(
            std::count_if(
                bindings->persisted_references->occurrence_paths.begin(),
                bindings->persisted_references->occurrence_paths.end(),
                [](const auto & path) { return path.site == PersistedTypeOccurrenceSite::StoredExpression; }),
            1);

        EXPECT_NE(stored_select->formatWithSecretsOneLine().find("app.UserId"), String::npos);
        prepared.applyPhysicalTypeASTs();
        EXPECT_EQ(stored_select->formatWithSecretsOneLine().find("app.UserId"), String::npos);
        auto released = std::move(prepared).releaseViewBindings();
        EXPECT_TRUE(released.persisted_references);
        EXPECT_EQ(released.physical_outputs.size(), 1u);
    }

    {
        const auto ast = parseCreate(
            "CREATE MATERIALIZED VIEW app.mv TO app.dst AS "
            "SELECT toUInt64(1) AS id");
        auto & create = asCreate(ast);
        ASTPtr stored_select = create.select->ptr();
        SelectedOutputTypeBindings outputs{
            physicalSelectedOutput("id", "UInt64"),
        };

        auto prepared = prepareStoredObjectSelectedOutputAlterBindings(
            stored_select, StoredObjectKind::MaterializedView, viewObject(), 3, "app", context, *adapter, outputs);
        const auto * bindings = prepared.tryGetViewBindings();
        ASSERT_NE(bindings, nullptr);
        EXPECT_FALSE(bindings->persisted_references);
        EXPECT_FALSE(bindings->bound_physical_schema);
        EXPECT_FALSE(bindings->sidecar_expectation);
        EXPECT_TRUE(bindings->dependency_edges.empty());
        prepared.applyPhysicalTypeASTs();
        auto released = std::move(prepared).releaseViewBindings();
        EXPECT_FALSE(released.persisted_references);
        EXPECT_EQ(released.physical_outputs.size(), 1u);
    }
}

TEST(UDTSelectedOutputHandoff, NestedSelectedSlicePreservesStackedOccurrencesAndRejectsWrongPrefix)
{
    const auto context = testContext();
    const auto adapter = authority();
    const auto tree = nestedStackedLogicalTree();

    {
        const auto ast = parseCreate("CREATE MATERIALIZED VIEW app.mv TO app.dst AS SELECT toUInt64(1) AS id");
        auto & create = asCreate(ast);
        ASTPtr stored_select = create.select->ptr();
        SelectedOutputTypeBindings outputs{
            logicalSelectedOutput("id", DataTypeFactory::instance().get("UInt64"), tree, {1, 0}),
        };
        auto prepared = prepareStoredObjectSelectedOutputAlterBindings(
            stored_select, StoredObjectKind::MaterializedView, viewObject(), 4, "app", context, *adapter, outputs);
        const auto * bindings = prepared.tryGetViewBindings();
        ASSERT_NE(bindings, nullptr);
        ASSERT_TRUE(bindings->persisted_references);
        ASSERT_EQ(bindings->persisted_references->occurrence_paths.size(), 2u);
        for (size_t index = 0; index < bindings->persisted_references->occurrence_paths.size(); ++index)
        {
            const auto & path = bindings->persisted_references->occurrence_paths[index];
            EXPECT_EQ(path.section, PersistedTypePathSection::ViewExpression);
            EXPECT_EQ(path.object_ordinal, 0u);
            EXPECT_EQ(path.occurrence_ordinal, index);
            EXPECT_TRUE(path.type_child_ordinals.empty());
        }
        EXPECT_EQ(bindings->persisted_references->descriptors.size(), 1u);
    }

    {
        const auto ast = parseCreate("CREATE MATERIALIZED VIEW app.mv TO app.dst AS SELECT toUInt64(1) AS id");
        auto & create = asCreate(ast);
        ASTPtr stored_select = create.select->ptr();
        SelectedOutputTypeBindings outputs{
            logicalSelectedOutput("id", DataTypeFactory::instance().get("UInt64"), tree, {1, 7}),
        };
        expectPreparationError(
            StoredObjectTypeBindingPreparationError::Code::MissingLogicalBinding,
            [&]
            {
                static_cast<void>(prepareStoredObjectSelectedOutputAlterBindings(
                    stored_select, StoredObjectKind::MaterializedView, viewObject(), 5, "app", context, *adapter, outputs));
            });
        EXPECT_EQ(stored_select->formatWithSecretsOneLine().find("app.UserId"), String::npos);
    }
}

TEST(UDTSelectedOutputHandoff, PhysicalAuxiliaryReplayCoversEveryOwnerAndRejectsMalformedMetadata)
{
    const auto ast = parseCreate(
        "CREATE VIEW app.v AS SELECT "
        "CAST(CAST(1 AS UInt64) AS Nullable(UInt64)) AS id, "
        "structureToProtobufSchema('copy UInt64') AS schema "
        "FROM null('source UInt64') "
        "SETTINGS schema_inference_hints = 'hint UInt64'");
    auto & create = asCreate(ast);
    auto endpoints = collectViewAuxiliaryPhysicalTypeBindings(create);
    ASSERT_EQ(endpoints.size(), 5u);
    EXPECT_EQ(
        std::count_if(
            endpoints.begin(),
            endpoints.end(),
            [](const auto & endpoint) { return endpoint.site == PersistedTypeOccurrenceSite::StoredExpression; }),
        2);
    EXPECT_EQ(
        std::count_if(
            endpoints.begin(),
            endpoints.end(),
            [](const auto & endpoint) { return endpoint.site == PersistedTypeOccurrenceSite::SchemaString; }),
        3);
    std::set<std::pair<PersistedTypeOccurrenceSite, UInt64>> keys;
    for (const auto & endpoint : endpoints)
    {
        EXPECT_TRUE(endpoint.physical_type);
        EXPECT_FALSE(endpoint.runtime_owner_key.empty());
        EXPECT_TRUE(keys.emplace(endpoint.site, endpoint.object_ordinal).second);
        const String expected_prefix
            = endpoint.site == PersistedTypeOccurrenceSite::StoredExpression ? "stored-expression:" : "schema-string:";
        EXPECT_TRUE(endpoint.runtime_owner_key.starts_with(expected_prefix));
    }

    ViewOutputTypeBindingLimits endpoint_limit;
    endpoint_limit.maximum_outputs = endpoints.size() - 1;
    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::LimitExceeded,
        [&] { static_cast<void>(collectViewAuxiliaryPhysicalTypeBindings(create, endpoint_limit)); });

    ViewOutputTypeBindingLimits owner_key_limit;
    owner_key_limit.maximum_single_runtime_owner_key_bytes = 1;
    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::LimitExceeded,
        [&] { static_cast<void>(collectViewAuxiliaryPhysicalTypeBindings(create, owner_key_limit)); });

    auto casts = findFunctions(create.select, "CAST");
    ASSERT_EQ(casts.size(), 2u);
    casts.front()->setUDTStoredExpressionOrdinal(9);
    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::QueryChanged,
        [&] { physicalizeViewStoredSelectRuntimeAnnotations(create.select->ptr()); });

    const auto logical_ast = parseCreate("CREATE VIEW app.bad AS SELECT * FROM null('id app.UserId')");
    expectPreparationError(
        StoredObjectTypeBindingPreparationError::Code::InvalidDeclaration,
        [&] { static_cast<void>(collectViewAuxiliaryPhysicalTypeBindings(asCreate(logical_ast))); });
}

}
