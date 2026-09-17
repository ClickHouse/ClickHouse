#include <Interpreters/UDT/StoredObjectTableFunctionSources.h>
#include <Interpreters/UDT/StoredObjectTypeStringSlots.h>
#include <Interpreters/UDT/StoredObjectTypeSupport.h>
#include <Interpreters/UDT/UDTExecutionBoundary.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

ASTPtr parseCreate(const String & sql)
{
    ParserCreateQuery parser;
    return parseQuery(parser, sql, "Stored-object type-string inventory test", 0, 512, 1'000'000);
}

ASTPtr parseStatement(const String & sql)
{
    ParserQuery parser(sql.data() + sql.size());
    return parseQuery(parser, sql, "UDT execution-boundary test", 0, 512, 1'000'000);
}

ASTCreateQuery & asCreate(const ASTPtr & ast)
{
    auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    if (!create)
        throw std::logic_error("test input is not a CREATE query");
    return *create;
}

boost::intrusive_ptr<ASTFunction> function(String name, ASTs arguments = {})
{
    auto result = make_intrusive<ASTFunction>();
    result->name = std::move(name);
    result->setKind(ASTFunction::Kind::ORDINARY_FUNCTION);
    result->arguments = make_intrusive<ASTExpressionList>();
    result->arguments->children = std::move(arguments);
    result->children.push_back(result->arguments);
    return result;
}

ASTPtr stringLiteral(String value)
{
    return make_intrusive<ASTLiteral>(std::move(value));
}

void expectUnsupported(const ASTCreateQuery & create, const StoredObjectCreateQueryClassification & classification)
{
    const auto decision = classifyStoredObjectCreatePreparation(create, classification, true);
    EXPECT_EQ(decision.route, StoredObjectCreatePreparationRoute::Unsupported);
    EXPECT_NE(decision.rejection, StoredObjectAdmissionRejection::None);
}

}

TEST(UDTTypeStringInventory, SourceInventoryIsClosedAndEvalRequiresExactAuthority)
{
    const auto contracts = getStoredObjectTableFunctionSourceContracts();
    ASSERT_FALSE(contracts.empty());
    for (size_t index = 1; index < contracts.size(); ++index)
        EXPECT_LT(contracts[index - 1].function_name, contracts[index].function_name);

    const auto * eval = tryGetStoredObjectTableFunctionSourceContract("eval");
    ASSERT_NE(eval, nullptr);
    EXPECT_EQ(eval->provenance, StoredObjectTableFunctionSourceProvenance::ExactLogicalAuthorityRequired);
    EXPECT_EQ(classifyStoredObjectTableFunctionSource("view"), StoredObjectTableFunctionSourceProvenance::ExactLogicalAuthorityRequired);
    EXPECT_EQ(
        classifyStoredObjectTableFunctionSource("dictionary"), StoredObjectTableFunctionSourceProvenance::ExactLogicalAuthorityRequired);
    EXPECT_EQ(classifyStoredObjectTableFunctionSource("null"), StoredObjectTableFunctionSourceProvenance::PhysicalInference);
    EXPECT_EQ(classifyStoredObjectTableFunctionSource("unclassified_source"), StoredObjectTableFunctionSourceProvenance::Unclassified);
}

TEST(UDTTypeStringInventory, ExactContextAndAdapterOwnedSlotsNeverUseHeuristics)
{
    const auto exact = function("null", {stringLiteral("id app.UserId")});
    const auto exact_slot = classifyTableFunctionTypeStringSlot(*exact);
    EXPECT_EQ(exact_slot.status, StoredObjectTypeStringSlotStatus::ExactExpression);
    EXPECT_EQ(exact_slot.argument_ordinal, 0u);
    EXPECT_EQ(exact_slot.expression, exact->arguments->children[0].get());

    EXPECT_EQ(classifyTableFunctionTypeStringSlot(*function("null")).status, StoredObjectTypeStringSlotStatus::NoExplicitSchemaString);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(*function("null", {stringLiteral("a UInt8"), stringLiteral("b UInt8")})).status,
        StoredObjectTypeStringSlotStatus::UnclassifiedLayout);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(*function("file", {stringLiteral("data.tsv"), stringLiteral("TSV")})).status,
        StoredObjectTypeStringSlotStatus::NoExplicitSchemaString);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(
            *function("file", {stringLiteral("data.tsv"), stringLiteral("TSV"), stringLiteral("id app.UserId")}))
            .status,
        StoredObjectTypeStringSlotStatus::ExactExpression);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(*function("values", {stringLiteral("id app.UserId"), make_intrusive<ASTLiteral>(UInt64{1})}))
            .status,
        StoredObjectTypeStringSlotStatus::ContextRequired);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(
            *function("values", {make_intrusive<ASTLiteral>(UInt64{1}), make_intrusive<ASTLiteral>(UInt64{2})}))
            .status,
        StoredObjectTypeStringSlotStatus::NoExplicitSchemaString);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(
            *function("url", {stringLiteral("http://127.0.0.1/data"), stringLiteral("CSV"), stringLiteral("id app.UserId")}))
            .status,
        StoredObjectTypeStringSlotStatus::ContextRequired);

    const auto scalar = function("unclassified_scalar", {stringLiteral("id app.UserId")});
    EXPECT_EQ(classifyTableFunctionTypeStringSlot(*scalar).status, StoredObjectTypeStringSlotStatus::Unregistered);
    EXPECT_EQ(classifyStoredExpressionTypeStringSlot(*scalar).status, StoredObjectTypeStringSlotStatus::Unregistered);
}

TEST(UDTTypeStringInventory, ExactLayoutsAndStoredExpressionPositionsAreOwnerSpecific)
{
    {
        const auto format = function("FoRmAt", {stringLiteral("CSV"), stringLiteral("id app.UserId"), stringLiteral("1")});
        const auto slot = classifyTableFunctionTypeStringSlot(*format);
        EXPECT_EQ(slot.status, StoredObjectTypeStringSlotStatus::ExactExpression);
        EXPECT_EQ(slot.occurrence_site, StoredObjectOccurrenceSite::FormatSchemaString);
        EXPECT_EQ(slot.argument_ordinal, 1u);
        EXPECT_EQ(slot.expression, format->arguments->children[1].get());
    }
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(*function("format", {stringLiteral("CSV"), stringLiteral("auto"), stringLiteral("1")})).status,
        StoredObjectTypeStringSlotStatus::NoExplicitSchemaString);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(*function("NULL", {stringLiteral("id app.UserId")})).status,
        StoredObjectTypeStringSlotStatus::Unregistered);

    {
        const auto generated = function("generateRandom", {stringLiteral("id app.UserId")});
        const auto slot = classifyTableFunctionTypeStringSlot(*generated);
        EXPECT_EQ(slot.status, StoredObjectTypeStringSlotStatus::ExactExpression);
        EXPECT_EQ(slot.argument_ordinal, 0u);
    }
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(*function("generateRandom", {make_intrusive<ASTLiteral>(UInt64{10})})).status,
        StoredObjectTypeStringSlotStatus::NoExplicitSchemaString);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(*function("file", {stringLiteral("data.tsv"), stringLiteral("TSV"), stringLiteral("auto")}))
            .status,
        StoredObjectTypeStringSlotStatus::NoExplicitSchemaString);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(
            *function("executable", {stringLiteral("cmd"), stringLiteral("TSV"), stringLiteral("id app.UserId")}))
            .argument_ordinal,
        2u);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(
            *function("redis", {stringLiteral("host"), stringLiteral("key"), stringLiteral("id app.UserId")}))
            .argument_ordinal,
        2u);
    EXPECT_EQ(
        classifyTableFunctionTypeStringSlot(
            *function(
                "hive",
                {stringLiteral("url"), stringLiteral("db"), stringLiteral("table"), stringLiteral("id app.UserId"), stringLiteral("x")}))
            .argument_ordinal,
        3u);

    {
        const auto cast = function("cAsT", {make_intrusive<ASTLiteral>(UInt64{1}), stringLiteral("app.UserId")});
        const auto slot = classifyStoredExpressionTypeStringSlot(*cast);
        EXPECT_EQ(slot.status, StoredObjectTypeStringSlotStatus::ExactExpression);
        EXPECT_EQ(slot.argument_ordinal, 1u);
        EXPECT_EQ(slot.expression, cast->arguments->children[1].get());
    }
    {
        const auto json = function("JSONExtract", {stringLiteral("{\"id\":1}"), stringLiteral("id"), stringLiteral("app.UserId")});
        const auto slot = classifyStoredExpressionTypeStringSlot(*json);
        EXPECT_EQ(slot.status, StoredObjectTypeStringSlotStatus::ExactExpression);
        EXPECT_EQ(slot.argument_ordinal, 2u);
    }
    {
        const auto schema = function("structureToCapnProtoSchema", {stringLiteral("id app.UserId")});
        const auto slot = classifyStoredExpressionTypeStringSlot(*schema);
        EXPECT_EQ(slot.occurrence_site, StoredObjectOccurrenceSite::FormatSchemaString);
        EXPECT_EQ(slot.argument_ordinal, 0u);
    }
    EXPECT_EQ(
        classifyStoredExpressionTypeStringSlot(*function("JSONExtract")).status, StoredObjectTypeStringSlotStatus::UnclassifiedLayout);

    auto parameterized_cast = function("CAST", {make_intrusive<ASTLiteral>(UInt64{1}), stringLiteral("app.UserId")});
    parameterized_cast->parameters = make_intrusive<ASTExpressionList>();
    parameterized_cast->children.push_back(parameterized_cast->parameters);
    EXPECT_EQ(classifyStoredExpressionTypeStringSlot(*parameterized_cast).status, StoredObjectTypeStringSlotStatus::UnclassifiedLayout);
}

TEST(UDTTypeStringInventory, LoopAndViewIfPermittedOwnOnlyTheirExactNestedBranch)
{
    const auto terminal = function("null", {stringLiteral("id app.UserId")});
    const auto loop = function("loop", {terminal});
    auto tree = classifyStoredObjectTableFunctionTypeStringTree(*loop);
    ASSERT_EQ(tree.status, StoredObjectTableFunctionTypeStringTreeStatus::Complete);
    EXPECT_EQ(tree.schema_owner, terminal.get());
    EXPECT_EQ(tree.schema_slot.expression, terminal->arguments->children[0].get());
    EXPECT_EQ(tree.nested_depth, 1u);

    const auto view_if = function("viewIfPermitted", {make_intrusive<ASTSelectWithUnionQuery>(), loop});
    tree = classifyStoredObjectTableFunctionTypeStringTree(*view_if);
    ASSERT_EQ(tree.status, StoredObjectTableFunctionTypeStringTreeStatus::Complete);
    EXPECT_EQ(tree.schema_owner, terminal.get());
    EXPECT_EQ(tree.nested_depth, 2u);

    const auto scalar_function_0 = function("databaseName", {stringLiteral("app")});
    const auto scalar_function_1 = function("tableName", {stringLiteral("events")});
    const auto scalar_loop = function("loop", {scalar_function_0, scalar_function_1});
    tree = classifyStoredObjectTableFunctionTypeStringTree(*scalar_loop);
    ASSERT_EQ(tree.status, StoredObjectTableFunctionTypeStringTreeStatus::Complete);
    EXPECT_EQ(tree.schema_owner, scalar_loop.get());
    EXPECT_EQ(tree.nested_depth, 0u);
    EXPECT_EQ(tree.schema_slot.status, StoredObjectTypeStringSlotStatus::Unregistered);

    const auto identifier_loop = function("loop", {make_intrusive<ASTIdentifier>("events")});
    tree = classifyStoredObjectTableFunctionTypeStringTree(*identifier_loop);
    EXPECT_EQ(tree.status, StoredObjectTableFunctionTypeStringTreeStatus::Complete);
    EXPECT_EQ(tree.nested_depth, 0u);
}

TEST(UDTTypeStringInventory, MalformedUnknownDeepAndCyclicNestedOwnersFailClosed)
{
    const std::array malformed{
        function("loop"),
        function("loop", {stringLiteral("not a table-function AST")}),
        function("loop", {function("null"), function("null"), function("null")}),
        function("viewIfPermitted", {stringLiteral("SELECT 1"), function("null")}),
        function("viewIfPermitted", {make_intrusive<ASTSelectWithUnionQuery>(), make_intrusive<ASTIdentifier>("events")}),
    };
    for (const auto & owner : malformed)
        EXPECT_EQ(
            classifyStoredObjectTableFunctionTypeStringTree(*owner).status,
            StoredObjectTableFunctionTypeStringTreeStatus::UnclassifiedLayout);

    const auto unknown = function("loop", {function("unclassified_nested_source", {stringLiteral("id app.UserId")})});
    const auto unknown_tree = classifyStoredObjectTableFunctionTypeStringTree(*unknown);
    ASSERT_EQ(unknown_tree.status, StoredObjectTableFunctionTypeStringTreeStatus::Complete);
    EXPECT_EQ(unknown_tree.nested_depth, 1u);
    EXPECT_EQ(unknown_tree.schema_slot.status, StoredObjectTypeStringSlotStatus::Unregistered);

    auto deep = function("null", {stringLiteral("id app.UserId")});
    for (size_t depth = 0; depth < 257; ++depth)
        deep = function("loop", {deep});
    EXPECT_EQ(
        classifyStoredObjectTableFunctionTypeStringTree(*deep).status, StoredObjectTableFunctionTypeStringTreeStatus::UnclassifiedLayout);

    auto cyclic = function("loop");
    cyclic->arguments->children.push_back(cyclic);
    EXPECT_EQ(
        classifyStoredObjectTableFunctionTypeStringTree(*cyclic).status, StoredObjectTableFunctionTypeStringTreeStatus::UnclassifiedLayout);
    cyclic->arguments->children.clear();
}

TEST(UDTTypeStringInventory, NestedOwnerClassificationRejectsSharedOrParameterizedGrammar)
{
    const auto terminal = function("null", {stringLiteral("id app.UserId")});
    auto shared_arguments = function("loop", {terminal});
    shared_arguments->children.push_back(shared_arguments->arguments);
    EXPECT_EQ(
        classifyStoredObjectNestedTableFunctionSlot(*shared_arguments).status,
        StoredObjectNestedTableFunctionSlotStatus::UnclassifiedLayout);
    EXPECT_EQ(
        classifyStoredObjectTableFunctionTypeStringTree(*shared_arguments).status,
        StoredObjectTableFunctionTypeStringTreeStatus::UnclassifiedLayout);

    auto parameterized = function("loop", {terminal});
    parameterized->parameters = make_intrusive<ASTExpressionList>();
    parameterized->children.push_back(parameterized->parameters);
    EXPECT_EQ(
        classifyStoredObjectNestedTableFunctionSlot(*parameterized).status, StoredObjectNestedTableFunctionSlotStatus::UnclassifiedLayout);

    const auto case_changed = function("Loop", {terminal});
    const auto tree = classifyStoredObjectTableFunctionTypeStringTree(*case_changed);
    ASSERT_EQ(tree.status, StoredObjectTableFunctionTypeStringTreeStatus::Complete);
    EXPECT_EQ(tree.schema_owner, case_changed.get());
    EXPECT_EQ(tree.nested_depth, 0u);
    EXPECT_EQ(tree.schema_slot.status, StoredObjectTypeStringSlotStatus::Unregistered);
}

TEST(UDTTypeStringInventory, CreateClassificationRejectsMutableUnknownAndMalformedSchemas)
{
    static constexpr std::array queries{
        std::string_view{"CREATE VIEW app.v AS SELECT * FROM values('id app.UserId', (1))"},
        std::string_view{"CREATE VIEW app.v AS SELECT * FROM url('http://127.0.0.1/data', 'CSV', 'id app.UserId')"},
        std::string_view{"CREATE VIEW app.v AS SELECT * FROM loop(unclassified_nested_source('id app.UserId'))"},
        std::string_view{
            "CREATE VIEW app.v AS SELECT * FROM loop(`null`('id app.UserId'), `null`('id app.UserId'), `null`('id app.UserId'))"},
    };
    for (const auto query : queries)
    {
        SCOPED_TRACE(query);
        const auto ast = parseCreate(String(query));
        const auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        EXPECT_TRUE(
            classification.hasUnresolvedTypeStringSlot(StoredObjectOccurrenceSite::TableFunctionSchemaString)
            || classification.hasUnresolvedTypeStringSlot(StoredObjectOccurrenceSite::FormatSchemaString));
        expectUnsupported(create, classification);
    }
}

TEST(UDTTypeStringInventory, EvalAndUnregisteredSourcesCannotSupplyPhysicalProvenance)
{
    {
        const auto ast = parseCreate("CREATE VIEW app.v AS SELECT * FROM eval('SELECT 1')");
        const auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        EXPECT_TRUE(classification.source_query_requires_exact_logical_authority);
        EXPECT_FALSE(classification.source_query_has_unclassified_table_function);
    }
    {
        const auto ast = parseCreate("CREATE VIEW app.v AS SELECT * FROM unclassified_source()");
        const auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        EXPECT_TRUE(classification.source_query_has_unclassified_table_function);
        expectUnsupported(create, classification);
    }
}

TEST(UDTTypeStringInventory, SourceModesAreMutuallyExclusiveAndStable)
{
    struct Case
    {
        std::string_view query;
        StoredObjectSourceMode source_mode;
        bool explicit_columns;
    };
    static constexpr std::array cases{
        Case{"CREATE TABLE app.t (id UInt64) ENGINE = Memory", StoredObjectSourceMode::ExplicitColumns, true},
        Case{"CREATE TABLE app.t AS app.source", StoredObjectSourceMode::AsSourceTable, false},
        Case{"CREATE TABLE app.t CLONE AS app.source", StoredObjectSourceMode::CloneAsSourceTable, false},
        Case{"CREATE TABLE app.t ENGINE = Memory AS SELECT 1 AS id", StoredObjectSourceMode::AsSelect, false},
        Case{"CREATE TABLE app.t ENGINE = Memory EMPTY AS SELECT 1 AS id", StoredObjectSourceMode::EmptyAsSelect, false},
        Case{"CREATE TABLE app.t AS null('id UInt64')", StoredObjectSourceMode::AsTableFunction, false},
        Case{"CREATE VIEW app.v AS SELECT 1 AS id", StoredObjectSourceMode::AsSelect, false},
    };

    for (const auto & test : cases)
    {
        SCOPED_TRACE(test.query);
        const auto ast = parseCreate(String(test.query));
        const auto classification = classifyStoredObjectCreateQuery(asCreate(ast));
        EXPECT_EQ(classification.source_mode, test.source_mode);
        EXPECT_EQ(classification.has_explicit_destination_columns, test.explicit_columns);
        EXPECT_NE(classification.object_kind, StoredObjectKind::Unclassified);
    }

    {
        const auto ast = parseCreate(
            "ATTACH TABLE app.t UUID '00000000-0000-0001-0000-000000000002' "
            "(id UInt64) ENGINE = Memory");
        const auto classification = classifyStoredObjectCreateQuery(asCreate(ast));
        EXPECT_EQ(classification.source_mode, StoredObjectSourceMode::AttachMetadata);
        EXPECT_FALSE(classification.has_explicit_destination_columns);
    }

    {
        const auto ast = parseCreate("CREATE TABLE app.t (id UInt64) ENGINE = Memory");
        auto & create = asCreate(ast);
        create.markUDTDialectAdapterPhysicalOnly();
        const auto classification = classifyStoredObjectCreateQuery(create);
        EXPECT_EQ(classification.source_mode, StoredObjectSourceMode::DialectLike);
        EXPECT_TRUE(classification.has_explicit_destination_columns);
    }
}

TEST(UDTTypeStringInventory, ViewMaterializedViewAndDictionaryRoutesMatchTheirDurableSurfaces)
{
    struct Case
    {
        std::string_view query;
        StoredObjectKind kind;
        StoredObjectCreatePreparationRoute route;
        StoredObjectOccurrenceSite site;
        bool has_external_target = false;
        bool populate = false;
    };
    static constexpr std::array cases{
        Case{
            "CREATE VIEW app.v (id app.UserId) AS SELECT toUInt64(1) AS id",
            StoredObjectKind::View,
            StoredObjectCreatePreparationRoute::PrepareViewExplicitOutputs,
            StoredObjectOccurrenceSite::ViewOutputDeclaration},
        Case{
            "CREATE VIEW app.v AS SELECT CAST(1 AS app.UserId) AS id",
            StoredObjectKind::View,
            StoredObjectCreatePreparationRoute::PrepareViewSelectedOutputs,
            StoredObjectOccurrenceSite::ViewOutputDeclaration},
        Case{
            "CREATE MATERIALIZED VIEW app.mv TO app.dst (id app.UserId) AS SELECT toUInt64(1) AS id",
            StoredObjectKind::MaterializedView,
            StoredObjectCreatePreparationRoute::PrepareMaterializedViewExplicitOutputs,
            StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration,
            true},
        Case{
            "CREATE MATERIALIZED VIEW app.mv (id app.UserId) ENGINE = MergeTree ORDER BY id POPULATE AS SELECT toUInt64(1) AS id",
            StoredObjectKind::MaterializedView,
            StoredObjectCreatePreparationRoute::PrepareMaterializedViewExplicitOutputs,
            StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration,
            false,
            true},
        Case{
            "CREATE MATERIALIZED VIEW app.mv TO app.dst AS SELECT CAST(1 AS app.UserId) AS id",
            StoredObjectKind::MaterializedView,
            StoredObjectCreatePreparationRoute::PrepareMaterializedViewSelectedOutputs,
            StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration,
            true},
        Case{
            "CREATE DICTIONARY app.d (id app.UserId, value String) PRIMARY KEY id "
            "SOURCE(CLICKHOUSE(TABLE 'source')) LIFETIME(0) LAYOUT(FLAT())",
            StoredObjectKind::Dictionary,
            StoredObjectCreatePreparationRoute::PrepareDictionaryAttributes,
            StoredObjectOccurrenceSite::DictionaryAttribute},
    };

    for (const auto & test : cases)
    {
        SCOPED_TRACE(test.query);
        const auto ast = parseCreate(String(test.query));
        const auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        EXPECT_EQ(classification.object_kind, test.kind);
        const auto decision = classifyStoredObjectCreatePreparation(create, classification, true);
        EXPECT_EQ(decision.route, test.route);
        EXPECT_EQ(decision.occurrence_site, test.site);
        EXPECT_EQ(decision.rejection, StoredObjectAdmissionRejection::None);
        EXPECT_EQ(create.targets && create.targets->hasTableID(ViewTarget::To), test.has_external_target);
        EXPECT_EQ(create.is_populate, test.populate);
    }
}

TEST(UDTTypeStringInventory, FreshnessAndUnknownChildrenCannotReuseAnEarlierClassification)
{
    const auto ast = parseCreate("CREATE VIEW app.v (id app.UserId) AS SELECT toUInt64(1) AS id");
    auto & create = asCreate(ast);
    const auto initial = classifyStoredObjectCreateQuery(create);
    ASSERT_EQ(
        classifyStoredObjectCreatePreparation(create, initial, true).route, StoredObjectCreatePreparationRoute::PrepareViewExplicitOutputs);

    create.if_not_exists = true;
    const auto stale_freshness = classifyStoredObjectCreatePreparation(create, initial, true);
    EXPECT_EQ(stale_freshness.route, StoredObjectCreatePreparationRoute::Unsupported);
    EXPECT_EQ(stale_freshness.rejection, StoredObjectAdmissionRejection::InvalidProvenanceSource);
    create.if_not_exists = false;

    create.children.push_back(make_intrusive<ASTIdentifier>("unexpected_metadata"));
    const auto malformed = classifyStoredObjectCreateQuery(create);
    EXPECT_FALSE(malformed.structured_udt_scan_complete);
    EXPECT_EQ(classifyStoredObjectCreatePreparation(create, malformed, true).route, StoredObjectCreatePreparationRoute::Unsupported);
}

TEST(UDTTypeStringInventory, ExplicitLogicalColumnsOwnProvenanceOverTransientSelect)
{
    const auto ast = parseCreate(
        "CREATE TABLE app.t (id app.UserId) ENGINE = Memory "
        "AS SELECT toUInt64(1) AS id");
    auto & create = asCreate(ast);
    const auto classification = classifyStoredObjectCreateQuery(create);

    EXPECT_EQ(classification.object_kind, StoredObjectKind::Table);
    EXPECT_EQ(classification.source_mode, StoredObjectSourceMode::AsSelect);
    EXPECT_TRUE(classification.has_explicit_destination_columns);
    EXPECT_TRUE(classification.hasStructuredUDTReference(StoredObjectOccurrenceSite::TableColumnDeclaration));
    EXPECT_FALSE(classification.source_query_has_structured_udt_reference);

    const auto decision = classifyStoredObjectCreatePreparation(create, classification, true);
    EXPECT_EQ(decision.route, StoredObjectCreatePreparationRoute::TableExplicitColumns);
    EXPECT_EQ(decision.rejection, StoredObjectAdmissionRejection::None);

    create.if_not_exists = true;
    const auto non_fresh_decision = classifyStoredObjectCreatePreparation(create, classification, true);
    EXPECT_EQ(non_fresh_decision.route, StoredObjectCreatePreparationRoute::Unsupported);
    EXPECT_EQ(non_fresh_decision.rejection, StoredObjectAdmissionRejection::InvalidProvenanceSource);
}

TEST(UDTTypeStringInventory, TableFunctionSchemaRouteDependsOnTheDurableOwner)
{
    struct Case
    {
        std::string_view query;
        StoredObjectCreatePreparationRoute route;
        StoredObjectOccurrenceSite occurrence_site;
    };
    static constexpr std::array cases{
        Case{
            "CREATE TABLE app.t AS `null`('id app.UserId')",
            StoredObjectCreatePreparationRoute::PhysicalizeTableFunctionSchema,
            StoredObjectOccurrenceSite::TableFunctionSchemaString},
        Case{
            "CREATE TABLE app.t (id UInt64) AS `null`('source app.UserId')",
            StoredObjectCreatePreparationRoute::PhysicalOnly,
            StoredObjectOccurrenceSite::Unclassified},
        Case{
            "CREATE VIEW app.v AS SELECT * FROM loop(`null`('id app.UserId'))",
            StoredObjectCreatePreparationRoute::PrepareViewSelectedOutputs,
            StoredObjectOccurrenceSite::ViewOutputDeclaration},
    };

    for (const auto & test : cases)
    {
        SCOPED_TRACE(test.query);
        const auto ast = parseCreate(String(test.query));
        const auto & create = asCreate(ast);
        const auto classification = classifyStoredObjectCreateQuery(create);
        ASSERT_TRUE(classification.hasQualifiedTypeReferenceCandidate(StoredObjectOccurrenceSite::TableFunctionSchemaString));
        const auto decision = classifyStoredObjectCreatePreparation(create, classification, true);
        EXPECT_EQ(decision.route, test.route);
        EXPECT_EQ(decision.occurrence_site, test.occurrence_site);
        EXPECT_EQ(decision.rejection, StoredObjectAdmissionRejection::None);
    }
}

TEST(UDTTypeStringInventory, ExecutionBoundarySeesQualifiedSchemaAtNestedLeaf)
{
    const UDTExecutionBoundaryOptions options{
        .allow_experimental_analyzer = true,
        .allow_experimental_user_defined_types = false,
    };

    auto query = parseStatement("SELECT * FROM loop(`null`('id app.UserId'))");
    auto proof = validateUDTExecutionBoundaryAndSize(query, 1'000'000, options);
    EXPECT_TRUE(proof.hasPotentialUDTSemanticSinkCandidate());

    auto physical_query = parseStatement("SELECT * FROM loop(`null`('id UInt64'))");
    auto physical_proof = validateUDTExecutionBoundaryAndSize(physical_query, 1'000'000, options);
    EXPECT_FALSE(physical_proof.hasPotentialUDTSemanticSinkCandidate());
}

TEST(UDTTypeStringInventory, UnlimitedCacheInspectionCollectsOnlyAnalyzerRelevantCandidates)
{
    const UDTExecutionBoundaryOptions inspected_options{
        .allow_experimental_analyzer = true,
        .allow_experimental_user_defined_types = false,
        .inspect_query_result_cache_candidates = true,
    };

    auto query = parseStatement(
        "SELECT equals(1, 1), 1 IN (1), has([1], 1) "
        "FROM loop(`null`('id app.UserId')) SETTINGS use_query_cache = 1");
    auto proof = validateUDTExecutionBoundary(query, inspected_options);
    EXPECT_TRUE(proof.hasPotentialUDTSemanticSinkCandidate());
    EXPECT_TRUE(proof.hasPotentialStorageReference());
    EXPECT_TRUE(proof.hasObservedStorageReference());
    EXPECT_TRUE(proof.hasPotentialQueryResultCacheUse());
    EXPECT_EQ(
        proof.getPotentialQueryResultCacheContextualSinkCandidates(),
        queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::Equality)
            | queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::In)
            | queryResultCacheContextualSinkCandidateBit(QueryResultCacheContextualSinkCandidate::Has));

    auto uninspected_query = parseStatement("SELECT * FROM loop(`null`('id app.UserId'))");
    auto uninspected_proof = validateUDTExecutionBoundary(
        uninspected_query,
        {
            .allow_experimental_analyzer = true,
            .allow_experimental_user_defined_types = false,
            .inspect_query_result_cache_candidates = false,
        });
    EXPECT_FALSE(uninspected_proof.hasPotentialUDTSemanticSinkCandidate());
    EXPECT_FALSE(uninspected_proof.hasPotentialStorageReference());
    EXPECT_FALSE(uninspected_proof.hasObservedStorageReference());
    EXPECT_FALSE(uninspected_proof.hasPotentialQueryResultCacheUse());
    EXPECT_EQ(uninspected_proof.getPotentialQueryResultCacheContextualSinkCandidates(), 0);

    auto physical_query = parseStatement("SELECT * FROM loop(`null`('id UInt64'))");
    auto physical_proof = validateUDTExecutionBoundary(physical_query, inspected_options);
    EXPECT_FALSE(physical_proof.hasPotentialUDTSemanticSinkCandidate());
    EXPECT_TRUE(physical_proof.hasPotentialStorageReference());
    EXPECT_TRUE(physical_proof.hasObservedStorageReference());
}

}
