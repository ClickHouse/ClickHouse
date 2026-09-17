#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterUDTQuery.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/UDT/UDTExecutionBoundary.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/typeid_cast.h>

#include <gtest/gtest.h>

#include <array>
#include <string_view>
#include <utility>


namespace DB
{
void registerInterpreterUDTQuery(InterpreterFactory & factory);
}

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
extern const int TOO_BIG_AST;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_TYPE_OF_QUERY;
}

namespace DB::Setting
{
extern const SettingsBool allow_experimental_analyzer;
extern const SettingsBool allow_experimental_user_defined_types;
}

namespace
{
using namespace DB;

ASTPtr parseStatement(const String & text)
{
    ParserQuery parser(text.data() + text.size());
    return parseQuery(parser, text, "user-defined type execution gate test", 0, 150, 0);
}

ASTPtr parseExpression(const String & text)
{
    ParserExpression parser;
    return parseQuery(parser, text, "user-defined type CAST execution gate test", 0, 150, 0);
}

InterpreterFactory::InterpreterPtr getValidatedInterpreter(InterpreterFactory & factory, ASTPtr & query, const ContextMutablePtr & context)
{
    const auto & settings = context->getSettingsRef();
    auto boundary = UDT::validateUDTExecutionBoundary(
        query,
        {
            .allow_experimental_analyzer = static_cast<bool>(settings[Setting::allow_experimental_analyzer]),
            .allow_experimental_user_defined_types = static_cast<bool>(settings[Setting::allow_experimental_user_defined_types]),
        });
    return factory.get(query, context, std::move(boundary));
}

void analyzeWithQueryTree(const String & expression_text, const ContextPtr & context)
{
    auto query_tree = buildQueryTree(parseExpression(expression_text), context);
    QueryTreePassManager pass_manager(context);
    addQueryTreePasses(pass_manager, true);
    pass_manager.runOnlyResolve(query_tree);
}

void analyzeWithExpressionAnalyzer(const String & expression_text, const ContextPtr & context)
{
    ASTPtr expression = parseExpression(expression_text);
    auto syntax = TreeRewriter(context).analyze(expression, NamesAndTypesList{});
    ExpressionAnalyzer analyzer(expression, syntax, context);
    static_cast<void>(analyzer.getActionsDAG(false));
}

template <typename Callback>
void expectExceptionCode(Callback && callback, int expected_code, std::string_view source)
{
    try
    {
        callback();
        FAIL() << "Expected exception for: " << source;
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), expected_code) << source << ": " << exception.message();
    }
}

template <typename Callback>
void expectSupportIsDisabled(Callback && callback, std::string_view source)
{
    expectExceptionCode(std::forward<Callback>(callback), ErrorCodes::SUPPORT_IS_DISABLED, source);
}

}

TEST(UDTFeatureGate, EveryStatementRoutesToTheSideEffectFreeInterpreter)
{
    static constexpr std::array queries{
        std::string_view{"CREATE TYPE __udt_missing_db__.Recursive(T TYPE, N UInt16) "
                         "ON CLUSTER __udt_missing_cluster__ DECREASES N "
                         "AS TYPE_IF(N = 0, T, __udt_missing_db__.Recursive(T, N - 1))"},
        std::string_view{"ATTACH TYPE __udt_missing_db__.Attached "
                         "UUID '11111111-1111-1111-1111-111111111111' REVISION 1 "
                         "ON CLUSTER __udt_missing_cluster__ AS UInt64 "
                         "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'"},
        std::string_view{"DROP TYPE IF EXISTS __udt_missing_db__.Dropped "
                         "ON CLUSTER __udt_missing_cluster__ RESTRICT"},
        std::string_view{"ALTER TYPE IF EXISTS __udt_missing_db__.OldName "
                         "ON CLUSTER __udt_missing_cluster__ RENAME TO NewName"},
        std::string_view{"ALTER TYPE IF EXISTS __udt_missing_db__.OldName "
                         "ON CLUSTER __udt_missing_cluster__ COMMENT 'must not persist'"},
        std::string_view{"SHOW TYPES FROM __udt_missing_db__ LIKE '%Id' FORMAT JSONEachRow"},
        std::string_view{"SHOW CREATE TYPE __udt_missing_db__.Missing "
                         "INTO OUTFILE '/__udt_must_not_be_created__.sql'"},
        std::string_view{"DESCRIBE TYPE __udt_missing_db__.Missing "
                         "SETTINGS output_format_json_quote_64bit_integers = 0"},
        std::string_view{"PHYSICALIZE TYPE REFERENCES OBJECT TABLE __udt_missing_db__.missing_table "
                         "ON CLUSTER __udt_missing_cluster__ DROP UNUSED TYPES DRY RUN FORMAT JSONEachRow"},
        std::string_view{"PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'udt-test-token'"},
    };

    InterpreterFactory factory;
    registerInterpreterUDTQuery(factory);
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_analyzer", Field{UInt64{0}});
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{0}});

    for (const auto query_text : queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr query = parseStatement(String{query_text});
        ASSERT_NE(query, nullptr);

        auto interpreter = getValidatedInterpreter(factory, query, context);
        ASSERT_NE(interpreter, nullptr);
        ASSERT_NE(typeid_cast<InterpreterUDTQuery *>(interpreter.get()), nullptr);

        expectSupportIsDisabled([&] { static_cast<void>(interpreter->execute()); }, query_text);
    }
}

TEST(UDTFeatureGate, EnabledStructuredCastRoutesOnlyThroughTheSupportedAnalyzer)
{
    tryRegisterFunctions();
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_user_defined_types", 1);
    static constexpr std::array expressions{
        std::string_view{"CAST(1 AS Array(__udt_missing_db__.MissingType))"},
        std::string_view{"CAST(1, 'Array(__udt_missing_db__.MissingType)')"},
    };
    for (const auto expression_text : expressions)
    {
        SCOPED_TRACE(expression_text);
        {
            expectExceptionCode(
                [&] { analyzeWithQueryTree(String{expression_text}, context); }, ErrorCodes::UNKNOWN_DATABASE, "query-tree analyzer");
        }

        expectSupportIsDisabled([&] { analyzeWithExpressionAnalyzer(String{expression_text}, context); }, "ExpressionAnalyzer");
    }
}

TEST(UDTFeatureGate, NonConstantCastTargetsStopAtBothAnalyzerBoundaries)
{
    tryRegisterFunctions();
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_user_defined_types", 1);
    static constexpr std::array expressions{
        std::string_view{"CAST(1, materialize('app.MissingType'))"},
        std::string_view{"accurateCast(1, materialize('app.MissingType'))"},
        std::string_view{"accurateCastOrNull(1, materialize('app.MissingType'))"},
        std::string_view{"accurateCastOrDefault(1, materialize('app.MissingType'), 0)"},
        std::string_view{"CAST(1, 42)"},
    };

    for (const auto expression_text : expressions)
    {
        SCOPED_TRACE(expression_text);
        expectExceptionCode(
            [&] { analyzeWithQueryTree(String{expression_text}, context); }, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "query-tree analyzer");
        expectExceptionCode(
            [&] { analyzeWithExpressionAnalyzer(String{expression_text}, context); },
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "ExpressionAnalyzer");
    }
}

TEST(UDTFeatureGate, QualifiedBuiltInFamiliesAndAliasesAreRejectedBeforeTheGenericGate)
{
    tryRegisterFunctions();
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_user_defined_types", 1);

    {
        InterpreterFactory factory;
        registerInterpreterUDTQuery(factory);
        ASTPtr query = parseStatement("CREATE TYPE app.uInT64 AS UInt8");
        auto interpreter = getValidatedInterpreter(factory, query, context);
        ASSERT_NE(interpreter, nullptr);
        expectExceptionCode([&] { static_cast<void>(interpreter->execute()); }, ErrorCodes::BAD_ARGUMENTS, "CREATE TYPE");
    }

    static constexpr std::array expressions{
        std::string_view{"CAST(1 AS app.vArChAr)"},
        std::string_view{"CAST(1, 'app.vArChAr')"},
    };
    for (const auto expression_text : expressions)
    {
        SCOPED_TRACE(expression_text);
        {
            expectExceptionCode(
                [&] { analyzeWithQueryTree(String{expression_text}, context); }, ErrorCodes::BAD_ARGUMENTS, expression_text);
        }

        expectExceptionCode(
            [&] { analyzeWithExpressionAnalyzer(String{expression_text}, context); }, ErrorCodes::BAD_ARGUMENTS, expression_text);
    }
}

TEST(UDTFeatureGate, PersistedOrDispatchedExpressionsStopAtFactoryRouting)
{
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_user_defined_types", 1);
    InterpreterFactory factory;

    static constexpr std::array queries{
        std::string_view{"CREATE FUNCTION udt_structured_guard ON CLUSTER missing_cluster AS x -> CAST(x AS app.MissingType)"},
        std::string_view{"CREATE FUNCTION udt_string_guard AS x -> CAST(x, 'app.MissingType')"},
        std::string_view{"CREATE FUNCTION udt_accurate_guard AS x -> accurateCast(x, 'app.MissingType')"},
        std::string_view{"CREATE FUNCTION udt_accurate_null_guard AS x -> accurateCastOrNull(x, 'app.MissingType')"},
        std::string_view{"CREATE FUNCTION udt_accurate_default_guard AS x -> accurateCastOrDefault(x, 'app.MissingType', 0)"},
        std::string_view{"CREATE TABLE udt_default_guard (x UInt64 DEFAULT CAST(1 AS app.MissingType)) ENGINE = Memory"},
        std::string_view{"CREATE TABLE udt_ttl_guard (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x "
                         "TTL d GROUP BY CAST(x AS app.MissingType) SET x = x"},
        std::string_view{"CREATE TABLE udt_ttl_assignment_guard (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x "
                         "TTL d GROUP BY x SET x = CAST(x AS app.MissingType)"},
        std::string_view{"CREATE TABLE udt_ttl_recompression_guard (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x "
                         "TTL d RECOMPRESS CODEC(CAST(1, 'app.MissingType'))"},
        std::string_view{"CREATE ROW POLICY udt_detached_guard ON db.t USING CAST(1 AS app.MissingType) TO ALL"},
        std::string_view{"CREATE MASKING POLICY udt_mask_update_guard ON db.t "
                         "UPDATE x = CAST(1 AS app.MissingType) TO ALL"},
        std::string_view{"CREATE MASKING POLICY udt_mask_where_guard ON db.t "
                         "UPDATE x = x WHERE CAST(1, 'app.MissingType') TO ALL"},
        std::string_view{"BACKUP TABLE db.t PARTITION CAST(1 AS app.MissingType) TO File('/udt-must-not-start')"},
        std::string_view{"BACKUP TABLE db.t TO File('/udt-must-not-start') "
                         "SETTINGS cluster_host_ids = [CAST(1 AS app.MissingType)]"},
    };

    for (const auto query_text : queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr query = parseStatement(String{query_text});
        expectSupportIsDisabled([&] { static_cast<void>(getValidatedInterpreter(factory, query, context)); }, query_text);
    }

    static constexpr std::array collision_queries{
        std::string_view{"CREATE FUNCTION udt_collision_guard AS x -> CAST(x AS app.uInT64)"},
        std::string_view{"CREATE FUNCTION udt_string_collision_guard AS x -> CAST(x, 'app.uInT64')"},
        std::string_view{"CREATE MASKING POLICY udt_mask_update_collision_guard ON db.t "
                         "UPDATE x = CAST(1, 'app.uInT64') TO ALL"},
        std::string_view{"CREATE MASKING POLICY udt_mask_where_collision_guard ON db.t "
                         "UPDATE x = x WHERE CAST(1 AS app.uInT64) TO ALL"},
        std::string_view{"CREATE TABLE udt_ttl_assignment_collision_guard (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d GROUP BY x SET x = CAST(x, 'app.uInT64')"},
        std::string_view{"CREATE TABLE udt_ttl_recompression_collision_guard (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d RECOMPRESS CODEC(CAST(1 AS app.uInT64))"},
    };

    for (const auto query_text : collision_queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr collision = parseStatement(String{query_text});
        expectExceptionCode(
            [&] { static_cast<void>(getValidatedInterpreter(factory, collision, context)); },
            ErrorCodes::BAD_ARGUMENTS,
            "stored built-in collision");
    }
}

TEST(UDTFeatureGate, ExecutionOutputSettingsAreNotTreatedAsPersistedTypeStrings)
{
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = true,
    };

    const auto validate = [&](const String & query_text)
    {
        auto query = parseStatement(query_text);
        static_cast<void>(UDT::validateUDTExecutionBoundary(query, options));
    };
    const auto validate_with_size = [&](const String & query_text)
    {
        auto query = parseStatement(query_text);
        static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(query, 10'000, options));
    };

    const String transient
        = "DESCRIBE TABLE numbers(1) SETTINGS schema_inference_hints = 'id app.UserId'";
    EXPECT_NO_THROW(validate(transient));
    EXPECT_NO_THROW(validate_with_size(transient));

    static constexpr std::array persisted_or_session_settings{
        std::string_view{"SET schema_inference_hints = 'id app.UserId'"},
        std::string_view{"CREATE VIEW app.v AS SELECT 1 SETTINGS schema_inference_hints = 'id app.UserId'"},
        std::string_view{"ALTER TABLE app.mv MODIFY QUERY SELECT 1 SETTINGS schema_inference_hints = 'id app.UserId'"},
    };
    for (const auto query_text : persisted_or_session_settings)
    {
        SCOPED_TRACE(query_text);
        expectSupportIsDisabled([&] { validate(String{query_text}); }, query_text);
        expectSupportIsDisabled([&] { validate_with_size(String{query_text}); }, query_text);
    }
}

TEST(UDTFeatureGate, NonConstantPersistedCastTargetsStopBeforeInterpreterRouting)
{
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_user_defined_types", 1);
    InterpreterFactory factory;

    static constexpr std::array queries{
        std::string_view{"CREATE FUNCTION udt_dynamic_cluster_guard ON CLUSTER missing_cluster "
                         "AS x -> CAST(x, materialize('app.MissingType'))"},
        std::string_view{"CREATE FUNCTION udt_dynamic_store_guard "
                         "AS x -> CAST(x, materialize('app.MissingType'))"},
        std::string_view{"CREATE FUNCTION udt_dynamic_accurate_guard "
                         "AS x -> accurateCast(x, materialize('app.MissingType'))"},
        std::string_view{"CREATE FUNCTION udt_dynamic_accurate_null_guard "
                         "AS x -> accurateCastOrNull(x, materialize('app.MissingType'))"},
        std::string_view{"CREATE FUNCTION udt_dynamic_accurate_default_guard "
                         "AS x -> accurateCastOrDefault(x, materialize('app.MissingType'), 0)"},
        std::string_view{"CREATE FUNCTION udt_non_string_guard AS x -> CAST(x, 42)"},
    };

    for (const auto query_text : queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr query = parseStatement(String{query_text});
        expectExceptionCode(
            [&] { static_cast<void>(getValidatedInterpreter(factory, query, context)); }, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, query_text);
    }
}

TEST(UDTFeatureGate, DisabledFeaturePreservesDeferredNonConstantCastValidation)
{
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_analyzer", Field{UInt64{0}});
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{0}});
    InterpreterFactory factory;
    ASTPtr query = parseStatement(
        "CREATE FUNCTION udt_disabled_dynamic_guard ON CLUSTER missing_cluster "
        "AS x -> CAST(x, materialize('app.MissingType'))");

    expectExceptionCode(
        [&] { static_cast<void>(getValidatedInterpreter(factory, query, context)); },
        ErrorCodes::UNKNOWN_TYPE_OF_QUERY,
        "disabled feature retains deferred validation");
}

TEST(UDTFeatureGate, ExecutionBoundaryProofCannotBeReusedForAnotherRootOrSettings)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
#endif

    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_analyzer", Field{UInt64{0}});
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{0}});

    ASTPtr validated_query = parseStatement("CREATE FUNCTION udt_proof_guard AS x -> x");
    ASTPtr different_query = parseStatement("CREATE FUNCTION udt_other_proof_guard AS x -> x");
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = false,
    };

    InterpreterFactory factory;
    auto different_root_proof = UDT::validateUDTExecutionBoundary(validated_query, options);
#ifdef DEBUG_OR_SANITIZER_BUILD
    EXPECT_DEATH(
        static_cast<void>(factory.get(different_query, context, std::move(different_root_proof))),
        "UDT execution-boundary proof belongs to a different AST root");
#else
    expectExceptionCode(
        [&] { static_cast<void>(factory.get(different_query, context, std::move(different_root_proof))); },
        ErrorCodes::LOGICAL_ERROR,
        "execution-boundary proof reused for another AST root");
#endif

    auto stale_settings_proof = UDT::validateUDTExecutionBoundary(validated_query, options);
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{1}});
#ifdef DEBUG_OR_SANITIZER_BUILD
    EXPECT_DEATH(
        static_cast<void>(factory.get(validated_query, context, std::move(stale_settings_proof))),
        "UDT execution-boundary settings changed before interpreter dispatch");
#else
    expectExceptionCode(
        [&] { static_cast<void>(factory.get(validated_query, context, std::move(stale_settings_proof))); },
        ErrorCodes::LOGICAL_ERROR,
        "execution-boundary proof reused after settings changed");
#endif
}

TEST(UDTFeatureGate, ExecutionBoundaryProofIsConsumedByInterpreterDispatch)
{
    auto context = Context::createCopy(getContext().context);
    context->setSetting("allow_experimental_analyzer", Field{UInt64{0}});
    context->setSetting("allow_experimental_user_defined_types", Field{UInt64{0}});

    ASTPtr query = parseStatement("CREATE FUNCTION udt_one_shot_proof_guard AS x -> x");
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = false,
    };

    InterpreterFactory factory;
    factory.registerInterpreter(
        "InterpreterCreateFunctionQuery", [](const InterpreterFactory::Arguments &) -> InterpreterFactory::InterpreterPtr { return {}; });

    auto proof = UDT::validateUDTExecutionBoundary(query, options);
    EXPECT_NO_THROW(static_cast<void>(factory.get(query, context, std::move(proof))));
#ifdef DEBUG_OR_SANITIZER_BUILD
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(
        static_cast<void>(factory.get(query, context, std::move(proof))),
        "UDT execution-boundary proof belongs to a different AST root");
#else
    expectExceptionCode(
        [&] { static_cast<void>(factory.get(query, context, std::move(proof))); },
        ErrorCodes::LOGICAL_ERROR,
        "execution-boundary proof consumed more than once");
#endif
}

TEST(UDTFeatureGate, CombinedExecutionBoundaryPreservesASTSizeErrorPriority)
{
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = true,
    };
    static constexpr std::array queries{
        std::string_view{"CREATE FUNCTION udt_size_priority_early_guard AS x -> "
                         "tuple(CAST(x AS app.MissingType), plus(x, 1), plus(x, 2))"},
        std::string_view{"CREATE FUNCTION udt_size_priority_late_guard AS x -> "
                         "tuple(plus(x, 1), plus(x, 2), CAST(x AS app.MissingType))"},
    };

    for (const auto query_text : queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr oversized_query = parseStatement(String{query_text});
        expectExceptionCode(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(oversized_query, 1, options)); },
            ErrorCodes::TOO_BIG_AST,
            "AST size failure precedes an unresolved UDT CAST");

        ASTPtr udt_query = parseStatement(String{query_text});
        expectSupportIsDisabled(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(udt_query, 1'000'000, options)); },
            "fixture retains its unresolved UDT CAST under the normal size limit");
    }
}

TEST(UDTFeatureGate, CombinedExecutionBoundaryValidatesHiddenExecutionFields)
{
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = true,
    };
    static constexpr std::array queries{
        std::string_view{"CREATE ROW POLICY udt_hidden_size_row_guard ON db.t USING CAST(1 AS app.MissingType) TO ALL"},
        std::string_view{"CREATE MASKING POLICY udt_hidden_size_mask_update_guard ON db.t "
                         "UPDATE x = CAST(1 AS app.MissingType) TO ALL"},
        std::string_view{"CREATE MASKING POLICY udt_hidden_size_mask_where_guard ON db.t "
                         "UPDATE x = x WHERE CAST(1, 'app.MissingType') TO ALL"},
        std::string_view{"BACKUP TABLE db.t PARTITION CAST(1 AS app.MissingType) TO File('/udt-size-must-not-start')"},
        std::string_view{"BACKUP TABLE db.t TO File('/udt-size-must-not-start') "
                         "SETTINGS cluster_host_ids = [CAST(1 AS app.MissingType)]"},
        std::string_view{"CREATE TABLE udt_hidden_size_ttl_group (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d GROUP BY CAST(x AS app.MissingType) SET x = x"},
        std::string_view{"CREATE TABLE udt_hidden_size_ttl_assignment (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d GROUP BY x SET x = CAST(x AS app.MissingType)"},
        std::string_view{"CREATE TABLE udt_hidden_size_ttl_recompression (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d RECOMPRESS CODEC(CAST(1, 'app.MissingType'))"},
        std::string_view{"ALTER TABLE db.t MODIFY TTL d GROUP BY x SET x = CAST(x AS app.MissingType)"},
    };

    for (const auto query_text : queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr oversized_query = parseStatement(String{query_text});
        const size_t visible_size = oversized_query->checkSize(1'000'000);
        expectExceptionCode(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(oversized_query, visible_size, options)); },
            ErrorCodes::TOO_BIG_AST,
            "hidden execution field consumes the common size budget before its UDT error");

        ASTPtr query = parseStatement(String{query_text});
        expectSupportIsDisabled(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(query, 1'000'000, options)); },
            "hidden execution field under combined size validation");
    }
}

TEST(UDTFeatureGate, CombinedExecutionBoundaryCountsValidHiddenExecutionFields)
{
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = true,
    };
    static constexpr std::array queries{
        std::string_view{"CREATE ROW POLICY udt_hidden_budget_row_guard ON db.t USING plus(1, 1) TO ALL"},
        std::string_view{"CREATE MASKING POLICY udt_hidden_budget_mask_update_guard ON db.t "
                         "UPDATE x = plus(x, 1) TO ALL"},
        std::string_view{"CREATE MASKING POLICY udt_hidden_budget_mask_where_guard ON db.t "
                         "UPDATE x = x WHERE equals(x, 1) TO ALL"},
        std::string_view{"BACKUP TABLE db.t PARTITION tuple(1, 2) TO File('/udt-size-must-not-start')"},
        std::string_view{"BACKUP TABLE db.t TO File('/udt-size-must-not-start') "
                         "SETTINGS cluster_host_ids = [1, 2]"},
        std::string_view{"CREATE TABLE udt_hidden_budget_ttl_group (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d GROUP BY plus(x, 1) SET x = x"},
        std::string_view{"CREATE TABLE udt_hidden_budget_ttl_assignment (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d GROUP BY x SET x = plus(x, 1)"},
        std::string_view{"CREATE TABLE udt_hidden_budget_ttl_recompression (d DateTime, x UInt64) "
                         "ENGINE = MergeTree ORDER BY x TTL d RECOMPRESS CODEC(CAST(1, 'UInt64'))"},
        std::string_view{"ALTER TABLE db.t MODIFY TTL d GROUP BY x SET x = plus(x, 1)"},
    };

    for (const auto query_text : queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr oversized_query = parseStatement(String{query_text});
        const size_t visible_size = oversized_query->checkSize(1'000'000);
        expectExceptionCode(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(oversized_query, visible_size, options)); },
            ErrorCodes::TOO_BIG_AST,
            "valid hidden execution field consumes the common size budget");

        ASTPtr accepted_query = parseStatement(String{query_text});
        EXPECT_NO_THROW({
            auto proof = UDT::validateUDTExecutionBoundaryAndSize(accepted_query, 1'000'000, options);
            static_cast<void>(proof);
        });
    }
}

TEST(UDTFeatureGate, CombinedExecutionBoundaryDoesNotDoubleCountSpecialFieldsInChildren)
{
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = true,
    };
    ASTPtr query = parseStatement("BACKUP TABLE db.t TO File('/udt-size-must-not-start') SETTINGS cluster_host_ids = [1, 2]");
    auto * backup = query->as<ASTBackupQuery>();
    ASSERT_NE(backup, nullptr);
    ASSERT_NE(backup->cluster_host_ids, nullptr);

    /// `clickhouse_json` deserialization exposes this special field as a
    /// regular child, unlike the SQL parser. Model that shape directly.
    query->children.push_back(backup->cluster_host_ids);
    const size_t visible_size = query->checkSize(1'000'000);
    EXPECT_NO_THROW({
        auto proof = UDT::validateUDTExecutionBoundaryAndSize(query, visible_size, options);
        static_cast<void>(proof);
    });
}

TEST(UDTFeatureGate, ExecutionBoundaryPreservesRepeatedStringTargetSemantics)
{
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = true,
    };
    static constexpr std::array deferred_queries{
        std::string_view{R"(CREATE FUNCTION udt_dotted_enum_guard AS x -> tuple(
            CAST(x, 'Enum8(\'dotted.value\' = 1)'), CAST(x, 'Enum8(\'dotted.value\' = 1)')))"},
        std::string_view{R"(CREATE FUNCTION udt_malformed_dotted_guard AS x -> tuple(
            CAST(x, 'app.'), CAST(x, 'app.')))"},
        std::string_view{R"(CREATE FUNCTION udt_deferred_syntax_guard AS x -> tuple(
            CAST(x, 'UInt64 /* app.Type'), CAST(x, 'UInt64 /* app.Type')))"},
    };

    for (const auto query_text : deferred_queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr query = parseStatement(String{query_text});
        EXPECT_NO_THROW({
            auto proof = UDT::validateUDTExecutionBoundaryAndSize(query, 1'000'000, options);
            static_cast<void>(proof);
        });
    }

    ASTPtr missing_query = parseStatement(
        "CREATE FUNCTION udt_repeated_missing_guard AS x -> "
        "tuple(CAST(x, 'app.MissingType'), CAST(x, 'app.MissingType'))");
    expectSupportIsDisabled(
        [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(missing_query, 1'000'000, options)); },
        "repeated qualified missing target");

    ASTPtr collision_query = parseStatement(
        "CREATE FUNCTION udt_repeated_collision_guard AS x -> "
        "tuple(CAST(x, 'app.uInT64'), CAST(x, 'app.uInT64'))");
    expectExceptionCode(
        [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(collision_query, 1'000'000, options)); },
        ErrorCodes::BAD_ARGUMENTS,
        "repeated qualified built-in collision");
}

TEST(UDTFeatureGate, StatementContainersValidateNestedExecutionFieldsBeforeDispatch)
{
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = true,
        .allow_experimental_user_defined_types = true,
    };
    static constexpr std::array queries{
        std::string_view{"CREATE ROW POLICY udt_parallel_row_guard ON db.t USING CAST(1 AS app.MissingType) TO ALL "
                         "PARALLEL WITH CREATE FUNCTION udt_parallel_row_peer AS x -> x"},
        std::string_view{"BACKUP TABLE db.t PARTITION CAST(1 AS app.MissingType) TO File('/udt-parallel-must-not-start') "
                         "PARALLEL WITH CREATE FUNCTION udt_parallel_backup_peer AS x -> x"},
        std::string_view{"CREATE TABLE udt_parallel_ttl_guard (d DateTime, x UInt64) ENGINE = MergeTree ORDER BY x "
                         "TTL d GROUP BY x SET x = CAST(x AS app.MissingType) "
                         "PARALLEL WITH CREATE FUNCTION udt_parallel_ttl_peer AS x -> x"},
        std::string_view{"SELECT CAST(1 AS app.MissingType) "
                         "PARALLEL WITH CREATE FUNCTION udt_parallel_select_peer AS x -> x"},
        std::string_view{"EXECUTE AS default CREATE MASKING POLICY udt_execute_as_mask_guard ON db.t "
                         "UPDATE x = x WHERE CAST(1, 'app.MissingType') TO ALL"},
        std::string_view{"EXECUTE AS default ALTER TABLE db.t MODIFY TTL d GROUP BY x "
                         "SET x = CAST(x AS app.MissingType)"},
    };

    for (const auto query_text : queries)
    {
        SCOPED_TRACE(query_text);
        ASTPtr validation_query = parseStatement(String{query_text});
        expectSupportIsDisabled(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundary(validation_query, options)); },
            "nested statement under validation-only boundary");

        ASTPtr combined_query = parseStatement(String{query_text});
        expectSupportIsDisabled(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(combined_query, 1'000'000, options)); },
            "nested statement under combined boundary");

        ASTPtr oversized_query = parseStatement(String{query_text});
        expectExceptionCode(
            [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(oversized_query, 1, options)); },
            ErrorCodes::TOO_BIG_AST,
            "outer AST size failure precedes a nested hidden-field UDT error");
    }
}

TEST(UDTFeatureGate, SelectExecutionBoundaryDefersUDTValidationToTheAnalyzer)
{
    ASTPtr query = parseStatement("SELECT CAST(1 AS app.MissingType)");
    const auto options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = false,
    };

    EXPECT_NO_THROW({
        auto proof = UDT::validateUDTExecutionBoundaryAndSize(query, 1'000'000, options);
        static_cast<void>(proof);
    });
    ASTPtr oversized_query = parseStatement("SELECT CAST(1 AS app.MissingType)");
    expectExceptionCode(
        [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(oversized_query, 1, options)); },
        ErrorCodes::TOO_BIG_AST,
        "SELECT still crosses the AST size boundary");
}

TEST(UDTFeatureGate, AnalyzerOwnedInsertSelectIsValidatedByItsAnalyzer)
{
    ASTPtr query = parseStatement("INSERT INTO db.t SELECT CAST(1 AS app.MissingType)");
    const auto analyzer_options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = true,
        .allow_experimental_user_defined_types = true,
    };

    EXPECT_NO_THROW({
        auto proof = UDT::validateUDTExecutionBoundaryAndSize(query, 1'000'000, analyzer_options);
        static_cast<void>(proof);
    });

    ASTPtr validation_only_query = parseStatement("INSERT INTO db.t SELECT CAST(1 AS app.MissingType)");
    EXPECT_NO_THROW({
        auto proof = UDT::validateUDTExecutionBoundary(validation_only_query, analyzer_options);
        static_cast<void>(proof);
    });

    ASTPtr nested_query = parseStatement(
        "INSERT INTO db.t SELECT CAST(1 AS app.MissingType) "
        "PARALLEL WITH CREATE FUNCTION udt_nested_insert_peer AS x -> x");
    expectSupportIsDisabled(
        [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(nested_query, 1'000'000, analyzer_options)); },
        "nested INSERT SELECT is fail-closed before a sibling can start");

    ASTPtr nested_validation_only_query = parseStatement(
        "INSERT INTO db.t SELECT CAST(1 AS app.MissingType) "
        "PARALLEL WITH CREATE FUNCTION udt_nested_insert_validation_peer AS x -> x");
    expectSupportIsDisabled(
        [&] { static_cast<void>(UDT::validateUDTExecutionBoundary(nested_validation_only_query, analyzer_options)); },
        "validation-only nested INSERT SELECT is fail-closed before a sibling can start");

    ASTPtr oversized_query = parseStatement("INSERT INTO db.t SELECT CAST(1 AS app.MissingType)");
    expectExceptionCode(
        [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(oversized_query, 1, analyzer_options)); },
        ErrorCodes::TOO_BIG_AST,
        "analyzer-owned INSERT SELECT still crosses the AST size boundary");

    const auto legacy_analyzer_options = UDT::UDTExecutionBoundaryOptions{
        .allow_experimental_analyzer = false,
        .allow_experimental_user_defined_types = true,
    };
    ASTPtr legacy_analyzer_query = parseStatement("INSERT INTO db.t SELECT CAST(1 AS app.MissingType)");
    expectSupportIsDisabled(
        [&] { static_cast<void>(UDT::validateUDTExecutionBoundaryAndSize(legacy_analyzer_query, 1'000'000, legacy_analyzer_options)); },
        "INSERT SELECT without analyzer ownership");
}
