#include <Parsers/ASTAlterTypeCommentQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDescribeTypeQuery.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPhysicalizeTypeReferencesQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTRenameTypeQuery.h>
#include <Parsers/ASTShowCreateTypeQuery.h>
#include <Parsers/ASTShowTypesQuery.h>
#include <Parsers/ASTUDTTemplate.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <string_view>
#include <vector>


namespace DB::ErrorCodes
{
extern const int UNSUPPORTED_TYPE_CLAUSE;
extern const int TOO_DEEP_AST;
extern const int TOO_DEEP_RECURSION;
}

namespace
{
using namespace DB;

ASTPtr parseTypeQuery(const String & text, size_t max_parser_depth = 150)
{
    ParserQuery parser(text.data() + text.size());
    return parseQuery(parser, text, "user-defined type query parser test", 0, max_parser_depth, 0);
}

void expectParseFailure(const String & text)
{
    EXPECT_THROW(static_cast<void>(parseTypeQuery(text)), Exception) << text;
}

void expectErrorCode(const String & text, int expected_code)
{
    try
    {
        static_cast<void>(parseTypeQuery(text));
        FAIL() << "Expected query to fail: " << text;
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), expected_code) << text << ": " << exception.message();
    }
}

bool parsesWithinDepth(const String & text, size_t max_parser_depth)
{
    try
    {
        static_cast<void>(parseTypeQuery(text, max_parser_depth));
        return true;
    }
    catch (const Exception & exception)
    {
        if (exception.code() != ErrorCodes::TOO_DEEP_RECURSION && exception.code() != ErrorCodes::TOO_DEEP_AST)
            throw;
        return false;
    }
}

String nestedArrayType(size_t depth)
{
    String result;
    result.reserve(depth * 7 + 5);
    for (size_t index = 0; index < depth; ++index)
        result += "Array(";
    result += "UInt8";
    result.append(depth, ')');
    return result;
}

void expectCloneAndRoundTrip(const String & text)
{
    const auto ast = parseTypeQuery(text);
    ASSERT_NE(ast, nullptr) << text;

    const auto clone = ast->clone();
    ASSERT_NE(clone, nullptr) << text;
    EXPECT_EQ(ast->getTreeHash(false), clone->getTreeHash(false)) << text;
    EXPECT_EQ(ast->formatWithSecretsOneLine(), clone->formatWithSecretsOneLine()) << text;

    const String formatted = ast->formatWithSecretsOneLine();
    const auto reparsed = parseTypeQuery(formatted);
    ASSERT_NE(reparsed, nullptr) << formatted;
    EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false)) << text;
    EXPECT_EQ(formatted, reparsed->formatWithSecretsOneLine()) << text;
}

String literalString(const ASTPtr & ast)
{
    if (const auto * literal = ast ? ast->as<ASTLiteral>() : nullptr; literal && literal->value.getType() == Field::Types::String)
        return literal->value.safeGet<String>();
    return {};
}

}

TEST(UDTQueries, CreateAndAttachExposeStructuredFields)
{
    const auto ast = parseTypeQuery(
        "CREATE TYPE IF NOT EXISTS `analytics.db`.`Nested.Type`(T TYPE, N UInt16) "
        "ON CLUSTER `cluster.name` DECREASES N "
        "AS TYPE_IF(N = 0, T, Tuple(head T, tail `analytics.db`.`Nested.Type`(T, N - 1))) "
        "COMMENT 'owner\\'s external id'");
    const auto * create = ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(create, nullptr);
    EXPECT_FALSE(create->attach);
    EXPECT_TRUE(create->if_not_exists);
    EXPECT_EQ(create->getDatabase(), "analytics.db");
    EXPECT_EQ(create->getTypeName(), "Nested.Type");
    EXPECT_EQ(create->cluster, "cluster.name");
    EXPECT_EQ(literalString(create->comment), "owner's external id");
    EXPECT_FALSE(create->uuid.has_value());
    EXPECT_FALSE(create->revision.has_value());
    EXPECT_FALSE(create->definition_hash.has_value());

    const auto * parameters = create->parameters->as<ASTExpressionList>();
    ASSERT_NE(parameters, nullptr);
    ASSERT_EQ(parameters->children.size(), 2);
    const auto * type_parameter = parameters->children[0]->as<ASTUDTParameterDeclaration>();
    const auto * value_parameter = parameters->children[1]->as<ASTUDTParameterDeclaration>();
    ASSERT_NE(type_parameter, nullptr);
    ASSERT_NE(value_parameter, nullptr);
    EXPECT_EQ(type_parameter->name, "T");
    EXPECT_EQ(type_parameter->ordinal, 0);
    EXPECT_EQ(type_parameter->kind, UDTParameterKind::Type);
    EXPECT_EQ(value_parameter->name, "N");
    EXPECT_EQ(value_parameter->ordinal, 1);
    EXPECT_EQ(value_parameter->kind, UDTParameterKind::UInt16);

    const auto * decreases = create->decreases->as<ASTUDTValueParameterReference>();
    ASSERT_NE(decreases, nullptr);
    EXPECT_EQ(decreases->name, "N");
    EXPECT_EQ(decreases->ordinal, 1);
    EXPECT_EQ(decreases->kind, UDTParameterKind::UInt16);
    EXPECT_NE(create->definition->as<ASTUDTTypeIf>(), nullptr);

    const String digest(64, 'A');
    const auto attach_ast = parseTypeQuery(
        "ATTACH TYPE IF NOT EXISTS app.UserId UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 7 "
        "AS UInt64 DEFINITION HASH '"
        + digest + "' COMMENT 'restored'");
    const auto * attach = attach_ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(attach, nullptr);
    EXPECT_TRUE(attach->attach);
    EXPECT_TRUE(attach->if_not_exists);
    EXPECT_EQ(attach->getDatabase(), "app");
    EXPECT_EQ(attach->getTypeName(), "UserId");
    EXPECT_TRUE(attach->uuid.has_value());
    ASSERT_TRUE(attach->revision.has_value());
    EXPECT_EQ(*attach->revision, 7);
    ASSERT_TRUE(attach->definition_hash.has_value());
    EXPECT_EQ(*attach->definition_hash, String(64, 'a'));
    EXPECT_EQ(literalString(attach->comment), "restored");
}

TEST(UDTQueries, EveryFormalKindKeepsDeclarationOrderAndKind)
{
    const auto ast = parseTypeQuery(
        "CREATE TYPE app.AllKinds("
        "T TYPE, B Bool, U8 UInt8, U16 UInt16, U32 UInt32, U64 UInt64, "
        "I8 Int8, I16 Int16, I32 Int32, I64 Int64, S String) AS T");
    const auto * create = ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(create, nullptr);
    const auto * parameters = create->parameters->as<ASTExpressionList>();
    ASSERT_NE(parameters, nullptr);

    const std::array expected_kinds{
        UDTParameterKind::Type,
        UDTParameterKind::Bool,
        UDTParameterKind::UInt8,
        UDTParameterKind::UInt16,
        UDTParameterKind::UInt32,
        UDTParameterKind::UInt64,
        UDTParameterKind::Int8,
        UDTParameterKind::Int16,
        UDTParameterKind::Int32,
        UDTParameterKind::Int64,
        UDTParameterKind::String,
    };
    ASSERT_EQ(parameters->children.size(), expected_kinds.size());
    for (size_t index = 0; index < expected_kinds.size(); ++index)
    {
        const auto * declaration = parameters->children[index]->as<ASTUDTParameterDeclaration>();
        ASSERT_NE(declaration, nullptr) << index;
        EXPECT_EQ(declaration->ordinal, index) << index;
        EXPECT_EQ(declaration->kind, expected_kinds[index]) << index;
    }
}

TEST(UDTQueries, QuotedUnicodeAndKeywordIdentifiersRemainStructuredAndRoundTrip)
{
    const String query = "CREATE TYPE `данные`.`TYPE`(`AS` TYPE, `COMMENT` UInt16) DECREASES `COMMENT` "
                         "AS TYPE_IF(`COMMENT` = 0, `AS`, `данные`.`TYPE`(`AS`, `COMMENT` - 1)) COMMENT ''";
    const auto ast = parseTypeQuery(query);
    const auto * create = ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(create, nullptr);
    EXPECT_EQ(create->getDatabase(), "данные");
    EXPECT_EQ(create->getTypeName(), "TYPE");
    EXPECT_EQ(literalString(create->comment), "");

    const auto * parameters = create->parameters->as<ASTExpressionList>();
    ASSERT_NE(parameters, nullptr);
    ASSERT_EQ(parameters->children.size(), 2);
    EXPECT_EQ(parameters->children[0]->as<ASTUDTParameterDeclaration &>().name, "AS");
    EXPECT_EQ(parameters->children[1]->as<ASTUDTParameterDeclaration &>().name, "COMMENT");

    expectCloneAndRoundTrip(query);

    expectCloneAndRoundTrip("ALTER TYPE IF EXISTS `SELECT`.`TYPE` ON CLUSTER `ON` RENAME TO `COMMENT`");
    expectCloneAndRoundTrip("ALTER TYPE IF EXISTS `SELECT`.`TYPE` ON CLUSTER `ON` COMMENT 'new value'");
    expectCloneAndRoundTrip("SHOW TYPES FROM `FROM` LIKE '' FORMAT JSONEachRow");
}

TEST(UDTQueries, AttachMetadataClausePermutationsCanonicalizeIdentically)
{
    const String uuid = "'01234567-89ab-cdef-0123-456789abcdef'";
    const String digest = "'0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'";
    const String body = " AS TYPE_IF(N = 0, T, app.Restored(T, N - 1)) DEFINITION HASH " + digest + " COMMENT 'restored'";
    const std::array<String, 4> queries{
        "ATTACH TYPE app.Restored(T TYPE, N UInt16) UUID " + uuid + " REVISION 18446744073709551615 ON CLUSTER c DECREASES N" + body,
        "ATTACH TYPE app.Restored(T TYPE, N UInt16) REVISION 18446744073709551615 DECREASES N UUID " + uuid + " ON CLUSTER c" + body,
        "ATTACH TYPE app.Restored(T TYPE, N UInt16) ON CLUSTER c UUID " + uuid + " DECREASES N REVISION 18446744073709551615" + body,
        "ATTACH TYPE app.Restored(T TYPE, N UInt16) DECREASES N ON CLUSTER c REVISION 18446744073709551615 UUID " + uuid + body,
    };

    const auto canonical_ast = parseTypeQuery(queries.front());
    const String canonical = canonical_ast->formatWithSecretsOneLine();
    for (const auto & query : queries)
    {
        SCOPED_TRACE(query);
        const auto ast = parseTypeQuery(query);
        const auto * attach = ast->as<ASTCreateTypeQuery>();
        ASSERT_NE(attach, nullptr);
        ASSERT_TRUE(attach->revision.has_value());
        EXPECT_EQ(*attach->revision, std::numeric_limits<UInt64>::max());
        EXPECT_EQ(attach->cluster, "c");
        EXPECT_EQ(ast->getTreeHash(false), canonical_ast->getTreeHash(false));
        EXPECT_EQ(ast->formatWithSecretsOneLine(), canonical);
        expectCloneAndRoundTrip(query);
    }

    const auto first_create = parseTypeQuery(
        "CREATE TYPE app.Recursive(T TYPE, N UInt16) ON CLUSTER c DECREASES N "
        "AS TYPE_IF(N = 0, T, app.Recursive(T, N - 1))");
    const auto second_create = parseTypeQuery(
        "CREATE TYPE app.Recursive(T TYPE, N UInt16) DECREASES N ON CLUSTER c "
        "AS TYPE_IF(N = 0, T, app.Recursive(T, N - 1))");
    EXPECT_EQ(first_create->getTreeHash(false), second_create->getTreeHash(false));
    EXPECT_EQ(first_create->formatWithSecretsOneLine(), second_create->formatWithSecretsOneLine());
}

TEST(UDTQueries, RevisionCommentAndApplyTokenBoundariesAreExact)
{
    const String digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const auto max_revision_ast = parseTypeQuery(
        "ATTACH TYPE app.MaxRevision UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 18446744073709551615 "
        "AS UInt64 DEFINITION HASH '"
        + digest + "' COMMENT ''");
    const auto * max_revision = max_revision_ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(max_revision, nullptr);
    ASSERT_TRUE(max_revision->revision.has_value());
    EXPECT_EQ(*max_revision->revision, std::numeric_limits<UInt64>::max());
    EXPECT_EQ(literalString(max_revision->comment), "");
    expectCloneAndRoundTrip(max_revision_ast->formatWithSecretsOneLine());

    const String large_comment(256 * 1024, 'x');
    const auto large_comment_ast = parseTypeQuery("CREATE TYPE app.LargeComment AS UInt8 COMMENT '" + large_comment + "'");
    const auto * large_comment_create = large_comment_ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(large_comment_create, nullptr);
    const String parsed_large_comment = literalString(large_comment_create->comment);
    EXPECT_EQ(parsed_large_comment.size(), large_comment.size());
    EXPECT_TRUE(parsed_large_comment == large_comment);
    const auto large_comment_reparsed = parseTypeQuery(large_comment_ast->formatWithSecretsOneLine());
    EXPECT_EQ(large_comment_reparsed->getTreeHash(false), large_comment_ast->getTreeHash(false));

    const String max_apply_token(4096, 't');
    const auto apply_ast = parseTypeQuery("PHYSICALIZE TYPE REFERENCES APPLY TOKEN '" + max_apply_token + "'");
    const auto * apply = apply_ast->as<ASTApplyPhysicalizeTypeReferencesQuery>();
    ASSERT_NE(apply, nullptr);
    EXPECT_EQ(apply->getToken().size(), max_apply_token.size());
    EXPECT_TRUE(apply->getToken() == max_apply_token);
    EXPECT_EQ(apply_ast->formatWithSecretsOneLine().find(max_apply_token), String::npos);
}

TEST(UDTQueries, ParserDepthLimitAcceptsItsBoundaryAndRejectsTheNextLayer)
{
    const String at_boundary = "CREATE TYPE app.Deep AS " + nestedArrayType(24);
    const String over_boundary = "CREATE TYPE app.Deeper AS " + nestedArrayType(25);

    size_t rejected_depth = 0;
    size_t accepted_depth = 512;
    ASSERT_TRUE(parsesWithinDepth(at_boundary, accepted_depth));
    while (rejected_depth + 1 < accepted_depth)
    {
        const size_t candidate = rejected_depth + (accepted_depth - rejected_depth) / 2;
        if (parsesWithinDepth(at_boundary, candidate))
            accepted_depth = candidate;
        else
            rejected_depth = candidate;
    }

    ASSERT_GT(accepted_depth, 1);
    EXPECT_TRUE(parsesWithinDepth(at_boundary, accepted_depth));
    EXPECT_FALSE(parsesWithinDepth(at_boundary, accepted_depth - 1));
    EXPECT_FALSE(parsesWithinDepth(over_boundary, accepted_depth));
}

TEST(UDTQueries, BareFormalWinsWhenItMatchesTheDefinitionName)
{
    const auto ast = parseTypeQuery("CREATE TYPE app.T(T TYPE) AS T");
    const auto * create = ast->as<ASTCreateTypeQuery>();
    ASSERT_NE(create, nullptr);
    const auto * reference = create->definition->as<ASTUDTTypeParameterReference>();
    ASSERT_NE(reference, nullptr);
    EXPECT_EQ(reference->name, "T");
    EXPECT_EQ(reference->ordinal, 0);
}

TEST(UDTQueries, LifecycleAndIntrospectionExposeExactFields)
{
    const auto drop_ast = parseTypeQuery("DROP TYPE IF EXISTS app.PrincipalId ON CLUSTER c RESTRICT");
    const auto * drop = drop_ast->as<ASTDropTypeQuery>();
    ASSERT_NE(drop, nullptr);
    EXPECT_TRUE(drop->if_exists);
    EXPECT_EQ(drop->getDatabase(), "app");
    EXPECT_EQ(drop->getTypeName(), "PrincipalId");
    EXPECT_EQ(drop->cluster, "c");

    const auto rename_ast = parseTypeQuery("ALTER TYPE IF EXISTS app.UserId ON CLUSTER c RENAME TO PrincipalId");
    const auto * rename = rename_ast->as<ASTRenameTypeQuery>();
    ASSERT_NE(rename, nullptr);
    EXPECT_TRUE(rename->if_exists);
    EXPECT_EQ(rename->getDatabase(), "app");
    EXPECT_EQ(rename->getTypeName(), "UserId");
    EXPECT_EQ(rename->getNewTypeName(), "PrincipalId");
    EXPECT_EQ(rename->cluster, "c");

    const auto comment_ast = parseTypeQuery("ALTER TYPE IF EXISTS app.UserId ON CLUSTER c COMMENT 'public id'");
    const auto * comment = comment_ast->as<ASTAlterTypeCommentQuery>();
    ASSERT_NE(comment, nullptr);
    EXPECT_TRUE(comment->if_exists);
    EXPECT_EQ(comment->getDatabase(), "app");
    EXPECT_EQ(comment->getTypeName(), "UserId");
    EXPECT_EQ(comment->getComment(), "public id");
    EXPECT_EQ(comment->cluster, "c");

    const auto show_ast = parseTypeQuery("SHOW TYPES FROM app LIKE '%Id'");
    const auto * show = show_ast->as<ASTShowTypesQuery>();
    ASSERT_NE(show, nullptr);
    EXPECT_EQ(show->getDatabase(), "app");
    EXPECT_EQ(literalString(show->like_pattern), "%Id");

    const auto show_create_ast = parseTypeQuery("SHOW CREATE TYPE `analytics.db`.`User.Type`");
    const auto * show_create = show_create_ast->as<ASTShowCreateTypeQuery>();
    ASSERT_NE(show_create, nullptr);
    EXPECT_EQ(show_create->getDatabase(), "analytics.db");
    EXPECT_EQ(show_create->getTypeName(), "User.Type");

    const auto describe_ast = parseTypeQuery("DESCRIBE TYPE app.UserId");
    const auto * describe = describe_ast->as<ASTDescribeTypeQuery>();
    ASSERT_NE(describe, nullptr);
    EXPECT_EQ(describe->getDatabase(), "app");
    EXPECT_EQ(describe->getTypeName(), "UserId");
}

TEST(UDTQueries, OutputSuffixesBelongToTheOuterOutputQuery)
{
    const auto show_ast = parseTypeQuery(
        "SHOW TYPES FROM app LIKE '%Id' INTO OUTFILE 'types.json' FORMAT JSONEachRow "
        "SETTINGS output_format_json_quote_64bit_integers = 0");
    const auto * show = show_ast->as<ASTShowTypesQuery>();
    ASSERT_NE(show, nullptr);
    EXPECT_EQ(literalString(show->like_pattern), "%Id");
    EXPECT_EQ(literalString(show->out_file), "types.json");
    EXPECT_NE(show->format_ast, nullptr);
    EXPECT_NE(show->settings_ast, nullptr);

    const auto show_create_ast = parseTypeQuery("SHOW CREATE TYPE app.UserId INTO OUTFILE 'create.sql'");
    const auto * show_create = show_create_ast->as<ASTShowCreateTypeQuery>();
    ASSERT_NE(show_create, nullptr);
    EXPECT_EQ(literalString(show_create->out_file), "create.sql");

    const auto describe_ast
        = parseTypeQuery("DESCRIBE TYPE app.UserId SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow");
    const auto * describe = describe_ast->as<ASTDescribeTypeQuery>();
    ASSERT_NE(describe, nullptr);
    EXPECT_NE(describe->settings_ast, nullptr);
    EXPECT_NE(describe->format_ast, nullptr);
}

TEST(UDTQueries, TableNamedTypeKeepsItsExistingParserRoute)
{
    const std::array table_route_queries{
        std::string_view{"SHOW CREATE TYPE"},
        std::string_view{"SHOW CREATE TYPE FORMAT JSON"},
        std::string_view{"DESCRIBE TYPE"},
        std::string_view{"DESCRIBE TYPE alias"},
        std::string_view{"DESCRIBE TYPE FORMAT JSON"},
    };

    for (const auto query_text : table_route_queries)
    {
        SCOPED_TRACE(query_text);
        const auto ast = parseTypeQuery(String{query_text});
        ASSERT_NE(ast, nullptr);
        EXPECT_EQ(ast->as<ASTShowCreateTypeQuery>(), nullptr);
        EXPECT_EQ(ast->as<ASTDescribeTypeQuery>(), nullptr);
    }
}

TEST(UDTQueries, PhysicalizationScopesAreUnambiguous)
{
    const auto object_ast = parseTypeQuery(
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t ON CLUSTER c DROP UNUSED TYPES DRY RUN "
        "FORMAT JSONEachRow SETTINGS max_threads = 1");
    const auto * object = object_ast->as<ASTPhysicalizeTypeReferencesQuery>();
    ASSERT_NE(object, nullptr);
    EXPECT_EQ(object->scope, ASTPhysicalizeTypeReferencesQuery::Scope::Object);
    EXPECT_EQ(getIdentifierName(object->object_kind), "TABLE");
    EXPECT_EQ(object->getDatabase(), "app");
    EXPECT_EQ(object->getObjectName(), "t");
    EXPECT_EQ(object->cluster, "c");
    EXPECT_TRUE(object->drop_unused_types);
    EXPECT_NE(object->format_ast, nullptr);
    EXPECT_NE(object->settings_ast, nullptr);

    const auto closure_ast = parseTypeQuery("PHYSICALIZE TYPE REFERENCES CLOSURE OF VIEW `analytics.db`.`v.name` DRY RUN");
    const auto * closure = closure_ast->as<ASTPhysicalizeTypeReferencesQuery>();
    ASSERT_NE(closure, nullptr);
    EXPECT_EQ(closure->scope, ASTPhysicalizeTypeReferencesQuery::Scope::DependentClosure);
    EXPECT_EQ(getIdentifierName(closure->object_kind), "VIEW");
    EXPECT_EQ(closure->getDatabase(), "analytics.db");
    EXPECT_EQ(closure->getObjectName(), "v.name");

    const auto database_ast = parseTypeQuery("PHYSICALIZE TYPE REFERENCES DATABASE app ON CLUSTER c DRY RUN INTO OUTFILE 'plan.json'");
    const auto * database = database_ast->as<ASTPhysicalizeTypeReferencesQuery>();
    ASSERT_NE(database, nullptr);
    EXPECT_EQ(database->scope, ASTPhysicalizeTypeReferencesQuery::Scope::Database);
    EXPECT_EQ(database->getDatabase(), "app");
    EXPECT_EQ(database->cluster, "c");
    EXPECT_EQ(literalString(database->out_file), "plan.json");
}

TEST(UDTQueries, EveryAcceptedFormClonesAndRoundTrips)
{
    const std::vector<String> queries = {
        "CREATE TYPE app.UserId AS UInt64",
        "CREATE TYPE app.Pair(T TYPE) AS Tuple(left T, right T)",
        "CREATE TYPE ids.Raw(N UInt16) AS FixedString(N)",
        "CREATE TYPE app.Nested(T TYPE, N UInt16) DECREASES N "
        "AS TYPE_IF(N = 0, T, Tuple(head T, tail app.Nested(T, N - 1)))",
        "CREATE TYPE app.CommentedId AS UInt64 COMMENT 'external identifier'",
        "ATTACH TYPE app.UserId UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ALTER TYPE app.UserId RENAME TO PrincipalId",
        "DROP TYPE IF EXISTS app.PrincipalId RESTRICT",
        "SHOW TYPES FROM app LIKE '%Id' FORMAT JSONEachRow",
        "SHOW CREATE TYPE app.UserId INTO OUTFILE 'x'",
        "DESCRIBE TYPE app.UserId SETTINGS output_format_json_quote_64bit_integers = 0",
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t DRY RUN FORMAT JSONEachRow",
        "PHYSICALIZE TYPE REFERENCES CLOSURE OF VIEW app.v DROP UNUSED TYPES DRY RUN",
        "PHYSICALIZE TYPE REFERENCES DATABASE app ON CLUSTER c DRY RUN",
    };

    for (const auto & query : queries)
        expectCloneAndRoundTrip(query);
}

TEST(UDTQueries, ApplyTokenNeverAppearsInFormattingOrHashIdentity)
{
    const String secret = "opaque token: owner@example.test/123";
    const auto ast = parseTypeQuery("PHYSICALIZE TYPE REFERENCES APPLY TOKEN '" + secret + "'");
    const auto * apply = ast->as<ASTApplyPhysicalizeTypeReferencesQuery>();
    ASSERT_NE(apply, nullptr);
    EXPECT_EQ(apply->getToken(), secret);
    EXPECT_TRUE(apply->hasSecretParts());

    const String formatted = ast->formatWithSecretsOneLine();
    const String logged = ast->formatForLogging();
    EXPECT_EQ(formatted.find(secret), String::npos);
    EXPECT_EQ(logged.find(secret), String::npos);
    EXPECT_NE(formatted.find(ASTApplyPhysicalizeTypeReferencesQuery::redacted_token), String::npos);
    EXPECT_NE(logged.find(ASTApplyPhysicalizeTypeReferencesQuery::redacted_token), String::npos);

    const auto clone = ast->clone();
    const auto * cloned_apply = clone->as<ASTApplyPhysicalizeTypeReferencesQuery>();
    ASSERT_NE(cloned_apply, nullptr);
    EXPECT_EQ(cloned_apply->getToken(), secret);
    EXPECT_EQ(ast->getTreeHash(false), clone->getTreeHash(false));
    EXPECT_EQ(formatted, clone->formatWithSecretsOneLine());

    const auto other = parseTypeQuery("PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'different secret'");
    EXPECT_EQ(ast->getTreeHash(false), other->getTreeHash(false));
    EXPECT_EQ(formatted, other->formatWithSecretsOneLine());

    const auto reparsed = parseTypeQuery(formatted);
    EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false));
    EXPECT_EQ(formatted, reparsed->formatWithSecretsOneLine());
}

TEST(UDTQueries, MalformedApplyTokenNeverAppearsInParserDiagnostics)
{
    const auto expect_masked_diagnostic = [](const String & query, std::string_view secret)
    {
        try
        {
            static_cast<void>(parseTypeQuery(query));
            FAIL() << "Expected malformed APPLY TOKEN to fail";
        }
        catch (const Exception & exception)
        {
            EXPECT_EQ(exception.message().find(secret), String::npos) << exception.message();
            EXPECT_NE(exception.message().find("[HIDDEN]"), String::npos) << exception.message();
        }
    };

    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'client-visible-secret", "client-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCES APPLY TOKEN #\t'malformed-visible-secret'", "malformed-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCES APPLY #\t TOKEN 'pre-token-visible-secret'", "pre-token-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCES APPLY /* TOKEN 'block-visible-secret'", "block-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCES APPLY -- TOKEN 'line-visible-secret'", "line-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCES APPLY TOKEN /* token-visible-secret", "token-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCE APPLY TOKEN 'bad-reference-visible-secret'", "bad-reference-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCES APPLYY TOKEN 'bad-apply-visible-secret'", "bad-apply-visible-secret");
    expect_masked_diagnostic("PHYSICALIZE TYPE REFERENCE APPLY TOKN 'combined-visible-secret'", "combined-visible-secret");
    expect_masked_diagnostic("SELECT 1; PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'later-visible-secret'", "later-visible-secret");

    const String prefix = "PHYSICALIZE TYPE REFERENCES APPLY TOKEN ";
    const String bounded_secret = "bounded-visible-secret";
    const String bounded_query = prefix + "'" + bounded_secret + "'";
    for (const bool hilite : {false, true})
    {
        ParserQuery parser(bounded_query.data() + bounded_query.size());
        const char * position = bounded_query.data();
        String error;
        const auto ast = tryParseQuery(
            parser,
            position,
            bounded_query.data() + bounded_query.size(),
            error,
            hilite,
            "bounded APPLY TOKEN diagnostic",
            false,
            prefix.size(),
            150,
            0,
            true);
        EXPECT_EQ(ast, nullptr);
        EXPECT_EQ(error.find(bounded_secret), String::npos) << error;
        EXPECT_NE(error.find("[HIDDEN]"), String::npos) << error;
    }
}

TEST(UDTQueries, InvalidAndPrematureFormsAreRejected)
{
    const std::array<std::string_view, 56> invalid_queries{
        "CREATE OR REPLACE TYPE app.T AS UInt64",
        "CREATE TYPE IF NOT EXISTS IF NOT EXISTS app.T AS UInt64",
        "CREATE TYPE app.T(X Mystery) AS UInt64",
        "CREATE TYPE app.T(X TYPE, X UInt8) AS UInt64",
        "CREATE TYPE app.T(X) AS UInt64",
        "CREATE TYPE app.T(X TYPE...) AS UInt64",
        "CREATE TYPE app.T(X UInt8 = 1) AS UInt64",
        "CREATE TYPE app.T() AS UInt64",
        "CREATE TYPE app.T(X Int16) DECREASES X AS UInt64",
        "CREATE TYPE app.T(X UInt16) DECREASES Missing AS UInt64",
        "CREATE TYPE app.T(X UInt16) DECREASES X DECREASES X AS UInt64",
        "CREATE TYPE app.T(X UInt16) AS TYPE_IF(X = 1, UInt8, UInt16)",
        "CREATE TYPE app.T(X UInt16) DECREASES X AS app.T(UInt16)",
        "CREATE TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' AS UInt64",
        "CREATE TYPE app.T REVISION 1 AS UInt64",
        "CREATE TYPE app.T AS UInt64 DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "CREATE TYPE app.T ON CLUSTER c ON CLUSTER d AS UInt64",
        "CREATE TYPE app.T AS UInt64 COMMENT 'one' COMMENT 'two'",
        "CREATE TYPE app.T COMMENT '' AS UInt64",
        "CREATE TYPE app.T AS UInt64 ON CLUSTER c",
        "ATTACH TYPE app.T AS UInt64",
        "ATTACH TYPE IF NOT EXISTS IF NOT EXISTS app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '00000000-0000-0000-0000-000000000000' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 0 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 18446744073709551616 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION -1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1.5 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION '1' AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' "
        "UUID '11234567-89ab-cdef-0123-456789abcdef' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 REVISION 2 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' AS UInt64",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 COMMENT '' AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 AS UInt64 COMMENT '' "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0'",
        "ATTACH TYPE app.T UUID '01234567-89ab-cdef-0123-456789abcdef' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg'",
        "ATTACH TYPE app.T UUID 'not-a-uuid' REVISION 1 AS UInt64 "
        "DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'",
        "ALTER TYPE app.T AS UInt64",
        "ALTER TYPE app.T RENAME TO other.T",
        "ALTER TYPE IF EXISTS IF EXISTS app.T RENAME TO U",
        "ALTER TYPE app.T COMMENT",
        "ALTER TYPE app.T COMMENT 1",
        "ALTER TYPE app.T COMMENT 'one' COMMENT 'two'",
        "DROP TYPE app.T CASCADE",
        "DROP TYPE app.T RESTRICT RESTRICT",
        "DROP TYPE IF EXISTS IF EXISTS app.T",
        "SHOW TYPES LIKE 'T%' FROM app",
        "SHOW TYPES FROM app FROM other",
        "SHOW CREATE TYPE a.b.c",
        "DESCRIBE TYPE a.b.c",
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t",
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t OBJECT VIEW app.v DRY RUN",
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t DROP UNUSED TYPES ON CLUSTER c DRY RUN",
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t DRY RUN DROP UNUSED TYPES",
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t DRY RUN DRY RUN",
        "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.t DROP UNUSED TYPES DROP UNUSED TYPES DRY RUN",
    };

    for (const auto query : invalid_queries)
        expectParseFailure(String(query));

    expectParseFailure("PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'x' ON CLUSTER c");
    expectParseFailure("PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'x' DROP UNUSED TYPES");
    expectParseFailure("PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'x' DRY RUN");
    expectParseFailure("PHYSICALIZE TYPE REFERENCES APPLY TOKEN '" + String(4097, 'x') + "'");
    expectParseFailure("CREATE TYPE app.T AS UInt64 COMMENT 'bad" + String(1, '\0') + "comment'");
    expectParseFailure("ALTER TYPE app.T COMMENT 'bad" + String(1, '\0') + "comment'");
}

TEST(UDTQueries, InactiveClausesFailWithTheirStableCodeBeforeBodyParsing)
{
    const std::array<std::string_view, 8> inactive_clauses{
        "INPUT", "OUTPUT", "DEFAULT", "CONSTRAINT", "CHECK", "PRIMARY KEY", "FOREIGN KEY", "UNIQUE"};

    for (const auto clause : inactive_clauses)
    {
        expectErrorCode(
            "CREATE TYPE app.T " + String(clause) + " ((( this body is deliberately malformed and must not be parsed",
            ErrorCodes::UNSUPPORTED_TYPE_CLAUSE);
        expectErrorCode(
            "CREATE TYPE app.T AS UInt64 " + String(clause) + " ((( this body is deliberately malformed and must not be parsed",
            ErrorCodes::UNSUPPORTED_TYPE_CLAUSE);
    }


    expectErrorCode("CREATE TYPE app.T AS UInt64 INPUT 'unterminated inactive body", ErrorCodes::UNSUPPORTED_TYPE_CLAUSE);
    expectErrorCode("CREATE TYPE app.T AS UInt64 DEFAULT " + String(1024 * 1024, 'x'), ErrorCodes::UNSUPPORTED_TYPE_CLAUSE);
}
