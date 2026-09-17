#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTUDTReference.h>
#include <Parsers/ASTUDTTemplate.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <string_view>
#include <utility>
#include <vector>


namespace
{
using namespace DB;

DataTypeFamilyClassification classifyFamily(const void *, std::string_view, DataTypeFamilySyntaxKind syntax_kind) noexcept;

ASTPtr parseType(const String & text)
{
    ParserDataType parser;
    return parseQuery(parser, text, "user-defined type parser test", 0, 150, 0);
}

ASTPtr parseQualifiedType(const String & text)
{
    DataTypeFamilyClassificationSummary summary;
    ParserDataTypeWithFamilyClassification parser(DataTypeFamilyClassifier{.context = nullptr, .callback = classifyFamily}, summary);
    return parseQuery(parser, text, "classified user-defined type parser test", 0, 150, 0);
}

void expectParseFailure(const String & text)
{
    EXPECT_THROW(static_cast<void>(parseType(text)), Exception) << text;
}

void expectQualifiedParseFailure(const String & text)
{
    EXPECT_THROW(static_cast<void>(parseQualifiedType(text)), Exception) << text;
}

UDTExpressionParserContext nestedTemplateContext()
{
    return {
        .parameters = {
            {.name = "T", .ordinal = 0, .kind = UDTParameterKind::Type},
            {.name = "N", .ordinal = 1, .kind = UDTParameterKind::UInt16},
        },
        .definition_database = "app",
        .definition_name = "Recursive",
        .decreasing_parameter = 1,
    };
}

UDTExpressionParserContext unqualifiedNestedTemplateContext()
{
    auto context = nestedTemplateContext();
    context.definition_database.clear();
    return context;
}

ASTPtr parseTemplateType(const String & text)
{
    ParserUDTExpression parser(nestedTemplateContext());
    return parseQuery(parser, text, "user-defined type template parser test", 0, 150, 0);
}

void expectTemplateParseFailure(const String & text)
{
    EXPECT_THROW(static_cast<void>(parseTemplateType(text)), Exception) << text;
}

DataTypeFamilyClassification classifyFamily(const void *, std::string_view, DataTypeFamilySyntaxKind syntax_kind) noexcept
{
    if (syntax_kind == DataTypeFamilySyntaxKind::QualifiedReference)
        return {.is_built_in = false, .is_qualified_reference = true};
    return {.is_built_in = true, .is_qualified_reference = false};
}

}

TEST(UDTParser, QualifiedReferenceHasDedicatedStructuredAST)
{
    const auto ast = parseQualifiedType("app.UserId(UInt16, 3, 'external')");
    const auto * reference = ast->as<ASTUDTReference>();
    ASSERT_NE(reference, nullptr);
    EXPECT_EQ(ast->as<ASTDataType>(), nullptr);
    EXPECT_EQ(reference->database_name, "app");
    EXPECT_EQ(reference->type_name, "UserId");

    const auto arguments = reference->getArguments();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 3);
    ASSERT_NE(arguments->children[0]->as<ASTDataType>(), nullptr);
    EXPECT_EQ(arguments->children[0]->as<ASTDataType &>().name, "UInt16");
    ASSERT_NE(arguments->children[1]->as<ASTLiteral>(), nullptr);
    EXPECT_EQ(arguments->children[1]->as<ASTLiteral &>().value.safeGet<UInt64>(), 3);
    ASSERT_NE(arguments->children[2]->as<ASTLiteral>(), nullptr);
    EXPECT_EQ(arguments->children[2]->as<ASTLiteral &>().value.safeGet<String>(), "external");
}

TEST(UDTParser, NestedQualifiedReferencePropagatesClassification)
{
    DataTypeFamilyClassificationSummary summary;
    ParserDataTypeWithFamilyClassification parser(DataTypeFamilyClassifier{.context = nullptr, .callback = classifyFamily}, summary);
    const String text = "Array(Tuple(app.UserId, ids.Raw(16)))";
    const auto ast = parseQuery(parser, text, "classified user-defined type parser test", 0, 150, 0);

    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->as<ASTDataType>(), nullptr);
    EXPECT_FALSE(summary.allFamiliesAreBuiltIn());
    EXPECT_TRUE(summary.hasQualifiedLogicalFamily());

    const auto outer_arguments = ast->as<ASTDataType &>().getArguments();
    ASSERT_NE(outer_arguments, nullptr);
    ASSERT_EQ(outer_arguments->children.size(), 1);
    const auto * tuple = outer_arguments->children[0]->as<ASTTupleDataType>();
    ASSERT_NE(tuple, nullptr);
    const auto tuple_arguments = tuple->getArguments();
    ASSERT_NE(tuple_arguments, nullptr);
    ASSERT_EQ(tuple_arguments->children.size(), 2);
    EXPECT_NE(tuple_arguments->children[0]->as<ASTUDTReference>(), nullptr);
    EXPECT_NE(tuple_arguments->children[1]->as<ASTUDTReference>(), nullptr);
}

TEST(UDTParser, QualifiedBuiltInSpellingsStayOnTheLogicalReferencePath)
{
    struct Case
    {
        std::string_view text;
        std::string_view database;
        std::string_view type;
    };

    constexpr std::array cases{
        Case{"system.Array(UInt8)", "system", "Array"},
        Case{"Array.UInt64", "Array", "UInt64"},
        Case{"Tuple.Nullable(String)", "Tuple", "Nullable"},
    };

    for (const auto & test_case : cases)
    {
        SCOPED_TRACE(test_case.text);
        DataTypeFamilyClassificationSummary summary;
        ParserDataTypeWithFamilyClassification parser(DataTypeFamilyClassifier{.context = nullptr, .callback = classifyFamily}, summary);
        const String text{test_case.text};
        const auto ast = parseQuery(parser, text, "qualified built-in spelling parser test", 0, 150, 0);
        const auto * reference = ast->as<ASTUDTReference>();
        ASSERT_NE(reference, nullptr);
        EXPECT_EQ(ast->as<ASTDataType>(), nullptr);
        EXPECT_EQ(reference->database_name, String(test_case.database));
        EXPECT_EQ(reference->type_name, String(test_case.type));
        EXPECT_FALSE(summary.allFamiliesAreBuiltIn());
        EXPECT_TRUE(summary.hasQualifiedLogicalFamily());
    }

    DataTypeFamilyClassificationSummary nested_summary;
    ParserDataTypeWithFamilyClassification nested_parser(
        DataTypeFamilyClassifier{.context = nullptr, .callback = classifyFamily}, nested_summary);
    const String nested_text = "Array(system.Array(UInt8))";
    const auto nested = parseQuery(nested_parser, nested_text, "nested qualified built-in spelling parser test", 0, 150, 0);
    const auto * outer = nested->as<ASTDataType>();
    ASSERT_NE(outer, nullptr);
    const auto arguments = outer->getArguments();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 1);
    const auto * logical_child = arguments->children[0]->as<ASTUDTReference>();
    ASSERT_NE(logical_child, nullptr);
    EXPECT_EQ(logical_child->database_name, "system");
    EXPECT_EQ(logical_child->type_name, "Array");
    EXPECT_FALSE(nested_summary.allFamiliesAreBuiltIn());
    EXPECT_TRUE(nested_summary.hasQualifiedLogicalFamily());
}

TEST(UDTParser, QuotedComponentsCloneAndRoundTripWithoutFlattening)
{
    const String text = "`analytics.db`.`User.Type`(Array(UInt8), 'x')";
    const auto ast = parseQualifiedType(text);
    const auto * reference = ast->as<ASTUDTReference>();
    ASSERT_NE(reference, nullptr);
    EXPECT_EQ(reference->database_name, "analytics.db");
    EXPECT_EQ(reference->type_name, "User.Type");

    const auto cloned = ast->clone();
    ASSERT_NE(cloned->as<ASTUDTReference>(), nullptr);
    EXPECT_EQ(ast->getTreeHash(false), cloned->getTreeHash(false));
    EXPECT_EQ(ast->formatWithSecretsOneLine(), cloned->formatWithSecretsOneLine());

    const String formatted = ast->formatWithSecretsOneLine();
    const auto reparsed = parseQualifiedType(formatted);
    ASSERT_NE(reparsed->as<ASTUDTReference>(), nullptr);
    EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false));
    EXPECT_EQ(formatted, reparsed->formatWithSecretsOneLine());

    const auto left = parseQualifiedType("a.`b.c`");
    const auto right = parseQualifiedType("`a.b`.c");
    EXPECT_NE(left->getTreeHash(false), right->getTreeHash(false));
}

TEST(UDTParser, QuotedUnicodeAndKeywordComponentsRoundTripExactly)
{
    const String text = "`данные`.`SELECT`(`схема`.`Тип`, 'значение')";
    const auto ast = parseQualifiedType(text);
    const auto * reference = ast->as<ASTUDTReference>();
    ASSERT_NE(reference, nullptr);
    EXPECT_EQ(reference->database_name, "данные");
    EXPECT_EQ(reference->type_name, "SELECT");

    const auto arguments = reference->getArguments();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 2);
    const auto * nested = arguments->children[0]->as<ASTUDTReference>();
    ASSERT_NE(nested, nullptr);
    EXPECT_EQ(nested->database_name, "схема");
    EXPECT_EQ(nested->type_name, "Тип");
    ASSERT_NE(arguments->children[1]->as<ASTLiteral>(), nullptr);
    EXPECT_EQ(arguments->children[1]->as<ASTLiteral &>().value.safeGet<String>(), "значение");

    const auto clone = ast->clone();
    EXPECT_EQ(clone->getTreeHash(false), ast->getTreeHash(false));
    const String formatted = ast->formatWithSecretsOneLine();
    const auto reparsed = parseQualifiedType(formatted);
    EXPECT_EQ(reparsed->getTreeHash(false), ast->getTreeHash(false));
    EXPECT_EQ(reparsed->formatWithSecretsOneLine(), formatted);
}

TEST(UDTParser, EmptyArgumentListCanonicalizesAndMalformedQualificationRejects)
{
    const auto ast = parseQualifiedType("app.UserId()");
    const auto * reference = ast->as<ASTUDTReference>();
    ASSERT_NE(reference, nullptr);
    EXPECT_EQ(reference->getArguments(), nullptr);
    EXPECT_EQ(ast->formatWithSecretsOneLine(), "app.UserId");
    EXPECT_EQ(ast->getTreeHash(false), parseQualifiedType("app.UserId")->getTreeHash(false));

    expectQualifiedParseFailure("app.UserId.extra");
    expectQualifiedParseFailure("app.UserId(1,)");
    expectQualifiedParseFailure("app.UserId(UInt8,)");
    expectQualifiedParseFailure("app.");
    expectQualifiedParseFailure("app..UserId");
    expectQualifiedParseFailure(".UserId");
}

TEST(UDTParser, UnqualifiedBuiltInASTRemainsUnchanged)
{
    const auto ast = parseType("Array(UInt64)");
    ASSERT_NE(ast->as<ASTDataType>(), nullptr);
    EXPECT_EQ(ast->as<ASTUDTReference>(), nullptr);
    EXPECT_EQ(ast->formatWithSecretsOneLine(), "Array(UInt64)");
}

TEST(UDTParser, TupleSpecializedASTKeepsUnambiguousAndAmbiguousElementForms)
{
    const auto unnamed_ast = parseQualifiedType("Tuple(UInt64, Array(String), app.UserId)");
    const auto * unnamed_tuple = unnamed_ast->as<ASTTupleDataType>();
    ASSERT_NE(unnamed_tuple, nullptr);
    EXPECT_TRUE(unnamed_tuple->element_names.empty());
    const auto unnamed_arguments = unnamed_tuple->getArguments();
    ASSERT_NE(unnamed_arguments, nullptr);
    ASSERT_EQ(unnamed_arguments->children.size(), 3);
    EXPECT_NE(unnamed_arguments->children[0]->as<ASTDataType>(), nullptr);
    EXPECT_NE(unnamed_arguments->children[1]->as<ASTDataType>(), nullptr);
    EXPECT_NE(unnamed_arguments->children[2]->as<ASTUDTReference>(), nullptr);
    EXPECT_EQ(unnamed_ast->formatWithSecretsOneLine(), "Tuple(UInt64, Array(String), app.UserId)");

    const auto named_ast = parseQualifiedType("Tuple(id UInt64, payload Array(String), logical app.UserId)");
    const auto * named_tuple = named_ast->as<ASTTupleDataType>();
    ASSERT_NE(named_tuple, nullptr);
    EXPECT_EQ(named_tuple->element_names, (Strings{"id", "payload", "logical"}));
    const auto named_arguments = named_tuple->getArguments();
    ASSERT_NE(named_arguments, nullptr);
    ASSERT_EQ(named_arguments->children.size(), 3);
    EXPECT_NE(named_arguments->children[0]->as<ASTDataType>(), nullptr);
    EXPECT_NE(named_arguments->children[1]->as<ASTDataType>(), nullptr);
    EXPECT_NE(named_arguments->children[2]->as<ASTUDTReference>(), nullptr);
    EXPECT_EQ(named_ast->formatWithSecretsOneLine(), "Tuple(id UInt64, payload Array(String), logical app.UserId)");
}

TEST(UDTParser, TupleSpecializedASTKeepsQuotedMultiwordAndMalformedForms)
{
    const auto quoted_type = parseQualifiedType("Tuple(`UInt64`, app.UserId)");
    const auto * quoted_tuple = quoted_type->as<ASTTupleDataType>();
    ASSERT_NE(quoted_tuple, nullptr);
    EXPECT_TRUE(quoted_tuple->element_names.empty());
    EXPECT_EQ(quoted_type->formatWithSecretsOneLine(), "Tuple(UInt64, app.UserId)");

    const auto quoted_names = parseQualifiedType("Tuple(`id` UInt64, \"payload\" String)");
    const auto * named_tuple = quoted_names->as<ASTTupleDataType>();
    ASSERT_NE(named_tuple, nullptr);
    EXPECT_EQ(named_tuple->element_names, (Strings{"id", "payload"}));
    EXPECT_EQ(quoted_names->formatWithSecretsOneLine(), "Tuple(id UInt64, payload String)");

    const auto ambiguous_multiword = parseQualifiedType("Tuple(Double Precision, UInt64 Unsigned)");
    const auto * ambiguous_tuple = ambiguous_multiword->as<ASTTupleDataType>();
    ASSERT_NE(ambiguous_tuple, nullptr);
    EXPECT_EQ(ambiguous_tuple->element_names, (Strings{"Double", "UInt64"}));
    const String formatted_multiword = ambiguous_multiword->formatWithSecretsOneLine();
    EXPECT_EQ(parseQualifiedType(formatted_multiword)->getTreeHash(false), ambiguous_multiword->getTreeHash(false));

    for (const String malformed : {"Tuple(app.)", "Tuple(UInt64,, String)", "Tuple(Array(, UInt64)", "Tuple(UInt64 app.)"})
    {
        SCOPED_TRACE(malformed);
        expectQualifiedParseFailure(malformed);
    }
}

TEST(UDTParser, DefaultDataTypeParserRejectsQualifiedReferencesButColumnDeclarationsAcceptThem)
{
    expectParseFailure("app.UserId");
    expectParseFailure("Array(app.UserId)");
    expectParseFailure("Nested(item app.UserId)");
    expectParseFailure("system.Array");

    const String query = "CREATE TABLE udt_parser_boundary (value app.UserId, nested Nested(item app.Box)) ENGINE = Memory";
    ParserQuery parser(query.data() + query.size());
    const auto ast = parseQuery(parser, query, "CREATE TABLE qualified data-type boundary test", 0, 150, 0);

    const auto * create = ast->as<ASTCreateQuery>();
    ASSERT_NE(create, nullptr);
    ASSERT_NE(create->columns_list, nullptr);
    ASSERT_NE(create->columns_list->columns, nullptr);
    ASSERT_EQ(create->columns_list->columns->children.size(), 2);

    const auto * direct_column = create->columns_list->columns->children[0]->as<ASTColumnDeclaration>();
    ASSERT_NE(direct_column, nullptr);
    const auto direct_column_type = direct_column->getType();
    ASSERT_NE(direct_column_type, nullptr);
    const auto * direct_reference = direct_column_type->as<ASTUDTReference>();
    ASSERT_NE(direct_reference, nullptr);
    EXPECT_EQ(direct_reference->database_name, "app");
    EXPECT_EQ(direct_reference->type_name, "UserId");

    const auto * nested_column = create->columns_list->columns->children[1]->as<ASTColumnDeclaration>();
    ASSERT_NE(nested_column, nullptr);
    const auto nested_column_type = nested_column->getType();
    ASSERT_NE(nested_column_type, nullptr);
    const auto * nested_type = nested_column_type->as<ASTDataType>();
    ASSERT_NE(nested_type, nullptr);
    EXPECT_EQ(nested_type->name, "Nested");
    const auto nested_arguments = nested_type->getArguments();
    ASSERT_NE(nested_arguments, nullptr);
    ASSERT_EQ(nested_arguments->children.size(), 1);
    const auto * nested_item = nested_arguments->children[0]->as<ASTNameTypePair>();
    ASSERT_NE(nested_item, nullptr);
    EXPECT_EQ(nested_item->name, "item");
    const auto * nested_reference = nested_item->type->as<ASTUDTReference>();
    ASSERT_NE(nested_reference, nullptr);
    EXPECT_EQ(nested_reference->database_name, "app");
    EXPECT_EQ(nested_reference->type_name, "Box");
}

TEST(UDTParser, AlterAddAndModifyColumnAcceptQualifiedReferences)
{
    const String query = "ALTER TABLE udt_parser_boundary ADD COLUMN added app.UserId, MODIFY COLUMN value app.Box";
    ParserQuery parser(query.data() + query.size());
    const auto ast = parseQuery(parser, query, "ALTER TABLE qualified data-type boundary test", 0, 150, 0);

    const auto * alter = ast->as<ASTAlterQuery>();
    ASSERT_NE(alter, nullptr);
    ASSERT_NE(alter->command_list, nullptr);
    ASSERT_EQ(alter->command_list->children.size(), 2);

    constexpr std::array expected_commands{
        std::pair{ASTAlterCommand::ADD_COLUMN, std::pair{"app", "UserId"}},
        std::pair{ASTAlterCommand::MODIFY_COLUMN, std::pair{"app", "Box"}},
    };
    for (size_t index = 0; index < expected_commands.size(); ++index)
    {
        const auto * command = alter->command_list->children[index]->as<ASTAlterCommand>();
        ASSERT_NE(command, nullptr);
        EXPECT_EQ(command->type, expected_commands[index].first);
        ASSERT_NE(command->col_decl, nullptr);
        const auto * column = command->col_decl->as<ASTColumnDeclaration>();
        ASSERT_NE(column, nullptr);
        const auto column_type = column->getType();
        ASSERT_NE(column_type, nullptr);
        const auto * reference = column_type->as<ASTUDTReference>();
        ASSERT_NE(reference, nullptr);
        EXPECT_EQ(reference->database_name, expected_commands[index].second.first);
        EXPECT_EQ(reference->type_name, expected_commands[index].second.second);
    }
}

TEST(UDTParser, QualifiedReferenceOwnersRemainTraversableAfterWholeCreateClone)
{
    ASTPtr ast;
    const String query = "CREATE TABLE app.events ("
                         "payload Nested(owner app.UserId, details Tuple("
                         "primary app.Box(Array(app.UserId)), labels Map(String, app.UserId))), "
                         "document JSON(z app.UserId, a app.Box(app.UserId))) ENGINE = Memory";
    ParserQuery parser(query.data() + query.size());
    ast = parseQuery(parser, query, "qualified reference ownership test", 0, 150, 0);

    const String canonical = ast->formatWithSecretsOneLine();
    ASTPtr clone = ast->clone();
    ast.reset();
    EXPECT_EQ(clone->formatWithSecretsOneLine(), canonical);

    size_t name_type_pairs = 0;
    size_t object_arguments = 0;
    size_t typed_paths = 0;
    size_t logical_references = 0;
    std::vector<const IAST *> pending{clone.get()};
    while (!pending.empty())
    {
        const IAST * node = pending.back();
        pending.pop_back();

        if (const auto * pair = node->as<ASTNameTypePair>())
        {
            ++name_type_pairs;
            ASSERT_TRUE(pair->type);
            ASSERT_EQ(pair->children.size(), 1);
            EXPECT_EQ(pair->children.front().get(), pair->type.get());
        }
        if (const auto * object = node->as<ASTObjectTypeArgument>(); object && object->path_with_type)
        {
            ++object_arguments;
            EXPECT_NE(
                std::find_if(
                    object->children.begin(),
                    object->children.end(),
                    [&](const ASTPtr & child) { return child.get() == object->path_with_type.get(); }),
                object->children.end());
        }
        if (const auto * typed = node->as<ASTObjectTypedPathArgument>())
        {
            ++typed_paths;
            ASSERT_TRUE(typed->type);
            ASSERT_EQ(typed->children.size(), 1);
            EXPECT_EQ(typed->children.front().get(), typed->type.get());
        }
        if (node->as<ASTUDTReference>())
            ++logical_references;

        for (const auto & child : node->children)
        {
            ASSERT_TRUE(child);
            pending.push_back(child.get());
        }
    }

    EXPECT_EQ(name_type_pairs, 2);
    EXPECT_EQ(object_arguments, 2);
    EXPECT_EQ(typed_paths, 2);
    EXPECT_EQ(logical_references, 7);
}

TEST(UDTParser, TemplateFormalsRemainDedicatedNodes)
{
    const auto tuple_ast = parseTemplateType("Tuple(left T, right T)");
    const auto * tuple = tuple_ast->as<ASTTupleDataType>();
    ASSERT_NE(tuple, nullptr);
    const auto arguments = tuple->getArguments();
    ASSERT_NE(arguments, nullptr);
    ASSERT_EQ(arguments->children.size(), 2);
    for (const auto & argument : arguments->children)
    {
        const auto * reference = argument->as<ASTUDTTypeParameterReference>();
        ASSERT_NE(reference, nullptr);
        EXPECT_EQ(reference->name, "T");
        EXPECT_EQ(reference->ordinal, 0);
    }

    const auto fixed_string_ast = parseTemplateType("FixedString(N)");
    const auto * fixed_string = fixed_string_ast->as<ASTDataType>();
    ASSERT_NE(fixed_string, nullptr);
    const auto fixed_string_arguments = fixed_string->getArguments();
    ASSERT_NE(fixed_string_arguments, nullptr);
    ASSERT_EQ(fixed_string_arguments->children.size(), 1);
    const auto * value_reference = fixed_string_arguments->children[0]->as<ASTUDTValueParameterReference>();
    ASSERT_NE(value_reference, nullptr);
    EXPECT_EQ(value_reference->name, "N");
    EXPECT_EQ(value_reference->ordinal, 1);
    EXPECT_EQ(value_reference->kind, UDTParameterKind::UInt16);

    const auto nested_ast = parseTemplateType("Nested(item T)");
    const auto * nested = nested_ast->as<ASTDataType>();
    ASSERT_NE(nested, nullptr);
    const auto nested_arguments = nested->getArguments();
    ASSERT_NE(nested_arguments, nullptr);
    ASSERT_EQ(nested_arguments->children.size(), 1);
    const auto * name_type_pair = nested_arguments->children[0]->as<ASTNameTypePair>();
    ASSERT_NE(name_type_pair, nullptr);
    EXPECT_EQ(name_type_pair->name, "item");
    EXPECT_NE(name_type_pair->type->as<ASTUDTTypeParameterReference>(), nullptr);
}

TEST(UDTParser, RestrictedTypeIfAndDecreasingSelfCallAreStructural)
{
    const String text = "TYPE_IF(N = 0, T, Tuple(head T, tail app.Recursive(T, N - 1)))";
    const auto ast = parseTemplateType(text);
    const auto * type_if = ast->as<ASTUDTTypeIf>();
    ASSERT_NE(type_if, nullptr);

    const auto * predicate = type_if->predicate->as<ASTUDTIsZero>();
    ASSERT_NE(predicate, nullptr);
    const auto * predicate_parameter = predicate->parameter_reference->as<ASTUDTValueParameterReference>();
    ASSERT_NE(predicate_parameter, nullptr);
    EXPECT_EQ(predicate_parameter->ordinal, 1);

    const auto * tuple = type_if->else_type->as<ASTTupleDataType>();
    ASSERT_NE(tuple, nullptr);
    const auto tuple_arguments = tuple->getArguments();
    ASSERT_NE(tuple_arguments, nullptr);
    ASSERT_EQ(tuple_arguments->children.size(), 2);
    const auto * self_reference = tuple_arguments->children[1]->as<ASTUDTReference>();
    ASSERT_NE(self_reference, nullptr);
    const auto self_arguments = self_reference->getArguments();
    ASSERT_NE(self_arguments, nullptr);
    ASSERT_EQ(self_arguments->children.size(), 2);
    const auto * decrement = self_arguments->children[1]->as<ASTUDTDecrement>();
    ASSERT_NE(decrement, nullptr);
    EXPECT_EQ(decrement->amount, 1);

    const auto clone = ast->clone();
    EXPECT_EQ(ast->getTreeHash(false), clone->getTreeHash(false));
    EXPECT_EQ(ast->formatWithSecretsOneLine(), text);
    EXPECT_EQ(parseTemplateType(ast->formatWithSecretsOneLine())->getTreeHash(false), ast->getTreeHash(false));

    const auto unqualified_spelling = parseTemplateType("Recursive(T, N - 1)");
    const auto * canonical_self_reference = unqualified_spelling->as<ASTUDTReference>();
    ASSERT_NE(canonical_self_reference, nullptr);
    EXPECT_EQ(canonical_self_reference->database_name, "app");
    EXPECT_EQ(unqualified_spelling->formatWithSecretsOneLine(), "app.Recursive(T, N - 1)");
}

TEST(UDTParser, UnqualifiedDefinitionKeepsDecreasingSelfCallStructural)
{
    ParserUDTExpression parser(unqualifiedNestedTemplateContext());
    const String text = "TYPE_IF(N = 0, T, Recursive(T, N - 1))";
    const auto ast = parseQuery(parser, text, "unqualified user-defined type template parser test", 0, 150, 0);
    ASSERT_NE(ast->as<ASTUDTTypeIf>(), nullptr);
    EXPECT_EQ(ast->formatWithSecretsOneLine(), text);

    ParserUDTExpression reparsing_parser(unqualifiedNestedTemplateContext());
    const auto reparsed
        = parseQuery(reparsing_parser, ast->formatWithSecretsOneLine(), "unqualified user-defined type template parser test", 0, 150, 0);
    EXPECT_EQ(ast->getTreeHash(false), reparsed->getTreeHash(false));
}

TEST(UDTParser, TemplateGrammarRejectsRuntimeAndNonDecreasingForms)
{
    expectTemplateParseFailure("N");
    expectTemplateParseFailure("TYPE_IF(N = 1, T, UInt64)");
    expectTemplateParseFailure("TYPE_IF(N > 0, T, UInt64)");
    expectTemplateParseFailure("app.Recursive(UInt64, N - 1)");
    expectTemplateParseFailure("Recursive");
    expectTemplateParseFailure("app.Recursive");
    expectTemplateParseFailure("app.Recursive(T, N)");
    expectTemplateParseFailure("app.Other(T, N - 1)");
    expectTemplateParseFailure("app.Recursive(T, N - 1,)");
    expectTemplateParseFailure("`TYPE_IF`(UInt8)");

    UDTExpressionParserContext invalid_context;
    invalid_context.definition_name = "Broken";
    invalid_context.parameters.push_back({.name = "T", .ordinal = 0});
    ParserUDTExpression invalid_parser(std::move(invalid_context));
    EXPECT_THROW(
        static_cast<void>(parseQuery(invalid_parser, "T", "invalid user-defined type template parser test", 0, 150, 0)), Exception);
}
