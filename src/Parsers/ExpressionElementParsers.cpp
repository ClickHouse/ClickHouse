#include <cerrno>
#include <cstdlib>
#include <Poco/String.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <Common/BinStringDecodeHelper.h>
#include <Common/PODArray.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCollation.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserExplainQuery.h>
#include <Parsers/queryToString.h>

#include <Interpreters/StorageID.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
}

/*
 * Build an AST with the following structure:
 *
 * ```
 * SelectWithUnionQuery (children 1)
 *  ExpressionList (children 1)
 *   SelectQuery (children 2)
 *    ExpressionList (children 1)
 *     Asterisk
 *    TablesInSelectQuery (children 1)
 *     TablesInSelectQueryElement (children 1)
 *      TableExpression (children 1)
 *       Function <...>
 * ```
 */
static ASTPtr buildSelectFromTableFunction(const std::shared_ptr<ASTFunction> & ast_function)
{
    auto result_select_query = std::make_shared<ASTSelectWithUnionQuery>();

    {
        auto select_ast = std::make_shared<ASTSelectQuery>();
        select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
        select_ast->select()->children.push_back(std::make_shared<ASTAsterisk>());

        auto list_of_selects = std::make_shared<ASTExpressionList>();
        list_of_selects->children.push_back(select_ast);

        result_select_query->children.push_back(std::move(list_of_selects));
        result_select_query->list_of_selects = result_select_query->children.back();

        {
            auto tables = std::make_shared<ASTTablesInSelectQuery>();
            select_ast->setExpression(ASTSelectQuery::Expression::TABLES, tables);
            auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_expr = std::make_shared<ASTTableExpression>();
            tables->children.push_back(tables_elem);
            tables_elem->table_expression = table_expr;
            tables_elem->children.push_back(table_expr);

            table_expr->table_function = ast_function;
            table_expr->children.push_back(table_expr->table_function);
        }
    }

    return result_select_query;
}

bool ParserSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserSelectWithUnionQuery select;
    ParserExplainQuery explain;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ASTPtr result_node = nullptr;

    if (ASTPtr select_node; select.parse(pos, select_node, expected))
    {
        result_node = std::move(select_node);
    }
    else if (ASTPtr explain_node; explain.parse(pos, explain_node, expected))
    {
        const auto & explain_query = explain_node->as<const ASTExplainQuery &>();

        if (explain_query.getTableFunction() || explain_query.getTableOverride())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "EXPLAIN in a subquery cannot have a table function or table override");

        /// Replace subquery `(EXPLAIN <kind> <explain_settings> SELECT ...)`
        /// with `(SELECT * FROM viewExplain('<kind>', '<explain_settings>', (SELECT ...)))`

        String kind_str = ASTExplainQuery::toString(explain_query.getKind());

        String settings_str;
        if (ASTPtr settings_ast = explain_query.getSettings())
        {
            if (!settings_ast->as<ASTSetQuery>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "EXPLAIN settings must be a SET query");
            settings_str = queryToString(settings_ast);
        }

        const ASTPtr & explained_ast = explain_query.getExplainedQuery();
        if (explained_ast)
        {
            auto view_explain = makeASTFunction("viewExplain",
                std::make_shared<ASTLiteral>(kind_str),
                std::make_shared<ASTLiteral>(settings_str),
                std::make_shared<ASTSubquery>(explained_ast));
            result_node = buildSelectFromTableFunction(view_explain);
        }
        else
        {
            auto view_explain = makeASTFunction("viewExplain",
                std::make_shared<ASTLiteral>(kind_str),
                std::make_shared<ASTLiteral>(settings_str));
            result_node = buildSelectFromTableFunction(view_explain);
        }
    }
    else
    {
        return false;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    node = std::make_shared<ASTSubquery>(std::move(result_node));
    return true;
}


bool ParserIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Identifier in backquotes or in double quotes or in English-style Unicode double quotes
    if (pos->type == TokenType::QuotedIdentifier)
    {
        /// The case of Unicode quotes. No escaping is supported. Assuming UTF-8.
        if (*pos->begin == '\xE2' && pos->size() > 6) /// Empty identifiers are not allowed.
        {
            node = std::make_shared<ASTIdentifier>(String(pos->begin + 3, pos->end - 3));
            ++pos;
            return true;
        }

        ReadBufferFromMemory buf(pos->begin, pos->size());
        String s;

        if (*pos->begin == '`')
            readBackQuotedStringWithSQLStyle(s, buf);
        else
            readDoubleQuotedStringWithSQLStyle(s, buf);

        if (s.empty())    /// Identifiers "empty string" are not allowed.
            return false;

        node = std::make_shared<ASTIdentifier>(s);
        ++pos;
        return true;
    }
    if (pos->type == TokenType::BareWord)
    {
        node = std::make_shared<ASTIdentifier>(String(pos->begin, pos->end));
        ++pos;
        return true;
    }
    if (allow_query_parameter && pos->type == TokenType::OpeningCurlyBrace)
    {
        ++pos;
        if (pos->type != TokenType::BareWord)
        {
            expected.add(pos, "substitution name (identifier)");
            return false;
        }

        String name(pos->begin, pos->end);
        ++pos;

        if (pos->type != TokenType::Colon)
        {
            expected.add(pos, "colon between name and type");
            return false;
        }

        ++pos;

        if (pos->type != TokenType::BareWord)
        {
            expected.add(pos, "substitution type (identifier)");
            return false;
        }

        String type(pos->begin, pos->end);
        ++pos;

        if (type != "Identifier")
        {
            expected.add(pos, "substitution type (identifier)");
            return false;
        }

        if (pos->type != TokenType::ClosingCurlyBrace)
        {
            expected.add(pos, "closing curly brace");
            return false;
        }
        ++pos;

        node = std::make_shared<ASTIdentifier>("", std::make_shared<ASTQueryParameter>(name, type));
        return true;
    }
    return false;
}


bool ParserTableAsStringLiteralIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::StringLiteral)
        return false;

    ReadBufferFromMemory in(pos->begin, pos->size());
    String s;

    if (!tryReadQuotedString(s, in))
    {
        expected.add(pos, "string literal");
        return false;
    }

    if (in.count() != pos->size())
    {
        expected.add(pos, "string literal");
        return false;
    }

    if (s.empty())
    {
        expected.add(pos, "non-empty string literal");
        return false;
    }

    node = std::make_shared<ASTTableIdentifier>(s);
    ++pos;
    return true;
}

namespace
{

/// Parser of syntax sugar for reading JSON subcolumns of type Array(JSON):
/// json.a.b[][].c -> json.a.b.:Array(Array(JSON)).c
class ParserArrayOfJSONIdentifierAddition : public IParserBase
{
public:
    String getLastArrayOfJSONSubcolumnIdentifier() const
    {
        String subcolumn = ":`";
        for (size_t i = 0; i != last_array_level; ++i)
            subcolumn += "Array(";
        subcolumn += "JSON";
        for (size_t i = 0; i != last_array_level; ++i)
            subcolumn += ")";
        return subcolumn + "`";
    }

protected:
    const char * getName() const override { return "ParserArrayOfJSONIdentifierDelimiter"; }

    bool parseImpl(Pos & pos, ASTPtr & /*node*/, Expected & expected) override
    {
        last_array_level = 0;
        ParserTokenSequence brackets_parser(std::vector<TokenType>{TokenType::OpeningSquareBracket, TokenType::ClosingSquareBracket});
        if (!brackets_parser.check(pos, expected))
            return false;
        ++last_array_level;
        while (brackets_parser.check(pos, expected))
            ++last_array_level;
        return true;
    }

private:
    size_t last_array_level;
};

}

bool ParserCompoundIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto element_parser = std::make_unique<ParserIdentifier>(allow_query_parameter, highlight_type);
    std::vector<std::pair<ParserPtr, SpecialDelimiter>> delimiter_parsers;
    delimiter_parsers.emplace_back(std::make_unique<ParserTokenSequence>(std::vector<TokenType>{TokenType::Dot, TokenType::Colon}), SpecialDelimiter::JSON_PATH_DYNAMIC_TYPE);
    delimiter_parsers.emplace_back(std::make_unique<ParserTokenSequence>(std::vector<TokenType>{TokenType::Dot, TokenType::Caret}), SpecialDelimiter::JSON_PATH_PREFIX);
    delimiter_parsers.emplace_back(std::make_unique<ParserToken>(TokenType::Dot), SpecialDelimiter::NONE);
    ParserArrayOfJSONIdentifierAddition array_of_json_identifier_addition;

    std::vector<String> parts;
    SpecialDelimiter last_special_delimiter = SpecialDelimiter::NONE;
    ASTs params;

    bool is_first = true;
    Pos begin = pos;
    while (true)
    {
        ASTPtr element;
        if (!element_parser->parse(pos, element, expected))
        {
            if (is_first)
                return false;
            pos = begin;
            break;
        }

        if (last_special_delimiter != SpecialDelimiter::NONE)
        {
            parts.push_back(static_cast<char>(last_special_delimiter) + backQuote(getIdentifierName(element)));
        }
        else
        {
            parts.push_back(getIdentifierName(element));
            /// Check if we have Array of JSON subcolumn additioon after identifier
            /// and replace it with corresponding type subcolumn.
            if (!is_first && array_of_json_identifier_addition.check(pos, expected))
                parts.push_back(array_of_json_identifier_addition.getLastArrayOfJSONSubcolumnIdentifier());
        }

        if (parts.back().empty())
            params.push_back(element->as<ASTIdentifier>()->getParam());

        is_first = false;
        begin = pos;
        bool parsed_delimiter = false;
        for (const auto & [parser, special_delimiter] : delimiter_parsers)
        {
            if (parser->check(pos, expected))
            {
                parsed_delimiter = true;
                last_special_delimiter = special_delimiter;
                break;
            }
        }

        if (!parsed_delimiter)
        {
            pos = begin;
            break;
        }
    }

    ParserKeyword s_uuid(Keyword::UUID);
    UUID uuid = UUIDHelpers::Nil;

    if (table_name_with_optional_uuid)
    {
        if (parts.size() > 2)
            return false;

        if (s_uuid.ignore(pos, expected))
        {
            ParserStringLiteral uuid_p;
            ASTPtr ast_uuid;
            if (!uuid_p.parse(pos, ast_uuid, expected))
                return false;
            uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.safeGet<String>());
        }

        if (parts.size() == 1) node = std::make_shared<ASTTableIdentifier>(parts[0], std::move(params));
        else node = std::make_shared<ASTTableIdentifier>(parts[0], parts[1], std::move(params));
        node->as<ASTTableIdentifier>()->uuid = uuid;
    }
    else
        node = std::make_shared<ASTIdentifier>(std::move(parts), false, std::move(params));

    return true;
}


ASTPtr createFunctionCast(const ASTPtr & expr_ast, const ASTPtr & type_ast)
{
    /// Convert to canonical representation in functional form: CAST(expr, 'type')
    auto type_literal = std::make_shared<ASTLiteral>(queryToString(type_ast));
    return makeASTFunction("CAST", expr_ast, std::move(type_literal));
}


bool ParserFilterClause::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    assert(node);
    ASTFunction & function = dynamic_cast<ASTFunction &>(*node);

    ParserToken parser_opening_bracket(TokenType::OpeningRoundBracket);
    if (!parser_opening_bracket.ignore(pos, expected))
    {
        return false;
    }

    ParserKeyword parser_where(Keyword::WHERE);
    if (!parser_where.ignore(pos, expected))
    {
        return false;
    }
    ParserExpressionList parser_condition(false);
    ASTPtr condition;
    if (!parser_condition.parse(pos, condition, expected) || condition->children.size() != 1)
    {
        return false;
    }

    ParserToken parser_closing_bracket(TokenType::ClosingRoundBracket);
    if (!parser_closing_bracket.ignore(pos, expected))
    {
        return false;
    }

    if (function.name == "count")
    {
        /// Remove child from function.arguments if it's '*' because countIf(*) is not supported.
        /// See https://github.com/ClickHouse/ClickHouse/issues/61004
        std::erase_if(function.arguments->children, [] (const ASTPtr & child)
        {
            return typeid_cast<const ASTAsterisk *>(child.get()) || typeid_cast<const ASTQualifiedAsterisk *>(child.get());
        });
    }

    function.name += "If";
    function.arguments->children.push_back(condition->children[0]);
    return true;
}

bool ParserWindowReference::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    assert(node);
    ASTFunction & function = dynamic_cast<ASTFunction &>(*node);

    // Variant 1:
    // function_name ( * ) OVER window_name
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        ASTPtr window_name_ast;
        ParserIdentifier window_name_parser;
        if (window_name_parser.parse(pos, window_name_ast, expected))
        {
            function.window_name = getIdentifierName(window_name_ast);
            return true;
        }

        return false;
    }

    // Variant 2:
    // function_name ( * ) OVER ( window_definition )
    ParserWindowDefinition parser_definition;
    return parser_definition.parse(pos, function.window_definition, expected);
}

static bool tryParseFrameDefinition(ASTWindowDefinition * node, IParser::Pos & pos,
    Expected & expected)
{
    ParserKeyword keyword_rows(Keyword::ROWS);
    ParserKeyword keyword_groups(Keyword::GROUPS);
    ParserKeyword keyword_range(Keyword::RANGE);

    node->frame_is_default = false;
    if (keyword_rows.ignore(pos, expected))
    {
        node->frame_type = WindowFrame::FrameType::ROWS;
    }
    else if (keyword_groups.ignore(pos, expected))
    {
        node->frame_type = WindowFrame::FrameType::GROUPS;
    }
    else if (keyword_range.ignore(pos, expected))
    {
        node->frame_type = WindowFrame::FrameType::RANGE;
    }
    else
    {
        /* No frame clause. */
        node->frame_is_default = true;
        return true;
    }

    ParserKeyword keyword_between(Keyword::BETWEEN);
    ParserKeyword keyword_unbounded(Keyword::UNBOUNDED);
    ParserKeyword keyword_preceding(Keyword::PRECEDING);
    ParserKeyword keyword_following(Keyword::FOLLOWING);
    ParserKeyword keyword_and(Keyword::AND);
    ParserKeyword keyword_current_row(Keyword::CURRENT_ROW);

    // There are two variants of grammar for the frame:
    // 1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    // 2) ROWS UNBOUNDED PRECEDING
    // When the frame end is not specified (2), it defaults to CURRENT ROW.
    const bool has_frame_end = keyword_between.ignore(pos, expected);

    if (keyword_current_row.ignore(pos, expected))
    {
        node->frame_begin_type = WindowFrame::BoundaryType::Current;
    }
    else
    {
        ParserExpression parser_expression;
        if (keyword_unbounded.ignore(pos, expected))
        {
            node->frame_begin_type = WindowFrame::BoundaryType::Unbounded;
        }
        else if (parser_expression.parse(pos, node->frame_begin_offset, expected))
        {
            // We will evaluate the expression for offset expression later.
            node->frame_begin_type = WindowFrame::BoundaryType::Offset;
        }
        else
        {
            return false;
        }

        if (keyword_preceding.ignore(pos, expected))
        {
            node->frame_begin_preceding = true;
        }
        else if (keyword_following.ignore(pos, expected))
        {
            node->frame_begin_preceding = false;
            if (node->frame_begin_type == WindowFrame::BoundaryType::Unbounded)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Frame start cannot be UNBOUNDED FOLLOWING");
            }
        }
        else
        {
            return false;
        }
    }

    if (has_frame_end)
    {
        if (!keyword_and.ignore(pos, expected))
        {
            return false;
        }

        if (keyword_current_row.ignore(pos, expected))
        {
            node->frame_end_type = WindowFrame::BoundaryType::Current;
        }
        else
        {
            ParserExpression parser_expression;
            if (keyword_unbounded.ignore(pos, expected))
            {
                node->frame_end_type = WindowFrame::BoundaryType::Unbounded;
            }
            else if (parser_expression.parse(pos, node->frame_end_offset, expected))
            {
                // We will evaluate the expression for offset expression later.
                node->frame_end_type = WindowFrame::BoundaryType::Offset;
            }
            else
            {
                return false;
            }

            if (keyword_preceding.ignore(pos, expected))
            {
                node->frame_end_preceding = true;
                if (node->frame_end_type == WindowFrame::BoundaryType::Unbounded)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Frame end cannot be UNBOUNDED PRECEDING");
                }
            }
            else if (keyword_following.ignore(pos, expected))
            {
                // Positive offset or UNBOUNDED FOLLOWING.
                node->frame_end_preceding = false;
            }
            else
            {
                return false;
            }
        }
    }

    return true;
}

// All except parent window name.
static bool parseWindowDefinitionParts(IParser::Pos & pos,
    ASTWindowDefinition & node, Expected & expected)
{
    ParserKeyword keyword_partition_by(Keyword::PARTITION_BY);
    ParserNotEmptyExpressionList columns_partition_by(
        false /* we don't allow declaring aliases here*/);
    ParserKeyword keyword_order_by(Keyword::ORDER_BY);
    ParserOrderByExpressionList columns_order_by;

    if (keyword_partition_by.ignore(pos, expected))
    {
        ASTPtr partition_by_ast;
        if (columns_partition_by.parse(pos, partition_by_ast, expected))
        {
            node.children.push_back(partition_by_ast);
            node.partition_by = partition_by_ast;
        }
        else
        {
            return false;
        }
    }

    if (keyword_order_by.ignore(pos, expected))
    {
        ASTPtr order_by_ast;
        if (columns_order_by.parse(pos, order_by_ast, expected))
        {
            node.children.push_back(order_by_ast);
            node.order_by = order_by_ast;
        }
        else
        {
            return false;
        }
    }

    return tryParseFrameDefinition(&node, pos, expected);
}

bool ParserWindowDefinition::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto result = std::make_shared<ASTWindowDefinition>();

    ParserToken parser_openging_bracket(TokenType::OpeningRoundBracket);
    if (!parser_openging_bracket.ignore(pos, expected))
    {
        return false;
    }

    // We can have a parent window name specified before all other things. No
    // easy way to distinguish identifier from keywords, so just try to parse it
    // both ways.
    if (parseWindowDefinitionParts(pos, *result, expected))
    {
        // Successfully parsed without parent window specifier. It can be empty,
        // so check that it is followed by the closing bracket.
        ParserToken parser_closing_bracket(TokenType::ClosingRoundBracket);
        if (parser_closing_bracket.ignore(pos, expected))
        {
            node = result;
            return true;
        }
    }

    // Try to parse with parent window specifier.
    ParserIdentifier parser_parent_window;
    ASTPtr window_name_identifier;
    if (!parser_parent_window.parse(pos, window_name_identifier, expected))
    {
        return false;
    }
    result->parent_window_name = window_name_identifier->as<const ASTIdentifier &>().name();

    if (!parseWindowDefinitionParts(pos, *result, expected))
    {
        return false;
    }

    ParserToken parser_closing_bracket(TokenType::ClosingRoundBracket);
    if (!parser_closing_bracket.ignore(pos, expected))
    {
        return false;
    }

    node = result;
    return true;
}

bool ParserWindowList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto result = std::make_shared<ASTExpressionList>();

    for (;;)
    {
        auto elem = std::make_shared<ASTWindowListElement>();

        ParserIdentifier parser_window_name;
        ASTPtr window_name_identifier;
        if (!parser_window_name.parse(pos, window_name_identifier, expected))
        {
            return false;
        }
        elem->name = getIdentifierName(window_name_identifier);

        ParserKeyword keyword_as(Keyword::AS);
        if (!keyword_as.ignore(pos, expected))
        {
            return false;
        }

        ParserWindowDefinition parser_window_definition;
        if (!parser_window_definition.parse(pos, elem->definition, expected))
        {
            return false;
        }

        result->children.push_back(elem);

        // If the list countinues, there should be a comma.
        ParserToken parser_comma(TokenType::Comma);
        if (!parser_comma.ignore(pos))
        {
            break;
        }
    }

    node = result;
    return true;
}

bool ParserCodecDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserList(std::make_unique<ParserIdentifierWithOptionalParameters>(),
        std::make_unique<ParserToken>(TokenType::Comma), false).parse(pos, node, expected);
}

bool ParserCodec::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserCodecDeclarationList codecs;
    ASTPtr expr_list_args;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;

    ++pos;
    if (!codecs.parse(pos, expr_list_args, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "CODEC";
    function_node->kind = ASTFunction::Kind::CODEC;
    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    node = function_node;
    return true;
}

bool ParserStatisticsType::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserList stat_type_parser(std::make_unique<ParserIdentifierWithOptionalParameters>(),
        std::make_unique<ParserToken>(TokenType::Comma), false);

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ASTPtr stat_type;

    ++pos;

    if (!stat_type_parser.parse(pos, stat_type, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "STATISTICS";
    function_node->kind = ASTFunction::Kind::STATISTICS;
    function_node->arguments = stat_type;
    function_node->children.push_back(function_node->arguments);
    node = function_node;
    return true;
}

bool ParserCollation::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr collation;

    if (!ParserIdentifier(true).parse(pos, collation, expected))
        return false;

    // check the collation name is valid
    const String name = getIdentifierName(collation);

    bool valid_collation = name == "binary" ||
                           endsWith(name, "_bin") ||
                           endsWith(name, "_ci") ||
                           endsWith(name, "_cs") ||
                           endsWith(name, "_ks");

    if (!valid_collation)
        return false;

    auto collation_node = std::make_shared<ASTCollation>();
    collation_node->collation = collation;
    node = collation_node;
    return true;
}


template <TokenType ...tokens>
static bool isOneOf(TokenType token)
{
    return ((token == tokens) || ...);
}

bool ParserCastOperator::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    using enum TokenType;

    /// Parse numbers (including decimals), strings, arrays and tuples of them.

    Pos begin = pos;
    const char * data_begin = pos->begin;
    const char * data_end = pos->end;
    ASTPtr string_literal;

    if (pos->type == Minus)
    {
        ++pos;
        if (pos->type != Number)
            return false;

        data_end = pos->end;
        ++pos;
    }
    else if (pos->type == Number)
    {
        ++pos;
    }
    else if (pos->type == StringLiteral)
    {
        if (!ParserStringLiteral().parse(begin, string_literal, expected))
            return false;
    }
    else if (isOneOf<OpeningSquareBracket, OpeningRoundBracket>(pos->type))
    {
        TokenType last_token = OpeningSquareBracket;
        std::vector<TokenType> stack;
        while (pos.isValid())
        {
            if (isOneOf<OpeningSquareBracket, OpeningRoundBracket>(pos->type))
            {
                stack.push_back(pos->type);
                if (!isOneOf<OpeningSquareBracket, OpeningRoundBracket, Comma>(last_token))
                    return false;
            }
            else if (pos->type == ClosingSquareBracket)
            {
                if (isOneOf<Comma, OpeningRoundBracket, Minus>(last_token))
                    return false;
                if (stack.empty() || stack.back() != OpeningSquareBracket)
                    return false;
                stack.pop_back();
            }
            else if (pos->type == ClosingRoundBracket)
            {
                if (isOneOf<Comma, OpeningSquareBracket, Minus>(last_token))
                    return false;
                if (stack.empty() || stack.back() != OpeningRoundBracket)
                    return false;
                stack.pop_back();
            }
            else if (pos->type == Comma)
            {
                if (isOneOf<OpeningSquareBracket, OpeningRoundBracket, Comma, Minus>(last_token))
                    return false;
                if (stack.empty())
                    break;
            }
            else if (pos->type == Number)
            {
                if (!isOneOf<OpeningSquareBracket, OpeningRoundBracket, Comma, Minus>(last_token))
                    return false;
            }
            else if (isOneOf<StringLiteral, Minus>(pos->type))
            {
                if (!isOneOf<OpeningSquareBracket, OpeningRoundBracket, Comma>(last_token))
                    return false;
            }
            else
            {
                break;
            }

            /// Update data_end on every iteration to avoid appearances of extra trailing
            /// whitespaces into data. Whitespaces are skipped at operator '++' of Pos.
            data_end = pos->end;
            last_token = pos->type;
            ++pos;
        }

        if (!stack.empty())
            return false;
    }
    else
        return false;

    ASTPtr type_ast;
    if (ParserToken(DoubleColon).ignore(pos, expected)
        && ParserDataType().parse(pos, type_ast, expected))
    {
        size_t data_size = data_end - data_begin;
        if (string_literal)
        {
            node = createFunctionCast(string_literal, type_ast);
            return true;
        }

        auto literal = std::make_shared<ASTLiteral>(String(data_begin, data_size));
        node = createFunctionCast(literal, type_ast);
        return true;
    }

    return false;
}


bool ParserNull::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword nested_parser(Keyword::NULL_KEYWORD);
    if (nested_parser.parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(Null());
        return true;
    }
    return false;
}


bool ParserBool::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserKeyword(Keyword::TRUE_KEYWORD).parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(true);
        return true;
    }
    if (ParserKeyword(Keyword::FALSE_KEYWORD).parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(false);
        return true;
    }
    return false;
}

static bool parseNumber(char * buffer, size_t size, bool negative, int base, Field & res)
{
    errno = 0;    /// Functions strto* don't clear errno.

    char * pos_integer = buffer;
    UInt64 uint_value = std::strtoull(buffer, &pos_integer, base);

    if (pos_integer == buffer + size && errno != ERANGE && (!negative || uint_value <= (1ULL << 63)))
    {
        /// -0 should be still parsed as UInt instead of Int,
        /// because otherwise it is not preserved during formatting-parsing roundtrip
        /// (the signedness is lost during formatting)

        if (negative && uint_value != 0)
            res = static_cast<Int64>(-uint_value);
        else
            res = uint_value;

        return true;
    }

    return false;
}

bool ParserNumber::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos literal_begin = pos;
    bool negative = false;

    if (pos->type == TokenType::Minus)
    {
        ++pos;
        negative = true;
    }
    else if (pos->type == TokenType::Plus)  /// Leading plus is simply ignored.
        ++pos;

    Field res;

    if (!pos.isValid())
        return false;

    auto try_read_float = [&](const char * it, const char * end)
    {
        std::string buf(it, end); /// Copying is needed to ensure the string is 0-terminated.
        char * str_end;
        errno = 0;    /// Functions strto* don't clear errno.
        /// The usage of strtod is needed, because we parse hex floating point literals as well.
        Float64 float_value = std::strtod(buf.c_str(), &str_end);
        if (str_end == buf.c_str() + buf.size() && errno != ERANGE)
        {
            if (float_value < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Token number cannot begin with minus, "
                                "but parsed float number is less than zero.");

            if (negative)
                float_value = -float_value;

            res = float_value;

            auto literal = std::make_shared<ASTLiteral>(res);
            literal->begin = literal_begin;
            literal->end = ++pos;
            node = literal;

            return true;
        }

        expected.add(pos, "number");
        return false;
    };

    /// NaN and Inf
    if (pos->type == TokenType::BareWord)
    {
        return try_read_float(pos->begin, pos->end);
    }

    if (pos->type != TokenType::Number)
    {
        expected.add(pos, "number");
        return false;
    }

    /** Maximum length of number. 319 symbols is enough to write maximum double in decimal form.
      * Copy is needed to use strto* functions, which require 0-terminated string.
      */
    static constexpr size_t MAX_LENGTH_OF_NUMBER = 319;

    char buf[MAX_LENGTH_OF_NUMBER + 1];

    size_t buf_size = 0;
    for (const auto * it = pos->begin; it != pos->end; ++it)
    {
        if (*it != '_')
        {
            buf[buf_size] = *it;
            ++buf_size;
        }
        if (unlikely(buf_size > MAX_LENGTH_OF_NUMBER))
        {
            expected.add(pos, "number");
            return false;
        }
    }

    size_t size = buf_size;
    buf[size] = 0;
    char * start_pos = buf;

    if (*start_pos == '0')
    {
        ++start_pos;
        --size;

        /// binary
        if (*start_pos == 'b')
        {
            ++start_pos;
            --size;
            if (parseNumber(start_pos, size, negative, 2, res))
            {
                auto literal = std::make_shared<ASTLiteral>(res);
                literal->begin = literal_begin;
                literal->end = ++pos;
                node = literal;

                return true;
            }
            return false;
        }

        /// hexadecimal
        if (*start_pos == 'x' || *start_pos == 'X')
        {
            ++start_pos;
            --size;
            if (parseNumber(start_pos, size, negative, 16, res))
            {
                auto literal = std::make_shared<ASTLiteral>(res);
                literal->begin = literal_begin;
                literal->end = ++pos;
                node = literal;

                return true;
            }
        }
        else
        {
            /// possible leading zeroes in integer
            while (*start_pos == '0')
            {
                ++start_pos;
                --size;
            }
            if (parseNumber(start_pos, size, negative, 10, res))
            {
                auto literal = std::make_shared<ASTLiteral>(res);
                literal->begin = literal_begin;
                literal->end = ++pos;
                node = literal;

                return true;
            }
        }
    }
    else if (parseNumber(start_pos, size, negative, 10, res))
    {
        auto literal = std::make_shared<ASTLiteral>(res);
        literal->begin = literal_begin;
        literal->end = ++pos;
        node = literal;

        return true;
    }

    return try_read_float(buf, buf + buf_size);
}


bool ParserUnsignedInteger::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Field res;

    if (!pos.isValid())
        return false;

    UInt64 x = 0;
    ReadBufferFromMemory in(pos->begin, pos->size());
    if (!tryReadIntText(x, in) || in.count() != pos->size())
    {
        expected.add(pos, "unsigned integer");
        return false;
    }

    res = x;
    auto literal = std::make_shared<ASTLiteral>(res);
    literal->begin = pos;
    literal->end = ++pos;
    node = literal;
    return true;
}

inline static bool makeStringLiteral(IParser::Pos & pos, ASTPtr & node, String str)
{
    auto literal = std::make_shared<ASTLiteral>(str);
    literal->begin = pos;
    literal->end = ++pos;
    node = literal;
    return true;
}

inline static bool makeHexOrBinStringLiteral(IParser::Pos & pos, ASTPtr & node, bool hex, size_t word_size)
{
    const char * str_begin = pos->begin + 2;
    const char * str_end = pos->end - 1;
    if (str_begin == str_end)
        return makeStringLiteral(pos, node, "");

    PODArray<UInt8> res;
    res.resize((pos->size() + word_size) / word_size + 1);
    char * res_begin = reinterpret_cast<char *>(res.data());
    char * res_pos = res_begin;

    if (hex)
    {
        hexStringDecode(str_begin, str_end, res_pos, word_size);
    }
    else
    {
        binStringDecode(str_begin, str_end, res_pos, word_size);
    }

    return makeStringLiteral(pos, node, String(reinterpret_cast<char *>(res.data()), (res_pos - res_begin - 1)));
}

bool ParserStringLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::StringLiteral && pos->type != TokenType::HereDoc)
        return false;

    String s;

    if (pos->type == TokenType::StringLiteral)
    {
        char first_char = *pos->begin;

        if (first_char == 'x' || first_char == 'X')
        {
            constexpr size_t word_size = 2;
            return makeHexOrBinStringLiteral(pos, node, true, word_size);
        }

        if (first_char == 'b' || first_char == 'B')
        {
            constexpr size_t word_size = 8;
            return makeHexOrBinStringLiteral(pos, node, false, word_size);
        }

        /// The case of Unicode quotes. No escaping is supported. Assuming UTF-8.
        if (first_char == '\xE2' && pos->size() >= 6)
        {
            return makeStringLiteral(pos, node, String(pos->begin + 3, pos->end - 3));
        }

        ReadBufferFromMemory in(pos->begin, pos->size());

        try
        {
            readQuotedStringWithSQLStyle(s, in);
        }
        catch (const Exception &)
        {
            expected.add(pos, "string literal");
            return false;
        }

        if (in.count() != pos->size())
        {
            expected.add(pos, "string literal");
            return false;
        }
    }
    else if (pos->type == TokenType::HereDoc)
    {
        std::string_view here_doc(pos->begin, pos->size());
        size_t heredoc_size = here_doc.find('$', 1) + 1;
        assert(heredoc_size != std::string_view::npos);
        s = String(pos->begin + heredoc_size, pos->size() - heredoc_size * 2);
    }

    return makeStringLiteral(pos, node, s);
}

template <typename Collection>
struct CollectionOfLiteralsLayer
{
    explicit CollectionOfLiteralsLayer(IParser::Pos & pos) : literal_begin(pos)
    {
        ++pos;
    }

    IParser::Pos literal_begin;
    Collection arr;
};

template <typename Collection>
bool ParserCollectionOfLiterals<Collection>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != opening_bracket)
        return false;

    std::vector<CollectionOfLiteralsLayer<Collection>> layers;
    layers.emplace_back(pos);
    pos.increaseDepth();

    ParserLiteral literal_p;

    while (pos.isValid())
    {
        if (!layers.back().arr.empty())
        {
            if (pos->type == closing_bracket)
            {
                std::shared_ptr<ASTLiteral> literal;

                /// Parse one-element tuples (e.g. (1)) later as single values for backward compatibility.
                if (std::is_same_v<Collection, Tuple> && layers.back().arr.size() == 1)
                    return false;

                literal = std::make_shared<ASTLiteral>(std::move(layers.back().arr));
                literal->begin = layers.back().literal_begin;
                literal->end = ++pos;

                layers.pop_back();
                pos.decreaseDepth();

                if (layers.empty())
                {
                    node = literal;
                    return true;
                }

                layers.back().arr.push_back(literal->value);
                continue;
            }
            if (pos->type == TokenType::Comma)
            {
                ++pos;
            }
            else if (pos->type == TokenType::Colon && std::is_same_v<Collection, Map> && layers.back().arr.size() % 2 == 1)
            {
                ++pos;
            }
            else
            {
                expected.add(pos, "comma or closing bracket");
                return false;
            }
        }

        ASTPtr literal_node;
        if (literal_p.parse(pos, literal_node, expected))
        {
            layers.back().arr.push_back(literal_node->as<ASTLiteral &>().value);
        }
        else if (pos->type == opening_bracket)
        {
            layers.emplace_back(pos);
            pos.increaseDepth();
        }
        else
            return false;
    }

    expected.add(pos, getTokenName(closing_bracket));
    return false;
}

template bool ParserCollectionOfLiterals<Array>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
template bool ParserCollectionOfLiterals<Tuple>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected);


namespace
{

class ICollection;
using Collections = std::vector<std::unique_ptr<ICollection>>;

class ICollection
{
public:
    virtual ~ICollection() = default;
    virtual bool parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected, bool allow_map) = 0;
};

template <class Container, TokenType end_token>
class CommonCollection : public ICollection
{
public:
    explicit CommonCollection(const IParser::Pos & pos) : begin(pos) {}

    bool parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected, bool allow_map) override;

private:
    Container container;
    IParser::Pos begin;
};

class MapCollection : public ICollection
{
public:
    explicit MapCollection(const IParser::Pos & pos) : begin(pos) {}

    bool parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected, bool allow_map) override;

private:
    Map container;
    IParser::Pos begin;
};

bool parseAllCollectionsStart(IParser::Pos & pos, Collections & collections, Expected & /*expected*/, bool allow_map)
{
    if (allow_map && pos->type == TokenType::OpeningCurlyBrace)
        collections.push_back(std::make_unique<MapCollection>(pos));
    else if (pos->type == TokenType::OpeningRoundBracket)
        collections.push_back(std::make_unique<CommonCollection<Tuple, TokenType::ClosingRoundBracket>>(pos));
    else if (pos->type == TokenType::OpeningSquareBracket)
        collections.push_back(std::make_unique<CommonCollection<Array, TokenType::ClosingSquareBracket>>(pos));
    else
        return false;

    ++pos;
    return true;
}

template <class Container, TokenType end_token>
bool CommonCollection<Container, end_token>::parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected, bool allow_map)
{
    if (node)
    {
        container.push_back(std::move(node->as<ASTLiteral &>().value));
        node.reset();
    }

    ASTPtr literal;
    ParserLiteral literal_p;
    ParserToken comma_p(TokenType::Comma);
    ParserToken end_p(end_token);

    while (true)
    {
        if (end_p.ignore(pos, expected))
        {
            auto result = std::make_shared<ASTLiteral>(std::move(container));
            result->begin = begin;
            result->end = pos;

            node = std::move(result);
            break;
        }

        if (!container.empty() && !comma_p.ignore(pos, expected))
            return false;

        if (literal_p.parse(pos, literal, expected))
            container.push_back(std::move(literal->as<ASTLiteral &>().value));
        else
            return parseAllCollectionsStart(pos, collections, expected, allow_map);
    }

    return true;
}

bool MapCollection::parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected, bool allow_map)
{
    if (node)
    {
        container.push_back(std::move(node->as<ASTLiteral &>().value));
        node.reset();
    }

    ASTPtr literal;
    ParserLiteral literal_p;
    ParserToken comma_p(TokenType::Comma);
    ParserToken colon_p(TokenType::Colon);
    ParserToken end_p(TokenType::ClosingCurlyBrace);

    while (true)
    {
        if (end_p.ignore(pos, expected))
        {
            auto result = std::make_shared<ASTLiteral>(std::move(container));
            result->begin = begin;
            result->end = pos;

            node = std::move(result);
            break;
        }

        if (!container.empty() && !comma_p.ignore(pos, expected))
            return false;

        if (!literal_p.parse(pos, literal, expected))
            return false;

        if (!colon_p.parse(pos, literal, expected))
            return false;

        container.push_back(std::move(literal->as<ASTLiteral &>().value));

        if (literal_p.parse(pos, literal, expected))
            container.push_back(std::move(literal->as<ASTLiteral &>().value));
        else
            return parseAllCollectionsStart(pos, collections, expected, allow_map);
    }

    return true;
}

}


bool ParserAllCollectionsOfLiterals::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Collections collections;

    if (!parseAllCollectionsStart(pos, collections, expected, allow_map))
        return false;

    while (!collections.empty())
    {
        if (!collections.back()->parse(pos, collections, node, expected, allow_map))
            return false;

        if (node)
            collections.pop_back();
    }

    return true;
}


bool ParserLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserNull null_p;
    ParserNumber num_p;
    ParserBool bool_p;
    ParserStringLiteral str_p;

    if (null_p.parse(pos, node, expected))
        return true;

    if (num_p.parse(pos, node, expected))
        return true;

    if (bool_p.parse(pos, node, expected))
        return true;

    if (str_p.parse(pos, node, expected))
        return true;

    return false;
}


const char * ParserAlias::restricted_keywords[] =
{
    "ALL",
    "ANTI",
    "ANY",
    "ARRAY",
    "ASOF",
    "BETWEEN",
    "CROSS",
    "PASTE",
    "FINAL",
    "FORMAT",
    "FROM",
    "FULL",
    "GLOBAL",
    "GROUP",
    "HAVING",
    "ILIKE",
    "INNER",
    "INTO",
    "JOIN",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NOT",
    "OFFSET",
    "ON",
    "ONLY", /// YQL's synonym for ANTI. Note: YQL is the name of one of proprietary languages, completely unrelated to ClickHouse.
    "ORDER",
    "PREWHERE",
    "RIGHT",
    "SAMPLE",
    "SEMI",
    "SETTINGS",
    "UNION",
    "USING",
    "WHERE",
    "WINDOW",
    "QUALIFY",
    "WITH",
    "INTERSECT",
    "EXCEPT",
    "ELSE",
    nullptr
};

bool ParserAlias::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_as(Keyword::AS);
    ParserIdentifier id_p{true, Highlight::alias};

    bool has_as_word = s_as.ignore(pos, expected);
    if (!allow_alias_without_as_keyword && !has_as_word)
        return false;

    bool is_quoted = pos->type == TokenType::QuotedIdentifier;

    if (!id_p.parse(pos, node, expected))
        return false;

    if (!has_as_word && !is_quoted)
    {
        /** In this case, the alias can not match the keyword -
          *  so that in the query "SELECT x FROM t", the word FROM was not considered an alias,
          *  and in the query "SELECT x FR FROM t", the word FR was considered an alias.
          */

        const String name = getIdentifierName(node);

        for (const char ** keyword = restricted_keywords; *keyword != nullptr; ++keyword)
            if (0 == strcasecmp(name.data(), *keyword))
                return false;
    }

    return true;
}

bool ParserColumnsTransformers::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword apply(Keyword::APPLY);
    ParserKeyword except(Keyword::EXCEPT);
    ParserKeyword replace(Keyword::REPLACE);
    ParserKeyword as(Keyword::AS);
    ParserKeyword strict(Keyword::STRICT);

    if (allowed_transformers.isSet(ColumnTransformer::APPLY) && apply.ignore(pos, expected))
    {
        bool with_open_round_bracket = false;

        if (pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;
            with_open_round_bracket = true;
        }

        ASTPtr lambda;
        String lambda_arg;
        ASTPtr func_name;
        ASTPtr expr_list_args;
        auto opos = pos;
        if (ParserExpression().parse(pos, lambda, expected))
        {
            if (auto * func = lambda->as<ASTFunction>(); func && func->name == "lambda")
            {
                if (!isASTLambdaFunction(*func))
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Lambda function definition expects two arguments, first argument must be a tuple of arguments");

                const auto * lambda_args_tuple = func->arguments->children.at(0)->as<ASTFunction>();
                if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "First argument of lambda must be a tuple");

                const ASTs & lambda_arg_asts = lambda_args_tuple->arguments->children;
                if (lambda_arg_asts.size() != 1)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "APPLY column transformer can only accept lambda with one argument");

                if (auto opt_arg_name = tryGetIdentifierName(lambda_arg_asts[0]); opt_arg_name)
                    lambda_arg = *opt_arg_name;
                else
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "lambda argument declarations must be identifiers");

                func->is_lambda_function = true;
            }
            else
            {
                lambda = nullptr;
                pos = opos;
            }
        }

        if (!lambda)
        {
            if (!ParserIdentifier().parse(pos, func_name, expected))
                return false;

            if (pos->type == TokenType::OpeningRoundBracket)
            {
                ++pos;
                if (!ParserExpressionList(false).parse(pos, expr_list_args, expected))
                    return false;

                if (pos->type != TokenType::ClosingRoundBracket)
                    return false;
                ++pos;
            }
        }

        String column_name_prefix;
        if (with_open_round_bracket && pos->type == TokenType::Comma)
        {
            ++pos;

            ParserStringLiteral parser_string_literal;
            ASTPtr ast_prefix_name;
            if (!parser_string_literal.parse(pos, ast_prefix_name, expected))
                return false;

            column_name_prefix = ast_prefix_name->as<ASTLiteral &>().value.safeGet<const String &>();
        }

        if (with_open_round_bracket)
        {
            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }

        auto res = std::make_shared<ASTColumnsApplyTransformer>();
        if (lambda)
        {
            res->lambda = lambda;
            res->lambda_arg = lambda_arg;
        }
        else
        {
            res->func_name = getIdentifierName(func_name);
            res->parameters = expr_list_args;
        }
        res->column_name_prefix = column_name_prefix;
        node = std::move(res);
        return true;
    }
    if (allowed_transformers.isSet(ColumnTransformer::EXCEPT) && except.ignore(pos, expected))
    {
        if (strict.ignore(pos, expected))
            is_strict = true;

        ASTs identifiers;
        ASTPtr regexp_node;
        ParserStringLiteral regex;
        auto parse_id = [&identifiers, &pos, &expected]
        {
            ASTPtr identifier;
            if (!ParserIdentifier(true).parse(pos, identifier, expected))
                return false;

            identifiers.emplace_back(std::move(identifier));
            return true;
        };

        if (pos->type == TokenType::OpeningRoundBracket)
        {
            // support one or more parameter
            ++pos;
            if (!ParserList::parseUtil(pos, expected, parse_id, false) && !regex.parse(pos, regexp_node, expected))
                return false;

            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }
        else
        {
            // only one parameter
            if (!parse_id() && !regex.parse(pos, regexp_node, expected))
                return false;
        }

        auto res = std::make_shared<ASTColumnsExceptTransformer>();
        if (regexp_node)
            res->setPattern(regexp_node->as<ASTLiteral &>().value.safeGet<String>());
        else
            res->children = std::move(identifiers);
        res->is_strict = is_strict;
        node = std::move(res);
        return true;
    }
    if (allowed_transformers.isSet(ColumnTransformer::REPLACE) && replace.ignore(pos, expected))
    {
        if (strict.ignore(pos, expected))
            is_strict = true;

        ASTs replacements;
        ParserExpression element_p;
        ParserIdentifier ident_p;
        auto parse_id = [&]
        {
            ASTPtr expr;

            if (!element_p.parse(pos, expr, expected))
                return false;
            if (!as.ignore(pos, expected))
                return false;

            ASTPtr ident;
            if (!ident_p.parse(pos, ident, expected))
                return false;

            auto replacement = std::make_shared<ASTColumnsReplaceTransformer::Replacement>();
            replacement->name = getIdentifierName(ident);
            replacement->children.push_back(std::move(expr));
            replacements.emplace_back(std::move(replacement));
            return true;
        };

        if (pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;

            if (!ParserList::parseUtil(pos, expected, parse_id, false))
                return false;

            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }
        else
        {
            // only one parameter
            if (!parse_id())
                return false;
        }

        auto res = std::make_shared<ASTColumnsReplaceTransformer>();
        res->children = std::move(replacements);
        res->is_strict = is_strict;
        node = std::move(res);
        return true;
    }

    return false;
}


bool ParserAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type == TokenType::Asterisk)
    {
        ++pos;
        auto asterisk = std::make_shared<ASTAsterisk>();
        auto transformers = std::make_shared<ASTColumnsTransformerList>();
        ParserColumnsTransformers transformers_p(allowed_transformers);
        ASTPtr transformer;
        while (transformers_p.parse(pos, transformer, expected))
        {
            transformers->children.push_back(transformer);
        }

        if (!transformers->children.empty())
        {
            asterisk->transformers = std::move(transformers);
            asterisk->children.push_back(asterisk->transformers);
        }

        node = std::move(asterisk);
        return true;
    }
    return false;
}


bool ParserQualifiedAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserCompoundIdentifier(false, true).parse(pos, node, expected))
        return false;

    if (pos->type != TokenType::Dot)
        return false;
    ++pos;

    if (pos->type != TokenType::Asterisk)
        return false;
    ++pos;

    auto res = std::make_shared<ASTQualifiedAsterisk>();
    auto transformers = std::make_shared<ASTColumnsTransformerList>();
    ParserColumnsTransformers transformers_p;
    ASTPtr transformer;
    while (transformers_p.parse(pos, transformer, expected))
    {
        transformers->children.push_back(transformer);
    }

    res->qualifier = std::move(node);
    res->children.push_back(res->qualifier);

    if (!transformers->children.empty())
    {
        res->transformers = std::move(transformers);
        res->children.push_back(res->transformers);
    }

    node = std::move(res);
    return true;
}

/// Parse (columns_list) or ('REGEXP').
static bool parseColumnsMatcherBody(IParser::Pos & pos, ASTPtr & node, Expected & expected, ParserColumnsTransformers::ColumnTransformers allowed_transformers)
{
    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ParserList columns_p(std::make_unique<ParserCompoundIdentifier>(false, true), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserStringLiteral regexp;

    ASTPtr column_list;
    ASTPtr regexp_node;
    if (!columns_p.parse(pos, column_list, expected) && !regexp.parse(pos, regexp_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto transformers = std::make_shared<ASTColumnsTransformerList>();
    ParserColumnsTransformers transformers_p(allowed_transformers);
    ASTPtr transformer;
    while (transformers_p.parse(pos, transformer, expected))
    {
        transformers->children.push_back(transformer);
    }

    ASTPtr res;
    if (column_list)
    {
        auto list_matcher = std::make_shared<ASTColumnsListMatcher>();

        list_matcher->column_list = std::move(column_list);
        list_matcher->children.push_back(list_matcher->column_list);

        if (!transformers->children.empty())
        {
            list_matcher->transformers = std::move(transformers);
            list_matcher->children.push_back(list_matcher->transformers);
        }

        node = std::move(list_matcher);
    }
    else
    {
        auto regexp_matcher = std::make_shared<ASTColumnsRegexpMatcher>();
        regexp_matcher->setPattern(regexp_node->as<ASTLiteral &>().value.safeGet<String>());

        if (!transformers->children.empty())
        {
            regexp_matcher->transformers = std::move(transformers);
            regexp_matcher->children.push_back(regexp_matcher->transformers);
        }

        node = std::move(regexp_matcher);
    }

    return true;
}

bool ParserColumnsMatcher::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword columns(Keyword::COLUMNS);

    if (!columns.ignore(pos, expected))
        return false;

    return parseColumnsMatcherBody(pos, node, expected, allowed_transformers);
}

bool ParserQualifiedColumnsMatcher::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserCompoundIdentifier(false, true).parse(pos, node, expected))
        return false;

    auto identifier_node = node;
    auto & identifier_node_typed = identifier_node->as<ASTIdentifier &>();
    auto & name_parts = identifier_node_typed.name_parts;

    /// ParserCompoundIdentifier parse identifier.COLUMNS
    if (name_parts.size() == 1 || name_parts.back() != "COLUMNS")
        return false;

    name_parts.pop_back();
    identifier_node = std::make_shared<ASTIdentifier>(std::move(name_parts), false, std::move(node->children));

    if (!parseColumnsMatcherBody(pos, node, expected, allowed_transformers))
        return false;

    if (auto * columns_list_matcher = node->as<ASTColumnsListMatcher>())
    {
        auto result = std::make_shared<ASTQualifiedColumnsListMatcher>();
        result->qualifier = std::move(identifier_node);
        result->column_list = std::move(columns_list_matcher->column_list);

        result->children.push_back(result->qualifier);
        result->children.push_back(result->column_list);

        if (columns_list_matcher->transformers)
        {
            result->transformers = std::move(columns_list_matcher->transformers);
            result->children.push_back(result->transformers);
        }

        node = std::move(result);
    }
    else if (auto * column_regexp_matcher = node->as<ASTColumnsRegexpMatcher>())
    {
        auto result = std::make_shared<ASTQualifiedColumnsRegexpMatcher>();
        result->setPattern(column_regexp_matcher->getPattern());

        result->qualifier = std::move(identifier_node);
        result->children.push_back(result->qualifier);

        if (column_regexp_matcher->transformers)
        {
            result->transformers = std::move(column_regexp_matcher->transformers);
            result->children.push_back(result->transformers);
        }

        node = std::move(result);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Qualified COLUMNS matcher expected to be list or regexp");
    }

    return true;
}

bool ParserSubstitution::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::OpeningCurlyBrace)
        return false;

    ++pos;

    if (pos->type != TokenType::BareWord)
    {
        expected.add(pos, "substitution name (identifier)");
        return false;
    }

    String name(pos->begin, pos->end);
    ++pos;

    if (pos->type != TokenType::Colon)
    {
        expected.add(pos, "colon between name and type");
        return false;
    }

    ++pos;

    auto old_pos = pos;
    ParserDataType type_parser;
    if (!type_parser.ignore(pos, expected))
    {
        expected.add(pos, "substitution type");
        return false;
    }

    String type(old_pos->begin, pos->begin);

    if (pos->type != TokenType::ClosingCurlyBrace)
    {
        expected.add(pos, "closing curly brace");
        return false;
    }

    ++pos;
    node = std::make_shared<ASTQueryParameter>(name, type);
    return true;
}


bool ParserMySQLGlobalVariable::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::DoubleAt)
        return false;

    ++pos;

    if (pos->type != TokenType::BareWord)
    {
        expected.add(pos, "variable name");
        return false;
    }

    String name(pos->begin, pos->end);
    ++pos;

    /// SELECT @@session|global.variable style
    if (pos->type == TokenType::Dot)
    {
        ++pos;

        if (pos->type != TokenType::BareWord)
        {
            expected.add(pos, "variable name");
            return false;
        }
        name = String(pos->begin, pos->end);
        ++pos;
    }

    auto name_literal = std::make_shared<ASTLiteral>(name);
    node = makeASTFunction("globalVariable", name_literal);
    node->setAlias("@@" + name);

    return true;
}


bool ParserWithOptionalAlias::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!elem_parser->parse(pos, node, expected))
        return false;

    /** Little hack.
      *
      * In the SELECT section, we allow parsing aliases without specifying the AS keyword.
      * These aliases can not be the same as the query keywords.
      * And the expression itself can be an identifier that matches the keyword.
      * For example, a column may be called where. And in the query it can be written `SELECT where AS x FROM table` or even `SELECT where x FROM table`.
      * Even can be written `SELECT where AS from FROM table`, but it can not be written `SELECT where from FROM table`.
      * See the ParserAlias implementation for details.
      *
      * But there is a small problem - an inconvenient error message if there is an extra comma in the SELECT section at the end.
      * Although this error is very common. Example: `SELECT x, y, z, FROM tbl`
      * If you do nothing, it's parsed as a column with the name FROM and alias tbl.
      * To avoid this situation, we do not allow the parsing of the alias without the AS keyword for the identifier with the name FROM.
      *
      * Note: this also filters the case when the identifier is quoted.
      * Example: SELECT x, y, z, `FROM` tbl. But such a case could be solved.
      *
      * In the future it would be easier to disallow unquoted identifiers that match the keywords.
      */
    bool allow_alias_without_as_keyword_now = allow_alias_without_as_keyword;
    if (allow_alias_without_as_keyword)
        if (auto opt_id = tryGetIdentifierName(node))
            if (0 == strcasecmp(opt_id->data(), "FROM"))
                allow_alias_without_as_keyword_now = false;

    ASTPtr alias_node;
    if (ParserAlias(allow_alias_without_as_keyword_now).parse(pos, alias_node, expected))
    {
        /// FIXME: try to prettify this cast using `as<>()`
        if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(node.get()))
        {
            tryGetIdentifierNameInto(alias_node, ast_with_alias->alias);

            // the alias is parametrised and will be resolved later when the query context is known
            if (!alias_node->children.empty() && alias_node->children.front()->as<ASTQueryParameter>())
                ast_with_alias->parametrised_alias = std::dynamic_pointer_cast<ASTQueryParameter>(alias_node->children.front());
        }
        else
        {
            expected.add(pos, "alias cannot be here");
            return false;
        }
    }

    return true;
}


bool ParserOrderByElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserExpressionWithOptionalAlias elem_p(false);
    ParserKeyword ascending(Keyword::ASCENDING);
    ParserKeyword descending(Keyword::DESCENDING);
    ParserKeyword asc(Keyword::ASC);
    ParserKeyword desc(Keyword::DESC);
    ParserKeyword nulls(Keyword::NULLS);
    ParserKeyword first(Keyword::FIRST);
    ParserKeyword last(Keyword::LAST);
    ParserKeyword collate(Keyword::COLLATE);
    ParserKeyword with_fill(Keyword::WITH_FILL);
    ParserKeyword from(Keyword::FROM);
    ParserKeyword to(Keyword::TO);
    ParserKeyword step(Keyword::STEP);
    ParserStringLiteral collate_locale_parser;
    ParserExpressionWithOptionalAlias exp_parser(false);

    ASTPtr expr_elem;
    if (!elem_p.parse(pos, expr_elem, expected))
        return false;

    int direction = 1;

    if (descending.ignore(pos, expected) || desc.ignore(pos, expected))
        direction = -1;
    else
        ascending.ignore(pos, expected) || asc.ignore(pos, expected);

    int nulls_direction = direction;
    bool nulls_direction_was_explicitly_specified = false;

    if (nulls.ignore(pos, expected))
    {
        nulls_direction_was_explicitly_specified = true;

        if (first.ignore(pos, expected))
            nulls_direction = -direction;
        else if (last.ignore(pos, expected))
            ;
        else
            return false;
    }

    ASTPtr locale_node;
    if (collate.ignore(pos, expected))
    {
        if (!collate_locale_parser.parse(pos, locale_node, expected))
            return false;
    }

    /// WITH FILL [FROM x] [TO y] [STEP z]
    bool has_with_fill = false;
    ASTPtr fill_from;
    ASTPtr fill_to;
    ASTPtr fill_step;
    if (with_fill.ignore(pos, expected))
    {
        has_with_fill = true;
        if (from.ignore(pos, expected) && !exp_parser.parse(pos, fill_from, expected))
            return false;

        if (to.ignore(pos, expected) && !exp_parser.parse(pos, fill_to, expected))
            return false;

        if (step.ignore(pos, expected) && !exp_parser.parse(pos, fill_step, expected))
            return false;
    }

    auto elem = std::make_shared<ASTOrderByElement>();

    elem->children.push_back(expr_elem);

    elem->direction = direction;
    elem->nulls_direction = nulls_direction;
    elem->nulls_direction_was_explicitly_specified = nulls_direction_was_explicitly_specified;
    elem->setCollation(locale_node);
    elem->with_fill = has_with_fill;
    elem->setFillFrom(fill_from);
    elem->setFillTo(fill_to);
    elem->setFillStep(fill_step);

    node = elem;

    return true;
}

bool ParserInterpolateElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword as(Keyword::AS);
    ParserExpression element_p;
    ParserIdentifier ident_p;

    ASTPtr ident;
    if (!ident_p.parse(pos, ident, expected))
        return false;

    ASTPtr expr;
    if (as.ignore(pos, expected))
    {
        if (!element_p.parse(pos, expr, expected))
            return false;
    }
    else
        expr = ident;

    auto elem = std::make_shared<ASTInterpolateElement>();
    elem->column = ident->getColumnName();
    elem->expr = expr;
    elem->children.push_back(expr);

    node = elem;

    return true;
}

bool ParserFunctionWithKeyValueArguments::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier id_parser;
    ParserKeyValuePairsList pairs_list_parser;

    ASTPtr identifier;
    ASTPtr expr_list_args;
    if (!id_parser.parse(pos, identifier, expected))
        return false;


    bool left_bracket_found = false;
    if (pos.get().type != TokenType::OpeningRoundBracket)
    {
        if (!brackets_can_be_omitted)
             return false;
    }
    else
    {
        ++pos;
        left_bracket_found = true;
    }

    if (!pairs_list_parser.parse(pos, expr_list_args, expected))
        return false;

    if (left_bracket_found)
    {
        if (pos.get().type != TokenType::ClosingRoundBracket)
            return false;
        ++pos;
    }

    auto function = std::make_shared<ASTFunctionWithKeyValueArguments>(left_bracket_found);
    function->name = Poco::toLower(identifier->as<ASTIdentifier>()->name());
    function->elements = expr_list_args;
    function->children.push_back(function->elements);
    node = function;

    return true;
}

bool ParserTTLElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_to_disk(Keyword::TO_DISK);
    ParserKeyword s_to_volume(Keyword::TO_VOLUME);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_delete(Keyword::DELETE);
    ParserKeyword s_where(Keyword::WHERE);
    ParserKeyword s_group_by(Keyword::GROUP_BY);
    ParserKeyword s_set(Keyword::SET);
    ParserKeyword s_recompress(Keyword::RECOMPRESS);
    ParserKeyword s_codec(Keyword::CODEC);
    ParserKeyword s_materialize(Keyword::MATERIALIZE);
    ParserKeyword s_remove(Keyword::REMOVE);
    ParserKeyword s_modify(Keyword::MODIFY);

    ParserIdentifier parser_identifier;
    ParserStringLiteral parser_string_literal;
    ParserExpression parser_exp;
    ParserExpressionList parser_keys_list(false);
    ParserCodec parser_codec;

    if (s_materialize.checkWithoutMoving(pos, expected) ||
        s_remove.checkWithoutMoving(pos, expected) ||
        s_modify.checkWithoutMoving(pos, expected))

        return false;

    ASTPtr ttl_expr;
    if (!parser_exp.parse(pos, ttl_expr, expected))
        return false;

    TTLMode mode;
    DataDestinationType destination_type = DataDestinationType::DELETE;
    String destination_name;

    if (s_to_disk.ignore(pos, expected))
    {
        mode = TTLMode::MOVE;
        destination_type = DataDestinationType::DISK;
    }
    else if (s_to_volume.ignore(pos, expected))
    {
        mode = TTLMode::MOVE;
        destination_type = DataDestinationType::VOLUME;
    }
    else if (s_group_by.ignore(pos, expected))
    {
        mode = TTLMode::GROUP_BY;
    }
    else if (s_recompress.ignore(pos, expected))
    {
        mode = TTLMode::RECOMPRESS;
    }
    else
    {
        s_delete.ignore(pos, expected);
        mode = TTLMode::DELETE;
    }

    ASTPtr where_expr;
    ASTPtr group_by_key;
    ASTPtr recompression_codec;
    ASTPtr group_by_assignments;
    bool if_exists = false;

    if (mode == TTLMode::MOVE)
    {
        if (s_if_exists.ignore(pos, expected))
            if_exists = true;

        ASTPtr ast_space_name;
        if (!parser_string_literal.parse(pos, ast_space_name, expected))
            return false;

        destination_name = ast_space_name->as<ASTLiteral &>().value.safeGet<const String &>();
    }
    else if (mode == TTLMode::GROUP_BY)
    {
        if (!parser_keys_list.parse(pos, group_by_key, expected))
            return false;

        if (s_set.ignore(pos, expected))
        {
            ParserList parser_assignment_list(
                std::make_unique<ParserAssignment>(), std::make_unique<ParserToken>(TokenType::Comma));

            if (!parser_assignment_list.parse(pos, group_by_assignments, expected))
                return false;
        }
    }
    else if (mode == TTLMode::DELETE && s_where.ignore(pos, expected))
    {
        if (!parser_exp.parse(pos, where_expr, expected))
            return false;
    }
    else if (mode == TTLMode::RECOMPRESS)
    {
        if (!s_codec.ignore(pos, expected))
            return false;

        if (!parser_codec.parse(pos, recompression_codec, expected))
            return false;
    }

    auto ttl_element = std::make_shared<ASTTTLElement>(mode, destination_type, destination_name, if_exists);
    ttl_element->setTTL(std::move(ttl_expr));
    if (where_expr)
        ttl_element->setWhere(std::move(where_expr));

    if (mode == TTLMode::GROUP_BY)
    {
        ttl_element->group_by_key = std::move(group_by_key->children);
        if (group_by_assignments)
            ttl_element->group_by_assignments = std::move(group_by_assignments->children);
    }

    if (mode == TTLMode::RECOMPRESS)
        ttl_element->recompression_codec = recompression_codec;

    node = ttl_element;
    return true;
}

bool ParserIdentifierWithOptionalParameters::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier non_parametric;
    ParserIdentifierWithParameters parametric;

    if (parametric.parse(pos, node, expected))
    {
        auto * func = node->as<ASTFunction>();
        func->no_empty_args = true;
        return true;
    }

    ASTPtr ident;
    if (non_parametric.parse(pos, ident, expected))
    {
        auto func = std::make_shared<ASTFunction>();
        tryGetIdentifierNameInto(ident, func->name);
        func->no_empty_args = true;
        node = func;
        return true;
    }

    return false;
}

bool ParserAssignment::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto assignment = std::make_shared<ASTAssignment>();
    node = assignment;

    ParserIdentifier p_identifier;
    ParserToken s_equals(TokenType::Equals);
    ParserExpression p_expression;

    ASTPtr column;
    if (!p_identifier.parse(pos, column, expected))
        return false;

    if (!s_equals.ignore(pos, expected))
        return false;

    ASTPtr expression;
    if (!p_expression.parse(pos, expression, expected))
        return false;

    tryGetIdentifierNameInto(column, assignment->column_name);
    if (expression)
        assignment->children.push_back(expression);

    return true;
}

}
