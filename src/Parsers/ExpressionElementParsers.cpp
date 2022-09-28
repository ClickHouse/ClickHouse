#include <cerrno>
#include <cstdlib>

#include <Poco/String.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/DumpASTNode.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>

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

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>


#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseIntervalKind.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserCase.h>

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
        result_node = buildSelectFromTableFunction(makeASTFunction("viewExplain", explain_node));
    }
    else
    {
        return false;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    node = std::make_shared<ASTSubquery>();
    node->children.push_back(result_node);
    return true;
}


bool ParserIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Identifier in backquotes or in double quotes
    if (pos->type == TokenType::QuotedIdentifier)
    {
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
    else if (pos->type == TokenType::BareWord)
    {
        node = std::make_shared<ASTIdentifier>(String(pos->begin, pos->end));
        ++pos;
        return true;
    }
    else if (allow_query_parameter && pos->type == TokenType::OpeningCurlyBrace)
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


bool ParserCompoundIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr id_list;
    if (!ParserList(std::make_unique<ParserIdentifier>(allow_query_parameter), std::make_unique<ParserToken>(TokenType::Dot), false)
             .parse(pos, id_list, expected))
        return false;

    std::vector<String> parts;
    std::vector<ASTPtr> params;
    const auto & list = id_list->as<ASTExpressionList &>();
    for (const auto & child : list.children)
    {
        parts.emplace_back(getIdentifierName(child));
        if (parts.back().empty())
            params.push_back(child->as<ASTIdentifier>()->getParam());
    }

    ParserKeyword s_uuid("UUID");
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
            uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.get<String>());
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

    ParserKeyword parser_where("WHERE");
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
        else
        {
            return false;
        }
    }

    // Variant 2:
    // function_name ( * ) OVER ( window_definition )
    ParserWindowDefinition parser_definition;
    return parser_definition.parse(pos, function.window_definition, expected);
}

static bool tryParseFrameDefinition(ASTWindowDefinition * node, IParser::Pos & pos,
    Expected & expected)
{
    ParserKeyword keyword_rows("ROWS");
    ParserKeyword keyword_groups("GROUPS");
    ParserKeyword keyword_range("RANGE");

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

    ParserKeyword keyword_between("BETWEEN");
    ParserKeyword keyword_unbounded("UNBOUNDED");
    ParserKeyword keyword_preceding("PRECEDING");
    ParserKeyword keyword_following("FOLLOWING");
    ParserKeyword keyword_and("AND");
    ParserKeyword keyword_current_row("CURRENT ROW");

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
    ParserKeyword keyword_partition_by("PARTITION BY");
    ParserNotEmptyExpressionList columns_partition_by(
        false /* we don't allow declaring aliases here*/);
    ParserKeyword keyword_order_by("ORDER BY");
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

        ParserKeyword keyword_as("AS");
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
    function_node->arguments = expr_list_args;
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
    /// Parse numbers (including decimals), strings, arrays and tuples of them.

    const char * data_begin = pos->begin;
    const char * data_end = pos->end;
    bool is_string_literal = pos->type == TokenType::StringLiteral;

    if (pos->type == TokenType::Minus)
    {
        ++pos;
        if (pos->type != TokenType::Number)
            return false;

        data_end = pos->end;
        ++pos;
    }
    else if (pos->type == TokenType::Number || is_string_literal)
    {
        ++pos;
    }
    else if (isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket>(pos->type))
    {
        TokenType last_token = TokenType::OpeningSquareBracket;
        std::vector<TokenType> stack;
        while (pos.isValid())
        {
            if (isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket>(pos->type))
            {
                stack.push_back(pos->type);
                if (!isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma>(last_token))
                    return false;
            }
            else if (pos->type == TokenType::ClosingSquareBracket)
            {
                if (isOneOf<TokenType::Comma, TokenType::OpeningRoundBracket, TokenType::Minus>(last_token))
                    return false;
                if (stack.empty() || stack.back() != TokenType::OpeningSquareBracket)
                    return false;
                stack.pop_back();
            }
            else if (pos->type == TokenType::ClosingRoundBracket)
            {
                if (isOneOf<TokenType::Comma, TokenType::OpeningSquareBracket, TokenType::Minus>(last_token))
                    return false;
                if (stack.empty() || stack.back() != TokenType::OpeningRoundBracket)
                    return false;
                stack.pop_back();
            }
            else if (pos->type == TokenType::Comma)
            {
                if (isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma, TokenType::Minus>(last_token))
                    return false;
            }
            else if (pos->type == TokenType::Number)
            {
                if (!isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma, TokenType::Minus>(last_token))
                    return false;
            }
            else if (isOneOf<TokenType::StringLiteral, TokenType::Minus>(pos->type))
            {
                if (!isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma>(last_token))
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
    if (ParserToken(TokenType::DoubleColon).ignore(pos, expected)
        && ParserDataType().parse(pos, type_ast, expected))
    {
        String s;
        size_t data_size = data_end - data_begin;
        if (is_string_literal)
        {
            ReadBufferFromMemory buf(data_begin, data_size);
            readQuotedStringWithSQLStyle(s, buf);
            assert(buf.count() == data_size);
        }
        else
            s = String(data_begin, data_size);

        auto literal = std::make_shared<ASTLiteral>(std::move(s));
        node = createFunctionCast(literal, type_ast);
        return true;
    }

    return false;
}


bool ParserNull::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword nested_parser("NULL");
    if (nested_parser.parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(Null());
        return true;
    }
    else
        return false;
}


bool ParserBool::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserKeyword("true").parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(true);
        return true;
    }
    else if (ParserKeyword("false").parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(false);
        return true;
    }
    else
        return false;
}

static bool parseNumber(char * buffer, size_t size, bool negative, int base, Field & res)
{
    errno = 0;    /// Functions strto* don't clear errno.

    char * pos_integer = buffer;
    UInt64 uint_value = std::strtoull(buffer, &pos_integer, base);

    if (pos_integer == buffer + size && errno != ERANGE && (!negative || uint_value <= (1ULL << 63)))
    {
        if (negative)
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

    /** Maximum length of number. 319 symbols is enough to write maximum double in decimal form.
      * Copy is needed to use strto* functions, which require 0-terminated string.
      */
    static constexpr size_t MAX_LENGTH_OF_NUMBER = 319;

    if (pos->size() > MAX_LENGTH_OF_NUMBER)
    {
        expected.add(pos, "number");
        return false;
    }

    char buf[MAX_LENGTH_OF_NUMBER + 1];

    size_t size = pos->size();
    memcpy(buf, pos->begin, size);
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
            else
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

    char * pos_double = buf;
    errno = 0;    /// Functions strto* don't clear errno.
    Float64 float_value = std::strtod(buf, &pos_double);
    if (pos_double == buf + pos->size() && errno != ERANGE)
    {
        if (float_value < 0)
            throw Exception("Logical error: token number cannot begin with minus, but parsed float number is less than zero.", ErrorCodes::LOGICAL_ERROR);

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


bool ParserStringLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::StringLiteral && pos->type != TokenType::HereDoc)
        return false;

    String s;

    if (pos->type == TokenType::StringLiteral)
    {
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

    auto literal = std::make_shared<ASTLiteral>(s);
    literal->begin = pos;
    literal->end = ++pos;
    node = literal;
    return true;
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
            else if (pos->type == TokenType::Comma)
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
template bool ParserCollectionOfLiterals<Map>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected);

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
    "WITH",
    "INTERSECT",
    "EXCEPT",
    "ELSE",
    nullptr
};

bool ParserAlias::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_as("AS");
    ParserIdentifier id_p;

    bool has_as_word = s_as.ignore(pos, expected);
    if (!allow_alias_without_as_keyword && !has_as_word)
        return false;

    if (!id_p.parse(pos, node, expected))
        return false;

    if (!has_as_word)
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


bool ParserColumnsMatcher::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword columns("COLUMNS");
    ParserList columns_p(std::make_unique<ParserCompoundIdentifier>(false, true), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserStringLiteral regex;

    if (!columns.ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ASTPtr column_list;
    ASTPtr regex_node;
    if (!columns_p.parse(pos, column_list, expected) && !regex.parse(pos, regex_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    ASTPtr res;
    if (column_list)
    {
        auto list_matcher = std::make_shared<ASTColumnsListMatcher>();
        list_matcher->column_list = column_list;
        res = list_matcher;
    }
    else
    {
        auto regexp_matcher = std::make_shared<ASTColumnsRegexpMatcher>();
        regexp_matcher->setPattern(regex_node->as<ASTLiteral &>().value.get<String>());
        res = regexp_matcher;
    }

    ParserColumnsTransformers transformers_p(allowed_transformers);
    ASTPtr transformer;
    while (transformers_p.parse(pos, transformer, expected))
    {
        res->children.push_back(transformer);
    }
    node = std::move(res);
    return true;
}


bool ParserColumnsTransformers::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword apply("APPLY");
    ParserKeyword except("EXCEPT");
    ParserKeyword replace("REPLACE");
    ParserKeyword as("AS");
    ParserKeyword strict("STRICT");

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
            if (const auto * func = lambda->as<ASTFunction>(); func && func->name == "lambda")
            {
                if (func->arguments->children.size() != 2)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "lambda requires two arguments");

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

            column_name_prefix = ast_prefix_name->as<ASTLiteral &>().value.get<const String &>();
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
    else if (allowed_transformers.isSet(ColumnTransformer::EXCEPT) && except.ignore(pos, expected))
    {
        if (strict.ignore(pos, expected))
            is_strict = true;

        ASTs identifiers;
        ASTPtr regex_node;
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
            if (!ParserList::parseUtil(pos, expected, parse_id, false) && !regex.parse(pos, regex_node, expected))
                return false;

            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }
        else
        {
            // only one parameter
            if (!parse_id() && !regex.parse(pos, regex_node, expected))
                return false;
        }

        auto res = std::make_shared<ASTColumnsExceptTransformer>();
        if (regex_node)
            res->setPattern(regex_node->as<ASTLiteral &>().value.get<String>());
        else
            res->children = std::move(identifiers);
        res->is_strict = is_strict;
        node = std::move(res);
        return true;
    }
    else if (allowed_transformers.isSet(ColumnTransformer::REPLACE) && replace.ignore(pos, expected))
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
            replacement->expr = std::move(expr);
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
        ParserColumnsTransformers transformers_p(allowed_transformers);
        ASTPtr transformer;
        while (transformers_p.parse(pos, transformer, expected))
        {
            asterisk->children.push_back(transformer);
        }
        node = asterisk;
        return true;
    }
    return false;
}


bool ParserQualifiedAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserCompoundIdentifier(true, true).parse(pos, node, expected))
        return false;

    if (pos->type != TokenType::Dot)
        return false;
    ++pos;

    if (pos->type != TokenType::Asterisk)
        return false;
    ++pos;

    auto res = std::make_shared<ASTQualifiedAsterisk>();
    res->children.push_back(node);
    ParserColumnsTransformers transformers_p;
    ASTPtr transformer;
    while (transformers_p.parse(pos, transformer, expected))
    {
        res->children.push_back(transformer);
    }
    node = std::move(res);
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
    ParserKeyword ascending("ASCENDING");
    ParserKeyword descending("DESCENDING");
    ParserKeyword asc("ASC");
    ParserKeyword desc("DESC");
    ParserKeyword nulls("NULLS");
    ParserKeyword first("FIRST");
    ParserKeyword last("LAST");
    ParserKeyword collate("COLLATE");
    ParserKeyword with_fill("WITH FILL");
    ParserKeyword from("FROM");
    ParserKeyword to("TO");
    ParserKeyword step("STEP");
    ParserStringLiteral collate_locale_parser;
    ParserExpressionWithOptionalAlias exp_parser(false);

    ASTPtr expr_elem;
    if (!elem_p.parse(pos, expr_elem, expected))
        return false;

    int direction = 1;

    if (descending.ignore(pos) || desc.ignore(pos))
        direction = -1;
    else
        ascending.ignore(pos) || asc.ignore(pos);

    int nulls_direction = direction;
    bool nulls_direction_was_explicitly_specified = false;

    if (nulls.ignore(pos))
    {
        nulls_direction_was_explicitly_specified = true;

        if (first.ignore(pos))
            nulls_direction = -direction;
        else if (last.ignore(pos))
            ;
        else
            return false;
    }

    ASTPtr locale_node;
    if (collate.ignore(pos))
    {
        if (!collate_locale_parser.parse(pos, locale_node, expected))
            return false;
    }

    /// WITH FILL [FROM x] [TO y] [STEP z]
    bool has_with_fill = false;
    ASTPtr fill_from;
    ASTPtr fill_to;
    ASTPtr fill_step;
    if (with_fill.ignore(pos))
    {
        has_with_fill = true;
        if (from.ignore(pos) && !exp_parser.parse(pos, fill_from, expected))
            return false;

        if (to.ignore(pos) && !exp_parser.parse(pos, fill_to, expected))
            return false;

        if (step.ignore(pos) && !exp_parser.parse(pos, fill_step, expected))
            return false;
    }

    auto elem = std::make_shared<ASTOrderByElement>();

    elem->direction = direction;
    elem->nulls_direction = nulls_direction;
    elem->nulls_direction_was_explicitly_specified = nulls_direction_was_explicitly_specified;
    elem->collation = locale_node;
    elem->with_fill = has_with_fill;
    elem->fill_from = fill_from;
    elem->fill_to = fill_to;
    elem->fill_step = fill_step;
    elem->children.push_back(expr_elem);
    if (locale_node)
        elem->children.push_back(locale_node);

    node = elem;

    return true;
}

bool ParserInterpolateElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword as("AS");
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
    ParserKeyword s_to_disk("TO DISK");
    ParserKeyword s_to_volume("TO VOLUME");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_delete("DELETE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_group_by("GROUP BY");
    ParserKeyword s_set("SET");
    ParserKeyword s_recompress("RECOMPRESS");
    ParserKeyword s_codec("CODEC");
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_eq(TokenType::Equals);

    ParserIdentifier parser_identifier;
    ParserStringLiteral parser_string_literal;
    ParserExpression parser_exp;
    ParserExpressionList parser_keys_list(false);
    ParserCodec parser_codec;

    ParserList parser_assignment_list(
        std::make_unique<ParserAssignment>(), std::make_unique<ParserToken>(TokenType::Comma));

    ASTPtr ttl_expr;
    if (!parser_exp.parse(pos, ttl_expr, expected))
        return false;

    TTLMode mode;
    DataDestinationType destination_type = DataDestinationType::DELETE;
    String destination_name;

    if (s_to_disk.ignore(pos))
    {
        mode = TTLMode::MOVE;
        destination_type = DataDestinationType::DISK;
    }
    else if (s_to_volume.ignore(pos))
    {
        mode = TTLMode::MOVE;
        destination_type = DataDestinationType::VOLUME;
    }
    else if (s_group_by.ignore(pos))
    {
        mode = TTLMode::GROUP_BY;
    }
    else if (s_recompress.ignore(pos))
    {
        mode = TTLMode::RECOMPRESS;
    }
    else
    {
        s_delete.ignore(pos);
        mode = TTLMode::DELETE;
    }

    ASTPtr where_expr;
    ASTPtr group_by_key;
    ASTPtr recompression_codec;
    ASTPtr group_by_assignments;
    bool if_exists = false;

    if (mode == TTLMode::MOVE)
    {
        if (s_if_exists.ignore(pos))
            if_exists = true;

        ASTPtr ast_space_name;
        if (!parser_string_literal.parse(pos, ast_space_name, expected))
            return false;

        destination_name = ast_space_name->as<ASTLiteral &>().value.get<const String &>();
    }
    else if (mode == TTLMode::GROUP_BY)
    {
        if (!parser_keys_list.parse(pos, group_by_key, expected))
            return false;

        if (s_set.ignore(pos))
        {
            if (!parser_assignment_list.parse(pos, group_by_assignments, expected))
                return false;
        }
    }
    else if (mode == TTLMode::DELETE && s_where.ignore(pos))
    {
        if (!parser_exp.parse(pos, where_expr, expected))
            return false;
    }
    else if (mode == TTLMode::RECOMPRESS)
    {
        if (!s_codec.ignore(pos))
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
