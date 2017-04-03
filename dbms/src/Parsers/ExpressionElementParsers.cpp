#include <errno.h>
#include <cstdlib>

#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWeightedZooKeeperPath.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserCase.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserArray::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    ASTPtr contents_node;
    ParserString open("["), close("]");
    ParserExpressionList contents(false);
    ParserWhiteSpaceOrComments ws;

    if (!open.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);
    if (!contents.parse(pos, end, contents_node, max_parsed_pos, expected))
        return false;
    ws.ignore(pos, end);

    if (!close.ignore(pos, end, max_parsed_pos, expected))
        return false;

    auto function_node = std::make_shared<ASTFunction>(StringRange(begin, pos));
    function_node->name = "array";
    function_node->arguments = contents_node;
    function_node->children.push_back(contents_node);
    node = function_node;

    return true;
}


bool ParserParenthesisExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    ASTPtr contents_node;
    ParserString open("("), close(")");
    ParserExpressionList contents(false);
    ParserWhiteSpaceOrComments ws;

    if (!open.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);
    if (!contents.parse(pos, end, contents_node, max_parsed_pos, expected))
        return false;
    ws.ignore(pos, end);

    if (!close.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ASTExpressionList & expr_list = typeid_cast<ASTExpressionList &>(*contents_node);

    /// empty expression in parentheses is not allowed
    if (expr_list.children.empty())
    {
        expected = "non-empty parenthesized list of expressions";
        return false;
    }

    if (expr_list.children.size() == 1)
    {
        node = expr_list.children.front();
    }
    else
    {
        auto function_node = std::make_shared<ASTFunction>(StringRange(begin, pos));
        function_node->name = "tuple";
        function_node->arguments = contents_node;
        function_node->children.push_back(contents_node);
        node = function_node;
    }

    return true;
}


bool ParserSubquery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    ASTPtr select_node;
    ParserString open("("), close(")");
    ParserSelectQuery select;
    ParserWhiteSpaceOrComments ws;

    if (!open.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);
    if (!select.parse(pos, end, select_node, max_parsed_pos, expected))
        return false;
    ws.ignore(pos, end);

    if (!close.ignore(pos, end, max_parsed_pos, expected))
        return false;

    node = std::make_shared<ASTSubquery>(StringRange(begin, pos));
    typeid_cast<ASTSubquery &>(*node).children.push_back(select_node);
    return true;
}


bool ParserIdentifier::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    /// Identifier in backquotes
    if (pos != end && *pos == '`')
    {
        ReadBufferFromMemory buf(pos, end - pos);
        String s;
        readBackQuotedString(s, buf);

        if (s.empty())    /// Identifiers "empty string" are not allowed.
            return false;

        pos += buf.count();
        node = std::make_shared<ASTIdentifier>(StringRange(begin, pos), s);
        return true;
    }
    else
    {
        while (pos != end
            && (isAlphaASCII(*pos)
                || (*pos == '_')
                || (pos != begin && isNumericASCII(*pos))))
            ++pos;

        if (pos != begin)
        {
            node = std::make_shared<ASTIdentifier>(StringRange(begin, pos), String(begin, pos - begin));
            return true;
        }
        else
            return false;
    }
}


bool ParserCompoundIdentifier::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ASTPtr id_list;
    if (!ParserList(std::make_unique<ParserIdentifier>(), std::make_unique<ParserString>("."), false)
        .parse(pos, end, id_list, max_parsed_pos, expected))
        return false;

    String name;
    const ASTExpressionList & list = static_cast<const ASTExpressionList &>(*id_list.get());
    for (const auto & child : list.children)
    {
        if (!name.empty())
            name += '.';
        name += static_cast<const ASTIdentifier &>(*child.get()).name;
    }

    node = std::make_shared<ASTIdentifier>(StringRange(begin, pos), name);

    /// In `children`, remember the identifiers-components, if there are more than one.
    if (list.children.size() > 1)
        node->children.insert(node->children.end(), list.children.begin(), list.children.end());

    return true;
}


bool ParserFunction::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserIdentifier id_parser;
    ParserString open("("), close(")");
    ParserString distinct("DISTINCT", true, true);
    ParserExpressionList contents(false);
    ParserWhiteSpaceOrComments ws;

    bool has_distinct_modifier = false;

    ASTPtr identifier;
    ASTPtr expr_list_args;
    ASTPtr expr_list_params;

    if (!id_parser.parse(pos, end, identifier, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (!open.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    if (distinct.ignore(pos, end, max_parsed_pos, expected))
    {
        has_distinct_modifier = true;
        ws.ignore(pos, end);
    }

    Pos contents_begin = pos;
    if (!contents.parse(pos, end, expr_list_args, max_parsed_pos, expected))
        return false;
    Pos contents_end = pos;
    ws.ignore(pos, end);

    if (!close.ignore(pos, end, max_parsed_pos, expected))
        return false;

    /** Check for a common error case - often due to the complexity of quoting command-line arguments,
      *  an expression of the form toDate(2014-01-01) appears in the query instead of toDate('2014-01-01').
      * If you do not report that the first option is an error, then the argument will be interpreted as 2014 - 01 - 01 - some number,
      *  and the query silently returns an unexpected result.
      */
    if (typeid_cast<const ASTIdentifier &>(*identifier).name == "toDate"
        && contents_end - contents_begin == strlen("2014-01-01")
        && contents_begin[0] >= '2' && contents_begin[0] <= '3'
        && contents_begin[1] >= '0' && contents_begin[1] <= '9'
        && contents_begin[2] >= '0' && contents_begin[2] <= '9'
        && contents_begin[3] >= '0' && contents_begin[3] <= '9'
        && contents_begin[4] == '-'
        && contents_begin[5] >= '0' && contents_begin[5] <= '9'
        && contents_begin[6] >= '0' && contents_begin[6] <= '9'
        && contents_begin[7] == '-'
        && contents_begin[8] >= '0' && contents_begin[8] <= '9'
        && contents_begin[9] >= '0' && contents_begin[9] <= '9')
    {
        std::string contents(contents_begin, contents_end - contents_begin);
        throw Exception("Argument of function toDate is unquoted: toDate(" + contents + "), must be: toDate('" + contents + "')"
            , ErrorCodes::SYNTAX_ERROR);
    }

    /// The parametric aggregate function has two lists (parameters and arguments) in parentheses. Example: quantile(0.9)(x).
    if (open.ignore(pos, end, max_parsed_pos, expected))
    {
        /// Parametric aggregate functions cannot have DISTINCT in parameters list.
        if (has_distinct_modifier)
            return false;

        expr_list_params = expr_list_args;
        expr_list_args = nullptr;

        ws.ignore(pos, end);

        if (distinct.ignore(pos, end, max_parsed_pos, expected))
        {
            has_distinct_modifier = true;
            ws.ignore(pos, end);
        }

        if (!contents.parse(pos, end, expr_list_args, max_parsed_pos, expected))
            return false;
        ws.ignore(pos, end);

        if (!close.ignore(pos, end, max_parsed_pos, expected))
            return false;
    }

    auto function_node = std::make_shared<ASTFunction>(StringRange(begin, pos));
    function_node->name = typeid_cast<ASTIdentifier &>(*identifier).name;

    /// func(DISTINCT ...) is equivalent to funcDistinct(...)
    if (has_distinct_modifier)
        function_node->name += "Distinct";

    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    if (expr_list_params)
    {
        function_node->parameters = expr_list_params;
        function_node->children.push_back(function_node->parameters);
    }

    node = function_node;
    return true;
}


bool ParserCastExpression::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    const auto begin = pos;

    ParserIdentifier id_parser;

    ASTPtr identifier;

    if (!id_parser.parse(pos, end, identifier, max_parsed_pos, expected))
        return false;

    const auto & id = typeid_cast<const ASTIdentifier &>(*identifier).name;
    if (id.length() != strlen(name) || 0 != strcasecmp(id.c_str(), name))
    {
        /// Parse as a CASE expression.
        return ParserCase{}.parse(pos = begin, end, node, max_parsed_pos, expected);
    }

    /// Parse as CAST(expression AS type)
    ParserString open("("), close(")"), comma(",");
    ParserExpressionInCastExpression expression_and_type(false);
    ParserWhiteSpaceOrComments ws;

    ASTPtr expr_list_args;

    ws.ignore(pos, end);

    if (!open.ignore(pos, end, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    const auto contents_begin = pos;
    ASTPtr first_argument;
    if (!expression_and_type.parse(pos, end, first_argument, max_parsed_pos, expected))
        return false;

    ws.ignore(pos, end);

    /// check for subsequent comma ","
    if (!comma.ignore(pos, end, max_parsed_pos, expected))
    {
        /// CAST(expression AS type)
        const auto type = first_argument->tryGetAlias();
        if (type.empty())
        {
            /// there is only one argument and it has no alias
            expected = "type identifier";
            return false;
        }

        expr_list_args = std::make_shared<ASTExpressionList>(StringRange{contents_begin, end});
        first_argument->setAlias({});
        expr_list_args->children.push_back(first_argument);
        expr_list_args->children.emplace_back(std::make_shared<ASTLiteral>(StringRange(), type));
    }
    else
    {
        /// CAST(expression, 'type')
        /// Reparse argument list from scratch
        max_parsed_pos = pos = contents_begin;

        ParserExpressionWithOptionalAlias expression{false};
        if (!expression.parse(pos, end, first_argument, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end, max_parsed_pos, expected);

        if (!comma.ignore(pos, end, max_parsed_pos, expected))
            return false;

        ws.ignore(pos, end, max_parsed_pos, expected);

        ParserStringLiteral p_type;
        ASTPtr type_as_literal;

        if (!p_type.parse(pos, end, type_as_literal, max_parsed_pos, expected))
        {
            expected = "string literal depicting type";
            return false;
        }

        expr_list_args = std::make_shared<ASTExpressionList>(StringRange{contents_begin, end});
        expr_list_args->children.push_back(first_argument);
        expr_list_args->children.push_back(type_as_literal);
    }

    ws.ignore(pos, end);

    if (!close.ignore(pos, end, max_parsed_pos, expected))
    {
        expected = ")";
        return false;
    }

    const auto function_node = std::make_shared<ASTFunction>(StringRange(begin, pos));
    ASTPtr node_holder{function_node};
    function_node->name = name;

    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    node = node_holder;
    return true;
}


bool ParserNull::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    ParserString nested_parser("NULL", true, true);
    if (nested_parser.parse(pos, end, node, max_parsed_pos, expected))
    {
        node = std::make_shared<ASTLiteral>(StringRange(StringRange(begin, pos)), Null());
        return true;
    }
    else
        return false;
}


bool ParserNumber::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Field res;

    Pos begin = pos;
    if (pos == end)
        return false;

    /** Maximum length of number. 319 symbols is enough to write maximum double in decimal form.
      * Copy is needed to use strto* functions, which require 0-terminated string.
      */
    char buf[320];

    size_t bytes_to_copy = end - pos < 319 ? end - pos : 319;
    memcpy(buf, pos, bytes_to_copy);
    buf[bytes_to_copy] = 0;

    char * pos_double = buf;
    errno = 0;    /// Functions strto* don't clear errno.
    Float64 float_value = std::strtod(buf, &pos_double);
    if (pos_double == buf || errno == ERANGE)
    {
        expected = "number";
        return false;
    }

    /// excessive "word" symbols after number
    if (pos_double < buf + bytes_to_copy
        && isWordCharASCII(*pos_double))
    {
        expected = "number";
        return false;
    }

    res = float_value;

    /// try to use more exact type: UInt64 or Int64

    char * pos_integer = buf;
    if (float_value < 0)
    {
        errno = 0;
        Int64 int_value = std::strtoll(buf, &pos_integer, 0);
        if (pos_integer == pos_double && errno != ERANGE)
            res = int_value;
    }
    else
    {
        errno = 0;
        UInt64 uint_value = std::strtoull(buf, &pos_integer, 0);
        if (pos_integer == pos_double && errno != ERANGE)
            res = uint_value;
    }

    pos += pos_double - buf;
    node = std::make_shared<ASTLiteral>(StringRange(begin, pos), res);
    return true;
}


bool ParserUnsignedInteger::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Field res;

    Pos begin = pos;
    if (pos == end)
        return false;

    UInt64 x = 0;
    ReadBufferFromMemory in(pos, end - pos);
    if (!tryReadIntText(x, in) || in.count() == 0)
    {
        expected = "unsigned integer";
        return false;
    }

    res = x;
    pos += in.count();
    node = std::make_shared<ASTLiteral>(StringRange(begin, pos), res);
    return true;
}


bool ParserStringLiteral::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    String s;

    if (pos == end || *pos != '\'')
    {
        expected = "opening single quote";
        return false;
    }

    ReadBufferFromMemory in(pos, end - pos);

    try
    {
        readQuotedString(s, in);
    }
    catch (const Exception & e)
    {
        expected = "string literal";
        return false;
    }

    pos += in.count();
    node = std::make_shared<ASTLiteral>(StringRange(begin, pos), s);
    return true;
}


bool ParserArrayOfLiterals::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    Array arr;

    if (pos == end || *pos != '[')
    {
        expected = "opening square bracket";
        return false;
    }

    ParserWhiteSpaceOrComments ws;
    ParserLiteral literal_p;

    ++pos;

    while (pos != end)
    {
        ws.ignore(pos, end);

        if (!arr.empty())
        {
            if (*pos == ']')
            {
                ++pos;
                node = std::make_shared<ASTLiteral>(StringRange(begin, pos), arr);
                return true;
            }
            else if (*pos == ',')
            {
                ++pos;
            }
            else
            {
                expected = "comma or closing square bracket";
                return false;
            }
        }

        ws.ignore(pos, end);

        ASTPtr literal_node;
        if (!literal_p.parse(pos, end, literal_node, max_parsed_pos, expected))
            return false;

        arr.push_back(typeid_cast<const ASTLiteral &>(*literal_node).value);
    }

    expected = "closing square bracket";
    return false;
}


bool ParserLiteral::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserNull null_p;
    ParserNumber num_p;
    ParserStringLiteral str_p;

    if (null_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (num_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (str_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    expected = "literal: one of NULL, number, single quoted string";
    return false;
}


const char * ParserAliasBase::restricted_keywords[] =
{
    "FROM",
    "FINAL",
    "SAMPLE",
    "ARRAY",
    "LEFT",
    "RIGHT",
    "INNER",
    "FULL",
    "CROSS",
    "JOIN",
    "GLOBAL",
    "ANY",
    "ALL",
    "ON",
    "USING",
    "PREWHERE",
    "WHERE",
    "GROUP",
    "WITH",
    "HAVING",
    "ORDER",
    "LIMIT",
    "SETTINGS",
    "FORMAT",
    "UNION",
    "INTO",
    nullptr
};

template <typename ParserIdentifier>
bool ParserAliasImpl<ParserIdentifier>::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;
    ParserString s_as("AS", true, true);
    ParserIdentifier id_p;

    bool has_as_word = s_as.parse(pos, end, node, max_parsed_pos, expected);
    if (!allow_alias_without_as_keyword && !has_as_word)
        return false;

    ws.ignore(pos, end);

    if (!id_p.parse(pos, end, node, max_parsed_pos, expected))
        return false;

    if (!has_as_word)
    {
        /** In this case, the alias can not match the keyword -
          *  so that in the query "SELECT x FROM t", the word FROM was not considered an alias,
          *  and in the query "SELECT x FRO FROM t", the word FRO was considered an alias.
          */

        const String & name = static_cast<const ASTIdentifier &>(*node.get()).name;

        for (const char ** keyword = restricted_keywords; *keyword != nullptr; ++keyword)
            if (0 == strcasecmp(name.data(), *keyword))
                return false;
    }

    return true;
}

template class ParserAliasImpl<ParserIdentifier>;
template class ParserAliasImpl<ParserTypeInCastExpression>;


bool ParserAsterisk::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;
    if (ParserString("*").parse(pos, end, node, max_parsed_pos, expected))
    {
        node = std::make_shared<ASTAsterisk>(StringRange(begin, pos));
        return true;
    }
    return false;
}


bool ParserQualifiedAsterisk::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    if (!ParserCompoundIdentifier().parse(pos, end, node, max_parsed_pos, expected))
        return false;

    ParserWhiteSpaceOrComments().ignore(pos, end);

    if (!ParserString(".").ignore(pos, end, max_parsed_pos, expected))
        return false;

    ParserWhiteSpaceOrComments().ignore(pos, end);

    if (!ParserString("*").ignore(pos, end, max_parsed_pos, expected))
        return false;

    auto res = std::make_shared<ASTQualifiedAsterisk>(StringRange(begin, pos));
    res->children.push_back(node);
    node = std::move(res);
    return true;
}


bool ParserExpressionElement::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserParenthesisExpression paren_p;
    ParserSubquery subquery_p;
    ParserArray array_p;
    ParserArrayOfLiterals array_lite_p;
    ParserLiteral lit_p;
    ParserCastExpression fun_p;
    ParserCompoundIdentifier id_p;
    ParserAsterisk asterisk_p;
    ParserQualifiedAsterisk qualified_asterisk_p;

    if (subquery_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (paren_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (array_lite_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (array_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (lit_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (fun_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (qualified_asterisk_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (asterisk_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (id_p.parse(pos, end, node, max_parsed_pos, expected))
        return true;

    if (!expected)
        expected = "expression element: one of array, literal, function, identifier, asterisk, parenthesised expression, subquery";
    return false;
}


template <typename ParserAlias>
bool ParserWithOptionalAliasImpl<ParserAlias>::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserWhiteSpaceOrComments ws;

    if (!elem_parser->parse(pos, end, node, max_parsed_pos, expected))
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
        if (const ASTIdentifier * id = typeid_cast<const ASTIdentifier *>(node.get()))
            if (0 == strcasecmp(id->name.data(), "FROM"))
                allow_alias_without_as_keyword_now = false;

    ws.ignore(pos, end);

    ASTPtr alias_node;
    if (ParserAlias(allow_alias_without_as_keyword_now).parse(pos, end, alias_node, max_parsed_pos, expected))
    {
        String alias_name = typeid_cast<ASTIdentifier &>(*alias_node).name;

        if (ASTWithAlias * ast_with_alias = dynamic_cast<ASTWithAlias *>(node.get()))
            ast_with_alias->alias = alias_name;
        else
        {
            expected = "alias cannot be here";
            return false;
        }
    }

    return true;
}

template class ParserWithOptionalAliasImpl<ParserAlias>;
template class ParserWithOptionalAliasImpl<ParserCastExpressionAlias>;


bool ParserOrderByElement::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    Pos begin = pos;

    ParserWhiteSpaceOrComments ws;
    ParserExpressionWithOptionalAlias elem_p(false);
    ParserString ascending("ASCENDING", true, true);
    ParserString descending("DESCENDING", true, true);
    ParserString asc("ASC", true, true);
    ParserString desc("DESC", true, true);
    ParserString nulls("NULLS", true, true);
    ParserString first("FIRST", true, true);
    ParserString last("LAST", true, true);
    ParserString collate("COLLATE", true, true);
    ParserStringLiteral collate_locale_parser;

    ASTPtr expr_elem;
    if (!elem_p.parse(pos, end, expr_elem, max_parsed_pos, expected))
        return false;

    int direction = 1;
    ws.ignore(pos, end);

    if (descending.ignore(pos, end) || desc.ignore(pos, end))
        direction = -1;
    else
        ascending.ignore(pos, end) || asc.ignore(pos, end);

    ws.ignore(pos, end);

    int nulls_direction = direction;
    bool nulls_direction_was_explicitly_specified = false;

    if (nulls.ignore(pos, end))
    {
        nulls_direction_was_explicitly_specified = true;

        ws.ignore(pos, end);

        if (first.ignore(pos, end))
            nulls_direction = -direction;
        else if (last.ignore(pos, end))
            ;
        else
            return false;

        ws.ignore(pos, end);
    }

    ASTPtr locale_node;
    if (collate.ignore(pos, end))
    {
        ws.ignore(pos, end);

        if (!collate_locale_parser.parse(pos, end, locale_node, max_parsed_pos, expected))
            return false;
    }

    node = std::make_shared<ASTOrderByElement>(StringRange(begin, pos), direction, nulls_direction, nulls_direction_was_explicitly_specified, locale_node);
    node->children.push_back(expr_elem);
    if (locale_node)
        node->children.push_back(locale_node);

    return true;
}

bool ParserWeightedZooKeeperPath::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
    ParserString s_weight("WEIGHT", true, true);
    ParserStringLiteral path_p;
    ParserUnsignedInteger weight_p;
    ParserWhiteSpaceOrComments ws;

    auto weighted_zookeeper_path = std::make_shared<ASTWeightedZooKeeperPath>();
    node = weighted_zookeeper_path;

    ws.ignore(pos, end);

    ASTPtr path_node;
    if (!path_p.parse(pos, end, path_node, max_parsed_pos, expected))
        return false;

    weighted_zookeeper_path->path = typeid_cast<const ASTLiteral &>(*path_node).value.get<const String &>();

    ws.ignore(pos, end);

    bool is_weight_set = false;

    if (s_weight.ignore(pos, end, max_parsed_pos, expected))
    {
        ws.ignore(pos, end);

        ASTPtr weight_node;
        if (weight_p.parse(pos, end, weight_node, max_parsed_pos, expected))
        {
            is_weight_set = true;
            weighted_zookeeper_path->weight = typeid_cast<const ASTLiteral &>(*weight_node).value.get<const UInt64 &>();
        }
    }

    if (!is_weight_set)
        weighted_zookeeper_path->weight = 1;

    return true;
}

}

