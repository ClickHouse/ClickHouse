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
    extern const int LOGICAL_ERROR;
}


bool ParserArray::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;
    ASTPtr contents_node;
    ParserExpressionList contents(false);

    if (pos->type != TokenType::OpeningSquareBracket)
        return false;
    ++pos;

    if (!contents.parse(pos, contents_node, expected))
        return false;

    if (pos->type != TokenType::ClosingSquareBracket)
        return false;
    ++pos;

    auto function_node = std::make_shared<ASTFunction>(StringRange(begin, pos));
    function_node->name = "array";
    function_node->arguments = contents_node;
    function_node->children.push_back(contents_node);
    node = function_node;

    return true;
}


bool ParserParenthesisExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;
    ASTPtr contents_node;
    ParserExpressionList contents(false);

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!contents.parse(pos, contents_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    ASTExpressionList & expr_list = typeid_cast<ASTExpressionList &>(*contents_node);

    /// empty expression in parentheses is not allowed
    if (expr_list.children.empty())
    {
        expected.add(pos, "non-empty parenthesized list of expressions");
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


bool ParserSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;
    ASTPtr select_node;
    ParserSelectQuery select;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!select.parse(pos, select_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    node = std::make_shared<ASTSubquery>(StringRange(begin, pos));
    typeid_cast<ASTSubquery &>(*node).children.push_back(select_node);
    return true;
}


bool ParserIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected &)
{
    Pos begin = pos;

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

        ++pos;
        node = std::make_shared<ASTIdentifier>(StringRange(begin), s);
        return true;
    }
    else if (pos->type == TokenType::BareWord)
    {
        ++pos;
        node = std::make_shared<ASTIdentifier>(StringRange(begin), String(begin->begin, begin->end));
        return true;
    }

    return false;
}


bool ParserCompoundIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;

    ASTPtr id_list;
    if (!ParserList(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Dot), false)
        .parse(pos, id_list, expected))
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


bool ParserFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;

    ParserIdentifier id_parser;
    ParserKeyword distinct("DISTINCT");
    ParserExpressionList contents(false);

    bool has_distinct_modifier = false;

    ASTPtr identifier;
    ASTPtr expr_list_args;
    ASTPtr expr_list_params;

    if (!id_parser.parse(pos, identifier, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (distinct.ignore(pos, expected))
        has_distinct_modifier = true;

    const char * contents_begin = pos->begin;
    if (!contents.parse(pos, expr_list_args, expected))
        return false;
    const char * contents_end = pos->begin;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

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
    if (pos->type == TokenType::OpeningRoundBracket)
    {
        ++pos;

        /// Parametric aggregate functions cannot have DISTINCT in parameters list.
        if (has_distinct_modifier)
            return false;

        expr_list_params = expr_list_args;
        expr_list_args = nullptr;

        if (distinct.ignore(pos, expected))
            has_distinct_modifier = true;

        if (!contents.parse(pos, expr_list_args, expected))
            return false;

        if (pos->type != TokenType::ClosingRoundBracket)
            return false;
        ++pos;
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


bool ParserCastExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const auto begin = pos;

    ParserIdentifier id_parser;

    ASTPtr identifier;

    if (!id_parser.parse(pos, identifier, expected))
        return false;

    const auto & id = typeid_cast<const ASTIdentifier &>(*identifier).name;

    /// TODO This is ridiculous. Please get rid of this.
    if (id.length() != strlen(name) || 0 != strcasecmp(id.c_str(), name))
    {
        /// Parse as a CASE expression.
        pos = begin;
        return ParserCase{}.parse(pos, node, expected);
    }

    /// Parse as CAST(expression AS type)
    ParserExpressionInCastExpression expression_and_type(false);

    ASTPtr expr_list_args;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    const auto contents_begin = pos;
    ASTPtr first_argument;
    if (!expression_and_type.parse(pos, first_argument, expected))
        return false;

    /// check for subsequent comma ","
    if (pos->type != TokenType::Comma)
    {
        /// CAST(expression AS type)
        const auto type = first_argument->tryGetAlias();
        if (type.empty())
        {
            /// there is only one argument and it has no alias
            expected.add(pos, "type identifier");
            return false;
        }

        expr_list_args = std::make_shared<ASTExpressionList>(StringRange{contents_begin, pos});
        first_argument->setAlias({});
        expr_list_args->children.push_back(first_argument);
        expr_list_args->children.emplace_back(std::make_shared<ASTLiteral>(StringRange(), type));
    }
    else
    {
        pos = contents_begin;

        /// CAST(expression, 'type')
        /// Reparse argument list from scratch
        ParserExpressionWithOptionalAlias expression{false};
        if (!expression.parse(pos, first_argument, expected))
            return false;

        if (pos->type != TokenType::Comma)
            return false;
        ++pos;

        ParserStringLiteral p_type;
        ASTPtr type_as_literal;

        if (!p_type.parse(pos, type_as_literal, expected))
        {
            expected.add(pos, "string literal depicting type");
            return false;
        }

        expr_list_args = std::make_shared<ASTExpressionList>(StringRange{contents_begin, pos});
        expr_list_args->children.push_back(first_argument);
        expr_list_args->children.push_back(type_as_literal);
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    const auto function_node = std::make_shared<ASTFunction>(StringRange(begin, pos));
    ASTPtr node_holder{function_node};
    function_node->name = name;

    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    node = node_holder;
    return true;
}


bool ParserNull::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;
    ParserKeyword nested_parser("NULL");
    if (nested_parser.parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(StringRange(StringRange(begin, pos)), Null());
        return true;
    }
    else
        return false;
}


bool ParserNumber::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool negative = false;

    if (pos->type == TokenType::Minus)
    {
        ++pos;
        negative = true;
    }
    else if (pos->type == TokenType::Plus)  /// Leading plus is simply ignored.
        ++pos;

    Field res;

    Pos begin = pos;
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

    memcpy(buf, pos->begin, pos->size());
    buf[pos->size()] = 0;

    char * pos_double = buf;
    errno = 0;    /// Functions strto* don't clear errno.
    Float64 float_value = std::strtod(buf, &pos_double);
    if (pos_double != buf + pos->size() || errno == ERANGE)
    {
        expected.add(pos, "number");
        return false;
    }

    if (float_value < 0)
        throw Exception("Logical error: token number cannot begin with minus, but parsed float number is less than zero.", ErrorCodes::LOGICAL_ERROR);

    if (negative)
        float_value = -float_value;

    res = float_value;

    /// try to use more exact type: UInt64

    char * pos_integer = buf;

    errno = 0;
    UInt64 uint_value = std::strtoull(buf, &pos_integer, 0);
    if (pos_integer == pos_double && errno != ERANGE && (!negative || uint_value <= (1ULL << 63)))
    {
        if (negative)
            res = -static_cast<Int64>(uint_value);
        else
            res = uint_value;
    }

    ++pos;
    node = std::make_shared<ASTLiteral>(StringRange(begin, pos), res);
    return true;
}


bool ParserUnsignedInteger::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Field res;

    Pos begin = pos;
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
    ++pos;
    node = std::make_shared<ASTLiteral>(StringRange(begin, pos), res);
    return true;
}


bool ParserStringLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::StringLiteral)
        return false;

    Pos begin = pos;

    String s;
    ReadBufferFromMemory in(pos->begin, pos->size());

    try
    {
        readQuotedStringWithSQLStyle(s, in);
    }
    catch (const Exception & e)
    {
        expected.add(pos, "string literal");
        return false;
    }

    if (in.count() != pos->size())
    {
        expected.add(pos, "string literal");
        return false;
    }

    ++pos;
    node = std::make_shared<ASTLiteral>(StringRange(begin, pos), s);
    return true;
}


bool ParserArrayOfLiterals::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::OpeningSquareBracket)
        return false;

    Pos begin = pos;
    Array arr;

    ParserLiteral literal_p;

    ++pos;

    while (pos.isValid())
    {
        if (!arr.empty())
        {
            if (pos->type == TokenType::ClosingSquareBracket)
            {
                ++pos;
                node = std::make_shared<ASTLiteral>(StringRange(begin, pos), arr);
                return true;
            }
            else if (pos->type == TokenType::Comma)
            {
                ++pos;
            }
            else
            {
                expected.add(pos, "comma or closing square bracket");
                return false;
            }
        }

        ASTPtr literal_node;
        if (!literal_p.parse(pos, literal_node, expected))
            return false;

        arr.push_back(typeid_cast<const ASTLiteral &>(*literal_node).value);
    }

    expected.add(pos, "closing square bracket");
    return false;
}


bool ParserLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserNull null_p;
    ParserNumber num_p;
    ParserStringLiteral str_p;

    if (null_p.parse(pos, node, expected))
        return true;

    if (num_p.parse(pos, node, expected))
        return true;

    if (str_p.parse(pos, node, expected))
        return true;

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
bool ParserAliasImpl<ParserIdentifier>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_as("AS");
    ParserIdentifier id_p;

    bool has_as_word = s_as.parse(pos, node, expected);
    if (!allow_alias_without_as_keyword && !has_as_word)
        return false;

    if (!id_p.parse(pos, node, expected))
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


bool ParserAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected &)
{
    Pos begin = pos;
    if (pos->type == TokenType::Asterisk)
    {
        ++pos;
        node = std::make_shared<ASTAsterisk>(StringRange(begin, pos));
        return true;
    }
    return false;
}


bool ParserQualifiedAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;

    if (!ParserCompoundIdentifier().parse(pos, node, expected))
        return false;

    if (pos->type != TokenType::Dot)
        return false;
    ++pos;

    if (pos->type != TokenType::Asterisk)
        return false;
    ++pos;

    auto res = std::make_shared<ASTQualifiedAsterisk>(StringRange(begin, pos));
    res->children.push_back(node);
    node = std::move(res);
    return true;
}


bool ParserExpressionElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
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

    if (subquery_p.parse(pos, node, expected))
        return true;

    if (paren_p.parse(pos, node, expected))
        return true;

    if (array_lite_p.parse(pos, node, expected))
        return true;

    if (array_p.parse(pos, node, expected))
        return true;

    if (lit_p.parse(pos, node, expected))
        return true;

    if (fun_p.parse(pos, node, expected))
        return true;

    if (qualified_asterisk_p.parse(pos, node, expected))
        return true;

    if (asterisk_p.parse(pos, node, expected))
        return true;

    if (id_p.parse(pos, node, expected))
        return true;

    return false;
}


template <typename ParserAlias>
bool ParserWithOptionalAliasImpl<ParserAlias>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
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
        if (const ASTIdentifier * id = typeid_cast<const ASTIdentifier *>(node.get()))
            if (0 == strcasecmp(id->name.data(), "FROM"))
                allow_alias_without_as_keyword_now = false;

    ASTPtr alias_node;
    if (ParserAlias(allow_alias_without_as_keyword_now).parse(pos, alias_node, expected))
    {
        String alias_name = typeid_cast<ASTIdentifier &>(*alias_node).name;

        if (ASTWithAlias * ast_with_alias = dynamic_cast<ASTWithAlias *>(node.get()))
        {
            ast_with_alias->alias = alias_name;
            ast_with_alias->prefer_alias_to_column_name = prefer_alias_to_column_name;
        }
        else
        {
            expected.add(pos, "alias cannot be here");
            return false;
        }
    }

    return true;
}

template class ParserWithOptionalAliasImpl<ParserAlias>;
template class ParserWithOptionalAliasImpl<ParserCastExpressionAlias>;


bool ParserOrderByElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos begin = pos;

    ParserExpressionWithOptionalAlias elem_p(false);
    ParserKeyword ascending("ASCENDING");
    ParserKeyword descending("DESCENDING");
    ParserKeyword asc("ASC");
    ParserKeyword desc("DESC");
    ParserKeyword nulls("NULLS");
    ParserKeyword first("FIRST");
    ParserKeyword last("LAST");
    ParserKeyword collate("COLLATE");
    ParserStringLiteral collate_locale_parser;

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

    node = std::make_shared<ASTOrderByElement>(StringRange(begin, pos), direction, nulls_direction, nulls_direction_was_explicitly_specified, locale_node);
    node->children.push_back(expr_elem);
    if (locale_node)
        node->children.push_back(locale_node);

    return true;
}

bool ParserWeightedZooKeeperPath::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_weight("WEIGHT");
    ParserStringLiteral path_p;
    ParserUnsignedInteger weight_p;

    auto weighted_zookeeper_path = std::make_shared<ASTWeightedZooKeeperPath>();
    node = weighted_zookeeper_path;

    ASTPtr path_node;
    if (!path_p.parse(pos, path_node, expected))
        return false;

    weighted_zookeeper_path->path = typeid_cast<const ASTLiteral &>(*path_node).value.get<const String &>();

    bool is_weight_set = false;

    if (s_weight.ignore(pos, expected))
    {
        ASTPtr weight_node;
        if (weight_p.parse(pos, weight_node, expected))
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

