#include <errno.h>
#include <cstdlib>

#include <Poco/String.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/typeid_cast.h>
#include <Parsers/DumpASTNode.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSubquery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserCase.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>

#include <Parsers/queryToString.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
}


bool ParserArray::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
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

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "array";
    function_node->arguments = contents_node;
    function_node->children.push_back(contents_node);
    node = function_node;

    return true;
}


bool ParserParenthesisExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
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

    const auto & expr_list = contents_node->as<ASTExpressionList &>();

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
        auto function_node = std::make_shared<ASTFunction>();
        function_node->name = "tuple";
        function_node->arguments = contents_node;
        function_node->children.push_back(contents_node);
        node = function_node;
    }

    return true;
}


bool ParserSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr select_node;
    ParserSelectWithUnionQuery select;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!select.parse(pos, select_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    node = std::make_shared<ASTSubquery>();
    node->children.push_back(select_node);
    return true;
}


bool ParserIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected &)
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

    return false;
}


bool ParserCompoundIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr id_list;
    if (!ParserList(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Dot), false)
        .parse(pos, id_list, expected))
        return false;

    String name;
    std::vector<String> parts;
    const auto & list = id_list->as<ASTExpressionList &>();
    for (const auto & child : list.children)
    {
        if (!name.empty())
            name += '.';
        parts.emplace_back(*getIdentifierName(child));
        name += parts.back();
    }

    if (parts.size() == 1)
        parts.clear();
    node = std::make_shared<ASTIdentifier>(name, std::move(parts));

    return true;
}


bool ParserFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
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
    if (*getIdentifierName(identifier) == "toDate"
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
        std::string contents_str(contents_begin, contents_end - contents_begin);
        throw Exception("Argument of function toDate is unquoted: toDate(" + contents_str + "), must be: toDate('" + contents_str + "')"
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

    auto function_node = std::make_shared<ASTFunction>();
    getIdentifierName(identifier, function_node->name);

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

bool ParserCastExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Either CAST(expr AS type) or CAST(expr, 'type')
    /// The latter will be parsed normally as a function later.

    ASTPtr expr_node;
    ASTPtr type_node;

    if (ParserKeyword("CAST").ignore(pos, expected)
        && ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected)
        && ParserExpression().parse(pos, expr_node, expected)
        && ParserKeyword("AS").ignore(pos, expected)
        && ParserIdentifierWithOptionalParameters().parse(pos, type_node, expected)
        && ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
    {
        /// Convert to canonical representation in functional form: CAST(expr, 'type')

        auto type_literal = std::make_shared<ASTLiteral>(queryToString(type_node));

        auto expr_list_args = std::make_shared<ASTExpressionList>();
        expr_list_args->children.push_back(expr_node);
        expr_list_args->children.push_back(std::move(type_literal));

        auto func_node = std::make_shared<ASTFunction>();
        func_node->name = "CAST";
        func_node->arguments = std::move(expr_list_args);
        func_node->children.push_back(func_node->arguments);

        node = std::move(func_node);
        return true;
    }

    return false;
}

bool ParserSubstringExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Either SUBSTRING(expr FROM start) or SUBSTRING(expr FROM start FOR length) or SUBSTRING(expr, start, length)
    /// The latter will be parsed normally as a function later.

    ASTPtr expr_node;
    ASTPtr start_node;
    ASTPtr length_node;

    if (!ParserKeyword("SUBSTRING").ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!ParserExpression().parse(pos, expr_node, expected))
        return false;

    if (pos->type != TokenType::Comma)
    {
        if (!ParserKeyword("FROM").ignore(pos, expected))
            return false;
    }
    else
    {
        ++pos;
    }

    if (!ParserExpression().parse(pos, start_node, expected))
        return false;

    if (pos->type == TokenType::ClosingRoundBracket)
    {
        ++pos;
    }
    else
    {
        if (pos->type != TokenType::Comma)
        {
            if (!ParserKeyword("FOR").ignore(pos, expected))
                return false;
        }
        else
        {
            ++pos;
        }

        if (!ParserExpression().parse(pos, length_node, expected))
            return false;

        ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected);
    }

    /// Convert to canonical representation in functional form: SUBSTRING(expr, start, length)

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    expr_list_args->children = {expr_node, start_node};

    if (length_node)
        expr_list_args->children.push_back(length_node);

    auto func_node = std::make_shared<ASTFunction>();
    func_node->name = "substring";
    func_node->arguments = std::move(expr_list_args);
    func_node->children.push_back(func_node->arguments);

    node = std::move(func_node);
    return true;
}

bool ParserTrimExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Handles all possible TRIM/LTRIM/RTRIM call variants

    std::string func_name;
    bool trim_left = false;
    bool trim_right = false;
    bool char_override = false;
    ASTPtr expr_node;
    ASTPtr pattern_node;
    ASTPtr to_remove;

    if (ParserKeyword("LTRIM").ignore(pos, expected))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
            return false;
        ++pos;
        trim_left = true;
    }
    else if (ParserKeyword("RTRIM").ignore(pos, expected))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
            return false;
        ++pos;
        trim_right = true;
    }
    else if (ParserKeyword("TRIM").ignore(pos, expected))
    {
        if (pos->type != TokenType::OpeningRoundBracket)
            return false;
        ++pos;

        if (ParserKeyword("BOTH").ignore(pos, expected))
        {
            trim_left = true;
            trim_right = true;
            char_override = true;
        }
        else if (ParserKeyword("LEADING").ignore(pos, expected))
        {
            trim_left = true;
            char_override = true;
        }
        else if (ParserKeyword("TRAILING").ignore(pos, expected))
        {
            trim_right = true;
            char_override = true;
        }
        else
        {
            trim_left = true;
            trim_right = true;
        }

        if (char_override)
        {
            if (!ParserExpression().parse(pos, to_remove, expected))
                return false;
            if (!ParserKeyword("FROM").ignore(pos, expected))
                return false;

            auto quote_meta_func_node = std::make_shared<ASTFunction>();
            auto quote_meta_list_args = std::make_shared<ASTExpressionList>();
            quote_meta_list_args->children = {to_remove};

            quote_meta_func_node->name = "regexpQuoteMeta";
            quote_meta_func_node->arguments = std::move(quote_meta_list_args);
            quote_meta_func_node->children.push_back(quote_meta_func_node->arguments);

            to_remove = std::move(quote_meta_func_node);
        }
    }

    if (!(trim_left || trim_right))
        return false;

    if (!ParserExpression().parse(pos, expr_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    /// Convert to regexp replace function call

    if (char_override)
    {
        auto pattern_func_node = std::make_shared<ASTFunction>();
        auto pattern_list_args = std::make_shared<ASTExpressionList>();
        if (trim_left && trim_right)
        {
            pattern_list_args->children = {
                std::make_shared<ASTLiteral>("^["),
                to_remove,
                std::make_shared<ASTLiteral>("]*|["),
                to_remove,
                std::make_shared<ASTLiteral>("]*$")
            };
            func_name = "replaceRegexpAll";
        }
        else
        {
            if (trim_left)
            {
                pattern_list_args->children = {
                    std::make_shared<ASTLiteral>("^["),
                    to_remove,
                    std::make_shared<ASTLiteral>("]*")
                };
            }
            else
            {
                /// trim_right == false not possible
                pattern_list_args->children = {
                    std::make_shared<ASTLiteral>("["),
                    to_remove,
                    std::make_shared<ASTLiteral>("]*$")
                };
            }
            func_name = "replaceRegexpOne";
        }

        pattern_func_node->name = "concat";
        pattern_func_node->arguments = std::move(pattern_list_args);
        pattern_func_node->children.push_back(pattern_func_node->arguments);

        pattern_node = std::move(pattern_func_node);
    }
    else
    {
        if (trim_left && trim_right)
        {
            func_name = "trimBoth";
        }
        else
        {
            if (trim_left)
            {
                func_name = "trimLeft";
            }
            else
            {
                /// trim_right == false not possible
                func_name = "trimRight";
            }
        }
    }

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    if (char_override)
        expr_list_args->children = {expr_node, pattern_node, std::make_shared<ASTLiteral>("")};
    else
        expr_list_args->children = {expr_node};

    auto func_node = std::make_shared<ASTFunction>();
    func_node->name = func_name;
    func_node->arguments = std::move(expr_list_args);
    func_node->children.push_back(func_node->arguments);

    node = std::move(func_node);
    return true;
}

bool ParserLeftExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Rewrites left(expr, length) to SUBSTRING(expr, 1, length)

    ASTPtr expr_node;
    ASTPtr start_node;
    ASTPtr length_node;

    if (!ParserKeyword("LEFT").ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!ParserExpression().parse(pos, expr_node, expected))
        return false;

    ParserToken(TokenType::Comma).ignore(pos, expected);

    if (!ParserExpression().parse(pos, length_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    start_node = std::make_shared<ASTLiteral>(1);
    expr_list_args->children = {expr_node, start_node, length_node};

    auto func_node = std::make_shared<ASTFunction>();
    func_node->name = "substring";
    func_node->arguments = std::move(expr_list_args);
    func_node->children.push_back(func_node->arguments);

    node = std::move(func_node);
    return true;
}

bool ParserRightExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Rewrites RIGHT(expr, length) to substring(expr, -length)

    ASTPtr expr_node;
    ASTPtr length_node;

    if (!ParserKeyword("RIGHT").ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!ParserExpression().parse(pos, expr_node, expected))
        return false;

    ParserToken(TokenType::Comma).ignore(pos, expected);

    if (!ParserExpression().parse(pos, length_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto start_expr_list_args = std::make_shared<ASTExpressionList>();
    start_expr_list_args->children = {length_node};

    auto start_node = std::make_shared<ASTFunction>();
    start_node->name = "negate";
    start_node->arguments = std::move(start_expr_list_args);
    start_node->children.push_back(start_node->arguments);

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    expr_list_args->children = {expr_node, start_node};

    auto func_node = std::make_shared<ASTFunction>();
    func_node->name = "substring";
    func_node->arguments = std::move(expr_list_args);
    func_node->children.push_back(func_node->arguments);

    node = std::move(func_node);
    return true;
}

bool ParserExtractExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword("EXTRACT").ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ASTPtr expr;
    const char * function_name = nullptr;

    ParserInterval interval_parser;
    if (!interval_parser.ignore(pos, expected))
        return false;

    switch (interval_parser.interval_kind)
    {
        case ParserInterval::IntervalKind::Second:
            function_name = "toSecond";
            break;
        case ParserInterval::IntervalKind::Minute:
            function_name = "toMinute";
            break;
        case ParserInterval::IntervalKind::Hour:
            function_name = "toHour";
            break;
        case ParserInterval::IntervalKind::Day:
            function_name = "toDayOfMonth";
            break;
        case ParserInterval::IntervalKind::Week:
            // TODO: SELECT toRelativeWeekNum(toDate('2017-06-15')) - toRelativeWeekNum(toStartOfYear(toDate('2017-06-15')))
            // else if (ParserKeyword("WEEK").ignore(pos, expected))
            //    function_name = "toRelativeWeekNum";
            return false;
        case ParserInterval::IntervalKind::Month:
            function_name = "toMonth";
            break;
        case ParserInterval::IntervalKind::Quarter:
            function_name = "toQuarter";
            break;
        case ParserInterval::IntervalKind::Year:
            function_name = "toYear";
            break;
        default:
            return false;
    }

    ParserKeyword s_from("FROM");
    if (!s_from.ignore(pos, expected))
        return false;

    ParserExpression elem_parser;
    if (!elem_parser.parse(pos, expr, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto function = std::make_shared<ASTFunction>();
    auto exp_list = std::make_shared<ASTExpressionList>();
    function->name = function_name; //"toYear";
    function->arguments = exp_list;
    function->children.push_back(exp_list);
    exp_list->children.push_back(expr);
    node = function;

    return true;
}

bool ParserDateAddExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const char * function_name = nullptr;
    ASTPtr timestamp_node;
    ASTPtr offset_node;

    if (ParserKeyword("DATEADD").ignore(pos, expected) || ParserKeyword("DATE_ADD").ignore(pos, expected)
        || ParserKeyword("TIMESTAMPADD").ignore(pos, expected) || ParserKeyword("TIMESTAMP_ADD").ignore(pos, expected))
        function_name = "plus";
    else if (ParserKeyword("DATESUB").ignore(pos, expected) || ParserKeyword("DATE_SUB").ignore(pos, expected)
        || ParserKeyword("TIMESTAMPSUB").ignore(pos, expected) || ParserKeyword("TIMESTAMP_SUB").ignore(pos, expected))
        function_name = "minus";
    else
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ParserInterval interval_parser;
    if (interval_parser.ignore(pos, expected))
    {
        /// function(unit, offset, timestamp)
        if (pos->type != TokenType::Comma)
            return false;
        ++pos;

        if (!ParserExpression().parse(pos, offset_node, expected))
            return false;

        if (pos->type != TokenType::Comma)
            return false;
        ++pos;

        if (!ParserExpression().parse(pos, timestamp_node, expected))
            return false;
    }
    else
    {
        /// function(timestamp, INTERVAL offset unit)
        if (!ParserExpression().parse(pos, timestamp_node, expected))
            return false;

        if (pos->type != TokenType::Comma)
            return false;
        ++pos;

        if (!ParserKeyword("INTERVAL").ignore(pos, expected))
            return false;

        if (!ParserExpression().parse(pos, offset_node, expected))
            return false;

        interval_parser.ignore(pos, expected);

    }
    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    const char * interval_function_name = interval_parser.getToIntervalKindFunctionName();

    auto interval_expr_list_args = std::make_shared<ASTExpressionList>();
    interval_expr_list_args->children = {offset_node};

    auto interval_func_node = std::make_shared<ASTFunction>();
    interval_func_node->name = interval_function_name;
    interval_func_node->arguments = std::move(interval_expr_list_args);
    interval_func_node->children.push_back(interval_func_node->arguments);

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    expr_list_args->children = {timestamp_node, interval_func_node};

    auto func_node = std::make_shared<ASTFunction>();
    func_node->name = function_name;
    func_node->arguments = std::move(expr_list_args);
    func_node->children.push_back(func_node->arguments);

    node = std::move(func_node);

    return true;
}

bool ParserDateDiffExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    const char * interval_name = nullptr;
    ASTPtr left_node;
    ASTPtr right_node;

    if (!(ParserKeyword("DATEDIFF").ignore(pos, expected) || ParserKeyword("DATE_DIFF").ignore(pos, expected)
        || ParserKeyword("TIMESTAMPDIFF").ignore(pos, expected) || ParserKeyword("TIMESTAMP_DIFF").ignore(pos, expected)))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ParserInterval interval_parser;
    if (!interval_parser.ignore(pos, expected))
        return false;

    switch (interval_parser.interval_kind)
    {
        case ParserInterval::IntervalKind::Second:
            interval_name = "second";
            break;
        case ParserInterval::IntervalKind::Minute:
            interval_name = "minute";
            break;
        case ParserInterval::IntervalKind::Hour:
            interval_name = "hour";
            break;
        case ParserInterval::IntervalKind::Day:
            interval_name = "day";
            break;
        case ParserInterval::IntervalKind::Week:
            interval_name = "week";
            break;
        case ParserInterval::IntervalKind::Month:
            interval_name = "month";
            break;
        case ParserInterval::IntervalKind::Quarter:
            interval_name = "quarter";
            break;
        case ParserInterval::IntervalKind::Year:
            interval_name = "year";
            break;
        default:
            return false;
    }

    if (pos->type != TokenType::Comma)
        return false;
    ++pos;

    if (!ParserExpression().parse(pos, left_node, expected))
        return false;

    if (pos->type != TokenType::Comma)
        return false;
    ++pos;

    if (!ParserExpression().parse(pos, right_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    expr_list_args->children = {std::make_shared<ASTLiteral>(interval_name), left_node, right_node};

    auto func_node = std::make_shared<ASTFunction>();
    func_node->name = "dateDiff";
    func_node->arguments = std::move(expr_list_args);
    func_node->children.push_back(func_node->arguments);

    node = std::move(func_node);

    return true;
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
            res = static_cast<Int64>(-uint_value);
        else
            res = uint_value;
    }

    ++pos;
    node = std::make_shared<ASTLiteral>(res);
    return true;
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
    ++pos;
    node = std::make_shared<ASTLiteral>(res);
    return true;
}


bool ParserStringLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::StringLiteral)
        return false;

    String s;
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

    ++pos;
    node = std::make_shared<ASTLiteral>(s);
    return true;
}


bool ParserArrayOfLiterals::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (pos->type != TokenType::OpeningSquareBracket)
        return false;

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
                node = std::make_shared<ASTLiteral>(arr);
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

        arr.push_back(literal_node->as<ASTLiteral &>().value);
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


const char * ParserAlias::restricted_keywords[] =
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
    "ASOF",
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
    "NOT",
    "BETWEEN",
    "LIKE",
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
          *  and in the query "SELECT x FRO FROM t", the word FRO was considered an alias.
          */

        const String name = *getIdentifierName(node);

        for (const char ** keyword = restricted_keywords; *keyword != nullptr; ++keyword)
            if (0 == strcasecmp(name.data(), *keyword))
                return false;
    }

    return true;
}


bool ParserAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected &)
{
    if (pos->type == TokenType::Asterisk)
    {
        ++pos;
        node = std::make_shared<ASTAsterisk>();
        return true;
    }
    return false;
}


bool ParserQualifiedAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserCompoundIdentifier().parse(pos, node, expected))
        return false;

    if (pos->type != TokenType::Dot)
        return false;
    ++pos;

    if (pos->type != TokenType::Asterisk)
        return false;
    ++pos;

    auto res = std::make_shared<ASTQualifiedAsterisk>();
    res->children.push_back(node);
    node = std::move(res);
    return true;
}


bool ParserExpressionElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    return ParserSubquery().parse(pos, node, expected)
        || ParserParenthesisExpression().parse(pos, node, expected)
        || ParserArrayOfLiterals().parse(pos, node, expected)
        || ParserArray().parse(pos, node, expected)
        || ParserLiteral().parse(pos, node, expected)
        || ParserCastExpression().parse(pos, node, expected)
        || ParserExtractExpression().parse(pos, node, expected)
        || ParserDateAddExpression().parse(pos, node, expected)
        || ParserDateDiffExpression().parse(pos, node, expected)
        || ParserSubstringExpression().parse(pos, node, expected)
        || ParserTrimExpression().parse(pos, node, expected)
        || ParserLeftExpression().parse(pos, node, expected)
        || ParserRightExpression().parse(pos, node, expected)
        || ParserCase().parse(pos, node, expected)
        || ParserFunction().parse(pos, node, expected)
        || ParserQualifiedAsterisk().parse(pos, node, expected)
        || ParserAsterisk().parse(pos, node, expected)
        || ParserCompoundIdentifier().parse(pos, node, expected);
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
        if (auto opt_id = getIdentifierName(node))
            if (0 == strcasecmp(opt_id->data(), "FROM"))
                allow_alias_without_as_keyword_now = false;

    ASTPtr alias_node;
    if (ParserAlias(allow_alias_without_as_keyword_now).parse(pos, alias_node, expected))
    {
        /// FIXME: try to prettify this cast using `as<>()`
        if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(node.get()))
        {
            getIdentifierName(alias_node, ast_with_alias->alias);
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

    node = std::make_shared<ASTOrderByElement>(direction, nulls_direction, nulls_direction_was_explicitly_specified, locale_node);
    node->children.push_back(expr_elem);
    if (locale_node)
        node->children.push_back(locale_node);

    return true;
}

}
