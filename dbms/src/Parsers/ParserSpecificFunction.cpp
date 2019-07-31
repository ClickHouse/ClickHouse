#include <Parsers/ParserSpecificFunction.h>

#include <Poco/String.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

bool MySQLParseFunction(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserFunction().parse(pos, node, expected))
    {
        const auto & ast_function = node->as<ASTFunction>();
        const ASTPtr & function_arguments = ast_function->arguments;
        function_arguments->children.back()->as<ASTLiteral>()->password = true;
        return true;
    }

    return false;
}

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

bool ParserSpecificFunction::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    Pos pos_before_specifier = pos;
    {
        ParserIdentifier function_name_parser;

        ASTPtr identifier;
        if (!function_name_parser.parse(pos_before_specifier, identifier, expected))
            return false;

        if (const auto & function_name = identifier->as<const ASTIdentifier>())
        {
            if (const auto & parser_function = SpecificParserFunctionFactory::instance().tryGet(function_name->name))
                return (*parser_function)(pos, node, expected);
        }
    }

    return ParserFunction().parse(pos, node, expected);
}

ParserSpecificFunction::SpecificParserFunctionFactory::SpecificParserFunctionFactory()
{
    parser_function_dictionary["MySQL"] = MySQLParseFunction;
}

const ParserSpecificFunction::SpecificParserFunctionFactory::ParseFunction * ParserSpecificFunction::SpecificParserFunctionFactory::tryGet(const String & function_name)
{
    const auto & find_it = parser_function_dictionary.find(function_name);
    if (find_it != parser_function_dictionary.end())
        return &(find_it->second);

    return nullptr;
}

}
