#include <Access/OPA/OpaExpressions.h>

#include <Access/RowPolicy.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

ASTPtr parseOpaExpression(const String & expression, const String & description)
{
    if (expression.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "The Open Policy Agent policy returned an empty {}", description);

    ParserExpression parser;
    ASTPtr result;

    try
    {
        result = parseQuery(parser, expression, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (Exception & e)
    {
        /// Naming the offending text and what it was for is the difference between a policy author
        /// finding the broken rule and guessing at it.
        e.addMessage("while parsing the {} {} returned by the Open Policy Agent policy", description, backQuote(expression));
        throw;
    }

    return result;
}

ASTPtr parseOpaRowFilterExpression(const String & expression, const String & description)
{
    auto result = parseOpaExpression(expression, description);

    /// A row filter is evaluated as a per-row predicate by the reader, so a function that changes the
    /// number of rows breaks the reader's invariants. The same restriction applies to a row policy
    /// written in SQL.
    checkRowPolicyFilterExpression(result);

    return result;
}

}
