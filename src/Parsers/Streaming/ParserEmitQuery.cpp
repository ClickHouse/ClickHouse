#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Parsers/Streaming/ParserEmitQuery.h>
#include <Parsers/Streaming/ParserIntervalAliasExpression.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}


bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// EMIT [STREAM]
    ///         - [PERIODIC INTERVAL '3' SECONDS]
    /// ...
    if (!parse_only_internals)
    {
        ParserKeyword s_emit("EMIT");
        if (!s_emit.ignore(pos, expected))
            return false;
    }

    /// Optional `STREAM` keyword
    ParserKeyword("STREAM").ignore(pos, expected);

    ASTPtr periodic_interval;

    ParserIntervalOperatorExpression interval_alias_p;

    if (ParserKeyword("PERIODIC").ignore(pos, expected))
    {
        /// [PERIODIC INTERVAL '3' SECONDS]
        if (periodic_interval)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use repeat 'PERIODIC' in EMIT clause");

        if (!interval_alias_p.parse(pos, periodic_interval, expected))
            return false;
    }

    auto query = std::make_shared<ASTEmitQuery>();
    query->periodic_interval = periodic_interval;

    node = query;

    return true;
}

}
