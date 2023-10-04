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

// namespace
// {
// bool parseIntervalExpression(IParser::Pos & pos, ASTPtr & node, Expected & expected)
// {
//     if (ParserIntervalAliasExpression().parse(pos, node, expected))
//         return true;

//     if (ParserKeyword("INTERVAL").ignore(pos, expected))
//     {
//         auto start = std::make_unique<IntervalLayer>();
//         return ParserExpressionImpl().parse(std::move(start), pos, node, expected);
//     }

//     return false;
// }
// }

bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// EMIT [STREAM]
    ///         - [PERIODIC INTERVAL '3' SECONDS]
    ///         - [AFTER WATERMARK]
    ///         - [DELAY INTERVAL '3' SECONDS]
    ///         - [LAST <last-x>]
    /// For each sub-option can be combined with 'AND', we shall select a matching mod based on the combination of parsed content finally.
    /// For example:
    /// 1) EMIT STREAM PERIODIC INTERVAL '3' SECONDS AND AFTER WATERMARK
    /// 2) EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL '3' SECONDS
    /// 3) EMIT STREAM AFTER WATERMARK AND LAST <last-x>
    /// 4) EMIT STREAM LAST 1h ON PROCTIME
    /// ...
    if (!parse_only_internals)
    {
        ParserKeyword s_emit("EMIT");
        if (!s_emit.ignore(pos, expected))
            return false;
    }

    bool streaming = false;
    if (ParserKeyword("STREAM").ignore(pos, expected))
        streaming = true;

    bool after_watermark = false;
    bool proctime = false;
    ASTPtr periodic_interval;
    ASTPtr delay_interval;
    ASTPtr last_interval;
    ASTPtr timeout_interval;

    ParserIntervalOperatorExpression interval_alias_p;
    do
    {
        if (ParserKeyword("PERIODIC").ignore(pos, expected))
        {
            /// [PERIODIC INTERVAL '3' SECONDS]
            if (periodic_interval)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use repeat 'PERIODIC' in EMIT clause");

            if (!interval_alias_p.parse(pos, periodic_interval, expected))
                return false;
        }
        else if (ParserKeyword("AFTER").ignore(pos, expected))
        {
            if (!ParserKeyword("WATERMARK").ignore(pos, expected))
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Expect 'WATERMARK' after 'AFTER' in EMIT clause");

            /// [AFTER WATERMARK]
            if (after_watermark)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use repeat 'AFTER WATERMARK' in EMIT clause");

            after_watermark = true;
        }
        else if (ParserKeyword("DELAY").ignore(pos, expected))
        {
            /// [DELAY INTERVAL '3' SECONDS]
            if (delay_interval)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use repeat 'DELAY' in EMIT clause");

            if (!interval_alias_p.parse(pos, delay_interval, expected))
                return false;
        }
        else if (ParserKeyword("LAST").ignore(pos, expected))
        {
            /// [LAST <last-x>]
            if (last_interval)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use repeat 'LAST' in EMIT clause");

            if (!interval_alias_p.parse(pos, last_interval, expected))
                return false;

            if (ParserKeyword("ON").ignore(pos, expected))
            {
                if (ParserKeyword("PROCTIME").ignore(pos, expected))
                    proctime = true;
                else
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Expect 'PROCTIME' after 'ON' in EMIT clause");
            }
        }
        else if (ParserKeyword("TIMEOUT").ignore(pos, expected))
        {
            /// [TIMEOUT INTERVAL '5' SECONDS]
            if (timeout_interval)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Can not use repeat 'TIMEOUT' in EMIT clause");

            if (!interval_alias_p.parse(pos, timeout_interval, expected))
                return false;
        }
    } while (ParserKeyword("AND").ignore(pos, expected));

    auto query = std::make_shared<ASTEmitQuery>();
    query->streaming = streaming;
    query->after_watermark = after_watermark;
    query->proc_time = proctime;
    query->periodic_interval = periodic_interval;
    query->delay_interval = delay_interval;
    query->last_interval = last_interval;
    query->timeout_interval = timeout_interval;

    node = query;

    return true;
}

}
