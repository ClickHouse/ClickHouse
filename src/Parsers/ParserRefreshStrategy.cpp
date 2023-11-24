#include <Parsers/ParserRefreshStrategy.h>

#include <Parsers/ASTRefreshStrategy.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTimeInterval.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

bool ParserRefreshStrategy::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto refresh = std::make_shared<ASTRefreshStrategy>();

    if (ParserKeyword{"AFTER"}.ignore(pos, expected))
    {
        refresh->schedule_kind = ASTRefreshStrategy::ScheduleKind::AFTER;
        ASTPtr interval;
        if (!ParserTimeInterval{}.parse(pos, interval, expected))
            return false;

        refresh->set(refresh->interval, interval);
    }
    else if (ParserKeyword{"EVERY"}.ignore(pos, expected))
    {
        refresh->schedule_kind = ASTRefreshStrategy::ScheduleKind::EVERY;
        ASTPtr period;
        if (!ParserTimePeriod{}.parse(pos, period, expected))
            return false;
        refresh->set(refresh->period, period);
        if (ParserKeyword{"OFFSET"}.ignore(pos, expected))
        {
            ASTPtr periodic_offset;
            if (!ParserTimeInterval{}.parse(pos, periodic_offset, expected))
                return false;
            refresh->set(refresh->periodic_offset, periodic_offset);
        }
    }
    if (refresh->schedule_kind == ASTRefreshStrategy::ScheduleKind::UNKNOWN)
        return false;

    if (ParserKeyword{"RANDOMIZE FOR"}.ignore(pos, expected))
    {
        ASTPtr spread;
        if (!ParserTimePeriod{}.parse(pos, spread, expected))
            return false;

        refresh->set(refresh->spread, spread);
    }

    if (ParserKeyword{"DEPENDS ON"}.ignore(pos, expected))
    {
        ASTPtr dependencies;

        auto list_parser = ParserList{
            std::make_unique<ParserCompoundIdentifier>(
                /*table_name_with_optional_uuid_*/ true, /*allow_query_parameter_*/ false),
            std::make_unique<ParserToken>(TokenType::Comma),
            /*allow_empty*/ false};
        if (!list_parser.parse(pos, dependencies, expected))
            return false;
        refresh->set(refresh->dependencies, dependencies);
    }

    // Refresh SETTINGS
    if (ParserKeyword{"SETTINGS"}.ignore(pos, expected))
    {
        /// Settings are written like SET query, so parse them with ParserSetQuery
        ASTPtr settings;
        if (!ParserSetQuery{true}.parse(pos, settings, expected))
            return false;
        refresh->set(refresh->settings, settings);
    }
    node = refresh;
    return true;
}

}
