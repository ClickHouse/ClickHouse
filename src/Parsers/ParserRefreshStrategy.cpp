#include <Parsers/ParserRefreshStrategy.h>

#include <Parsers/ASTRefreshStrategy.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTimeInterval.h>
#include <Parsers/CommonParsers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool ParserRefreshStrategy::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto refresh = std::make_shared<ASTRefreshStrategy>();

    if (ParserKeyword{"AFTER"}.ignore(pos, expected))
    {
        refresh->schedule_kind = RefreshScheduleKind::AFTER;
        ASTPtr period;
        if (!ParserTimeInterval{}.parse(pos, period, expected))
            return false;

        refresh->set(refresh->period, period);
    }
    else if (ParserKeyword{"EVERY"}.ignore(pos, expected))
    {
        refresh->schedule_kind = RefreshScheduleKind::EVERY;
        ASTPtr period;
        if (!ParserTimeInterval{{.allow_mixing_calendar_and_clock_units = false}}.parse(pos, period, expected))
            return false;
        refresh->set(refresh->period, period);
        if (ParserKeyword{"OFFSET"}.ignore(pos, expected))
        {
            ASTPtr periodic_offset;
            if (!ParserTimeInterval{{.allow_zero = true}}.parse(pos, periodic_offset, expected))
                return false;

            if (periodic_offset->as<ASTTimeInterval>()->interval.maxSeconds()
                    >= period->as<ASTTimeInterval>()->interval.minSeconds())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "OFFSET must be less than the period");

            refresh->set(refresh->offset, periodic_offset);
        }
    }
    if (refresh->schedule_kind == RefreshScheduleKind::UNKNOWN)
        return false;

    if (ParserKeyword{"RANDOMIZE FOR"}.ignore(pos, expected))
    {
        ASTPtr spread;
        if (!ParserTimeInterval{{.allow_zero = true}}.parse(pos, spread, expected))
            return false;

        refresh->set(refresh->spread, spread);
    }

    if (ParserKeyword{"DEPENDS ON"}.ignore(pos, expected))
    {
        if (refresh->schedule_kind == RefreshScheduleKind::AFTER)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DEPENDS ON is allowed only for REFRESH EVERY, not REFRESH AFTER");

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
