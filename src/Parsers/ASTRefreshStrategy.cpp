#include <Parsers/ASTRefreshStrategy.h>

#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTRefreshStrategy::clone() const
{
    auto res = std::make_shared<ASTRefreshStrategy>(*this);
    res->children.clear();

    if (interval)
        res->set(res->interval, interval->clone());
    if (period)
        res->set(res->period, period->clone());
    if (periodic_offset)
        res->set(res->periodic_offset, periodic_offset->clone());
    if (spread)
        res->set(res->spread, spread->clone());
    if (settings)
        res->set(res->settings, settings->clone());
    if (dependencies)
        res->set(res->dependencies, dependencies->clone());
    res->schedule_kind = schedule_kind;
    return res;
}

void ASTRefreshStrategy::formatImpl(
    const IAST::FormatSettings & f_settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    frame.need_parens = false;

    f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << "REFRESH " << (f_settings.hilite ? hilite_none : "");
    using enum ScheduleKind;
    switch (schedule_kind)
    {
        case AFTER:
            f_settings.ostr << "AFTER " << (f_settings.hilite ? hilite_none : "");
            interval->formatImpl(f_settings, state, frame);
            break;
        case EVERY:
            f_settings.ostr << "EVERY " << (f_settings.hilite ? hilite_none : "");
            period->formatImpl(f_settings, state, frame);
            if (periodic_offset)
            {
                f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " OFFSET " << (f_settings.hilite ? hilite_none : "");
                periodic_offset->formatImpl(f_settings, state, frame);
            }
            break;
        default:
            f_settings.ostr << (f_settings.hilite ? hilite_none : "");
            break;
    }

    if (spread)
    {
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " RANDOMIZE FOR " << (f_settings.hilite ? hilite_none : "");
        spread->formatImpl(f_settings, state, frame);
    }
    if (dependencies)
    {
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " DEPENDS ON " << (f_settings.hilite ? hilite_none : "");
        dependencies->formatImpl(f_settings, state, frame);
    }
    if (settings)
    {
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " SETTINGS " << (f_settings.hilite ? hilite_none : "");
        settings->formatImpl(f_settings, state, frame);
    }
}

}
