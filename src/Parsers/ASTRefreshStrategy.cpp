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
    res->interval = interval;
    res->spread = spread;
    res->schedule_kind = schedule_kind;
    return res;
}

void ASTRefreshStrategy::formatImpl(
    const IAST::FormatSettings & f_settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    frame.need_parens = false;

    f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << "REFRESH ";
    using enum ScheduleKind;
    switch (schedule_kind)
    {
        case AFTER:
            f_settings.ostr << "AFTER ";
            interval->formatImpl(f_settings, state, frame);
            break;
        case EVERY:
            f_settings.ostr << "EVERY ";
            period->formatImpl(f_settings, state, frame);
            if (periodic_offset)
            {
                f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " OFFSET ";
                periodic_offset->formatImpl(f_settings, state, frame);
            }
            break;
        default:
            break;
    }

    if (spread)
    {
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " RANDOMIZE FOR ";
        spread->formatImpl(f_settings, state, frame);
    }
    if (dependencies)
    {
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " DEPENDS ON ";
        dependencies->formatImpl(f_settings, state, frame);
    }
    if (settings)
    {
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << " SETTINGS ";
        settings->formatImpl(f_settings, state, frame);
    }
}

}
