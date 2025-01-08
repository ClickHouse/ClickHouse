#include <Parsers/ASTRefreshStrategy.h>

#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTRefreshStrategy::clone() const
{
    auto res = std::make_shared<ASTRefreshStrategy>(*this);
    res->children.clear();

    if (period)
        res->set(res->period, period->clone());
    if (offset)
        res->set(res->offset, offset->clone());
    if (spread)
        res->set(res->spread, spread->clone());
    if (settings)
        res->set(res->settings, settings->clone());
    if (dependencies)
        res->set(res->dependencies, dependencies->clone());
    return res;
}

void ASTRefreshStrategy::formatImpl(
    WriteBuffer & ostr, const IAST::FormatSettings & f_settings, IAST::FormatState & state, IAST::FormatStateStacked frame) const
{
    frame.need_parens = false;

    ostr << (f_settings.hilite ? hilite_keyword : "") << "REFRESH " << (f_settings.hilite ? hilite_none : "");
    using enum RefreshScheduleKind;
    switch (schedule_kind)
    {
        case AFTER:
            ostr << "AFTER " << (f_settings.hilite ? hilite_none : "");
            period->formatImpl(ostr, f_settings, state, frame);
            break;
        case EVERY:
            ostr << "EVERY " << (f_settings.hilite ? hilite_none : "");
            period->formatImpl(ostr, f_settings, state, frame);
            if (offset)
            {
                ostr << (f_settings.hilite ? hilite_keyword : "") << " OFFSET " << (f_settings.hilite ? hilite_none : "");
                offset->formatImpl(ostr, f_settings, state, frame);
            }
            break;
        default:
            ostr << (f_settings.hilite ? hilite_none : "");
            break;
    }

    if (spread)
    {
        ostr << (f_settings.hilite ? hilite_keyword : "") << " RANDOMIZE FOR " << (f_settings.hilite ? hilite_none : "");
        spread->formatImpl(ostr, f_settings, state, frame);
    }
    if (dependencies)
    {
        ostr << (f_settings.hilite ? hilite_keyword : "") << " DEPENDS ON " << (f_settings.hilite ? hilite_none : "");
        dependencies->formatImpl(ostr, f_settings, state, frame);
    }
    if (settings)
    {
        ostr << (f_settings.hilite ? hilite_keyword : "") << " SETTINGS " << (f_settings.hilite ? hilite_none : "");
        settings->formatImpl(ostr, f_settings, state, frame);
    }
    if (append)
        ostr << (f_settings.hilite ? hilite_keyword : "") << " APPEND" << (f_settings.hilite ? hilite_none : "");
}

}
