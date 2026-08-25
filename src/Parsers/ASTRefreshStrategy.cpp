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

    ostr << "REFRESH ";
    using enum RefreshScheduleKind;
    switch (schedule_kind)
    {
        case AFTER:
            ostr << "AFTER ";
            period->format(ostr, f_settings, state, frame);
            break;
        case EVERY:
            ostr << "EVERY ";
            period->format(ostr, f_settings, state, frame);
            if (offset)
            {
                ostr << " OFFSET ";
                offset->format(ostr, f_settings, state, frame);
            }
            break;
        default:
            break;
    }

    if (spread)
    {
        ostr << " RANDOMIZE FOR ";
        spread->format(ostr, f_settings, state, frame);
    }
    if (dependencies)
    {
        ostr << " DEPENDS ON ";
        dependencies->format(ostr, f_settings, state, frame);
    }
    if (settings)
    {
        ostr << " SETTINGS ";
        settings->format(ostr, f_settings, state, frame);
    }
    if (append)
        ostr << " APPEND";
}

}
