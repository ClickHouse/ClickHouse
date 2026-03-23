#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTRefreshStrategy::clone() const
{
    auto res = make_intrusive<ASTRefreshStrategy>(*this);
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

void ASTRefreshStrategy::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "RefreshStrategy");
    w.writeInt("schedule_kind", static_cast<Int64>(schedule_kind));
    w.writeChild("period", period);
    w.writeChild("offset", offset);
    w.writeChild("spread", spread);
    w.writeChild("settings", settings);
    w.writeChild("dependencies", dependencies);
    if (append)
        w.writeBool("append", true);
}

void ASTRefreshStrategy::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    schedule_kind = static_cast<RefreshScheduleKind>(r.getInt("schedule_kind"));
    auto period_child = r.readChild("period");
    if (period_child)
        set(period, period_child);
    auto offset_child = r.readChild("offset");
    if (offset_child)
        set(offset, offset_child);
    auto spread_child = r.readChild("spread");
    if (spread_child)
        set(spread, spread_child);
    auto settings_child = r.readChild("settings");
    if (settings_child)
        set(settings, settings_child);
    auto dependencies_child = r.readChild("dependencies");
    if (dependencies_child)
        set(dependencies, dependencies_child);
    append = r.getBool("append");
}

}
