#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <IO/Operators.h>
#include <base/EnumReflection.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    ostr << "REFRESH";
    using enum RefreshScheduleKind;
    if (period)
    {
        switch (schedule_kind)
        {
            case AFTER:
                ostr << " AFTER ";
                period->format(ostr, f_settings, state, frame);
                break;
            case EVERY:
                ostr << " EVERY ";
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
    w.writeString("schedule_kind", std::string(magic_enum::enum_name(schedule_kind)));
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
    String schedule_kind_str = r.getString("schedule_kind");
    auto kind_opt = magic_enum::enum_cast<RefreshScheduleKind>(schedule_kind_str);
    if (kind_opt)
        schedule_kind = *kind_opt;
    else if (!schedule_kind_str.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown RefreshScheduleKind: '{}'", schedule_kind_str);
    auto period_child = r.readChild("period");
    if (period_child)
        set(period, period_child);
    else if (schedule_kind == RefreshScheduleKind::AFTER || schedule_kind == RefreshScheduleKind::EVERY)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Required field 'period' is missing in JSON AST for RefreshStrategy with schedule_kind '{}'",
            magic_enum::enum_name(schedule_kind));
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
