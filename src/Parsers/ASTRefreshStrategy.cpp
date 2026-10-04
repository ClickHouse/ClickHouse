#include <Parsers/ASTRefreshStrategy.h>
#include <Parsers/ASTIdentifier.h>
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
    if (!r.has("schedule_kind"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'schedule_kind' field in `RefreshStrategy` during AST JSON deserialization");
    String schedule_kind_str = r.getString("schedule_kind");
    auto kind_opt = magic_enum::enum_cast<RefreshScheduleKind>(schedule_kind_str);
    if (!kind_opt)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown RefreshScheduleKind: '{}'", schedule_kind_str);
    schedule_kind = *kind_opt;
    /// `period`/`offset`/`spread` are `ASTTimeInterval`, `settings` an `ASTSetQuery`, and `dependencies`
    /// an `ASTExpressionList` of `ASTTableIdentifier` — `RefreshSchedule`/`StorageMaterializedView`/
    /// `RefreshTask` dereference/downcast those exact shapes, so restore them with typed reads (a wrong
    /// type would otherwise reach `set` as a `LOGICAL_ERROR` or a later internal cast).
    auto period_child = r.readChildOfType<ASTTimeInterval>("period");
    if (period_child)
        set(period, period_child);
    auto offset_child = r.readChildOfType<ASTTimeInterval>("offset");
    if (offset_child)
        set(offset, offset_child);
    auto spread_child = r.readChildOfType<ASTTimeInterval>("spread");
    if (spread_child)
        set(spread, spread_child);
    auto settings_child = r.readChildOfType<ASTSetQuery>("settings");
    if (settings_child)
        set(settings, settings_child);
    auto dependencies_child = r.readChildOfType<ASTExpressionList>("dependencies");
    if (dependencies_child)
    {
        for (const auto & dependency : dependencies_child->children)
            if (!dependency || !dependency->as<ASTTableIdentifier>())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "`RefreshStrategy` 'dependencies' must contain only table identifiers during AST JSON deserialization");
        set(dependencies, dependencies_child);
    }
    append = r.getBool("append");

    /// Mirror `ParserRefreshStrategy`'s schedule-shape invariants. `REFRESH EVERY <interval>` always
    /// carries a period. `REFRESH AFTER <interval>` carries a period, but the `REFRESH DEPENDS ON ...`
    /// shorthand parses as `AFTER` with `dependencies` and *no* period, so reading the writer's own JSON
    /// for that shape must not require a period. `OFFSET` is parsed only in the `EVERY` branch and must
    /// be strictly less than the period (`offset.maxSeconds() < period.minSeconds()`).
    if (schedule_kind == RefreshScheduleKind::EVERY)
    {
        if (!period)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`REFRESH EVERY` requires a 'period' during AST JSON deserialization");
        /// `ParserTimeInterval` parses the `EVERY` period with `allow_zero = false`, and refresh
        /// scheduling (`CalendarTimeInterval::floor`) throws "Interval must be positive" on a zero
        /// period. Reject a zero interval here so it cannot reach that path.
        if (period->interval.seconds == 0 && period->interval.months == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`REFRESH EVERY` 'period' must be a positive interval during AST JSON deserialization");
    }
    if (schedule_kind == RefreshScheduleKind::AFTER && !period && !dependencies)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`REFRESH AFTER` requires a 'period' or DEPENDS ON 'dependencies' during AST JSON deserialization");
    if (offset)
    {
        if (schedule_kind != RefreshScheduleKind::EVERY)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`REFRESH` 'offset' is only valid for `REFRESH EVERY` during AST JSON deserialization");
        if (period && offset->interval.maxSeconds() >= period->interval.minSeconds())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "`REFRESH` 'offset' must be less than the 'period' during AST JSON deserialization");
    }
}

}
