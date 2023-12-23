#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Common/IntervalKind.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace Streaming
{

struct WindowInterval
{
    Int64 interval = 0;
    IntervalKind::Kind unit = IntervalKind::Second;

    operator bool() const { return interval != 0; }
};

void checkIntervalAST(const ASTPtr & ast, const String & msg);
WindowInterval extractInterval(const ASTPtr & ast, const ContextPtr & context);

/// BaseScaleInterval util class converts interval in different scale to a common base scale.
/// BaseScale-1: Nanosecond     Range: Nanosecond, Microsecond, Millisecond, Second, Minute, Hour, Day, Week
/// BaseScale-2: Month          Range: Month, Quarter, Year
/// example: '1m' -> '60000000000ns'   '1y' -> '12M'
class BaseScaleInterval
{
public:
    static constexpr IntervalKind::Kind SCALE_NANOSECOND = IntervalKind::Nanosecond;
    static constexpr IntervalKind::Kind SCALE_MONTH = IntervalKind::Month;

    Int64 num_units = 0;
    IntervalKind::Kind scale = SCALE_NANOSECOND;
    IntervalKind::Kind src_kind = SCALE_NANOSECOND;

    BaseScaleInterval() = default;

    static constexpr BaseScaleInterval toBaseScale(Int64 num_units, IntervalKind::Kind kind)
    {
        switch (kind)
        {
            /// FIXME: check overflow ?
            /// Based on SCALE_NANOSECOND
            case IntervalKind::Nanosecond:
                return BaseScaleInterval{num_units, SCALE_NANOSECOND, kind};
            case IntervalKind::Microsecond:
                return BaseScaleInterval{num_units * 1'000, SCALE_NANOSECOND, kind};
            case IntervalKind::Millisecond:
                return BaseScaleInterval{num_units * 1'000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Second:
                return BaseScaleInterval{num_units * 1'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Minute:
                return BaseScaleInterval{num_units * 60'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Hour:
                return BaseScaleInterval{num_units * 3600'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Day:
                return BaseScaleInterval{num_units * 86400'000000000, SCALE_NANOSECOND, kind};
            case IntervalKind::Week:
                return BaseScaleInterval{num_units * 604800'000000000, SCALE_NANOSECOND, kind};
            /// Based on SCALE_MONTH
            case IntervalKind::Month:
                return BaseScaleInterval{num_units, SCALE_MONTH, kind};
            case IntervalKind::Quarter:
                return BaseScaleInterval{num_units * 3, SCALE_MONTH, kind};
            case IntervalKind::Year:
                return BaseScaleInterval{num_units * 12, SCALE_MONTH, kind};
        }
        UNREACHABLE();
    }

    static BaseScaleInterval toBaseScale(const WindowInterval & interval)
    {
        return toBaseScale(interval.interval, interval.unit);
    }

    Int64 toIntervalKind(IntervalKind::Kind to_kind) const;

    BaseScaleInterval & operator+(const BaseScaleInterval & bs)
    {
        assert(scale == bs.scale);
        num_units += bs.num_units;
        return *this;
    }

    BaseScaleInterval & operator-(const BaseScaleInterval & bs)
    {
        assert(scale == bs.scale);
        num_units -= bs.num_units;
        return *this;
    }

    Int64 operator/(const BaseScaleInterval & bs) const
    {
        assert(scale == bs.scale);
        assert(bs.num_units != 0);
        return num_units / bs.num_units;
    }

    BaseScaleInterval operator/(Int64 num) const
    {
        assert(num != 0);
        return {num_units / num, scale, src_kind};
    }

    String toString() const;

protected:
    constexpr BaseScaleInterval(Int64 num_units_, IntervalKind::Kind scale_, IntervalKind::Kind src_kind_)
        : num_units(num_units_), scale(scale_), src_kind(src_kind_)
    {
    }
};
using BasedScaleIntervalPtr = std::shared_ptr<BaseScaleInterval>;

}
}
