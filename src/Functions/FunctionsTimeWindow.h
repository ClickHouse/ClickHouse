#pragma once

#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/IFunction.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>


namespace DB
{

/** Time window functions:
  *
  * tumble(time_attr, interval [, timezone])
  * tumbleStart(window_id)
  * tumbleStart(time_attr, interval [, timezone])
  * tumbleEnd(window_id)
  * tumbleEnd(time_attr, interval [, timezone])
  * hop(time_attr, hop_interval, window_interval [, timezone])
  * hopStart(window_id)
  * hopStart(time_attr, hop_interval, window_interval [, timezone])
  * hopEnd(window_id)
  * hopEnd(time_attr, hop_interval, window_interval [, timezone])
  */

template <IntervalKind::Kind unit>
struct ToStartOfTransform;

#define TRANSFORM_DATE(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::Kind::INTERVAL_KIND> \
    { \
        static auto execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.toStartOf##INTERVAL_KIND##Interval(time_zone.toDayNum(t), delta); \
        } \
    };
    TRANSFORM_DATE(Year)
    TRANSFORM_DATE(Quarter)
    TRANSFORM_DATE(Month)
    TRANSFORM_DATE(Week)
#undef TRANSFORM_DATE

    template <>
    struct ToStartOfTransform<IntervalKind::Kind::Day>
    {
        static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone)
        {
            return static_cast<UInt32>(time_zone.toStartOfDayInterval(time_zone.toDayNum(t), delta));
        }
    };

#define TRANSFORM_TIME(INTERVAL_KIND) \
    template <> \
    struct ToStartOfTransform<IntervalKind::Kind::INTERVAL_KIND> \
    { \
        static UInt32 execute(UInt32 t, UInt64 delta, const DateLUTImpl & time_zone) \
        { \
            return static_cast<UInt32>(time_zone.toStartOf##INTERVAL_KIND##Interval(t, delta)); \
        } \
    };
    TRANSFORM_TIME(Hour)
    TRANSFORM_TIME(Minute)
    TRANSFORM_TIME(Second)
#undef TRANSFORM_TIME

#define TRANSFORM_SUBSECONDS(INTERVAL_KIND, DEF_SCALE) \
template<> \
    struct ToStartOfTransform<IntervalKind::Kind::INTERVAL_KIND> \
    { \
        static Int64 execute(Int64 t, UInt64 delta, const UInt32 scale) \
        { \
            if (scale <= (DEF_SCALE)) \
            { \
                auto val = t * DecimalUtils::scaleMultiplier<DateTime64>((DEF_SCALE) - scale); \
                if (delta == 1) \
                    return val; \
                return val - (val % delta); \
            } \
            return t - (t % (delta * DecimalUtils::scaleMultiplier<DateTime64>(scale - (DEF_SCALE)))) ; \
        } \
    };
    TRANSFORM_SUBSECONDS(Millisecond, 3)
    TRANSFORM_SUBSECONDS(Microsecond, 6)
    TRANSFORM_SUBSECONDS(Nanosecond, 9)
#undef TRANSFORM_SUBSECONDS

    template <IntervalKind::Kind unit>
    struct AddTime;

#define ADD_DATE(INTERVAL_KIND) \
    template <> \
    struct AddTime<IntervalKind::Kind::INTERVAL_KIND> \
    { \
        static auto execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone) \
        { \
            return time_zone.add##INTERVAL_KIND##s(ExtendedDayNum(d), delta); \
        } \
    };
    ADD_DATE(Year)
    ADD_DATE(Quarter)
    ADD_DATE(Month)
#undef ADD_DATE

    template <>
    struct AddTime<IntervalKind::Kind::Week>
    {
        static NO_SANITIZE_UNDEFINED ExtendedDayNum execute(UInt16 d, UInt64 delta, const DateLUTImpl &)
        {
            return ExtendedDayNum(static_cast<Int32>(d + delta * 7));
        }
    };

#define ADD_TIME(INTERVAL_KIND, INTERVAL) \
    template <> \
    struct AddTime<IntervalKind::Kind::INTERVAL_KIND> \
    { \
        static NO_SANITIZE_UNDEFINED UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &) \
        { return static_cast<UInt32>(t + delta * (INTERVAL)); } \
    };
    ADD_TIME(Day, 86400)
    ADD_TIME(Hour, 3600)
    ADD_TIME(Minute, 60)
    ADD_TIME(Second, 1)
#undef ADD_TIME

#define ADD_SUBSECONDS(INTERVAL_KIND, DEF_SCALE) \
template <> \
    struct AddTime<IntervalKind::Kind::INTERVAL_KIND> \
    { \
        static NO_SANITIZE_UNDEFINED Int64 execute(Int64 t, UInt64 delta, const UInt32 scale) \
        { \
            if (scale < (DEF_SCALE)) \
                return t + delta * DecimalUtils::scaleMultiplier<DateTime64>((DEF_SCALE) - scale); \
            return t + delta * DecimalUtils::scaleMultiplier<DateTime64>(scale - (DEF_SCALE)); \
        } \
    };
    ADD_SUBSECONDS(Millisecond, 3)
    ADD_SUBSECONDS(Microsecond, 6)
    ADD_SUBSECONDS(Nanosecond, 9)
#undef ADD_SUBSECONDS

}
