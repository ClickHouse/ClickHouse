#pragma once
#include <regex>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <common/types.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/IFunctionImpl.h>
#include <Common/Exception.h>
#include <common/DateLUTImpl.h>

/// The default mode value to use for the WEEK() function
#define DEFAULT_WEEK_MODE 0

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/**
 * CustomWeek Transformations.
  */

static inline UInt32 dateIsNotSupported(const char * name)
{
    throw Exception("Illegal type Date of argument for function " + std::string(name), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/// This factor transformation will say that the function is monotone everywhere.
struct ZeroTransform
{
    static inline UInt16 execute(UInt32, UInt8, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt16, UInt8, const DateLUTImpl &) { return 0; }
};

struct ToWeekImpl
{
    static constexpr auto name = "toWeek";

    static inline UInt8 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode);
        return yw.second;
    }
    static inline UInt8 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(DayNum(d), week_mode);
        return yw.second;
    }

    using FactorTransform = ZeroTransform;
};

struct ToYearWeekImpl
{
    static constexpr auto name = "toYearWeek";

    static inline UInt32 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }
    static inline UInt32 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(DayNum(d), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfWeekImpl
{
    static constexpr auto name = "toStartOfWeek";

    static inline UInt16 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t), week_mode);
    }
    static inline UInt16 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(DayNum(d), week_mode);
    }

    using FactorTransform = ZeroTransform;
};

template <typename FromType, typename ToType, typename Transform>
struct Transformer
{
    explicit Transformer(Transform transform_)
        : transform(std::move(transform_))
    {}

    template <typename FromVectorType, typename ToVectorType>
    void
    vector(const FromVectorType & vec_from, ToVectorType & vec_to, UInt8 week_mode, const DateLUTImpl & time_zone) const
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(vec_from[i], week_mode, time_zone);
    }

private:
    const Transform transform;
};


template <typename FromDataType, typename ToDataType>
struct CustomWeekTransformImpl
{
    template <typename Transform>
    static ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/, Transform transform = {})
    {
        const auto op = Transformer<typename FromDataType::FieldType, typename ToDataType::FieldType, Transform>{std::move(transform)};

        UInt8 week_mode = DEFAULT_WEEK_MODE;
        if (arguments.size() > 1)
        {
            if (const auto * week_mode_column = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get()))
                week_mode = week_mode_column->getValue<UInt8>();
        }

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        const ColumnPtr source_col = arguments[0].column;
        if (const auto * sources = checkAndGetColumn<typename FromDataType::ColumnType>(source_col.get()))
        {
            auto col_to = ToDataType::ColumnType::create();
            op.vector(sources->getData(), col_to->getData(), week_mode, time_zone);
            return col_to;
        }
        else
        {
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function "
                    + Transform::name,
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

}
