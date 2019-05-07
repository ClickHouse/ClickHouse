#pragma once
#include <regex>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Types.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Common/Exception.h>
#include <common/DateLUTImpl.h>

/// Custom date defaults to January 1 ( 01-01 )
#define DEFAULT_CUSTOM_MONTH 1
#define DEFAULT_CUSTOM_DAY 1

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/** CustomDate Transformations.
  * Represents two functions - from datetime (UInt32) and from date (UInt16), both with custom_month and custom_day.
  */

static inline UInt32 dateIsNotSupported(const char * name)
{
    throw Exception("Illegal type Date of argument for function " + std::string(name), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/// This factor transformation will say that the function is monotone everywhere.
struct ZeroTransform
{
    static inline UInt16 execute(UInt32, UInt8, UInt8, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt16, UInt8, UInt8, const DateLUTImpl &) { return 0; }
};

struct ToStartOfCustomYearImpl
{
    static constexpr auto name = "toStartOfCustomYear";

    static inline UInt16 execute(UInt32 t, UInt8 custom_month, UInt8 custom_day, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfCustomYear(time_zone.toDayNum(t), custom_month, custom_day);
    }
    static inline UInt16 execute(UInt16 d, UInt8 custom_month, UInt8 custom_day, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfCustomYear(DayNum(d), custom_month, custom_day);
    }

    using FactorTransform = ZeroTransform;
};

struct ToCustomYearImpl
{
    static constexpr auto name = "toCustomYear";

    static inline UInt16 execute(UInt32 t, UInt8 custom_month, UInt8 custom_day, const DateLUTImpl & time_zone)
    {
        return time_zone.toCustomYear(time_zone.toDayNum(t), custom_month, custom_day);
    }
    static inline UInt16 execute(UInt16 d, UInt8 custom_month, UInt8 custom_day, const DateLUTImpl & time_zone)
    {
        return time_zone.toCustomYear(DayNum(d), custom_month, custom_day);
    }

    using FactorTransform = ZeroTransform;
};

struct ToCustomWeekImpl
{
    static constexpr auto name = "toCustomWeek";

    static inline UInt8 execute(UInt32 t, UInt8 custom_month, UInt8 custom_day, const DateLUTImpl & time_zone)
    {
        return time_zone.toCustomWeek(time_zone.toDayNum(t), custom_month, custom_day);
    }
    static inline UInt8 execute(UInt16 d, UInt8 custom_month, UInt8 custom_day, const DateLUTImpl & time_zone)
    {
        return time_zone.toCustomWeek(DayNum(d), custom_month, custom_day);
    }

    using FactorTransform = ToCustomYearImpl;
};

template <typename FromType, typename ToType, typename Transform>
struct Transformer
{
    static void vector(
        const PaddedPODArray<FromType> & vec_from,
        PaddedPODArray<ToType> & vec_to,
        UInt8 custom_month,
        UInt8 custom_day,
        const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], custom_month, custom_day, time_zone);
    }
};


template <typename FromType, typename ToType, typename Transform>
struct CustomDateTransformImpl
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
    {
        using Op = Transformer<FromType, ToType, Transform>;

        UInt8 custom_month = DEFAULT_CUSTOM_MONTH;
        UInt8 custom_day = DEFAULT_CUSTOM_DAY;
        // With custom date parameter, fommat: MM-DD
        if (arguments.size() > 1)
        {
            auto * custom_date_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[1]).column.get());
            if (custom_date_column)
            {
                String custom_date = custom_date_column->getValue<String>();
                std::regex regex_mmdd("((1[0-2])|(0[1-9]))-(([12][0-9])|(3[01])|(0[1-9]))");
                if (custom_date.length() == 5 && std::regex_match(custom_date, regex_mmdd))
                {
                    custom_month = std::stoi(custom_date.substr(0, 2));
                    custom_day = std::stoi(custom_date.substr(3, 2));
                }
                else
                    throw Exception(
                        String("The second argument for function ") + Transform::name
                            + " must be a constant string with custom date(MM-DD)",
                        ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);
        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;
        if (const auto * sources = checkAndGetColumn<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();
            Op::vector(sources->getData(), col_to->getData(), custom_month, custom_day, time_zone);
            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function "
                    + Transform::name,
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

}
