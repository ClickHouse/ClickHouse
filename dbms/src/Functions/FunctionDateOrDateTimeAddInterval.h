#include <common/DateLUTImpl.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#include <Columns/ColumnVector.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


struct AddSecondsImpl
{
    static constexpr auto name = "addSeconds";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta;
    }
};

struct AddMinutesImpl
{
    static constexpr auto name = "addMinutes";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta * 60;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 60;
    }
};

struct AddHoursImpl
{
    static constexpr auto name = "addHours";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta * 3600;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 3600;
    }
};

struct AddDaysImpl
{
    static constexpr auto name = "addDays";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addDays(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &)
    {
        return d + delta;
    }
};

struct AddWeeksImpl
{
    static constexpr auto name = "addWeeks";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addWeeks(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &)
    {
        return d + delta * 7;
    }
};

struct AddMonthsImpl
{
    static constexpr auto name = "addMonths";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addMonths(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addMonths(DayNum(d), delta);
    }
};

struct AddQuartersImpl
{
    static constexpr auto name = "addQuarters";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addQuarters(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addQuarters(DayNum(d), delta);
    }
};

struct AddYearsImpl
{
    static constexpr auto name = "addYears";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addYears(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addYears(DayNum(d), delta);
    }
};


template <typename Transform>
struct SubtractIntervalImpl
{
    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(t, -delta, time_zone);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(d, -delta, time_zone);
    }
};

struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl> { static constexpr auto name = "subtractSeconds"; };
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl> { static constexpr auto name = "subtractMinutes"; };
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl> { static constexpr auto name = "subtractHours"; };
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl> { static constexpr auto name = "subtractDays"; };
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl> { static constexpr auto name = "subtractWeeks"; };
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl> { static constexpr auto name = "subtractMonths"; };
struct SubtractQuartersImpl : SubtractIntervalImpl<AddQuartersImpl> { static constexpr auto name = "subtractQuarters"; };
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl> { static constexpr auto name = "subtractYears"; };


template <typename FromType, typename ToType, typename Transform>
struct Adder
{
    static void vector_vector(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, const IColumn & delta, const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta.getInt(i), time_zone);
    }

    static void vector_constant(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, Int64 delta, const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta, time_zone);
    }

    static void constant_vector(const FromType & from, PaddedPODArray<ToType> & vec_to, const IColumn & delta, const DateLUTImpl & time_zone)
    {
        size_t size = delta.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(from, delta.getInt(i), time_zone);
    }
};


template <typename FromType, typename Transform>
struct DateTimeAddIntervalImpl
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        using ToType = decltype(Transform::execute(FromType(), 0, std::declval<DateLUTImpl>()));
        using Op = Adder<FromType, ToType, Transform>;

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;

        if (const auto * sources = checkAndGetColumn<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();

            const IColumn & delta_column = *block.getByPosition(arguments[1]).column;

            if (const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column))
                Op::vector_constant(sources->getData(), col_to->getData(), delta_const_column->getField().get<Int64>(), time_zone);
            else
                Op::vector_vector(sources->getData(), col_to->getData(), delta_column, time_zone);

            block.getByPosition(result).column = std::move(col_to);
        }
        else if (const auto * sources = checkAndGetColumnConst<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();
            Op::constant_vector(sources->template getValue<FromType>(), col_to->getData(), *block.getByPosition(arguments[1]).column, time_zone);
            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + Transform::name,
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


template <typename Transform>
class FunctionDateOrDateTimeAddInterval : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateOrDateTimeAddInterval>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isNumber(arguments[1].type))
            throw Exception("Second argument for function " + getName() + " (delta) must be number",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            if (!isDateOrDateTime(arguments[0].type))
                throw Exception{"Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                    ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        else
        {
            if (!WhichDataType(arguments[0].type).isDateTime()
                || !WhichDataType(arguments[2].type).isString())
                throw Exception(
                    "Function " + getName() + " supports 2 or 3 arguments. The 1st argument "
                    "must be of type Date or DateTime. The 2nd argument must be number. "
                    "The 3rd argument (optional) must be "
                    "a constant string with timezone name. The timezone argument is allowed "
                    "only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (WhichDataType(arguments[0].type).isDate())
        {
            if (std::is_same_v<decltype(Transform::execute(DataTypeDate::FieldType(), 0, std::declval<DateLUTImpl>())), UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else
        {
            if (std::is_same_v<decltype(Transform::execute(DataTypeDateTime::FieldType(), 0, std::declval<DateLUTImpl>())), UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform>::execute(block, arguments, result);
        else if (which.isDateTime())
            DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform>::execute(block, arguments, result);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

}

