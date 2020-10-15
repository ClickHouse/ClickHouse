#include <common/DateLUTImpl.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <Columns/ColumnsNumber.h>

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>
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

/// AddOnDateTime64DefaultImpl provides default implementation of add-X functionality for DateTime64.
///
/// Default implementation is not to change fractional part, but only modify whole part as if it was DateTime.
/// That means large whole values (for scale less than 9) might not fit into UInt32-range,
/// and hence default implementation will produce incorrect results.
template <typename T>
struct AddOnDateTime64DefaultImpl
{
    AddOnDateTime64DefaultImpl(UInt32 scale_ = 0)
        : scale_multiplier(DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale_))
    {}

    // Default implementation for add/sub on DateTime64: do math on whole part (the same way as for DateTime), leave fractional as it is.
    inline DateTime64 execute(const DateTime64 & t, Int64 delta, const DateLUTImpl & time_zone) const
    {
        const auto components = DecimalUtils::splitWithScaleMultiplier(t, scale_multiplier);

        const auto whole = static_cast<const T *>(this)->execute(static_cast<UInt32>(components.whole), delta, time_zone);
        return DecimalUtils::decimalFromComponentsWithMultiplier<DateTime64>(static_cast<DateTime64::NativeType>(whole), components.fractional, scale_multiplier);
    }

    UInt32 scale_multiplier = 1;
};


/// Type of first argument of 'execute' function overload defines what INPUT DataType it is used for.
/// Return type defines what is the OUTPUT (return) type of the CH function.
/// Corresponding types:
///  - UInt16     => DataTypeDate
///  - UInt32     => DataTypeDateTime
///  - DateTime64 => DataTypeDateTime64
/// Please note that INPUT and OUTPUT types may differ, e.g.:
///  - 'AddSecondsImpl::execute(UInt32, ...) -> UInt32' is available to the ClickHouse users as 'addSeconds(DateTime, ...) -> DateTime'
///  - 'AddSecondsImpl::execute(UInt16, ...) -> UInt32' is available to the ClickHouse users as 'addSeconds(Date, ...) -> DateTime'

struct AddSecondsImpl : public AddOnDateTime64DefaultImpl<AddSecondsImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddSecondsImpl>;
    using Base::Base;
    using Base::execute;

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

struct AddMinutesImpl : public AddOnDateTime64DefaultImpl<AddMinutesImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddMinutesImpl>;
    using Base::Base;
    using Base::execute;

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

struct AddHoursImpl : public AddOnDateTime64DefaultImpl<AddHoursImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddHoursImpl>;
    using Base::Base;
    using Base::execute;

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

struct AddDaysImpl : public AddOnDateTime64DefaultImpl<AddDaysImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddDaysImpl>;
    using Base::Base;
    using Base::execute;

    static constexpr auto name = "addDays";

//    static inline UInt32 execute(UInt64 t, Int64 delta, const DateLUTImpl & time_zone)
//    {
//        // TODO (nemkov): LUT does not support out-of range date values for now.
//        return time_zone.addDays(t, delta);
//    }

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addDays(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &)
    {
        return d + delta;
    }
};

struct AddWeeksImpl : public AddOnDateTime64DefaultImpl<AddWeeksImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddWeeksImpl>;
    using Base::Base;
    using Base::execute;

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

struct AddMonthsImpl : public AddOnDateTime64DefaultImpl<AddMonthsImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddMonthsImpl>;
    using Base::Base;
    using Base::execute;

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

struct AddQuartersImpl : public AddOnDateTime64DefaultImpl<AddQuartersImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddQuartersImpl>;
    using Base::Base;
    using Base::execute;

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

struct AddYearsImpl : public AddOnDateTime64DefaultImpl<AddYearsImpl>
{
    using Base = AddOnDateTime64DefaultImpl<AddYearsImpl>;
    using Base::Base;
    using Base::execute;

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
struct SubtractIntervalImpl : public Transform
{
    using Transform::Transform;

    template <typename T>
    inline auto execute(T t, Int64 delta, const DateLUTImpl & time_zone) const
    {
        return Transform::execute(t, -delta, time_zone);
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


template <typename Transform>
struct Adder
{
    const Transform transform;

    explicit Adder(Transform transform_)
        : transform(std::move(transform_))
    {}

    template <typename FromVectorType, typename ToVectorType>
    void NO_INLINE vectorConstant(const FromVectorType & vec_from, ToVectorType & vec_to, Int64 delta, const DateLUTImpl & time_zone) const
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(vec_from[i], delta, time_zone);
    }

    template <typename FromVectorType, typename ToVectorType>
    void vectorVector(const FromVectorType & vec_from, ToVectorType & vec_to, const IColumn & delta, const DateLUTImpl & time_zone) const
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        castTypeToEither<
            ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
            ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
            ColumnFloat32, ColumnFloat64>(
            &delta, [&](const auto & column){ vectorVector(vec_from, vec_to, column, time_zone, size); return true; });
    }

    template <typename FromType, typename ToVectorType>
    void constantVector(const FromType & from, ToVectorType & vec_to, const IColumn & delta, const DateLUTImpl & time_zone) const
    {
        size_t size = delta.size();
        vec_to.resize(size);

        castTypeToEither<
            ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
            ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
            ColumnFloat32, ColumnFloat64>(
            &delta, [&](const auto & column){ constantVector(from, vec_to, column, time_zone, size); return true; });
    }

private:
    template <typename FromVectorType, typename ToVectorType, typename DeltaColumnType>
    void NO_INLINE vectorVector(const FromVectorType & vec_from, ToVectorType & vec_to, const DeltaColumnType & delta, const DateLUTImpl & time_zone, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(vec_from[i], delta.getData()[i], time_zone);
    }

    template <typename FromType, typename ToVectorType, typename DeltaColumnType>
    void NO_INLINE constantVector(const FromType & from, ToVectorType & vec_to, const DeltaColumnType & delta, const DateLUTImpl & time_zone, size_t size) const
    {
        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(from, delta.getData()[i], time_zone);
    }
};


template <typename FromDataType, typename ToDataType, typename Transform>
struct DateTimeAddIntervalImpl
{
    static void execute(Transform transform, Block & block, const ColumnNumbers & arguments, size_t result)
    {
        using FromValueType = typename FromDataType::FieldType;
        using FromColumnType = typename FromDataType::ColumnType;
        using ToColumnType = typename ToDataType::ColumnType;

        auto op = Adder<Transform>{std::move(transform)};

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;

        auto result_col = block.getByPosition(result).type->createColumn();
        auto col_to = assert_cast<ToColumnType *>(result_col.get());

        if (const auto * sources = checkAndGetColumn<FromColumnType>(source_col.get()))
        {
            const IColumn & delta_column = *block.getByPosition(arguments[1]).column;

            if (const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column))
                op.vectorConstant(sources->getData(), col_to->getData(), delta_const_column->getInt(0), time_zone);
            else
                op.vectorVector(sources->getData(), col_to->getData(), delta_column, time_zone);
        }
        else if (const auto * sources_const = checkAndGetColumnConst<FromColumnType>(source_col.get()))
        {
            op.constantVector(
                sources_const->template getValue<FromValueType>(),
                col_to->getData(),
                *block.getByPosition(arguments[1]).column, time_zone);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + Transform::name,
                ErrorCodes::ILLEGAL_COLUMN);
        }

        block.getByPosition(result).column = std::move(result_col);
    }
};

namespace date_and_time_type_details
{
// Compile-time mapping of value (DataType::FieldType) types to corresponding DataType
template <typename FieldType> struct ResultDataTypeMap {};
template <> struct ResultDataTypeMap<UInt16>     { using ResultDataType = DataTypeDate; };
template <> struct ResultDataTypeMap<Int16>      { using ResultDataType = DataTypeDate; };
template <> struct ResultDataTypeMap<UInt32>     { using ResultDataType = DataTypeDateTime; };
template <> struct ResultDataTypeMap<Int32>      { using ResultDataType = DataTypeDateTime; };
template <> struct ResultDataTypeMap<DateTime64> { using ResultDataType = DataTypeDateTime64; };
}

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

        if (!isNativeNumber(arguments[1].type))
            throw Exception("Second argument for function " + getName() + " (delta) must be number",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            if (!isDateOrDateTime(arguments[0].type))
                throw Exception{"Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() +
                    ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        else
        {
            if (!WhichDataType(arguments[0].type).isDateTime()
                || !WhichDataType(arguments[2].type).isString())
            {
                throw Exception(
                    "Function " + getName() + " supports 2 or 3 arguments. The 1st argument "
                    "must be of type Date or DateTime. The 2nd argument must be number. "
                    "The 3rd argument (optional) must be "
                    "a constant string with timezone name. The timezone argument is allowed "
                    "only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        switch (arguments[0].type->getTypeId())
        {
            case TypeIndex::Date:
                return resolveReturnType<DataTypeDate>(arguments);
            case TypeIndex::DateTime:
                return resolveReturnType<DataTypeDateTime>(arguments);
            case TypeIndex::DateTime64:
                return resolveReturnType<DataTypeDateTime64>(arguments);
            default:
            {
                throw Exception("Invalid type of 1st argument of function " + getName() + ": "
                    + arguments[0].type->getName() + ", expected: Date, DateTime or DateTime64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
    }

    /// Helper templates to deduce return type based on argument type, since some overloads may promote or denote types,
    /// e.g. addSeconds(Date, 1) => DateTime
    template <typename FieldType>
    using TransformExecuteReturnType = decltype(std::declval<Transform>().execute(FieldType(), 0, std::declval<DateLUTImpl>()));

    // Deduces RETURN DataType from INTPUT DataType, based on return type of Transform{}.execute(INPUT_TYPE, UInt64, DateLUTImpl).
    // e.g. for Transform-type that has execute()-overload with 'UInt16' input and 'UInt32' return,
    // argument type is expected to be 'Date', and result type is deduced to be 'DateTime'.
    template <typename FromDataType>
    using TransformResultDataType = typename date_and_time_type_details::ResultDataTypeMap<TransformExecuteReturnType<typename FromDataType::FieldType>>::ResultDataType;

    template <typename FromDataType>
    DataTypePtr resolveReturnType(const ColumnsWithTypeAndName & arguments) const
    {
        using ResultDataType = TransformResultDataType<FromDataType>;

        if constexpr (std::is_same_v<ResultDataType, DataTypeDate>)
            return std::make_shared<DataTypeDate>();
        else if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime>)
        {
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime64>)
        {
            // TODO (vnemkov): what if there is an overload of Transform::execute() that returns DateTime64 from DateTime or Date ?
            // Shall we use the default scale or one from optional argument ?
            const auto & datetime64_type = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type);
            return std::make_shared<DataTypeDateTime64>(datetime64_type.getScale(), extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else
        {
            static_assert("Failed to resolve return type.");
        }

        //to make PVS and GCC happy.
        return nullptr;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        WhichDataType which(from_type);

        if (which.isDate())
        {
            DateTimeAddIntervalImpl<DataTypeDate, TransformResultDataType<DataTypeDate>, Transform>::execute(
                Transform{}, block, arguments, result);
        }
        else if (which.isDateTime())
        {
            DateTimeAddIntervalImpl<DataTypeDateTime, TransformResultDataType<DataTypeDateTime>, Transform>::execute(
                Transform{}, block, arguments, result);
        }
        else if (const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(from_type))
        {
            DateTimeAddIntervalImpl<DataTypeDateTime64, TransformResultDataType<DataTypeDateTime64>, Transform>::execute(
                Transform{datetime64_type->getScale()}, block, arguments, result);
        }
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

}

