#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>

#include <Common/DateLUT.h>
#include <Common/typeid_cast.h>

#include <array>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

/// A helper function to simplify comparisons of valid YYYY-MM-DD values for <,>,=
inline constexpr Int64 YearMonthDayToSingleInt(Int64 year, Int64 month, Int64 day)
{
    return year * 512 + month * 32 + day;
}

/// Common logic to handle numeric arguments like year, month, day, hour, minute, second
class FunctionWithNumericParamsBase : public IFunction
{
public:
    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return false; /// invalid argument values and timestamps that are out of supported range are converted into a default value
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

protected:
    template <class AgrumentNames>
    void checkRequiredArguments(const ColumnsWithTypeAndName & arguments, const AgrumentNames & argument_names, const size_t optional_argument_count) const
    {
        if (arguments.size() < argument_names.size() || arguments.size() > argument_names.size() + optional_argument_count)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires {} to {} arguments, but {} given",
                getName(), argument_names.size(), argument_names.size() + optional_argument_count, arguments.size());

        for (size_t i = 0; i < argument_names.size(); ++i)
            if (!isNumber(arguments[i].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {}", std::string(argument_names[i]), getName());
    }

    template <class DataTypeFloat = DataTypeFloat32, class AgrumentNames>
    void convertRequiredArguments(const ColumnsWithTypeAndName & arguments, const AgrumentNames & argument_names, Columns & converted_arguments) const
    {
        const DataTypePtr converted_argument_type = std::make_shared<DataTypeFloat>();
        converted_arguments.clear();
        converted_arguments.reserve(arguments.size());
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            ColumnPtr argument_column = castColumn(arguments[i], converted_argument_type);
            argument_column = argument_column->convertToFullColumnIfConst();
            converted_arguments.push_back(argument_column);
        }
    }
};

/// Common implementation for makeDate, makeDate32
template <typename Traits>
class FunctionMakeDate : public FunctionWithNumericParamsBase
{
private:
    static constexpr std::array<const char*, 3> argument_names = {"year", "month", "day"};

public:
    static constexpr auto name = Traits::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDate>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return argument_names.size(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkRequiredArguments(arguments, argument_names, 0);

        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        Columns converted_arguments;
        convertRequiredArguments(arguments, argument_names, converted_arguments);

        auto res_column = Traits::ReturnColumnType::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & year_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[0]).getData();
        const auto & month_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[1]).getData();
        const auto & day_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[2]).getData();

        const auto & date_lut = DateLUT::instance();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto year = year_data[i];
            const auto month = month_data[i];
            const auto day = day_data[i];

            Int32 day_num = 0;

            if (year >= Traits::MIN_YEAR &&
                year <= Traits::MAX_YEAR &&
                month >= 1 && month <= 12 &&
                day >= 1 && day <= 31 &&
                YearMonthDayToSingleInt(static_cast<Int64>(year), static_cast<Int64>(month), static_cast<Int64>(day)) <= Traits::MAX_DATE)
            {
                day_num = date_lut.makeDayNum(static_cast<Int16>(year), static_cast<UInt8>(month), static_cast<UInt8>(day));
            }

            result_data[i] = day_num;
        }

        return res_column;
    }
};

template<typename Traits>
class FunctionYYYYYMMDDToDate : public FunctionWithNumericParamsBase
{
private:
    static constexpr std::array<const char*, 1> required_argument_names = { "YYYYMMDD" };
    static constexpr auto MaxValue = 10000'0000;

public:
    static constexpr auto name = Traits::YYYYMMDDName;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYYMMDDToDate>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return required_argument_names.size(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkRequiredArguments(arguments, required_argument_names, 0);

        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        Columns float_arguments;
        convertRequiredArguments<DataTypeFloat64>(arguments, required_argument_names, float_arguments);

        auto res_column = Traits::ReturnColumnType::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & yyyymmdd_float = typeid_cast<const ColumnFloat64 &>(*float_arguments[0]).getData();

        const auto & date_lut = DateLUT::instance();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Int32 day_num = 0;

            if (yyyymmdd_float[i] > 0 && yyyymmdd_float[i] < MaxValue)
            {
                const auto yyyymmdd = static_cast<uint>(yyyymmdd_float[i]);
                const auto year = yyyymmdd / 10000;
                const auto month = yyyymmdd / 100 % 100;
                const auto day = yyyymmdd % 100;


                if (year >= Traits::MIN_YEAR &&
                    year <= Traits::MAX_YEAR &&
                    month >= 1 && month <= 12 &&
                    day >= 1 && day <= 31 &&
                    YearMonthDayToSingleInt(static_cast<Int64>(year), static_cast<Int64>(month),
                                            static_cast<Int64>(day)) <= Traits::MAX_DATE)
                    day_num = date_lut.makeDayNum(static_cast<Int16>(year), static_cast<UInt8>(month),
                                                  static_cast<UInt8>(day));
            }

            result_data[i] = day_num;
        }

        return res_column;
    }
};

/// makeDate(year, month, day)
struct MakeDateTraits
{
    static constexpr auto name = "makeDate";
    static constexpr auto YYYYMMDDName = "YYYYMMDDToDate";
    using ReturnDataType = DataTypeDate;
    using ReturnColumnType = ColumnDate;

    static constexpr auto MIN_YEAR = 1970;
    static constexpr auto MAX_YEAR = 2149;
    /// This date has the maximum day number that fits in 16-bit uint
    static constexpr auto MAX_DATE = YearMonthDayToSingleInt(MAX_YEAR, 6, 6);
};

/// makeDate32(year, month, day)
struct MakeDate32Traits
{
    static constexpr auto name = "makeDate32";
    static constexpr auto YYYYMMDDName = "YYYYMMDDToDate32";
    using ReturnDataType = DataTypeDate32;
    using ReturnColumnType = ColumnDate32;

    static constexpr auto MIN_YEAR = 1900;
    static constexpr auto MAX_YEAR = 2299;
    static constexpr auto MAX_DATE = YearMonthDayToSingleInt(MAX_YEAR, 12, 31);
};

/// Common implementation for makeDateTime, makeDateTime64
class FunctionDateTimeBase : public FunctionWithNumericParamsBase
{
public:
    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

protected:
    template <typename T>
    static Int64 dateTime(T year, T month, T day_of_month, T hour, T minute, T second, const DateLUTImpl & lut)
    {
        ///  Note that hour, minute and second are checked against 99 to behave consistently with parsing DateTime from String
        ///  E.g. "select cast('1984-01-01 99:99:99' as DateTime);" returns "1984-01-05 04:40:39"
        if (unlikely(std::isnan(year) || std::isnan(month) || std::isnan(day_of_month) ||
            std::isnan(hour) || std::isnan(minute) || std::isnan(second) ||
            year < DATE_LUT_MIN_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31 ||
            hour < 0 || hour > 99 || minute < 0 || minute > 99 || second < 0 || second > 99))
            return minDateTime(lut);

        if (unlikely(year > DATE_LUT_MAX_YEAR))
            return maxDateTime(lut);

        return lut.makeDateTime(
            static_cast<Int16>(year), static_cast<UInt8>(month), static_cast<UInt8>(day_of_month),
            static_cast<UInt8>(hour), static_cast<UInt8>(minute), static_cast<UInt8>(second));
    }

    static Int64 minDateTime(const DateLUTImpl & lut)
    {
        return lut.makeDateTime(DATE_LUT_MIN_YEAR - 1, 1, 1, 0, 0, 0);
    }

    static Int64 maxDateTime(const DateLUTImpl & lut)
    {
        return lut.makeDateTime(DATE_LUT_MAX_YEAR + 1, 1, 1, 23, 59, 59);
    }

    std::string extractTimezone(const ColumnWithTypeAndName & timezone_argument) const
    {
        std::string timezone;
        if (!isStringOrFixedString(timezone_argument.type) || !timezone_argument.column || (timezone_argument.column->size() != 1 && !typeid_cast<const ColumnConst*>(timezone_argument.column.get())))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 'timezone' for function {} must be const string", getName());
        timezone = timezone_argument.column->getDataAt(0).toString();

        return timezone;
    }
};

class FunctionMakeDateTimeBase : public FunctionDateTimeBase
{
protected:
    static constexpr std::array<const char*, 6> argument_names = {"year", "month", "day", "hour", "minute", "second"};

    void checkRequiredArguments(const ColumnsWithTypeAndName & arguments, const size_t optional_argument_count) const
    {
        FunctionWithNumericParamsBase::checkRequiredArguments(arguments, argument_names, optional_argument_count);
    }

    void convertRequiredArguments(const ColumnsWithTypeAndName & arguments, Columns & converted_arguments) const
    {
        FunctionWithNumericParamsBase::convertRequiredArguments(arguments, argument_names, converted_arguments);
    }
};

/// makeDateTime(year, month, day, hour, minute, second, [timezone])
class FunctionMakeDateTime : public FunctionMakeDateTimeBase
{
private:
    static constexpr std::array<const char*, 1> optional_argument_names = {"timezone"};

public:
    static constexpr auto name = "makeDateTime";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDateTime>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkRequiredArguments(arguments, optional_argument_names.size());

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        Columns converted_arguments;
        convertRequiredArguments(arguments, converted_arguments);

        auto res_column = ColumnDateTime::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & year_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[0]).getData();
        const auto & month_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[1]).getData();
        const auto & day_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[2]).getData();
        const auto & hour_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[3]).getData();
        const auto & minute_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[4]).getData();
        const auto & second_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[5]).getData();

        const auto & date_lut = DateLUT::instance(timezone);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto year = year_data[i];
            const auto month = month_data[i];
            const auto day = day_data[i];
            const auto hour = hour_data[i];
            const auto minute = minute_data[i];
            const auto second = second_data[i];

            auto date_time = dateTime(year, month, day, hour, minute, second, date_lut);
            if (unlikely(date_time < 0))
                date_time = 0;
            else if (unlikely(date_time > 0x0ffffffffll))
                date_time = 0x0ffffffffll;

            result_data[i] = static_cast<UInt32>(date_time);
        }

        return res_column;
    }
};

/// makeDateTime64(year, month, day, hour, minute, second, [fraction], [precision], [timezone])
class FunctionMakeDateTime64 : public FunctionMakeDateTimeBase
{
private:
    static constexpr std::array<const char*, 3> optional_argument_names = {"fraction", "precision", "timezone"};
    static constexpr UInt8 DEFAULT_PRECISION = 3;

public:
    static constexpr auto name = "makeDateTime64";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDateTime64>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkRequiredArguments(arguments, optional_argument_names.size());

        if (arguments.size() >= argument_names.size() + 1)
        {
            const auto& fraction_argument = arguments[argument_names.size()];
            if (!isNumber(fraction_argument.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument 'fraction' for function {} must be a number", getName());
        }

        /// Optional precision argument
        Int64 precision = DEFAULT_PRECISION;
        if (arguments.size() >= argument_names.size() + 2)
            precision = extractPrecision(arguments[argument_names.size() + 1]);

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == argument_names.size() + 3)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime64>(precision, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Optional precision argument
        Int64 precision = DEFAULT_PRECISION;
        if (arguments.size() >= argument_names.size() + 2)
            precision = extractPrecision(arguments[argument_names.size() + 1]);

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == argument_names.size() + 3)
            timezone = extractTimezone(arguments.back());

        Columns converted_arguments;
        convertRequiredArguments(arguments, converted_arguments);

        /// Optional fraction argument
        const ColumnVector<Float64>::Container * fraction_data = nullptr;
        if (arguments.size() >= argument_names.size() + 1)
        {
            ColumnPtr fraction_column = castColumn(arguments[argument_names.size()], std::make_shared<DataTypeFloat64>());
            fraction_column = fraction_column->convertToFullColumnIfConst();
            converted_arguments.push_back(fraction_column);
            fraction_data = &typeid_cast<const ColumnFloat64 &>(*converted_arguments[6]).getData();
        }

        auto res_column = ColumnDateTime64::create(input_rows_count, static_cast<UInt32>(precision));
        auto & result_data = res_column->getData();

        const auto & year_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[0]).getData();
        const auto & month_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[1]).getData();
        const auto & day_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[2]).getData();
        const auto & hour_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[3]).getData();
        const auto & minute_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[4]).getData();
        const auto & second_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[5]).getData();

        const auto & date_lut = DateLUT::instance(timezone);

        const auto max_fraction = pow(10, precision) - 1;
        const auto min_date_time = minDateTime(date_lut);
        const auto max_date_time = maxDateTime(date_lut);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto year = year_data[i];
            const auto month = month_data[i];
            const auto day = day_data[i];
            const auto hour = hour_data[i];
            const auto minute = minute_data[i];
            const auto second = second_data[i];

            auto date_time = dateTime(year, month, day, hour, minute, second, date_lut);

            double fraction = 0;
            if (unlikely(date_time == min_date_time))
                fraction = 0;
            else if (unlikely(date_time == max_date_time))
                fraction = 999999999;
            else
            {
                fraction = fraction_data ? (*fraction_data)[i] : 0;
                if (unlikely(std::isnan(fraction)))
                {
                    date_time = min_date_time;
                    fraction = 0;
                }
                else if (unlikely(fraction < 0))
                    fraction = 0;
                else if (unlikely(fraction > max_fraction))
                    fraction = max_fraction;
            }

            result_data[i] = DecimalUtils::decimalFromComponents<DateTime64>(
                date_time,
                static_cast<Int64>(fraction),
                static_cast<UInt32>(precision));
        }

        return res_column;
    }

private:
    UInt8 extractPrecision(const ColumnWithTypeAndName & precision_argument) const
    {
        Int64 precision = DEFAULT_PRECISION;
        if (!isNumber(precision_argument.type) || !precision_argument.column || (precision_argument.column->size() != 1 && !typeid_cast<const ColumnConst*>(precision_argument.column.get())))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 'precision' for function {} must be constant number", getName());
        precision = precision_argument.column->getInt(0);
        if (precision < 0 || precision > 9)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Argument 'precision' for function {} must be in range [0, 9]", getName());

        return precision;
    }
};

class FunctionYYYYMMDDhhmmssToDateTimeBase : public FunctionDateTimeBase
{
protected:
    static constexpr std::array<const char*, 1> required_argument_names = { "YYYYMMDDhhmmss" };
    static constexpr auto MaxValue = 10000'0000'000000ll;
    static constexpr auto SeparateTime = 1000000;

    void checkRequiredArguments(const ColumnsWithTypeAndName & arguments, const size_t optional_argument_count) const
    {
        FunctionWithNumericParamsBase::checkRequiredArguments(arguments, required_argument_names, optional_argument_count);
    }

    void convertRequiredArguments(const ColumnsWithTypeAndName & arguments, Columns & converted_arguments) const
    {
        FunctionWithNumericParamsBase::convertRequiredArguments<DataTypeFloat64>(arguments, required_argument_names, converted_arguments);
    }
};

class FunctionYYYYMMDDhhmmssToDateTime : public FunctionYYYYMMDDhhmmssToDateTimeBase
{
private:
    static constexpr std::array<const char*, 1> optional_argument_names = { "timezone" };

public:
    static constexpr auto name = "YYYYMMDDhhmmssToDateTime";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYMMDDhhmmssToDateTime>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkRequiredArguments(arguments, optional_argument_names.size());

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == required_argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::string timezone;
        if (arguments.size() == required_argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        Columns float_arguments;
        convertRequiredArguments(arguments, float_arguments);

        auto res_column = ColumnDateTime::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & yyyymmddhhmmss_float = typeid_cast<const ColumnFloat64 &>(*float_arguments[0]).getData();

        const auto & date_lut = DateLUT::instance(timezone);

        for (size_t i = 0; i < input_rows_count; i++)
        {
            Int64 date_time = 0;

            const auto float_date = yyyymmddhhmmss_float[i];

            if (float_date > 0 && float_date < MaxValue)
            {
                const auto yyyymmddhhmmss = static_cast<Int64>(float_date);

                const auto yyyymmdd = yyyymmddhhmmss / SeparateTime;
                const auto hhmmss = yyyymmddhhmmss % SeparateTime;

                const auto year = yyyymmdd / 10000;
                const auto month = yyyymmdd / 100 % 100;
                const auto day = yyyymmdd % 100;
                const auto hour = hhmmss / 10000;
                const auto minute = hhmmss / 100 % 100;
                const auto second = hhmmss % 100;

                date_time = dateTime(year, month, day, hour, minute, second, date_lut);
            }

            if (unlikely(float_date <= 0 || date_time < 0))
                date_time = 0;
            else if (unlikely(float_date >= MaxValue || date_time > 0x0ffffffffll))
                date_time = 0x0ffffffffll;

            result_data[i] = static_cast<UInt32>(date_time);
        }

        return res_column;
    }
};

class FunctionYYYYMMDDhhmmssToDateTime64 : public FunctionYYYYMMDDhhmmssToDateTimeBase
{
private:
    static constexpr std::array<const char*, 2> optional_argument_names = { "precision", "timezone" };
    static constexpr UInt8 DEFAULT_PRECISION = 3;

public:
    static constexpr auto name = "YYYYMMDDhhmmssToDateTime64";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYMMDDhhmmssToDateTime64>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkRequiredArguments(arguments, optional_argument_names.size());

        UInt8 precision = DEFAULT_PRECISION;
        if (arguments.size() >= required_argument_names.size() + 1)
            precision = extractPrecision(arguments[required_argument_names.size()]);

        std::string timezone;
        if (arguments.size() == required_argument_names.size() + 2)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime64>(precision, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        UInt8 precision = DEFAULT_PRECISION;
        if (arguments.size() >= required_argument_names.size() + 1)
            precision = extractPrecision(arguments[required_argument_names.size()]);

        std::string timezone;
        if (arguments.size() == required_argument_names.size() + 2)
            timezone = extractTimezone(arguments.back());

        Columns float_arguments;
        convertRequiredArguments(arguments, float_arguments);

        auto res_column = ColumnDateTime64::create(input_rows_count, static_cast<UInt32>(precision));
        auto & result_data = res_column->getData();

        const auto & yyyymmddhhmmss_float = typeid_cast<const ColumnFloat64 &>(*float_arguments[0]).getData();

        const auto & date_lut = DateLUT::instance(timezone);

        const auto fraction_pow = common::exp10_i32(precision);
        const auto min_date_time = minDateTime(date_lut);
        const auto max_date_time = maxDateTime(date_lut);

        for (size_t i = 0; i < input_rows_count; i++)
        {
            Int64 date_time = 0;
            Int32 fraction = 0;

            const auto float_date = yyyymmddhhmmss_float[i];

            if (float_date < 0 || std::isnan(float_date))
                date_time = min_date_time;
            else if (float_date > MaxValue)
            {
                date_time = max_date_time;
                fraction = 999999999;
            }
            else
            {
                const auto yyyymmddhhmmss = static_cast<UInt64>(float_date);
                const auto yyyymmdd = yyyymmddhhmmss / SeparateTime;
                const auto hhmmss = yyyymmddhhmmss % SeparateTime;
                const auto decimal = float_date - yyyymmddhhmmss;

                const auto year = yyyymmdd / 10000;
                const auto month = yyyymmdd / 100 % 100;
                const auto day = yyyymmdd % 100;
                const auto hour = hhmmss / 10000;
                const auto minute = hhmmss / 100 % 100;
                const auto second = hhmmss % 100;
                fraction = static_cast<Int32>(decimal * fraction_pow);

                date_time = dateTime(year, month, day, hour, minute, second, date_lut);
                if (date_time == max_date_time)
                    fraction = 999999999;
            }

            result_data[i] = DecimalUtils::decimalFromComponents<DateTime64>(
                date_time,
                static_cast<Int64>(fraction),
                static_cast<UInt32>(precision));
        }

        return res_column;
    }

private:
    UInt8 extractPrecision(const ColumnWithTypeAndName & precision_argument) const
    {
        Int64 precision = DEFAULT_PRECISION;
        if (!isNumber(precision_argument.type) || !precision_argument.column || (precision_argument.column->size() != 1 && !typeid_cast<const ColumnConst*>(precision_argument.column.get())))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 'precision' for function {} must be constant number", getName());
        precision = precision_argument.column->getInt(0);
        if (precision < 0 || precision > 9)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Argument 'precision' for function {} must be in range [0, 9]", getName());

        return precision;
    }
};

}

REGISTER_FUNCTION(MakeDate)
{
    factory.registerFunction<FunctionMakeDate<MakeDateTraits>>();
    factory.registerFunction<FunctionMakeDate<MakeDate32Traits>>();
    factory.registerFunction<FunctionMakeDateTime>();
    factory.registerFunction<FunctionMakeDateTime64>();
    factory.registerFunction<FunctionYYYYYMMDDToDate<MakeDateTraits>>();
    factory.registerFunction<FunctionYYYYYMMDDToDate<MakeDate32Traits>>();
    factory.registerFunction<FunctionYYYYMMDDhhmmssToDateTime>();
    factory.registerFunction<FunctionYYYYMMDDhhmmssToDateTime64>();
}

}
