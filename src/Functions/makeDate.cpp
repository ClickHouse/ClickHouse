#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
#include <Common/DateLUTImpl.h>
#include <Common/typeid_cast.h>

#include <array>
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Functionality common to
/// - makeDate, makeDate32, makeDateTime, makeDateTime64,
/// - YYYYMMDDToDate, YYYYMMDDToDate32, YYYYMMDDhhmmssToDateTime, YYYYMMDDhhmmssToDateTime64
class FunctionWithNumericParamsBase : public IFunction
{
public:
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

protected:
    template <class DataType = DataTypeFloat32, class ArgumentNames>
    Columns convertMandatoryArguments(const ColumnsWithTypeAndName & arguments, const ArgumentNames & argument_names) const
    {
        Columns converted_arguments;
        const DataTypePtr converted_argument_type = std::make_shared<DataType>();
        for (size_t i = 0; i < argument_names.size(); ++i)
        {
            ColumnPtr argument_column = castColumn(arguments[i], converted_argument_type);
            argument_column = argument_column->convertToFullColumnIfConst();
            converted_arguments.push_back(argument_column);
        }
        return converted_arguments;
    }
};

/// Implementation of makeDate, makeDate32
template <typename Traits>
class FunctionMakeDate : public FunctionWithNumericParamsBase
{
private:
    static constexpr std::array mandatory_argument_names_year_month_day = {"year", "month", "day"};
    static constexpr std::array mandatory_argument_names_year_dayofyear = {"year", "dayofyear"};

public:
    static constexpr auto name = Traits::makeDateName;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDate>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const bool is_year_month_variant = (arguments.size() == 3);

        if (is_year_month_variant)
        {
            FunctionArgumentDescriptors args{
                {mandatory_argument_names_year_month_day[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
                {mandatory_argument_names_year_month_day[1], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
                {mandatory_argument_names_year_month_day[2], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
            };
            validateFunctionArguments(*this, arguments, args);
        }
        else
        {
            FunctionArgumentDescriptors args{
                {mandatory_argument_names_year_dayofyear[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
                {mandatory_argument_names_year_dayofyear[1], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
            };
            validateFunctionArguments(*this, arguments, args);
        }

        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const bool is_year_month_day_variant = (arguments.size() == 3);

        Columns converted_arguments;
        if (is_year_month_day_variant)
            converted_arguments = convertMandatoryArguments(arguments, mandatory_argument_names_year_month_day);
        else
            converted_arguments = convertMandatoryArguments(arguments, mandatory_argument_names_year_dayofyear);

        auto res_column = Traits::ReturnDataType::ColumnType::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & date_lut = DateLUT::instance();
        const Int32 max_days_since_epoch = date_lut.makeDayNum(Traits::MAX_DATE[0], Traits::MAX_DATE[1], Traits::MAX_DATE[2]);

        if (is_year_month_day_variant)
        {
            const auto & year_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[0]).getData();
            const auto & month_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[1]).getData();
            const auto & day_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[2]).getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto year = year_data[i];
                const auto month = month_data[i];
                const auto day = day_data[i];

                Int32 day_num = 0;

                if (year >= Traits::MIN_YEAR && year <= Traits::MAX_YEAR &&
                    month >= 1 && month <= 12 &&
                    day >= 1 && day <= 31)
                {
                    Int32 days_since_epoch = date_lut.makeDayNum(static_cast<Int16>(year), static_cast<UInt8>(month), static_cast<UInt8>(day));
                    if (days_since_epoch <= max_days_since_epoch)
                        day_num = days_since_epoch;
                }

                result_data[i] = day_num;
            }
        }
        else
        {
            const auto & year_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[0]).getData();
            const auto & dayofyear_data = typeid_cast<const ColumnFloat32 &>(*converted_arguments[1]).getData();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto year = year_data[i];
                const auto dayofyear = dayofyear_data[i];

                Int32 day_num = 0;

                if (year >= Traits::MIN_YEAR && year <= Traits::MAX_YEAR &&
                    dayofyear >= 1 && dayofyear <= 365)
                {
                    Int32 days_since_epoch = date_lut.makeDayNum(static_cast<Int16>(year), 1, 1) + static_cast<Int32>(dayofyear) - 1;
                    if (days_since_epoch <= max_days_since_epoch)
                        day_num = days_since_epoch;
                }

                result_data[i] = day_num;
            }
        }

        return res_column;
    }
};

/// Implementation of YYYYMMDDToDate, YYYYMMDDToDate32
template<typename Traits>
class FunctionYYYYYMMDDToDate : public FunctionWithNumericParamsBase
{
private:
    static constexpr std::array mandatory_argument_names = { "YYYYMMDD" };

public:
    static constexpr auto name = Traits::YYYYMMDDName;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYYMMDDToDate>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return mandatory_argument_names.size(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {mandatory_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
        };

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        Columns converted_arguments = convertMandatoryArguments<DataTypeFloat64>(arguments, mandatory_argument_names);

        auto res_column = Traits::ReturnDataType::ColumnType::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & yyyymmdd_data = typeid_cast<const ColumnFloat64 &>(*converted_arguments[0]).getData();

        const auto & date_lut = DateLUT::instance();
        const Int32 max_days_since_epoch = date_lut.makeDayNum(Traits::MAX_DATE[0], Traits::MAX_DATE[1], Traits::MAX_DATE[2]);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (std::isinf(yyyymmdd_data[i]) || std::isnan(yyyymmdd_data[i])) [[unlikely]]
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument for function {} must be finite", getName());

            const auto yyyymmdd = std::llround(yyyymmdd_data[i]);

            const auto year = yyyymmdd / 10'000;
            const auto month = yyyymmdd / 100 % 100;
            const auto day = yyyymmdd % 100;

            Int32 day_num = 0;

            if (year >= Traits::MIN_YEAR && year <= Traits::MAX_YEAR &&
                month >= 1 && month <= 12 &&
                day >= 1 && day <= 31)
            {
                Int32 days_since_epoch = date_lut.makeDayNum(static_cast<Int16>(year), static_cast<UInt8>(month), static_cast<UInt8>(day));
                if (days_since_epoch <= max_days_since_epoch)
                    day_num = days_since_epoch;
            }

            result_data[i] = day_num;
        }

        return res_column;
    }
};

struct DateTraits
{
    static constexpr auto makeDateName = "makeDate";
    static constexpr auto YYYYMMDDName = "YYYYMMDDToDate";
    using ReturnDataType = DataTypeDate;

    static constexpr auto MIN_YEAR = 1970;
    static constexpr auto MAX_YEAR = 2149;
    static constexpr std::array MAX_DATE = {MAX_YEAR, 6, 6};
};

struct Date32Traits
{
    static constexpr auto makeDateName = "makeDate32";
    static constexpr auto YYYYMMDDName = "YYYYMMDDToDate32";
    using ReturnDataType = DataTypeDate32;

    static constexpr auto MIN_YEAR = 1900;
    static constexpr auto MAX_YEAR = 2299;
    static constexpr std::array MAX_DATE = {MAX_YEAR, 12, 31};
};

/// Functionality common to makeDateTime, makeDateTime64, YYYYMMDDhhmmssToDateTime, YYYYMMDDhhmmssToDateTime64
class FunctionDateTimeBase : public FunctionWithNumericParamsBase
{
protected:
    static constexpr UInt32 DEFAULT_PRECISION = 3;

    template <typename T>
    static Int64 dateTime(T year, T month, T day_of_month, T hour, T minute, T second, const DateLUTImpl & lut)
    {
        ///  Note that hour, minute and second are checked against 99 to behave consistently with parsing DateTime from String
        ///  E.g. "select cast('1984-01-01 99:99:99' as DateTime);" returns "1984-01-05 04:40:39"
        if (std::isnan(year) || std::isnan(month) || std::isnan(day_of_month) ||
            std::isnan(hour) || std::isnan(minute) || std::isnan(second) ||
            year < DATE_LUT_MIN_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31 ||
            hour < 0 || hour > 99 || minute < 0 || minute > 99 || second < 0 || second > 99) [[unlikely]]
            return minDateTime(lut);

        if (year > DATE_LUT_MAX_YEAR) [[unlikely]]
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
        if (!isStringOrFixedString(timezone_argument.type) || !timezone_argument.column || (timezone_argument.column->size() != 1 && !typeid_cast<const ColumnConst*>(timezone_argument.column.get())))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 'timezone' for function {} must be const string", getName());

        String timezone = timezone_argument.column->getDataAt(0).toString();

        return timezone;
    }

    UInt32 extractPrecision(const ColumnWithTypeAndName & precision_argument) const
    {
        if (!isNumber(precision_argument.type) || !precision_argument.column || (precision_argument.column->size() != 1 && !typeid_cast<const ColumnConst*>(precision_argument.column.get())))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 'precision' for function {} must be constant number", getName());

        Int64 precision = precision_argument.column->getInt(0);

        if (precision < 0 || precision > 9)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Argument 'precision' for function {} must be in range [0, 9]", getName());

        return static_cast<UInt32>(precision);
    }
};

class FunctionMakeDateTimeBase : public FunctionDateTimeBase
{
protected:
    static constexpr std::array mandatory_argument_names = {"year", "month", "day", "hour", "minute", "second"};
};

/// makeDateTime(year, month, day, hour, minute, second, [timezone])
class FunctionMakeDateTime : public FunctionMakeDateTimeBase
{
private:
    static constexpr std::array optional_argument_names = {"timezone"};

public:
    static constexpr auto name = "makeDateTime";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDateTime>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {mandatory_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[1], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[2], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[3], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[4], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[5], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
        };

        FunctionArgumentDescriptors optional_args{
            {optional_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        Columns converted_arguments = convertMandatoryArguments(arguments, mandatory_argument_names);

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
            if (date_time < 0) [[unlikely]]
                date_time = 0;
            else if (date_time > 0x0ffffffffll) [[unlikely]]
                date_time = 0x0ffffffffll;

            result_data[i] = static_cast<UInt32>(date_time);
        }

        return res_column;
    }
};

/// makeDateTime64(year, month, day, hour, minute, second[, fraction[, precision[, timezone]]])
class FunctionMakeDateTime64 : public FunctionMakeDateTimeBase
{
private:
    static constexpr std::array optional_argument_names = {"fraction", "precision", "timezone"};

public:
    static constexpr auto name = "makeDateTime64";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMakeDateTime64>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {mandatory_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[1], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[2], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[3], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[4], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {mandatory_argument_names[5], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
        };

        FunctionArgumentDescriptors optional_args{
            {optional_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "const Number"},
            {optional_argument_names[1], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), isColumnConst, "const Number"},
            {optional_argument_names[2], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

            validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        if (arguments.size() >= mandatory_argument_names.size() + 1)
        {
            const auto& fraction_argument = arguments[mandatory_argument_names.size()];
            if (!isNumber(fraction_argument.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument 'fraction' for function {} must be a number", getName());
        }

        /// Optional precision argument
        Int64 precision = DEFAULT_PRECISION;
        if (arguments.size() >= mandatory_argument_names.size() + 2)
            precision = extractPrecision(arguments[mandatory_argument_names.size() + 1]);

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 3)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime64>(precision, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Optional precision argument
        Int64 precision = DEFAULT_PRECISION;
        if (arguments.size() >= mandatory_argument_names.size() + 2)
            precision = extractPrecision(arguments[mandatory_argument_names.size() + 1]);

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 3)
            timezone = extractTimezone(arguments.back());

        Columns converted_arguments = convertMandatoryArguments(arguments, mandatory_argument_names);

        /// Optional fraction argument
        const ColumnVector<Float64>::Container * fraction_data = nullptr;
        if (arguments.size() >= mandatory_argument_names.size() + 1)
        {
            ColumnPtr fraction_column = castColumn(arguments[mandatory_argument_names.size()], std::make_shared<DataTypeFloat64>());
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
            if (date_time == min_date_time) [[unlikely]]
                fraction = 0;
            else if (date_time == max_date_time) [[unlikely]]
                fraction = 999999999;
            else
            {
                fraction = fraction_data ? (*fraction_data)[i] : 0;
                if (std::isnan(fraction)) [[unlikely]]
                {
                    date_time = min_date_time;
                    fraction = 0;
                }
                else if (fraction < 0) [[unlikely]]
                    fraction = 0;
                else if (fraction > max_fraction) [[unlikely]]
                    fraction = max_fraction;
            }

            result_data[i] = DecimalUtils::decimalFromComponents<DateTime64>(
                date_time,
                static_cast<Int64>(fraction),
                static_cast<UInt32>(precision));
        }

        return res_column;
    }
};

class FunctionYYYYMMDDhhmmssToDateTimeBase : public FunctionDateTimeBase
{
protected:
    static constexpr std::array mandatory_argument_names = { "YYYYMMDDhhmmss" };
};

/// YYYYMMDDhhmmssToDateTime
class FunctionYYYYMMDDhhmmssToDateTime : public FunctionYYYYMMDDhhmmssToDateTimeBase
{
private:
    static constexpr std::array optional_argument_names = { "timezone" };

public:
    static constexpr auto name = "YYYYMMDDhhmmssToDateTime";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYMMDDhhmmssToDateTime>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {mandatory_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
        };

        FunctionArgumentDescriptors optional_args{
            {optional_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime>(timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 1)
            timezone = extractTimezone(arguments.back());

        Columns converted_arguments = convertMandatoryArguments<DataTypeFloat64>(arguments, mandatory_argument_names);

        auto res_column = ColumnDateTime::create(input_rows_count);
        auto & result_data = res_column->getData();

        const auto & yyyymmddhhmmss_data = typeid_cast<const ColumnFloat64 &>(*converted_arguments[0]).getData();

        const auto & date_lut = DateLUT::instance(timezone);

        for (size_t i = 0; i < input_rows_count; i++)
        {
            if (std::isinf(yyyymmddhhmmss_data[i]) || std::isnan(yyyymmddhhmmss_data[i])) [[unlikely]]
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument for function {} must be finite", getName());

            const auto yyyymmddhhmmss = std::llround(yyyymmddhhmmss_data[i]);

            const auto yyyymmdd = yyyymmddhhmmss / 1'000'000;
            const auto hhmmss = yyyymmddhhmmss % 1'000'000;

            const auto year = yyyymmdd / 10'000;
            const auto month = yyyymmdd / 100 % 100;
            const auto day = yyyymmdd % 100;
            const auto hour = hhmmss / 10'000;
            const auto minute = hhmmss / 100 % 100;
            const auto second = hhmmss % 100;

            auto date_time = dateTime(year, month, day, hour, minute, second, date_lut);

            if (date_time < 0) [[unlikely]]
                date_time = 0;
            else if (date_time > 0x0ffffffffll) [[unlikely]]
                date_time = 0x0ffffffffll;

            result_data[i] = static_cast<UInt32>(date_time);
        }

        return res_column;
    }
};

/// YYYYMMDDhhmmssToDateTime64
class FunctionYYYYMMDDhhmmssToDateTime64 : public FunctionYYYYMMDDhhmmssToDateTimeBase
{
private:
    static constexpr std::array optional_argument_names = { "precision", "timezone" };

public:
    static constexpr auto name = "YYYYMMDDhhmmssToDateTime64";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYMMDDhhmmssToDateTime64>(); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {mandatory_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
        };

        FunctionArgumentDescriptors optional_args{
            {optional_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), isColumnConst, "const Number"},
            {optional_argument_names[0], static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        /// Optional precision argument
        auto precision = DEFAULT_PRECISION;
        if (arguments.size() >= mandatory_argument_names.size() + 1)
            precision = extractPrecision(arguments[mandatory_argument_names.size()]);

        /// Optional timezone argument
        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 2)
            timezone = extractTimezone(arguments.back());

        return std::make_shared<DataTypeDateTime64>(precision, timezone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        UInt32 precision = DEFAULT_PRECISION;
        if (arguments.size() >= mandatory_argument_names.size() + 1)
            precision = extractPrecision(arguments[mandatory_argument_names.size()]);

        std::string timezone;
        if (arguments.size() == mandatory_argument_names.size() + 2)
            timezone = extractTimezone(arguments.back());

        Columns converted_arguments = convertMandatoryArguments<DataTypeFloat64>(arguments, mandatory_argument_names);

        auto res_column = ColumnDateTime64::create(input_rows_count, precision);
        auto & result_data = res_column->getData();

        const auto & yyyymmddhhmmss_data = typeid_cast<const ColumnFloat64 &>(*converted_arguments[0]).getData();

        const auto & date_lut = DateLUT::instance(timezone);

        const auto fraction_pow = common::exp10_i32(precision);

        for (size_t i = 0; i < input_rows_count; i++)
        {
            const auto float_date = yyyymmddhhmmss_data[i];

            if (std::isinf(float_date) || std::isnan(float_date)) [[unlikely]]
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument for function {} must be finite", getName());

            const auto yyyymmddhhmmss = std::llround(float_date);

            const auto yyyymmdd = yyyymmddhhmmss / 1'000'000;
            const auto hhmmss = yyyymmddhhmmss % 1'000'000;

            const auto decimal = float_date - yyyymmddhhmmss;

            const auto year = yyyymmdd / 10'000;
            const auto month = yyyymmdd / 100 % 100;
            const auto day = yyyymmdd % 100;
            const auto hour = hhmmss / 10'000;
            const auto minute = hhmmss / 100 % 100;
            const auto second = hhmmss % 100;

            auto date_time = dateTime(year, month, day, hour, minute, second, date_lut);

            auto fraction = std::llround(decimal * fraction_pow);

            result_data[i] = DecimalUtils::decimalFromComponents<DateTime64>(date_time, fraction, precision);
        }

        return res_column;
    }
};

}

REGISTER_FUNCTION(MakeDate)
{
    factory.registerFunction<FunctionMakeDate<DateTraits>>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionMakeDate<Date32Traits>>();
    factory.registerFunction<FunctionMakeDateTime>();
    factory.registerFunction<FunctionMakeDateTime64>();

    factory.registerFunction<FunctionYYYYYMMDDToDate<DateTraits>>(
        FunctionDocumentation{
            .description = R"(
Converts a number containing the year, month and day number to a Date.
This functions is the opposite of function `toYYYYMMDD()`.
The output is undefined if the input does not encode a valid Date value.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionYYYYYMMDDToDate<Date32Traits>>(
        FunctionDocumentation{
            .description = R"(
Like function `YYYYMMDDToDate()` but produces a Date32.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionYYYYMMDDhhmmssToDateTime>(
        FunctionDocumentation{
            .description = R"(
Converts a number containing the year, month, day, hour, minute and second number to a DateTime.
The output is undefined if the input does not encode a valid DateTime value.
This functions is the opposite of function `toYYYYMMDD()`.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionYYYYMMDDhhmmssToDateTime64>(
        FunctionDocumentation{
            .description = R"(
Like function `YYYYMMDDhhmmssToDate()` but produces a DateTime64.
Accepts an additional, optional `precision` parameter after the `timezone` parameter.
)",
            .categories{"Dates and Times"}
        }
    );
}

}
