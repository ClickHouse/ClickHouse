#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionWithNumericParams.h>

#include <Common/DateLUT.h>

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
inline constexpr Int64 YearMonthDayToSingleInt(Int64 year, Int64 month, Int64 day) { return year * 512 + month * 32 + day; }

template <typename Traits>
class FunctionYYYYMMDDToDate : public FunctionWithNumericParamsBase
{
protected:
    static constexpr std::array<const char *, 1> argument_names = {"num"};

public:
    static constexpr auto name = Traits::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYMMDDToDate>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkRequiredArguments(arguments, argument_names, 0);
        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        Columns converted_arguments;
        convertRequiredArguments(arguments, argument_names, converted_arguments);

        const auto & num_data = typeid_cast<const ColumnUInt64 &>(*converted_arguments[0]).getData();
        auto res_column = Traits::ReturnColumnType::create(input_rows_count);
        auto & res_data = res_column->getData();

        const auto & date_lut = DateLUT::instance();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            const auto num = num_data[i];
            const int year = num % 10000;
            const int month = (num / 10000) % 100;
            const int day = (num / 1000000) % 100;

            Int32 day_num = 0;

            if (year >= Traits::MIN_YEAR && year <= Traits::MAX_YEAR && month >= 1 && month <= 12 && day >= 1 && day <= 31
                && YearMonthDayToSingleInt(year, month, day) <= Traits::MAX_DATE)
            {
                day_num = date_lut.makeDayNum(year, month, day);
            }

            res_data[i] = day_num;
        }

        return res_column;
    }
};

/// YYYYMMDDToDate(num)
struct YYYYMMDDToDateTraits
{
    static constexpr auto name = "YYYYMMDDToDate";
    using ReturnDataType = DataTypeDate;
    using ReturnColumnType = ColumnUInt16;

    static constexpr auto MIN_YEAR = 1970;
    static constexpr auto MAX_YEAR = 2149;
    /// This date has the maximum day number that fits in 16-bit uint
    static constexpr auto MAX_DATE = YearMonthDayToSingleInt(MAX_YEAR, 6, 6);
};

/// YYYYMMDDToDate32(num)
struct YYYYMMDDToDate32Traits
{
    static constexpr auto name = "YYYYMMDDToDate32";
    using ReturnDataType = DataTypeDate32;
    using ReturnColumnType = ColumnInt32;

    static constexpr auto MIN_YEAR = 1925;
    static constexpr auto MAX_YEAR = 2283;
    static constexpr auto MAX_DATE = YearMonthDayToSingleInt(MAX_YEAR, 11, 11);
};

template <typename Traits>
class FunctionYYYYMMDDhhmmssToDateTime : public FunctionWithNumericParamsBase
{
protected:
    static constexpr std::array<const char *, 1> argument_names = {"num"};

public:
    static constexpr auto name = Traits::name;

    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionYYYYMMDDhhmmssToDateTime>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkRequiredArguments(arguments, argument_names, 1);
        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        Columns converted_arguments;
        convertRequiredArguments(arguments, argument_names, converted_arguments);

        const auto & num_data = typeid_cast<const ColumnUInt64 &>(*converted_arguments[0]).getData();
        auto res_column = Traits::ReturnColumnType::create(input_rows_count);
        auto & res_data = res_column->getData();

        return res_column;
    }
};

void registerFunctionsMakeDate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionYYYYMMDDToDate<YYYYMMDDToDateTraits>>();
    factory.registerFunction<FunctionYYYYMMDDToDate<YYYYMMDDToDate32Traits>>();
}

}
}
