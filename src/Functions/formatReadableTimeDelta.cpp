#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionFormatReadableTimeDelta : public IFunction
{
public:
    static constexpr auto name = "formatReadableTimeDelta";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFormatReadableTimeDelta>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at most 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const IDataType & type = *arguments[0];

        if (!isNativeNumber(type))
            throw Exception("Cannot format " + type.getName() + " as time delta", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            const auto * maximum_unit_arg = arguments[1].get();
            if (!isStringOrFixedString(maximum_unit_arg))
                throw Exception("Illegal type " + maximum_unit_arg->getName() + " of argument maximum_unit of function "
                                + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        if (!(executeType<UInt8>(block, arguments, result)
            || executeType<UInt16>(block, arguments, result)
            || executeType<UInt32>(block, arguments, result)
            || executeType<UInt64>(block, arguments, result)
            || executeType<Int8>(block, arguments, result)
            || executeType<Int16>(block, arguments, result)
            || executeType<Int32>(block, arguments, result)
            || executeType<Int64>(block, arguments, result)
            || executeType<Float32>(block, arguments, result)
            || executeType<Float64>(block, arguments, result)))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:

    void formatReadableTimeDelta(double value, String maximum_unit, DB::WriteBuffer & out) const
    {
        // 60 SECONDS       = 1 MINUTE
        // 3600 SECONDS     = 1 HOUR
        // 86400 SECONDS    = 1 DAY
        // 2635200 SECONDS  = 30.5 DAYS = 1 MONTH
        // 31536000 SECONDS = 365 DAYS  = 1 YEAR

        int8_t sig = value < 0 ? -1 : 1;
        int maximum_unit_int = 6;

        if (maximum_unit == "seconds")
            maximum_unit_int = 1;
        else if (maximum_unit == "minutes")
            maximum_unit_int = 2;
        else if (maximum_unit == "hours")
            maximum_unit_int = 3;
        else if (maximum_unit == "days")
            maximum_unit_int = 4;
        else if (maximum_unit == "months")
            maximum_unit_int = 5;
        else if (maximum_unit == "years")
            maximum_unit_int = 6;

        value *= sig;

        double aux = 0;

        long long int years = maximum_unit_int < 6 ? 0 : value / 31536000;
        aux += years * 31536000;
        long long int months = maximum_unit_int < 5 ? 0 : (value - aux) / 2635200;
        aux += months * 2635200;
        long long int days = maximum_unit_int < 4 ? 0 : (value - aux) / 86400;
        aux += days * 86400;
        long long int hours = maximum_unit_int < 3 ? 0 : (value - aux) / 3600;
        aux += hours * 3600;
        long long int minutes = maximum_unit_int < 2 ? 0 : (value - aux) / 60;
        aux += minutes * 60;
        double seconds = value - aux;

        std::vector<String> parts;

        /* If value is bigger than 2**64 (292471208677 years) overflow happens
           To prevent wrong results the function shows only the year
           and maximum_unit is ignored
        */
        if (value > 9223372036854775808.0)
        {
            std::string years_str = std::to_string(value/31536000.0);
            years_str.erase(years_str.find('.'), std::string::npos);
            parts.push_back(years_str + " years");
        }
        else
        {
            if (years)
            {
                parts.push_back(std::to_string(years) + (years == 1 ? " year" : " years"));
            }
            if (months)
            {
                parts.push_back(std::to_string(months) + (months == 1 ? "month" : " months"));
            }
            if (days)
            {
                parts.push_back(std::to_string(days) + (days == 1 ? " day" : " days"));
            }
            if (hours)
            {
                parts.push_back(std::to_string(hours) + (hours == 1 ? " hour" : " hours"));
            }
            if (minutes)
            {
                parts.push_back(std::to_string(minutes) + (minutes == 1 ? " minute" : " minutes"));
            }
            if (seconds)
            {
                std::string seconds_str = std::to_string(seconds);
                seconds_str.erase(seconds_str.find_last_not_of('0') + 1, std::string::npos);
                seconds_str.erase(seconds_str.find_last_not_of('.') + 1, std::string::npos);
                parts.push_back(seconds_str + (seconds==1?" second":" seconds"));
            }
        }

        String str_value;
        for (size_t i = 0, parts_size = parts.size(); i < parts_size; ++i)
        {
            if (!str_value.empty())
            {
                if (i == parts.size() - 1)
                    str_value += " and ";
                else
                    str_value += ", ";
            }
            str_value += parts[i];
        }
        if (sig < 0)
            str_value = "- " + str_value;

        writeCString(str_value.c_str(), out);
    }

    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        String maximum_unit = "";
        if (arguments.size() == 2)
        {
            const ColumnPtr & maximum_unit_column = block.getByPosition(arguments[1]).column;
            const ColumnConst * maximum_unit_const_col = checkAndGetColumnConstStringOrFixedString(maximum_unit_column.get());
            if (maximum_unit_const_col)
                maximum_unit = maximum_unit_const_col->getValue<String>();
        }

        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();
            data_to.resize(size * 2);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                formatReadableTimeDelta(static_cast<double>(vec_from[i]), maximum_unit, buf_to);
                writeChar(0, buf_to);
                offsets_to[i] = buf_to.count();
            }

            buf_to.finalize();
            block.getByPosition(result).column = std::move(col_to);
            return true;
        }
        return false;
    }
};

}

void registerFunctionFormatReadableTimeDelta(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormatReadableTimeDelta>();
}

}

