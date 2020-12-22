#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int FUNCTION_THROW_IF_VALUE_IS_NON_ZERO;
}

namespace
{

/// Throw an exception if the argument is non zero.
class FunctionThrowIf : public IFunction
{
public:
    static constexpr auto name = "throwIf";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionThrowIf>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 1 || number_of_arguments > 2)
            throw Exception{"Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(number_of_arguments) + ", should be 1 or 2",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        if (!isNativeNumber(arguments[0]))
            throw Exception{"Argument for function " + getName() + " must be number", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (number_of_arguments > 1 && !isString(arguments[1]))
            throw Exception{"Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};


        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        std::optional<String> custom_message;
        if (arguments.size() == 2)
        {
            const auto * msg_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
            if (!msg_column)
                throw Exception{"Second argument for function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN};
            custom_message = msg_column->getValue<String>();
        }

        const auto * in = arguments.front().column.get();

        ColumnPtr res;
        if (!((res = execute<UInt8>(in, custom_message))
            || (res = execute<UInt16>(in, custom_message))
            || (res = execute<UInt32>(in, custom_message))
            || (res = execute<UInt64>(in, custom_message))
            || (res = execute<Int8>(in, custom_message))
            || (res = execute<Int16>(in, custom_message))
            || (res = execute<Int32>(in, custom_message))
            || (res = execute<Int64>(in, custom_message))
            || (res = execute<Float32>(in, custom_message))
            || (res = execute<Float64>(in, custom_message))))
            throw Exception{"Illegal column " + in->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

        return res;
    }

    template <typename T>
    ColumnPtr execute(const IColumn * in_untyped, const std::optional<String> & message) const
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped))
        {
            const auto & in_data = in->getData();
            if (!memoryIsZero(in_data.data(), in_data.size() * sizeof(in_data[0])))
                throw Exception{message.value_or("Value passed to '" + getName() + "' function is non zero"),
                                ErrorCodes::FUNCTION_THROW_IF_VALUE_IS_NON_ZERO};

            /// We return non constant to avoid constant folding.
            return  ColumnUInt8::create(in_data.size(), 0);
        }

        return nullptr;
    }
};

}

void registerFunctionThrowIf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionThrowIf>();
}

}
