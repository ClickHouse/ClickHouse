#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int FUNCTION_THROW_IF_VALUE_IS_NON_ZERO;
}


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

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNumber(arguments.front()))
            throw Exception{"Argument for function " + getName() + " must be number", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const auto in = block.getByPosition(arguments.front()).column.get();

        if (   !execute<UInt8>(block, in, result)
            && !execute<UInt16>(block, in, result)
            && !execute<UInt32>(block, in, result)
            && !execute<UInt64>(block, in, result)
            && !execute<Int8>(block, in, result)
            && !execute<Int16>(block, in, result)
            && !execute<Int32>(block, in, result)
            && !execute<Int64>(block, in, result)
            && !execute<Float32>(block, in, result)
            && !execute<Float64>(block, in, result))
            throw Exception{"Illegal column " + in->getName() + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename T>
    bool execute(Block & block, const IColumn * in_untyped, const size_t result)
    {
        if (const auto in = checkAndGetColumn<ColumnVector<T>>(in_untyped))
        {
            const auto & in_data = in->getData();
            if (!memoryIsZero(in_data.data(), in_data.size() * sizeof(in_data[0])))
                throw Exception("Value passed to 'throwIf' function is non zero", ErrorCodes::FUNCTION_THROW_IF_VALUE_IS_NON_ZERO);

            /// We return non constant to avoid constant folding.
            block.getByPosition(result).column = ColumnUInt8::create(in_data.size(), 0);
            return true;
        }

        return false;
    }
};


void registerFunctionThrowIf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionThrowIf>();
}

}
