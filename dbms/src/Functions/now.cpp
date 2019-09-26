#include <DataTypes/DataTypeDateTime.h>

#include <Core/DecimalFunctions.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

#include <time.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DateTime64::NativeType nowSubsecond(UInt8 scale) {
    timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);

    return decimalFromComponents<DateTime64::NativeType>(spec.tv_sec, spec.tv_nsec, scale);
}

/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class FunctionNow : public IFunction
{
public:
    static constexpr auto name = "now";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDateTime>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeDateTime().createColumnConst(input_rows_count, static_cast<UInt64>(time(nullptr)));
    }
};

class FunctionNow64 : public IFunction
{
public:
    static constexpr auto name = "now64";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow64>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        // Type check is similar to the validateArgumentType, trying to keep error codes and messages as close to the said function as possible.
        if (arguments.size() <= 1)
            throw Exception("Incorrect number of arguments of function " + getName(),
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & argument = arguments[0];
        if (!isInteger(argument.type) || !isColumnConst(*argument.column))
            throw Exception("Illegal type " + argument.type->getName() +
                            " of 0" +
                            " argument of function " + getName() +
                            ". Expected const integer.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const UInt64 scale = argument.column->get64(0);

        return std::make_shared<DataTypeDateTime64>(scale);
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IColumn * scale_column = block.getByPosition(arguments[0]).column.get();
        if (!isColumnConst(*scale_column))
            throw Exception("Unsupported argument type: " + scale_column->getName() +
                            + " for function " + getName() + ". Expected const integer.",
                            ErrorCodes::ILLEGAL_COLUMN);

        const UInt64 scale = scale_column->get64(0);
        block.getByPosition(result).column = DataTypeDateTime64(scale).createColumnConst(input_rows_count, nowSubsecond(scale));
    }
};

void registerFunctionNow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNow64>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionNow>(FunctionFactory::CaseInsensitive);
}

}
