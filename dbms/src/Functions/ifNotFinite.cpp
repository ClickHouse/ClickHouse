#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Core/ColumnNumbers.h>


namespace DB
{

/// ifNotFinite(x, y) is equivalent to isFinite(x) ? x : y.
class FunctionIfNotFinite : public IFunction
{
public:
    static constexpr auto name = "ifNotFinite";

    FunctionIfNotFinite(const Context & context_) : context(context_) {}

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionIfNotFinite>(context);
    }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return getLeastSupertype({arguments[0], arguments[1]});
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        Block temp_block = block;

        size_t is_finite_pos = temp_block.columns();
        temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

        auto is_finite = FunctionFactory::instance().get("isFinite", context)->build(
            {temp_block.getByPosition(arguments[0])});

        auto func_if = FunctionFactory::instance().get("if", context)->build(
            {temp_block.getByPosition(is_finite_pos), temp_block.getByPosition(arguments[0]), temp_block.getByPosition(arguments[1])});

        is_finite->execute(temp_block, {arguments[0]}, is_finite_pos, input_rows_count);
        func_if->execute(temp_block, {is_finite_pos, arguments[0], arguments[1]}, result, input_rows_count);

        block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
    }

private:
    const Context & context;
};


void registerFunctionIfNotFinite(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIfNotFinite>();
}

}

