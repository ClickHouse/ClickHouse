#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

/// Implements the function nullIf which takes 2 arguments and returns
/// NULL if both arguments have the same value. Otherwise it returns the
/// value of the first argument.
class FunctionNullIf : public IFunction
{
private:
    const Context & context;
public:
    static constexpr auto name = "nullIf";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionNullIf>(context);
    }

    FunctionNullIf(const Context & context) : context(context) {}

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return makeNullable(arguments[0]);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        /// nullIf(col1, col2) == if(col1 == col2, NULL, col1)

        Block temp_block = block;

        size_t res_pos = temp_block.columns();
        temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});

        {
            auto equals_func = FunctionFactory::instance().get("equals", context)->build(
                {temp_block.getByPosition(arguments[0]), temp_block.getByPosition(arguments[1])});
            equals_func->execute(temp_block, {arguments[0], arguments[1]}, res_pos, input_rows_count);
        }

        /// Argument corresponding to the NULL value.
        size_t null_pos = temp_block.columns();

        /// Append a NULL column.
        ColumnWithTypeAndName null_elem;
        null_elem.type = block.getByPosition(result).type;
        null_elem.column = null_elem.type->createColumnConstWithDefaultValue(input_rows_count);
        null_elem.name = "NULL";

        temp_block.insert(null_elem);

        auto func_if = FunctionFactory::instance().get("if", context)->build(
            {temp_block.getByPosition(res_pos), temp_block.getByPosition(null_pos), temp_block.getByPosition(arguments[0])});
        func_if->execute(temp_block, {res_pos, null_pos, arguments[0]}, result, input_rows_count);

        block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
    }
};



void registerFunctionNullIf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNullIf>(FunctionFactory::CaseInsensitive);
}

}

