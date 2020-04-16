#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

class FunctionBitEquals : public IFunction
{
public:
    static constexpr auto name = "bitEquals";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitEquals>(); }

    bool useDefaultImplementationForNulls() const override { return false; }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnWithTypeAndName & col_left = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & col_right = block.getByPosition(arguments[1]);

        auto common_type = getLeastSupertype({col_left.type, col_right.type});

        ColumnPtr col_left_converted = castColumn(col_left, common_type);
        ColumnPtr col_right_converted = castColumn(col_right, common_type);

        auto res_column = ColumnUInt8::create(input_rows_count);
        auto & res_data = res_column->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
            res_data[i] = (0 == col_left_converted->compareAt(i, i, *col_right_converted, 1));

        block.getByPosition(result).column = std::move(res_column);
    }
};

void registerFunctionBitEquals(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitEquals>();
}

}

