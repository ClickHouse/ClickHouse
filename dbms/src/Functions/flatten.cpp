#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>

namespace DB
{

/// flatten([[1, 2, 3], [4, 5]]) = [1, 2, 3, 4, 5] - flatten array.
class FunctionFlatten : public IFunction
{
public:
    static constexpr auto name = "flatten";

    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionFlatten>(context); }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isArray(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() +
                            " of argument of function " + getName() +
                            ", expected Array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        DataTypePtr nested_type = arguments[0];
        while (isArray(nested_type))
            nested_type = checkAndGetDataType<DataTypeArray>(nested_type.get())->getNestedType();

        return std::make_shared<DataTypeArray>(nested_type);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const auto * arg_col = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());
        const IColumn & arg_data = arg_col->getData();
        const IColumn::Offsets & arg_offsets = arg_col->getOffsets();

        const DataTypePtr & result_type = block.getByPosition(result).type;
        const DataTypePtr & result_nested_type = dynamic_cast<const DataTypeArray &>(*result_type).getNestedType();
        auto result_col = ColumnArray::create(result_nested_type->createColumn());
        IColumn & result_data = result_col->getData();
        IColumn::Offsets & result_offsets = result_col->getOffsets();

        // todo
        // result_data.reserve();
        // result_offsets.resize();

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto & flatten_data = flatten(arg_data, current_offset, arg_offsets[i]);
            result_data.insertRangeFrom(flatten_data, 0, flatten_data.size());
            current_offset += flatten_data.size();
            result_offsets[i] = current_offset;
        }

        block.getByPosition(result).column = std::move(result_col);
    }

private:
    String getName() const override
    {
        return name;
    }

    bool addField(DataTypePtr type_res, const Field & f, Array & arr) const;

    const ColumnArray & flatten(const IColumn & /*data*/, size_t /*from*/, size_t /*to*/) const
    {
        // todo
    }
};


void registerFunctionFlatten(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFlatten>();
}

}
