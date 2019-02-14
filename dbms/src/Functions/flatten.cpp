#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/castColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>
#include <ext/range.h>

namespace DB
{

    /// flatten([[1, 2, 3], [4, 5]]) = [1, 2, 3, 4, 5] - flatten array.
    class FunctionFlatten : public IFunction
    {
    public:
        static constexpr auto name = "flatten";
        static FunctionPtr create(const Context & context)
        {
            return std::make_shared<FunctionFlatten>(context);
        }

        FunctionFlatten(const Context & context)
                : context(context)
        {
        }

        size_t getNumberOfArguments() const override { return 1; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (!isArray(arguments[0]))
                throw Exception("Illegal type " + arguments[0]->getName() +
                                " of argument of function " + getName() +
                                ", expected Array", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            DataTypePtr nested_type = arguments[0];
            while (isArray(nested_type)) {
                nested_type = checkAndGetDataType<DataTypeArray>(nested_type.get())->getNestedType();
            }

            return std::make_shared<DataTypeArray>(nested_type);
        }

        void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
        {
            const auto & array_with_type_and_name = block.getByPosition(arguments[0]);
            ColumnPtr preprocessed_column = array_with_type_and_name.column;
            preprocessed_column = preprocessed_column->convertToFullColumnIfConst();
            const auto * arg_col = checkAndGetColumn<ColumnArray>(preprocessed_column.get());
            const IColumn & arg_data = arg_col->getData();
            const IColumn::Offsets & arg_offsets = arg_col->getOffsets();

            const DataTypePtr & result_type = block.getByPosition(result).type;
            const DataTypePtr & result_nested_type = dynamic_cast<const DataTypeArray &>(*result_type).getNestedType();
            auto result_col = ColumnArray::create(result_nested_type->createColumn());
            IColumn & result_data = result_col->getData();
            IColumn::Offsets & result_offsets = result_col->getOffsets();

            result_data.reserve(input_rows_count * 10);
            result_offsets.resize(input_rows_count);

            IColumn::Offset current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                for (size_t j = current_offset; j < arg_offsets[i]; ++j)
                    result_data.insertFrom(arg_data, j);

                current_offset += arg_offsets[i];
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

    private:
        const Context & context;
    };


    void registerFunctionFlatten(FunctionFactory & factory)
    {
        factory.registerFunction<FunctionFlatten>();
    }

}
