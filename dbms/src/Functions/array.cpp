#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/castColumn.h>


namespace DB
{

/// array(c1, c2, ...) - create an array.
class FunctionArray : public IFunction
{
public:
    static constexpr auto name = "array";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionArray>(context);
    }

    FunctionArray(const Context & context)
        : context(context)
    {
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeArray>(getLeastSupertype(arguments));
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        size_t num_elements = arguments.size();

        if (num_elements == 0)
        {
            /// We should return constant empty array.
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

        const DataTypePtr & return_type = block.getByPosition(result).type;
        const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*return_type).getNestedType();

        size_t block_size = input_rows_count;

        /** If part of columns have not same type as common type of all elements of array,
            *  then convert them to common type.
            * If part of columns are constants,
            *  then convert them to full columns.
            */

        Columns columns_holder(num_elements);
        const IColumn * columns[num_elements];

        for (size_t i = 0; i < num_elements; ++i)
        {
            const auto & arg = block.getByPosition(arguments[i]);

            ColumnPtr preprocessed_column = arg.column;

            if (!arg.type->equals(*elem_type))
                preprocessed_column = castColumn(arg, elem_type, context);

            if (ColumnPtr materialized_column = preprocessed_column->convertToFullColumnIfConst())
                preprocessed_column = materialized_column;

            columns_holder[i] = std::move(preprocessed_column);
            columns[i] = columns_holder[i].get();
        }

        /// Create and fill the result array.

        auto out = ColumnArray::create(elem_type->createColumn());
        IColumn & out_data = out->getData();
        IColumn::Offsets & out_offsets = out->getOffsets();

        out_data.reserve(block_size * num_elements);
        out_offsets.resize(block_size);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < block_size; ++i)
        {
            for (size_t j = 0; j < num_elements; ++j)
                out_data.insertFrom(*columns[j], i);

            current_offset += num_elements;
            out_offsets[i] = current_offset;
        }

        block.getByPosition(result).column = std::move(out);
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


void registerFunctionArray(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArray>();
}

}
