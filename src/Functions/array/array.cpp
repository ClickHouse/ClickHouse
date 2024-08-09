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
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionArray>();
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// array(..., Nothing, ...) -> Array(..., Nothing, ...)
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return std::make_shared<DataTypeArray>(getLeastSupertype(arguments));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t num_elements = arguments.size();

        if (num_elements == 0)
            /// We should return constant empty array.
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const DataTypePtr & elem_type = static_cast<const DataTypeArray &>(*result_type).getNestedType();

        /** If part of columns have not same type as common type of all elements of array,
            *  then convert them to common type.
            * If part of columns are constants,
            *  then convert them to full columns.
            */

        Columns columns_holder(num_elements);
        ColumnRawPtrs column_ptrs(num_elements);

        for (size_t i = 0; i < num_elements; ++i)
        {
            const auto & arg = arguments[i];

            ColumnPtr preprocessed_column = arg.column;

            if (!arg.type->equals(*elem_type))
                preprocessed_column = castColumn(arg, elem_type);

            preprocessed_column = preprocessed_column->convertToFullColumnIfConst();

            columns_holder[i] = std::move(preprocessed_column);
            column_ptrs[i] = columns_holder[i].get();
        }

        /// Create and fill the result array.

        auto out = ColumnArray::create(elem_type->createColumn());
        IColumn & out_data = out->getData();
        IColumn::Offsets & out_offsets = out->getOffsets();

        out_data.reserve(input_rows_count * num_elements);
        out_offsets.resize(input_rows_count);

        IColumn::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (size_t j = 0; j < num_elements; ++j)
                out_data.insertFrom(*column_ptrs[j], i);

            current_offset += num_elements;
            out_offsets[i] = current_offset;
        }

        return out;
    }

private:
    String getName() const override
    {
        return name;
    }

    bool addField(DataTypePtr type_res, const Field & f, Array & arr) const;
};


REGISTER_FUNCTION(Array)
{
    factory.registerFunction<FunctionArray>();
}

}
