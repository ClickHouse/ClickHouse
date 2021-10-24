#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

/// arrayZip(['a', 'b', 'c'], ['d', 'e', 'f']) = [('a', 'd'), ('b', 'e'), ('c', 'f')]
class FunctionArrayZip : public IFunction
{
public:
    static constexpr auto name = "arrayZip";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayZip>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Function " + getName() + " needs at least one argument; passed " + toString(arguments.size()) + "."
                , ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes arguments_types;
        for (size_t index = 0; index < arguments.size(); ++index)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[index].type.get());

            if (!array_type)
                throw Exception("Argument " + toString(index + 1) + " of function " + getName()
                    + " must be array. Found " + arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            arguments_types.emplace_back(array_type->getNestedType());
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(arguments_types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        size_t num_arguments = arguments.size();

        ColumnPtr first_array_column;
        Columns tuple_columns(num_arguments);

        for (size_t i = 0; i < num_arguments; ++i)
        {
            /// Constant columns cannot be inside tuple. It's only possible to have constant tuple as a whole.
            ColumnPtr holder = arguments[i].column->convertToFullColumnIfConst();

            const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(holder.get());

            if (!column_array)
                throw Exception("Argument " + toString(i + 1) + " of function " + getName() + " must be array."
                    " Found column " + holder->getName() + " instead.", ErrorCodes::ILLEGAL_COLUMN);

            if (i == 0)
            {
                first_array_column = holder;
            }
            else if (!column_array->hasEqualOffsets(static_cast<const ColumnArray &>(*first_array_column)))
            {
                throw Exception("The argument 1 and argument " + toString(i + 1) + " of function " + getName() + " have different array sizes",
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }

            tuple_columns[i] = column_array->getDataPtr();
        }

        return ColumnArray::create(
            ColumnTuple::create(tuple_columns), static_cast<const ColumnArray &>(*first_array_column).getOffsetsPtr());
    }
};

void registerFunctionArrayZip(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayZip>();
}

}

