#include <memory>

#include <Common/assert_cast.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

namespace
{

class FunctionNested : public IFunction
{
public:
    static constexpr auto name = "nested";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionNested>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {0};
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        // Validation is not exhaustive.
        FunctionArgumentDescriptors mandatory_args{{"column_names", &isArray, &isColumnConst, "const Array(String)"}};
        FunctionArgumentDescriptor variadic_args{"value", &isArray, nullptr, "Any"};
        validateFunctionArgumentsWithVariadics(*this, arguments, mandatory_args, variadic_args);

        if (const auto * const_column = typeid_cast<const ColumnConst *>(arguments[0].column.get()))
            if (auto res_type = getType(0, const_column->getDataColumn(), arguments))
                return res_type;

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function {} must be constant array of strings, got {}",
            getName(), arguments[0].type->getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
    {
        size_t arguments_size = arguments.size();
        Columns columns;
        columns.reserve(arguments_size);
        for (size_t i = 1; i < arguments_size; ++i)
            columns.push_back(arguments[i].column);

        return makeColumnForType(result_type.get(), std::move(columns));
    }

private:
    static DataTypePtr getInnerType(size_t array_depth, DataTypePtr type)
    {
        for (size_t i = 0; i < array_depth; ++i)
        {
            const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
            if (!array_type)
                return nullptr;

            type = array_type->getNestedType();
        }

        return type;
    }

    DataTypePtr getTupleType(size_t array_depth, const ColumnString & column, const ColumnsWithTypeAndName & arguments) const
    {
        if (column.size() + 1 != arguments.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Size of nested names array for function {} does not match arrays arguments size. Actual {}. Expected {}",
                getName(),
                column.size(),
                arguments.size() - 1);

        Names names;
        DataTypes types;
        names.reserve(column.size());
        types.reserve(column.size());

        for (size_t i = 0; i < column.size(); ++i)
        {
            const auto & argument = arguments[i + 1];
            auto type = getInnerType(array_depth, argument.type);

            if (!type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} for function {} must be {}-dimentional array. Actual {}",
                    i + 1,
                    array_depth,
                    getName(),
                    argument.type->getName());

            names.push_back(std::string{column.getDataAt(i)});
            types.push_back(std::move(type));
        }

        return std::make_shared<DataTypeTuple>(types, names);
    }

    DataTypePtr getType(size_t array_depth, const IColumn & column, const ColumnsWithTypeAndName & arguments) const
    {
        const auto * col_string = typeid_cast<const ColumnString *>(&column);
        if (col_string)
        {
            if (array_depth == 0)
                return nullptr;

            return getTupleType(array_depth, *col_string, arguments);
        }

        const auto * array_col = typeid_cast<const ColumnArray *>(&column);
        if (!array_col)
            return nullptr;

        if (array_col->size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "First argument for function {} must be constant column with N-dimentional array of strings, "
                "where the all arrays except the most inner one must have size = 1. "
                "The size of array at depth {} is {}",
                getName(), array_depth, array_col->size());

        auto type = getType(array_depth + 1, array_col->getData(), arguments);
        if (!type)
            return nullptr;

        return std::make_shared<DataTypeArray>(type);
    }

    ColumnPtr makeColumnForType(const IDataType * type, Columns columns) const
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type);
        if (tuple_type)
            return ColumnTuple::create(std::move(columns));

        const auto * array_type = typeid_cast<const DataTypeArray *>(type);
        if (!array_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Result type for function {} must be Array(Tuple()), got {}", getName(), type->getName());

        ColumnPtr first_array_materialized = columns[0]->convertToFullColumnIfConst();
        const ColumnArray & first_array = assert_cast<const ColumnArray &>(*first_array_materialized);

        for (size_t i = 1; i < columns.size(); ++i)
        {
            ColumnPtr other_array_materialized = columns[i]->convertToFullColumnIfConst();
            const ColumnArray & other_array = assert_cast<const ColumnArray &>(*other_array_materialized);

            if (!first_array.hasEqualOffsets(other_array))
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "The argument 2 and argument {} of function {} have different array offsets",
                    i + 2,
                    getName());

            columns[i] = other_array.getDataPtr();
        }

        auto nested_offsets = first_array.getOffsetsPtr();
        columns[0] = first_array.getDataPtr();
        auto nested_column = makeColumnForType(array_type->getNestedType().get(), std::move(columns));
        return ColumnArray::create(nested_column, nested_offsets);
    }
};

}

REGISTER_FUNCTION(Nested)
{
    factory.registerFunction<FunctionNested>(FunctionDocumentation{
        .description=R"(
This is a function used internally by ClickHouse and not meant to be used directly.

Returns the array of tuples from multiple arrays.

The first argument must be a constant array of Strings determining the names of the resulting Tuple.
The other arguments must be arrays of the same size.
)",
        .examples{{"nested", "SELECT nested(['keys', 'values'], ['key_1', 'key_2'], ['value_1','value_2'])", ""}},
        .introduced_in = {23, 2},
        .category = FunctionDocumentation::Category::Internal
    });
}

}
