#include <memory>

#include <Common/assert_cast.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnArray.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
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
        size_t arguments_size = arguments.size();
        if (arguments_size < 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 2",
                getName(),
                arguments_size);

        Names nested_names = extractNestedNames(arguments[0].column);
        if (nested_names.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "First argument for function {} must be constant column with array of strings",
                getName());

        if (nested_names.size() != arguments_size - 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Size of nested names array for function {} does not match arrays arguments size. Actual {}. Expected {}",
                getName(),
                nested_names.size(),
                arguments_size - 1);

        DataTypes nested_types;
        nested_types.reserve(arguments_size);

        for (size_t i = 1; i < arguments_size; ++i)
        {
            const auto & argument = arguments[i];
            const auto * argument_type_array = typeid_cast<const DataTypeArray *>(argument.type.get());

            if (!argument_type_array)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} for function {} must be array. Actual {}",
                    i + 1,
                    getName(),
                    argument.type->getName());

            nested_types.push_back(argument_type_array->getNestedType());
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(nested_types, nested_names));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        size_t arguments_size = arguments.size();

        const auto * lhs_array = assert_cast<const ColumnArray *>(arguments.at(1).column.get());

        Columns data_columns;
        data_columns.reserve(arguments_size);
        data_columns.push_back(lhs_array->getDataPtr());

        for (size_t i = 2; i < arguments_size; ++i)
        {
            const auto * rhs_array = assert_cast<const ColumnArray *>(arguments[i].column.get());

            if (!lhs_array->hasEqualOffsets(*rhs_array))
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "The argument 2 and argument {} of function {} have different array offsets",
                    i + 1,
                    getName());

            data_columns.push_back(rhs_array->getDataPtr());
        }

        auto tuple_column = ColumnTuple::create(std::move(data_columns));
        auto array_column = ColumnArray::create(std::move(tuple_column), lhs_array->getOffsetsPtr());

        return array_column;
    }
private:
    static Names extractNestedNames(const ColumnPtr & column)
    {
        const auto * const_column = typeid_cast<const ColumnConst *>(column.get());
        if (!const_column)
            return {};

        Field nested_names_field;
        const_column->get(0, nested_names_field);

        if (nested_names_field.getType() != Field::Types::Array)
            return {};

        const auto & nested_names_array = nested_names_field.safeGet<const Array &>();

        Names nested_names;
        nested_names.reserve(nested_names_array.size());

        for (const auto & nested_name_field : nested_names_array)
        {
            if (nested_name_field.getType() != Field::Types::String)
                return {};

            nested_names.push_back(nested_name_field.safeGet<const String &>());
        }

        return nested_names;
    }
};

}

REGISTER_FUNCTION(Nested)
{
    factory.registerFunction<FunctionNested>(FunctionDocumentation{
        .description=R"(
Returns the array of tuples from multiple arrays.
)",
        .examples{{"nested", "SELECT nested(['keys', 'values'], ['key_1', 'key_2'], ['value_1','value_2'])", ""}},
        .categories{"OtherFunctions"}
    });
}

}
