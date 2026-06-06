#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NullableUtils.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationVariantElement.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>
#include <memory>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_nullable_array_type;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** Extract element of Dynamic by type name.
  * Also the function looks through Arrays: you can get Array of Dynamic elements from Array of Dynamic.
  */
class FunctionDynamicElement final : public IFunction
{
public:
    static constexpr auto name = "dynamicElement";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDynamicElement>(context); }

    explicit FunctionDynamicElement(ContextPtr context)
        : allow_nullable_array_type(context && context->getSettingsRef()[Setting::allow_experimental_nullable_array_type])
    {
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2",
                            getName(), number_of_arguments);

        size_t count_arrays = 0;
        const IDataType * input_type = arguments[0].type.get();
        bool input_is_nullable_array = false;
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(input_type))
        {
            if (!isArray(nullable_type->getNestedType()))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "First argument for function {} must be Variant or Array of Variant. Actual {}",
                                getName(),
                                arguments[0].type->getName());

            input_type = nullable_type->getNestedType().get();
            input_is_nullable_array = true;
        }

        while (const DataTypeArray * array = checkAndGetDataType<DataTypeArray>(input_type))
        {
            input_type = array->getNestedType().get();
            ++count_arrays;
        }

        if (!isDynamic(*input_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be Variant or Array of Variant. Actual {}",
                            getName(),
                            arguments[0].type->getName());

        auto return_type = makeExtractedSubcolumnsNullableOrLowCardinalityNullableSafe(getRequestedType(arguments[1].column));

        for (; count_arrays; --count_arrays)
            return_type = std::make_shared<DataTypeArray>(return_type);

        if (input_is_nullable_array)
            return makeNullableAllowingArray(return_type);

        return return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        const auto & input_arg = arguments[0];
        const IDataType * input_type = input_arg.type.get();
        const IColumn * input_col = input_arg.column.get();

        bool input_arg_is_const = false;
        if (typeid_cast<const ColumnConst *>(input_col))
        {
            input_col = assert_cast<const ColumnConst *>(input_col)->getDataColumnPtr().get();
            input_arg_is_const = true;
        }

        const bool input_type_is_nullable = input_arg.type->isNullable();
        if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(input_col))
        {
            if (checkAndGetColumn<ColumnArray>(&nullable_array_column->getNestedColumn()))
            {
                ColumnsWithTypeAndName nested_arguments = arguments;
                nested_arguments[0].column = input_arg_is_const
                    ? ColumnConst::create(nullable_array_column->getNestedColumnPtr(), input_rows_count)
                    : nullable_array_column->getNestedColumnPtr();
                nested_arguments[0].type = removeNullable(input_arg.type);

                auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);
                if (!return_type->isNullable())
                    return nested_result;

                auto mutable_nested_result = IColumn::mutate(nested_result->convertToFullColumnIfConst());
                const size_t num_rows = input_arg_is_const ? input_rows_count : mutable_nested_result->size();
                if (mutable_nested_result->size() == 1 && num_rows != 1)
                    mutable_nested_result = mutable_nested_result->cloneResized(num_rows);

                auto null_map = ColumnUInt8::create();
                auto & null_map_data = null_map->getData();
                null_map_data.assign(nullable_array_column->getNullMapData().begin(), nullable_array_column->getNullMapData().end());
                if (null_map_data.size() == 1)
                    null_map_data.resize_fill(num_rows, nullable_array_column->getNullMapData()[0]);
                else if (null_map_data.size() != num_rows)
                    null_map_data.resize_fill(num_rows, 0);

                return ColumnNullable::create(std::move(mutable_nested_result), std::move(null_map));
            }
        }
        else if (input_type_is_nullable)
        {
            ColumnsWithTypeAndName nested_arguments = arguments;
            nested_arguments[0].type = removeNullable(input_arg.type);

            auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);
            if (!return_type->isNullable())
                return nested_result;

            auto null_map = ColumnUInt8::create();
            null_map->getData().resize_fill(nested_result->size(), 0);
            return ColumnNullable::create(nested_result->convertToFullColumnIfConst(), std::move(null_map));
        }

        Columns array_offsets;
        while (const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(input_type))
        {
            const ColumnArray * array_col = assert_cast<const ColumnArray *>(input_col);

            input_type = array_type->getNestedType().get();
            input_col = &array_col->getData();
            array_offsets.push_back(array_col->getOffsetsPtr());
        }

        const ColumnDynamic * input_col_as_dynamic = checkAndGetColumn<ColumnDynamic>(input_col);
        const DataTypeDynamic * input_type_as_dynamic = checkAndGetDataType<DataTypeDynamic>(input_type);
        if (!input_col_as_dynamic || !input_type_as_dynamic)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be Dynamic or array of Dynamics. Actual {}", getName(), input_arg.type->getName());

        auto type = getRequestedType(arguments[1].column);
        auto subcolumn = input_type_as_dynamic->getSubcolumn(type->getName(), input_col_as_dynamic->getPtr());
        return wrapInArraysAndConstIfNeeded(std::move(subcolumn), array_offsets, input_arg_is_const, input_rows_count);
    }

private:
    DataTypePtr getRequestedType(const ColumnPtr & type_name_column) const
    {
        const auto * name_col = checkAndGetColumnConst<ColumnString>(type_name_column.get());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument of {} must be a constant String", getName());

        String element_type_name = name_col->getValue<String>();
        auto element_type = DataTypeFactory::instance().tryGet(element_type_name);
        if (!element_type)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument of {} must be a valid type name. Got: {}", getName(), element_type_name);

        if (hasNullableArray(element_type) && !allow_nullable_array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} cannot use Nullable(Array) type while setting allow_experimental_nullable_array_type is disabled",
                getName());

        return element_type;
    }

    ColumnPtr wrapInArraysAndConstIfNeeded(ColumnPtr res, const Columns & array_offsets, bool input_arg_is_const, size_t input_rows_count) const
    {
        for (auto it = array_offsets.rbegin(); it != array_offsets.rend(); ++it)
            res = ColumnArray::create(res, *it);

        if (input_arg_is_const)
            res = ColumnConst::create(res, input_rows_count);

        return res;
    }

    bool allow_nullable_array_type;
};

}

REGISTER_FUNCTION(DynamicElement)
{
    FunctionDocumentation::Description description = R"(
Extracts a column with specified type from a `Dynamic` column.

This function allows you to extract values of a specific type from a Dynamic column. If a row contains a value
of the requested type, it returns that value. If the row contains a different type or NULL, it returns NULL
for scalar types or an empty array for array types.
    )";
    FunctionDocumentation::Syntax syntax = "dynamicElement(dynamic, type_name)";
    FunctionDocumentation::Arguments arguments =
    {
        {"dynamic", "Dynamic column to extract from.", {"Dynamic"}},
        {"type_name", "The name of the variant type to extract (e.g., 'String', 'Int64', 'Array(Int64)')."}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {
        "Returns values of the specified type from the Dynamic column. Returns NULL for non-matching types (or empty array for array types).",
        {"Any"}
    };
    FunctionDocumentation::Examples examples =
    {
    {
        "Extracting different types from Dynamic column",
        R"(
CREATE TABLE test (d Dynamic) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT d, dynamicType(d), dynamicElement(d, 'String'), dynamicElement(d, 'Int64'), dynamicElement(d, 'Array(Int64)'), dynamicElement(d, 'Date'), dynamicElement(d, 'Array(String)') FROM test
        )",
        R"(
┌─d─────────────┬─dynamicType(d)─┬─dynamicElement(d, 'String')─┬─dynamicElement(d, 'Int64')─┬─dynamicElement(d, 'Array(Int64)')─┬─dynamicElement(d, 'Date')─┬─dynamicElement(d, 'Array(String)')─┐
│ ᴺᵁᴸᴸ          │ None           │ ᴺᵁᴸᴸ                        │                       ᴺᵁᴸᴸ │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ 42            │ Int64          │ ᴺᵁᴸᴸ                        │                         42 │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ Hello, World! │ String         │ Hello, World!               │                       ᴺᵁᴸᴸ │ []                                │                      ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ Array(Int64)   │ ᴺᵁᴸᴸ                        │                       ᴺᵁᴸᴸ │ [1,2,3]                           │                      ᴺᵁᴸᴸ │ []                                 │
└───────────────┴────────────────┴─────────────────────────────┴────────────────────────────┴───────────────────────────────────┴───────────────────────────┴────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDynamicElement>(documentation);
}

}
