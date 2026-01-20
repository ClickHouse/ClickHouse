#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/EmptyImpl.h>
#include <Columns/ColumnObject.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct NameEmpty
{
    static constexpr auto name = "empty";
};

struct NameNotEmpty
{
    static constexpr auto name = "notEmpty";
};

/// Implements the empty function for JSON type.
template <bool negative, class Name>
class ExecutableFunctionJSONEmpty : public IExecutableFunction
{
public:
    std::string getName() const override { return Name::name; }

private:
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        const auto * object_column = typeid_cast<const ColumnObject *>(elem.column.get());
        if (!object_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column type in function {}. Expected Object column, got {}", getName(), elem.column->getName());

        auto res = DataTypeUInt8().createColumn();
        auto & data = typeid_cast<ColumnUInt8 &>(*res).getData();
        const auto & typed_paths = object_column->getTypedPaths();
        size_t size = object_column->size();
        /// If object column has at least 1 typed path, it will never be empty, because these paths always have values.
        if (!typed_paths.empty())
        {
            data.resize_fill(size, negative);
            return res;
        }

        const auto & dynamic_paths = object_column->getDynamicPaths();
        const auto & shared_data = object_column->getSharedDataPtr();
        data.reserve(size);
        for (size_t i = 0; i != size; ++i)
        {
            bool empty = true;
            /// Check if there is no paths in shared data.
            if (!shared_data->isDefaultAt(i))
            {
                empty = false;
            }
            /// Check that all dynamic paths have NULL value in this row.
            else
            {
                for (const auto & [path, column] : dynamic_paths)
                {
                    if (!column->isNullAt(i))
                    {
                        empty = false;
                        break;
                    }
                }
            }

            data.push_back(negative ^ empty);
        }

        return res;
    }
};

template <bool negative, class Name>
class FunctionEmptyJSON final : public IFunctionBase
{
public:
    FunctionEmptyJSON(const DataTypes & argument_types_, const DataTypePtr & return_type_) : argument_types(argument_types_), return_type(return_type_) {}

    String getName() const override { return Name::name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionJSONEmpty<negative, Name>>();
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};

template <bool negative, class Name>
class FunctionEmptyOverloadResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = Name::name;

    static FunctionOverloadResolverPtr create(ContextPtr)
    {
        return std::make_unique<FunctionEmptyOverloadResolver>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & arg : arguments)
            argument_types.push_back(arg.type);

        if (argument_types.size() == 1 && isObject(argument_types[0]))
            return std::make_shared<FunctionEmptyJSON<negative, Name>>(argument_types, return_type);

        return std::make_shared<FunctionToFunctionBaseAdaptor>(std::make_shared<FunctionStringOrArrayToT<EmptyImpl<negative>, Name, UInt8, false>>(), argument_types, return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0])
            && !isArray(arguments[0])
            && !isMap(arguments[0])
            && !isUUID(arguments[0])
            && !isIPv6(arguments[0])
            && !isIPv4(arguments[0])
            && !isObject(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }
};

}

REGISTER_FUNCTION(Empty)
{
    FunctionDocumentation::Description description_empty = R"(
Checks whether the input array is empty.

An array is considered empty if it does not contain any elements.

:::note
Can be optimized by enabling the [`optimize_functions_to_subcolumns` setting](/operations/settings/settings#optimize_functions_to_subcolumns). With `optimize_functions_to_subcolumns = 1` the function reads only [size0](/sql-reference/data-types/array#array-size) subcolumn instead of reading and processing the whole array column. The query `SELECT empty(arr) FROM TABLE;` transforms to `SELECT arr.size0 = 0 FROM TABLE;`.
:::

The function also works for Strings or UUIDs.
    )";
    FunctionDocumentation::Description description_not_empty = R"(
Checks whether the input array is non-empty.

An array is considered non-empty if it contains at least one element.

:::note
Can be optimized by enabling the [`optimize_functions_to_subcolumns`](/operations/settings/settings#optimize_functions_to_subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [size0](/sql-reference/data-types/array#array-size) subcolumn instead of reading and processing the whole array column. The query `SELECT notEmpty(arr) FROM table` transforms to `SELECT arr.size0 != 0 FROM TABLE`.
:::

The function also works for Strings or UUIDs.
    )";
    FunctionDocumentation::Syntax syntax_empty = "empty(arr)";
    FunctionDocumentation::Syntax syntax_not_empty = "notEmpty(arr)";
    FunctionDocumentation::Arguments arguments = {{"arr", "Input array.", {"Array(T)"}}};
    FunctionDocumentation::ReturnedValue returned_value_empty = {"Returns `1` for an empty array or `0` for a non-empty array", {"UInt8"}};
    FunctionDocumentation::ReturnedValue returned_value_not_empty = {"Returns `1` for a non-empty array or `0` for an empty array", {"UInt8"}};    FunctionDocumentation::Examples examples_empty = {{"Usage example", "SELECT empty([]);", "1"}};
    FunctionDocumentation::Examples examples_not_empty = {{"Usage example", "SELECT notEmpty([1,2]);", "1"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_empty = {description_empty, syntax_empty, arguments, returned_value_empty, examples_empty, introduced_in, category};
    FunctionDocumentation documentation_not_empty = {description_not_empty, syntax_not_empty, arguments, returned_value_not_empty, examples_not_empty, introduced_in, category};

    // String function documentation
    FunctionDocumentation::Description description_empty_string = R"(
Checks whether the input string is empty.
A string is considered non-empty if it contains at least one byte, even if this byte is a space or the null byte.
The function is also available for [arrays](/sql-reference/functions/array-functions#empty) and [UUIDs](/sql-reference/data-types/uuid).
)";
    FunctionDocumentation::Description description_not_empty_string = R"(
Checks whether the input string is non-empty.
A string is considered non-empty if it contains at least one byte, even if this byte is a space or the null byte.
The function is also available for [arrays](/sql-reference/functions/array-functions#empty) and [UUIDs](/sql-reference/data-types/uuid).
)";
    FunctionDocumentation::Syntax syntax_empty_string = "empty(x)";
    FunctionDocumentation::Syntax syntax_not_empty_string = "notEmpty(x)";
    FunctionDocumentation::Arguments arguments_string = {{"x", "Input value.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_empty_string = {"Returns `1` for an empty string or `0` for a non-empty string.", {"UInt8"}};
    FunctionDocumentation::ReturnedValue returned_value_not_empty_string = {"Returns `1` for a non-empty string or `0` for an empty string.", {"UInt8"}};
    FunctionDocumentation::Examples examples_empty_string = {
    {
        "Basic usage",
        "SELECT empty('')",
        R"(
┌─empty('')─┐
│         1 │
└───────────┘
        )"
    }
    };
    FunctionDocumentation::Examples examples_not_empty_string = {
    {
        "Basic usage",
        "SELECT notEmpty('text')",
        R"(
┌─notEmpty('text')─┐
│                1 │
└──────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category category_string = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation_empty_string = {description_empty_string, syntax_empty_string, arguments_string, returned_value_empty_string, examples_empty_string, introduced_in, category_string};
    FunctionDocumentation documentation_not_empty_string = {description_not_empty_string, syntax_not_empty_string, arguments_string, returned_value_not_empty_string, examples_not_empty_string, introduced_in, category_string};

    factory.registerFunction<FunctionEmptyOverloadResolver<true, NameNotEmpty>>(documentation_not_empty);
    factory.registerFunction<FunctionEmptyOverloadResolver<false, NameEmpty>>(documentation_empty);

}

}

