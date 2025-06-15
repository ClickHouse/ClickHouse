#include <Functions/reverse.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionAdaptors.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
class FunctionReverse : public IFunction
{
public:
    static constexpr auto name = "reverse";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReverse>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]) && !isArray(arguments[0]) && !isTuple(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        if (isTuple(arguments[0]))
        {
            const auto & data_type_tuple = checkAndGetDataType<DataTypeTuple>(*arguments[0]);
            const auto & original_elements = data_type_tuple.getElements();
            const size_t element_count = original_elements.size();

            DataTypes reversed_types;
            reversed_types.reserve(element_count);
            reversed_types.assign(original_elements.rbegin(), original_elements.rend());

            if (data_type_tuple.haveExplicitNames())
            {
                const auto & original_names = data_type_tuple.getElementNames();
                Names reversed_names;
                reversed_names.reserve(element_count);
                reversed_names.assign(original_names.rbegin(), original_names.rend());
                return std::make_shared<DataTypeTuple>(reversed_types, reversed_names);
            }

            return std::make_shared<DataTypeTuple>(reversed_types);
        }

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            ReverseImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), input_rows_count);
            return col_res;
        }
        if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());
            ReverseImpl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), col_res->getChars(), input_rows_count);
            return col_res;
        }
        if (const ColumnTuple * col_tuple = checkAndGetColumn<ColumnTuple>(column.get()))
        {
            size_t tuple_size = col_tuple->tupleSize();
            Columns tuple_columns(tuple_size);
            for (size_t i = 0; i < tuple_size; ++i)
            {
                tuple_columns[i] = col_tuple->getColumnPtr(tuple_size - i - 1);
            }
            return ColumnTuple::create(tuple_columns);
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};


/// Also works with arrays.
class ReverseOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "reverse";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<ReverseOverloadResolver>(context); }

    explicit ReverseOverloadResolver(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (isArray(arguments.at(0).type))
            return FunctionFactory::instance().getImpl("arrayReverse", context)->build(arguments);
        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            FunctionReverse::create(context),
            DataTypes{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })},
            return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return FunctionReverse{}.getReturnTypeImpl(arguments); }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(Reverse)
{
    FunctionDocumentation::Description description = "Reverses the order of the elements in the input array or the characters in the input string.";
    FunctionDocumentation::Syntax syntax = "reverse(arr | str)";
    FunctionDocumentation::Arguments arguments = {
        {"arr | str", "The source array or string. [`Array(T)`](/sql-reference/data-types/array), [`String`](/sql-reference/data-types/string)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "Returns an array or string with the order of elements or characters reversed.";
    FunctionDocumentation::Examples examples = {
        {"Reverse array", "SELECT reverse([1, 2, 3, 4]);", "[4, 3, 2, 1]"},
        {"Reverse string", "SELECT reverse('abcd');", "'dcba'"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = FunctionDocumentation::VERSION_UNKNOWN;
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<ReverseOverloadResolver>(documentation, FunctionFactory::Case::Insensitive);
}

}
