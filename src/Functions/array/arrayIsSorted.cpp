#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionArrayIsSorted final : public IFunction
{
public:
    static constexpr auto name = "arrayIsSorted";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayIsSorted>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isArray(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}, expected Array",
                arguments[0]->getName(),
                getName());

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!array)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments[0].column->getName(),
                getName());

        const auto & offsets = array->getOffsets();
        const auto & data = array->getData();

        auto result = ColumnUInt8::create(offsets.size());
        auto & result_data = result->getData();

        size_t begin = 0;
        for (size_t row = 0; row < offsets.size(); ++row)
        {
            const size_t end = offsets[row];
            UInt8 is_sorted = 1;

            for (size_t i = begin; i + 1 < end; ++i)
            {
                if (data.compareAt(i, i + 1, data, 1) > 0)
                {
                    is_sorted = 0;
                    break;
                }
            }

            result_data[row] = is_sorted;
            begin = end;
        }

        return result;
    }
};

REGISTER_FUNCTION(ArrayIsSorted)
{
    FunctionDocumentation::Description description = R"(
Checks whether the elements of an array are sorted in non-decreasing order.
The comparison order is the same as for `arraySort`.
    )";
    FunctionDocumentation::Syntax syntax = "arrayIsSorted(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The array to check.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns `1` if the elements are sorted in non-decreasing order, otherwise `0`.",
        {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Sorted array", "SELECT arrayIsSorted([1, 2, 2, 3]);", "1"},
        {"Unsorted array", "SELECT arrayIsSorted([1, 3, 2]);", "0"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {
        description,
        syntax,
        arguments,
        {},
        returned_value,
        examples,
        introduced_in,
        category};

    factory.registerFunction<FunctionArrayIsSorted>(documentation);
}

}
