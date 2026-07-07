#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/iota.h>
#include <Common/randomSeed.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Poco/Logger.h>
#include <numeric>
#include <pcg_random.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/// arrayRandomSample(arr, k) - Returns k random elements from the input array
class FunctionArrayRandomSample : public IFunction
{
public:
    static constexpr auto name = "arrayRandomSample";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayRandomSample>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"samples", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), isColumnConst, "const UInt*"},
        };
        validateFunctionArguments(*this, arguments, args);

        // Return an array with the same nested type as the input array
        const DataTypePtr & array_type = arguments[0].type;
        const DataTypeArray * array_data_type = checkAndGetDataType<DataTypeArray>(array_type.get());
        const DataTypePtr & nested_type = array_data_type->getNestedType();
        return std::make_shared<DataTypeArray>(nested_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", getName());

        const IColumn * col_samples = arguments[1].column.get();
        if (!col_samples)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "The second argument of function {} is empty or null, type = {}",
                    getName(), arguments[1].type->getName());

        const size_t samples = col_samples->getUInt(0);

        pcg64_fast rng(randomSeed());

        auto col_res_data = col_array->getDataPtr()->cloneEmpty();
        auto col_res_offsets = ColumnUInt64::create(input_rows_count);
        auto col_res = ColumnArray::create(std::move(col_res_data), std::move(col_res_offsets));

        const auto & array_offsets = col_array->getOffsets();
        auto & res_offsets = col_res->getOffsets();

        std::vector<size_t> indices;
        size_t prev_array_offset = 0;
        size_t prev_res_offset = 0;

        for (size_t row = 0; row < input_rows_count; row++)
        {
            const size_t num_elements = array_offsets[row] - prev_array_offset;
            const size_t cur_samples = std::min(num_elements, samples);

            indices.resize(num_elements);
            iota(indices.data(), indices.size(), prev_array_offset);
            std::shuffle(indices.begin(), indices.end(), rng);

            for (UInt64 i = 0; i < cur_samples; i++)
                col_res->getData().insertFrom(col_array->getData(), indices[i]);

            res_offsets[row] = prev_res_offset + cur_samples;

            prev_array_offset += num_elements;
            prev_res_offset += cur_samples;
            indices.clear();
        }

        return col_res;
    }
};

REGISTER_FUNCTION(ArrayRandomSample)
{
    FunctionDocumentation::Description description = "Returns a subset with `samples`-many random elements of an input array. If `samples` exceeds the size of the input array, the sample size is limited to the size of the array, i.e. all array elements are returned but their order is not guaranteed. The function can handle both flat arrays and nested arrays.";
    FunctionDocumentation::Syntax syntax = "arrayRandomSample(arr, samples)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The input array or multidimensional array from which to sample elements.", {"Array(T)"}},
        {"samples", "The number of elements to include in the random sample.", {"(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array containing a random sample of elements from the input array", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT arrayRandomSample(['apple', 'banana', 'cherry', 'date'], 2) as res;", "['cherry','apple']"},
        {"Using a multidimensional array", "SELECT arrayRandomSample([[1, 2], [3, 4], [5, 6]], 2) as res;", "[[3,4],[5,6]]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayRandomSample>(documentation);
}

}
