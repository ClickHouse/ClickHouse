#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <numeric>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

static constexpr size_t MAX_COMBINATION_RESULT_ELEMENTS = 1000000;

/// Compute C(n, k) exactly, return 0 if result > limit.
/// Uses GCD reduction to keep intermediates small and avoid overflow.
static size_t combinationCountCapped(size_t n, size_t k, size_t limit)
{
    if (k > n) return 0;
    if (k == 0 || k == n) return 1;
    if (k > n - k) k = n - k;

    /// Store numerator factors to allow cross-cancellation with denominators.
    std::vector<size_t> numer(k);
    for (size_t i = 0; i < k; ++i)
        numer[i] = n - i;

    for (size_t i = 2; i <= k; ++i)
    {
        size_t d = i;
        for (size_t j = 0; j < k && d > 1; ++j)
        {
            size_t g = std::gcd(numer[j], d);
            numer[j] /= g;
            d /= g;
        }
    }

    size_t result = 1;
    for (size_t i = 0; i < k; ++i)
    {
        if (numer[i] > 1 && result > limit / numer[i])
            return 0;
        result *= numer[i];
    }
    return result <= limit ? result : 0;
}

class FunctionArrayCombinations : public IFunction
{
public:
    static constexpr auto name = "arrayCombinations";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayCombinations>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"k", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"}
        };
        validateFunctionArguments(*this, arguments, args);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(array_type->getNestedType()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected array column for function {}", getName());

        const ColumnPtr & col_k = arguments[1].column;
        const auto & arr_offsets = col_array->getOffsets();
        const auto & arr_values = col_array->getData();

        auto col_res_data = arr_values.cloneEmpty();
        auto col_res_inner_offsets = ColumnArray::ColumnOffsets::create();
        auto col_res_outer_offsets = ColumnArray::ColumnOffsets::create();
        IColumn::Offsets & inner_offsets = col_res_inner_offsets->getData();
        IColumn::Offsets & outer_offsets = col_res_outer_offsets->getData();

        size_t inner_pos = 0;
        size_t outer_pos = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const Int64 k = col_k->getInt(row);
            const size_t arr_begin = arr_offsets[row - 1];
            const size_t n = arr_offsets[row] - arr_begin;

            if (k < 0 || static_cast<size_t>(k) > n)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Second argument of function {} must be between 0 and the array length ({}), got {}",
                    getName(), n, k);

            if (k == 0)
            {
                /// C(n,0) = 1: one result — the empty selection
                inner_offsets.push_back(inner_pos);
                ++outer_pos;
                outer_offsets.push_back(outer_pos);
                continue;
            }

            /// Check output size to prevent OOM
            size_t num_results = combinationCountCapped(n, static_cast<size_t>(k), MAX_COMBINATION_RESULT_ELEMENTS);
            if (num_results == 0)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Result of function {} would exceed {} elements for array of length {} with k={}",
                    getName(), MAX_COMBINATION_RESULT_ELEMENTS, n, k);

            /// Generate combinations using iterative approach with index array
            std::vector<size_t> indices(k);
            for (Int64 i = 0; i < k; ++i)
                indices[i] = i;

            while (true)
            {
                for (Int64 i = 0; i < k; ++i)
                    col_res_data->insertFrom(arr_values, arr_begin + indices[i]);
                inner_pos += k;
                inner_offsets.push_back(inner_pos);
                ++outer_pos;

                /// Advance to next combination
                Int64 i = k - 1;
                while (i >= 0 && indices[i] == n - k + static_cast<size_t>(i))
                    --i;
                if (i < 0)
                    break;
                ++indices[i];
                for (Int64 j = i + 1; j < k; ++j)
                    indices[j] = indices[j - 1] + 1;
            }

            outer_offsets.push_back(outer_pos);
        }

        return ColumnArray::create(
            ColumnArray::create(std::move(col_res_data), std::move(col_res_inner_offsets)),
            std::move(col_res_outer_offsets));
    }
};

REGISTER_FUNCTION(ArrayCombinations)
{
    FunctionDocumentation::Description description = "Returns all combinations of k elements from the input array. The order of elements inside each combination matches the original array order.";
    FunctionDocumentation::Syntax syntax = "arrayCombinations(arr, k)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The input array.", {"Array(T)"}},
        {"k", "The number of elements in each combination.", {"(U)Int*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array of arrays, where each inner array is a k-length combination.", {"Array(Array(T))"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayCombinations([1, 2, 3], 2)", "[[1,2],[1,3],[2,3]]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayCombinations>(documentation);
}

}
