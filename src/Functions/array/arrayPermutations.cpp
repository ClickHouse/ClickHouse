#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <algorithm>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

static constexpr size_t MAX_PERMUTATION_RESULT_ELEMENTS = 1000000;

/// Compute n! but return 0 on overflow or if result > limit
static size_t factorialCapped(size_t n, size_t limit)
{
    size_t result = 1;
    for (size_t i = 2; i <= n; ++i)
    {
        if (result > limit / i)
            return 0;
        result *= i;
    }
    return result <= limit ? result : 0;
}

/// Compute P(n, k) = n! / (n-k)! but return 0 on overflow or if result > limit
static size_t partialPermCountCapped(size_t n, size_t k, size_t limit)
{
    size_t result = 1;
    for (size_t i = 0; i < k; ++i)
    {
        if (result > limit / (n - i))
            return 0;
        result *= (n - i);
    }
    return result <= limit ? result : 0;
}

/// arrayPermutations(arr) — all permutations of the full array
/// arrayPartialPermutations(arr, k) — all k-length ordered selections
template <bool IsPartial>
class FunctionArrayPermutationsImpl : public IFunction
{
public:
    static constexpr auto name = IsPartial ? "arrayPartialPermutations" : "arrayPermutations";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPermutationsImpl>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return IsPartial ? 2 : 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (IsPartial)
        {
            FunctionArgumentDescriptors args{
                {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
                {"k", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isInteger), nullptr, "Integer"}
            };
            validateFunctionArguments(*this, arguments, args);
        }
        else
        {
            FunctionArgumentDescriptors args{
                {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            };
            validateFunctionArguments(*this, arguments, args);
        }

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(array_type->getNestedType()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected array column for function {}", getName());

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
            const size_t arr_begin = arr_offsets[row - 1];
            const size_t n = arr_offsets[row] - arr_begin;

            size_t k = n;
            if constexpr (IsPartial)
            {
                const Int64 k_arg = arguments[1].column->getInt(row);
                if (k_arg < 0 || static_cast<size_t>(k_arg) > n)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Second argument of function {} must be between 0 and the array length ({}), got {}",
                        getName(), n, k_arg);
                k = static_cast<size_t>(k_arg);
            }

            if (k == 0)
            {
                /// C(n,0) = P(n,0) = 1: one result — the empty selection
                inner_offsets.push_back(inner_pos);
                ++outer_pos;
                outer_offsets.push_back(outer_pos);
                continue;
            }

            /// Check total output elements (rows * k) to prevent OOM
            size_t max_rows = MAX_PERMUTATION_RESULT_ELEMENTS / k;
            size_t num_results = IsPartial || k < n
                ? partialPermCountCapped(n, k, max_rows)
                : factorialCapped(n, max_rows);

            if (num_results == 0)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Result of function {} would exceed {} total elements for array of length {} with k={}",
                    getName(), MAX_PERMUTATION_RESULT_ELEMENTS, n, k);

            /// Generate k-permutations of n elements using index arrays.
            std::vector<size_t> indices(n); // STYLE_CHECK_ALLOW_STD_CONTAINERS
            std::iota(indices.begin(), indices.end(), 0);

            if (k == n)
            {
                /// Full permutations via std::next_permutation (lexicographic order)
                do
                {
                    for (size_t i = 0; i < k; ++i)
                        col_res_data->insertFrom(arr_values, arr_begin + indices[i]);
                    inner_pos += k;
                    inner_offsets.push_back(inner_pos);
                    ++outer_pos;
                } while (std::next_permutation(indices.begin(), indices.end()));
            }
            else
            {
                /// Partial permutations in lexicographic order using used-flags approach
                std::vector<bool> used(n, false); // STYLE_CHECK_ALLOW_STD_CONTAINERS
                std::vector<size_t> current(k); // STYLE_CHECK_ALLOW_STD_CONTAINERS
                generatePartialPermutationsLex(arr_values, arr_begin, n, k, 0, used, current,
                    *col_res_data, inner_offsets, inner_pos, outer_pos);
            }

            outer_offsets.push_back(outer_pos);
        }

        return ColumnArray::create(
            ColumnArray::create(std::move(col_res_data), std::move(col_res_inner_offsets)),
            std::move(col_res_outer_offsets));
    }

private:
    static void generatePartialPermutationsLex(
        const IColumn & arr_values, size_t arr_begin, size_t n, size_t k, size_t depth,
        std::vector<bool> & used, std::vector<size_t> & current, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        IColumn & res_data, IColumn::Offsets & inner_offsets, size_t & inner_pos, size_t & outer_pos)
    {
        if (depth == k)
        {
            for (size_t i = 0; i < k; ++i)
                res_data.insertFrom(arr_values, arr_begin + current[i]);
            inner_pos += k;
            inner_offsets.push_back(inner_pos);
            ++outer_pos;
            return;
        }

        for (size_t i = 0; i < n; ++i)
        {
            if (!used[i])
            {
                used[i] = true;
                current[depth] = i;
                generatePartialPermutationsLex(arr_values, arr_begin, n, k, depth + 1, used, current,
                    res_data, inner_offsets, inner_pos, outer_pos);
                used[i] = false;
            }
        }
    }
};

using FunctionArrayPermutations = FunctionArrayPermutationsImpl<false>;
using FunctionArrayPartialPermutations = FunctionArrayPermutationsImpl<true>;

REGISTER_FUNCTION(ArrayPermutations)
{
    FunctionDocumentation::Description description = "Returns all permutations of the input array.";
    FunctionDocumentation::Syntax syntax = "arrayPermutations(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The input array.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array of arrays containing all permutations.", {"Array(Array(T))"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayPermutations([1, 2, 3])", "[[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPermutations>(documentation);
}

REGISTER_FUNCTION(ArrayPartialPermutations)
{
    FunctionDocumentation::Description description = "Returns all k-length partial permutations (ordered selections) of the input array.";
    FunctionDocumentation::Syntax syntax = "arrayPartialPermutations(arr, k)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The input array.", {"Array(T)"}},
        {"k", "The number of elements to select.", {"(U)Int*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array of arrays containing all k-length ordered selections.", {"Array(Array(T))"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayPartialPermutations([1, 2, 3], 2)", "[[1,2],[1,3],[2,1],[2,3],[3,1],[3,2]]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayPartialPermutations>(documentation);
}

}
