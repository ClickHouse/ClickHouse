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
                outer_offsets.push_back(outer_pos);
                continue;
            }

            /// Generate k-permutations of n elements using index arrays.
            /// For full permutations (k==n), use std::next_permutation.
            /// For partial permutations, use recursive generation.
            std::vector<size_t> indices(n);
            std::iota(indices.begin(), indices.end(), 0);

            if (k == n)
            {
                /// Full permutations via std::next_permutation (indices already sorted)
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
                /// Partial permutations: generate all k-length ordered selections
                generatePartialPermutations(arr_values, arr_begin, indices, k, 0,
                    *col_res_data, inner_offsets, inner_pos, outer_pos);
            }

            outer_offsets.push_back(outer_pos);
        }

        return ColumnArray::create(
            ColumnArray::create(std::move(col_res_data), std::move(col_res_inner_offsets)),
            std::move(col_res_outer_offsets));
    }

private:
    static void generatePartialPermutations(
        const IColumn & arr_values, size_t arr_begin,
        std::vector<size_t> & indices, size_t k, size_t depth,
        IColumn & res_data, IColumn::Offsets & inner_offsets, size_t & inner_pos, size_t & outer_pos)
    {
        if (depth == k)
        {
            for (size_t i = 0; i < k; ++i)
                res_data.insertFrom(arr_values, arr_begin + indices[i]);
            inner_pos += k;
            inner_offsets.push_back(inner_pos);
            ++outer_pos;
            return;
        }

        for (size_t i = depth; i < indices.size(); ++i)
        {
            std::swap(indices[depth], indices[i]);
            generatePartialPermutations(arr_values, arr_begin, indices, k, depth + 1,
                res_data, inner_offsets, inner_pos, outer_pos);
            std::swap(indices[depth], indices[i]);
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
