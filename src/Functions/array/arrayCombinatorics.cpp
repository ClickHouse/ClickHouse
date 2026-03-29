#include <algorithm>
#include <memory>
#include <numeric>
#include <vector>

#include <Columns/ColumnArray.h>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/FunctionDocumentation.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 function_range_max_elements_in_block;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

constexpr auto native_integer_argument_type = "(U)Int8/16/32/64";

struct NameArrayPermutations
{
    static constexpr auto name = "arrayPermutations";
};

struct NameArrayPartialPermutations
{
    static constexpr auto name = "arrayPartialPermutations";
};

struct NameArrayCombinations
{
    static constexpr auto name = "arrayCombinations";
};

enum class ArrayKCombinatoricsMode
{
    PartialPermutations,
    Combinations,
};

/// Gets ColumnArray and materializes const columns when needed.
const ColumnArray * getArrayColumn(const ColumnPtr & column, ColumnPtr & storage)
{
    if (const auto * col = checkAndGetColumn<ColumnArray>(column.get()))
        return col;

    storage = column->convertToFullColumnIfConst();
    return checkAndGetColumn<ColumnArray>(storage.get());
}

/// Builds `Array(Array(T))` and owns all offsets/state needed to append rows.
struct ArrayResultBuilder
{
    explicit ArrayResultBuilder(const IColumn & array_data, size_t input_rows_count)
        : result_data(array_data.cloneEmpty())
        , result_inner_offsets(ColumnArray::ColumnOffsets::create())
        , result_outer_offsets(ColumnArray::ColumnOffsets::create())
    {
        result_outer_offsets->getData().reserve(input_rows_count);
    }

    size_t getSourceRowBegin() const { return previous_array_offset; }

    size_t getTotalGeneratedElements() const { return total_generated_elements; }

    size_t getRemainingElementBudget(size_t max_elements) const
    {
        chassert(total_generated_elements <= max_elements);
        return max_elements - total_generated_elements;
    }

    void addGeneratedElements(size_t generated_elements) { total_generated_elements += generated_elements; }

    void appendEmptyRow(size_t current_array_offset)
    {
        // Append exactly one empty variant for the current row: `[[]]`.
        appendEmptyVariant();
        finalizeRow();
        previous_array_offset = current_array_offset;
    }

    void finishSourceRow(size_t current_array_offset)
    {
        finalizeRow();
        previous_array_offset = current_array_offset;
    }

    void appendVariant(const IColumn & source_data, size_t source_row_begin, const std::vector<size_t> & indexes)
    {
        for (size_t idx : indexes)
            result_data->insertFrom(source_data, source_row_begin + idx);

        current_result_data_offset += indexes.size();
        result_inner_offsets->getData().push_back(current_result_data_offset);
        ++current_result_array_offset;
    }

    ColumnPtr build()
    {
        return ColumnArray::create(
            ColumnArray::create(std::move(result_data), std::move(result_inner_offsets)), std::move(result_outer_offsets));
    }

private:
    void appendEmptyVariant()
    {
        result_inner_offsets->getData().push_back(current_result_data_offset);
        ++current_result_array_offset;
    }

    void finalizeRow() { result_outer_offsets->getData().push_back(current_result_array_offset); }

    MutableColumnPtr result_data;
    ColumnArray::ColumnOffsets::MutablePtr result_inner_offsets;
    ColumnArray::ColumnOffsets::MutablePtr result_outer_offsets;

    size_t previous_array_offset = 0;
    size_t current_result_data_offset = 0;
    size_t current_result_array_offset = 0;
    size_t total_generated_elements = 0;
};

/// Returns true if n! fits into count_limit and writes it to result.
bool tryCalculatePermutationsCount(size_t n, size_t count_limit, size_t & result)
{
    result = 1;
    for (size_t i = 2; i <= n; ++i)
    {
        if (result > count_limit / i)
            return false;
        result *= i;
    }
    return true;
}

/// Returns true if P(n, k) / C(n, k) fits into count_limit and writes it to result.
template <ArrayKCombinatoricsMode mode>
bool tryCalculateKResultCount(size_t n, size_t k, size_t count_limit, size_t & result)
{
    chassert(k <= n);
    chassert(k > 0);

    if constexpr (mode == ArrayKCombinatoricsMode::PartialPermutations)
    {
        result = 1;
        for (size_t i = 0; i < k; ++i)
        {
            const size_t factor = n - i;
            if (result > count_limit / factor)
                return false;
            result *= factor;
        }
        return true;
    }
    else
    {
        if (k == n)
        {
            result = 1;
            return true;
        }

        k = std::min(k, n - k);

        result = 1;
        for (size_t i = 0; i < k; ++i)
        {
            // Compute the next binomial coefficient in 128 bits to avoid intermediate overflow.
            const auto next
                = static_cast<unsigned __int128>(result) * static_cast<unsigned __int128>(n - i) / static_cast<unsigned __int128>(i + 1);

            if (next > static_cast<unsigned __int128>(count_limit))
                return false;

            result = static_cast<size_t>(next);
        }
        return true;
    }
}

class FunctionArrayPermutations : public IFunction
{
public:
    static constexpr auto name = NameArrayPermutations::name;

    explicit FunctionArrayPermutations(ContextPtr context_)
        : max_elements(context_->getSettingsRef()[Setting::function_range_max_elements_in_block])
    {
    }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayPermutations>(std::move(context_)); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
        };
        validateFunctionArguments(*this, arguments, args);

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(array_type->getNestedType()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr storage;
        const ColumnArray * column_array = getArrayColumn(arguments[0].column, storage);

        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", getName());

        const auto & array_offsets = column_array->getOffsets();
        const auto & array_data = column_array->getData();

        ArrayResultBuilder builder(array_data, input_rows_count);
        std::vector<size_t> permutation_indexes;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t source_row_begin = builder.getSourceRowBegin();
            const size_t current_array_offset = array_offsets[row];
            const size_t n = current_array_offset - source_row_begin;

            if (n == 0)
            {
                builder.appendEmptyRow(current_array_offset);
                continue;
            }

            const size_t remaining = builder.getRemainingElementBudget(max_elements);
            const size_t count_limit = remaining / n;
            size_t row_result_count = 0;
            const bool fits_row_limit = tryCalculatePermutationsCount(n, count_limit, row_result_count);

            if (!fits_row_limit || row_result_count > count_limit)
                throw Exception(
                    ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Result of function {} is too large in row {}: more than {} "
                    "arrays of size {} would be generated, "
                    "maximum remaining elements in block: {}",
                    getName(),
                    row,
                    count_limit,
                    n,
                    remaining);

            builder.addGeneratedElements(row_result_count * n);

            permutation_indexes.resize(n);
            std::iota(permutation_indexes.begin(), permutation_indexes.end(), 0);

            do
            {
                builder.appendVariant(array_data, source_row_begin, permutation_indexes);
            } while (std::next_permutation(permutation_indexes.begin(), permutation_indexes.end()));

            builder.finishSourceRow(current_array_offset);
        }

        return builder.build();
    }

private:
    const size_t max_elements;
};

template <ArrayKCombinatoricsMode mode, typename Name>
class FunctionArrayKCombinatorics : public IFunction
{
public:
    static constexpr auto name = Name::name;

    explicit FunctionArrayKCombinatorics(ContextPtr context_)
        : max_elements(context_->getSettingsRef()[Setting::function_range_max_elements_in_block])
    {
    }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayKCombinatorics>(std::move(context_)); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"k", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), nullptr, native_integer_argument_type},
        };
        validateFunctionArguments(*this, arguments, args);

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(array_type->getNestedType()));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr storage;
        const ColumnArray * column_array = getArrayColumn(arguments[0].column, storage);

        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be an array", getName());

        const WhichDataType which_k(arguments[1].type);
        chassert(which_k.isNativeInteger());

        const IColumn * column_k = arguments[1].column.get();
        const auto & array_offsets = column_array->getOffsets();
        const auto & array_data = column_array->getData();
        const bool k_is_unsigned = which_k.isNativeUInt();

        ArrayResultBuilder builder(array_data, input_rows_count);
        std::vector<size_t> partial_indexes;
        std::vector<size_t> partial_next_candidate;
        std::vector<UInt8> partial_used;
        std::vector<size_t> combination_indexes;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t source_row_begin = builder.getSourceRowBegin();
            const size_t current_array_offset = array_offsets[row];
            const size_t n = current_array_offset - source_row_begin;

            size_t k = 0;
            if (k_is_unsigned)
            {
                k = static_cast<size_t>(column_k->getUInt(row));
            }
            else
            {
                const Int64 k_int = column_k->getInt(row);
                if (k_int < 0)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must be non-negative, got {}", getName(), k_int);

                k = static_cast<size_t>(k_int);
            }
            if (k > n)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Second argument of function {} must not exceed array size, "
                    "got k = {}, array size = {}",
                    getName(),
                    k,
                    n);

            if (k == 0)
            {
                builder.appendEmptyRow(current_array_offset);
                continue;
            }

            const size_t remaining = builder.getRemainingElementBudget(max_elements);
            const size_t count_limit = remaining / k;
            size_t row_result_count = 0;
            const bool fits_row_limit = tryCalculateKResultCount<mode>(n, k, count_limit, row_result_count);

            if (!fits_row_limit || row_result_count > count_limit)
                throw Exception(
                    ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Result of function {} is too large in row {}: more than {} "
                    "arrays of size {} would be generated, "
                    "maximum remaining elements in block: {}",
                    getName(),
                    row,
                    count_limit,
                    k,
                    remaining);

            builder.addGeneratedElements(row_result_count * k);

            if constexpr (mode == ArrayKCombinatoricsMode::PartialPermutations)
            {
                partial_indexes.assign(k, 0);
                partial_next_candidate.assign(k, 0);
                partial_used.assign(n, 0);
                size_t depth = 0;

                while (true)
                {
                    if (depth == k)
                    {
                        builder.appendVariant(array_data, source_row_begin, partial_indexes);

                        --depth;
                        partial_used[partial_indexes[depth]] = 0;
                        continue;
                    }

                    bool found = false;
                    for (size_t candidate = partial_next_candidate[depth]; candidate < n; ++candidate)
                    {
                        if (partial_used[candidate])
                            continue;

                        partial_indexes[depth] = candidate;
                        partial_used[candidate] = 1;
                        partial_next_candidate[depth] = candidate + 1;
                        ++depth;
                        if (depth < k)
                            partial_next_candidate[depth] = 0;

                        found = true;
                        break;
                    }

                    if (found)
                        continue;

                    if (depth == 0)
                        break;

                    --depth;
                    partial_used[partial_indexes[depth]] = 0;
                }
            }
            else
            {
                combination_indexes.resize(k);
                std::iota(combination_indexes.begin(), combination_indexes.end(), 0);

                while (true)
                {
                    builder.appendVariant(array_data, source_row_begin, combination_indexes);

                    size_t i = k;
                    while (i > 0 && combination_indexes[i - 1] == n - k + (i - 1))
                        --i;

                    if (i == 0)
                        break;

                    ++combination_indexes[i - 1];
                    for (size_t j = i; j < k; ++j)
                        combination_indexes[j] = combination_indexes[j - 1] + 1;
                }
            }

            builder.finishSourceRow(current_array_offset);
        }

        return builder.build();
    }

private:
    const size_t max_elements;
};

using FunctionArrayPartialPermutations
    = FunctionArrayKCombinatorics<ArrayKCombinatoricsMode::PartialPermutations, NameArrayPartialPermutations>;

using FunctionArrayCombinations = FunctionArrayKCombinatorics<ArrayKCombinatoricsMode::Combinations, NameArrayCombinations>;

}

REGISTER_FUNCTION(ArrayCombinatorics)
{
    FunctionDocumentation::Description permutations_description
        = "Returns all permutations of elements from the source array. Enumeration is done by indexes, so equal values may produce "
          "duplicate rows in the result. For an empty source array, returns `[[]]`.";
    FunctionDocumentation::Syntax permutations_syntax = "arrayPermutations(arr)";
    FunctionDocumentation::Arguments permutations_arguments = {
        {"arr", "Source array.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue permutations_returned_value = {"All permutations of the source array.", {"Array(Array(T))"}};
    FunctionDocumentation::Examples permutations_examples = {
        {"Usage example", "SELECT arrayPermutations([1, 2, 3]);", "[[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]]"},
    };
    FunctionDocumentation::IntroducedIn permutations_introduced_in = {26, 4};
    FunctionDocumentation permutations_documentation
        = {permutations_description,
           permutations_syntax,
           permutations_arguments,
           {},
           permutations_returned_value,
           permutations_examples,
           permutations_introduced_in,
           FunctionDocumentation::Category::Array};
    factory.registerFunction<FunctionArrayPermutations>(permutations_documentation);

    FunctionDocumentation::Description partial_permutations_description
        = "Returns all partial permutations (ordered selections without repeated indexes) of length `k` from the source array. "
          "Enumeration is done by indexes, so equal values may produce duplicate rows in the result. For `k = 0`, returns `[[]]`.";
    FunctionDocumentation::Syntax partial_permutations_syntax = "arrayPartialPermutations(arr, k)";
    FunctionDocumentation::Arguments partial_permutations_arguments = {
        {"arr", "Source array.", {"Array(T)"}},
        {"k",
         "Length of each generated partial permutation. Must not exceed source array size. Supports `Int8`, `Int16`, `Int32`, `Int64`, "
         "`UInt8`, `UInt16`, `UInt32`, and `UInt64`.",
         {"(U)Int*"}},
    };
    FunctionDocumentation::ReturnedValue partial_permutations_returned_value
        = {"All partial permutations of length `k`.", {"Array(Array(T))"}};
    FunctionDocumentation::Examples partial_permutations_examples = {
        {"Usage example", "SELECT arrayPartialPermutations([1, 2, 3], 2);", "[[1,2],[1,3],[2,1],[2,3],[3,1],[3,2]]"},
        {"`k = 0` example", "SELECT arrayPartialPermutations([1, 2, 3], 0);", "[[]]"},
    };
    FunctionDocumentation::IntroducedIn partial_permutations_introduced_in = {26, 4};
    FunctionDocumentation partial_permutations_documentation
        = {partial_permutations_description,
           partial_permutations_syntax,
           partial_permutations_arguments,
           {},
           partial_permutations_returned_value,
           partial_permutations_examples,
           partial_permutations_introduced_in,
           FunctionDocumentation::Category::Array};
    factory.registerFunction<FunctionArrayPartialPermutations>(partial_permutations_documentation);

    FunctionDocumentation::Description combinations_description
        = "Returns all combinations (unordered selections without repeated indexes) of length `k` from the source array. "
          "Enumeration is done by indexes, so equal values may produce duplicate rows in the result. For `k = 0`, returns `[[]]`.";
    FunctionDocumentation::Syntax combinations_syntax = "arrayCombinations(arr, k)";
    FunctionDocumentation::Arguments combinations_arguments = {
        {"arr", "Source array.", {"Array(T)"}},
        {"k",
         "Length of each generated combination. Must not exceed source array size. Supports `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, "
         "`UInt16`, `UInt32`, and `UInt64`.",
         {"(U)Int*"}},
    };
    FunctionDocumentation::ReturnedValue combinations_returned_value = {"All combinations of length `k`.", {"Array(Array(T))"}};
    FunctionDocumentation::Examples combinations_examples = {
        {"Usage example", "SELECT arrayCombinations([1, 2, 3], 2);", "[[1,2],[1,3],[2,3]]"},
        {"`k = 0` example", "SELECT arrayCombinations([1, 2, 3], 0);", "[[]]"},
    };
    FunctionDocumentation::IntroducedIn combinations_introduced_in = {26, 4};
    FunctionDocumentation combinations_documentation
        = {combinations_description,
           combinations_syntax,
           combinations_arguments,
           {},
           combinations_returned_value,
           combinations_examples,
           combinations_introduced_in,
           FunctionDocumentation::Category::Array};
    factory.registerFunction<FunctionArrayCombinations>(combinations_documentation);
}

} // namespace DB
