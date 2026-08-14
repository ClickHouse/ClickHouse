#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}


/// arrayTranspose([[1, 2, 3], [4, 5, 6]]) = [[1, 4], [2, 5], [3, 6]]
class FunctionArrayTranspose : public IFunction
{
public:
    static constexpr auto name = "arrayTranspose";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayTranspose>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * outer_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!outer_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be Array(Array(T)), got {}",
                getName(), arguments[0]->getName());

        if (!checkAndGetDataType<DataTypeArray>(outer_type->getNestedType().get()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be Array(Array(T)), got {}",
                getName(), arguments[0]->getName());

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnArray * outer_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!outer_col)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} in argument of function {}",
                arguments[0].column->getName(), getName());

        const ColumnArray * inner_col = checkAndGetColumn<ColumnArray>(&outer_col->getData());
        if (!inner_col)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} in argument of function {}",
                outer_col->getData().getName(), getName());

        const IColumn & src_data = inner_col->getData();
        const ColumnArray::Offsets & outer_offsets = outer_col->getOffsets();
        const ColumnArray::Offsets & inner_offsets = inner_col->getOffsets();

        /// Validate that all inner arrays in each row have equal size, and compute the total
        /// number of result inner arrays (sum of inner_size across rows) for exact reservation.
        size_t total_inner_size = 0;
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];

            if (outer_start == outer_end)
                continue;

            size_t inner_size = inner_offsets[outer_start] - inner_offsets[outer_start - 1];
            for (ColumnArray::Offset j = outer_start + 1; j < outer_end; ++j)
            {
                size_t current_inner_size = inner_offsets[j] - inner_offsets[j - 1];
                if (current_inner_size != inner_size)
                    throw Exception(
                        ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "All inner arrays in argument of function {} must have equal sizes, "
                        "but row {} has inner arrays of size {} and {}",
                        getName(), i, inner_size, current_inner_size);
            }
            total_inner_size += inner_size;
        }

        auto result_outer_offsets_col = ColumnArray::ColumnOffsets::create();
        auto result_inner_offsets_col = ColumnArray::ColumnOffsets::create();
        auto result_data = src_data.cloneEmpty();

        ColumnArray::Offsets & result_outer_offsets = result_outer_offsets_col->getData();
        ColumnArray::Offsets & result_inner_offsets = result_inner_offsets_col->getData();

        result_outer_offsets.reserve(input_rows_count);
        result_inner_offsets.reserve(total_inner_size);
        result_data->reserve(src_data.size());

        ColumnArray::Offset result_outer_offset = 0;
        ColumnArray::Offset result_inner_offset = 0;

        /// Fill result offsets (same for all types: result has inner_size outer arrays of outer_size each).
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];
            size_t outer_size = outer_end - outer_start;
            size_t inner_size = outer_size > 0 ? inner_offsets[outer_start] - inner_offsets[outer_start - 1] : 0;

            for (size_t j = 0; j < inner_size; ++j)
            {
                result_inner_offset += outer_size;
                result_inner_offsets.push_back(result_inner_offset);
            }

            result_outer_offset += inner_size;
            result_outer_offsets.push_back(result_outer_offset);
        }

        execute(src_data, outer_offsets, inner_offsets, *result_data, input_rows_count);

        auto result_inner_array = ColumnArray::create(std::move(result_data), std::move(result_inner_offsets_col));
        return ColumnArray::create(std::move(result_inner_array), std::move(result_outer_offsets_col));
    }

private:
    static bool execute(
        const IColumn & src_data,
        const ColumnArray::Offsets & outer_offsets,
        const ColumnArray::Offsets & inner_offsets,
        IColumn & res_data,
        size_t input_rows_count)
    {
        return executeNumber<UInt8>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<UInt16>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<UInt32>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<UInt64>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Int8>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Int16>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Int32>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Int64>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Int128>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Int256>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<UInt128>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<UInt256>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<BFloat16>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Float32>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Float64>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Decimal32>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Decimal64>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Decimal128>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Decimal256>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<DateTime64>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNumber<Time64>(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeFixedString(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeString(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeNullable(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeTuple(src_data, outer_offsets, inner_offsets, res_data, input_rows_count)
            || executeGeneric(src_data, outer_offsets, inner_offsets, res_data, input_rows_count);
    }

    /// For Nullable we transpose the nested values and the null map independently.
    static bool executeNullable(
        const IColumn & src_data,
        const ColumnArray::Offsets & outer_offsets,
        const ColumnArray::Offsets & inner_offsets,
        IColumn & res_data,
        size_t input_rows_count)
    {
        const auto * src_nullable = typeid_cast<const ColumnNullable *>(&src_data);
        if (!src_nullable)
            return false;

        auto * res_nullable = typeid_cast<ColumnNullable *>(&res_data);

        execute(src_nullable->getNestedColumn(), outer_offsets, inner_offsets, res_nullable->getNestedColumn(), input_rows_count);
        executeNumber<UInt8>(src_nullable->getNullMapColumn(), outer_offsets, inner_offsets, res_nullable->getNullMapColumn(), input_rows_count);

        return true;
    }

    /// For Tuple we transpose each tuple component independently.
    static bool executeTuple(
        const IColumn & src_data,
        const ColumnArray::Offsets & outer_offsets,
        const ColumnArray::Offsets & inner_offsets,
        IColumn & res_data,
        size_t input_rows_count)
    {
        const auto * src_tuple = typeid_cast<const ColumnTuple *>(&src_data);
        if (!src_tuple)
            return false;

        auto * res_tuple = typeid_cast<ColumnTuple *>(&res_data);

        for (size_t i = 0; i < src_tuple->tupleSize(); ++i)
            execute(src_tuple->getColumn(i), outer_offsets, inner_offsets, res_tuple->getColumn(i), input_rows_count);

        return true;
    }

    /// L1 cache is typically 32 KB; stop recursing when both blocks (src+dst) fit in L1.
    static constexpr size_t l1_cache_size = 32 * 1024;

    /// Cache-oblivious block transpose for numeric types.
    /// Transposes an outer_size × inner_size submatrix from src into dst.
    /// src[i][j] = src_ptr[i * full_inner_size + j], dst[j][i] = dst_ptr[j * full_outer_size + i].
    template <typename T>
    static void transposeBlock(
        const T * src, T * dst,
        size_t full_outer_size, size_t full_inner_size,
        size_t outer_size, size_t inner_size)
    {
        /// Divide by 2: both the source and destination sub-blocks must fit in L1 simultaneously.
        static constexpr size_t max_elements = l1_cache_size / sizeof(T) / 2;

        if (outer_size * inner_size <= max_elements)
        {
            /// Base case: transpose naively — both blocks fit in L1.
            for (size_t i = 0; i < outer_size; ++i)
                for (size_t j = 0; j < inner_size; ++j)
                    dst[j * full_outer_size + i] = src[i * full_inner_size + j];
            return;
        }

        if (outer_size >= inner_size)
        {
            size_t half = outer_size / 2;
            transposeBlock(src,                          dst,         full_outer_size, full_inner_size, half,              inner_size);
            transposeBlock(src + half * full_inner_size, dst + half,  full_outer_size, full_inner_size, outer_size - half, inner_size);
        }
        else
        {
            size_t half = inner_size / 2;
            transposeBlock(src,        dst,                           full_outer_size, full_inner_size, outer_size, half);
            transposeBlock(src + half, dst + half * full_outer_size,  full_outer_size, full_inner_size, outer_size, inner_size - half);
        }
    }

    /// Cache-oblivious block transpose for FixedString (element size = n bytes).
    static void transposeBlockFixedString(
        const UInt8 * src, UInt8 * dst,
        size_t full_outer_size, size_t full_inner_size,
        size_t outer_size, size_t inner_size, size_t n)
    {
        /// When n is so large that a 4-elements already exceed the L1 cache,
        /// the cache-oblivious recursion provides no benefit over a naive loop.
        const size_t max_elements = (n <= l1_cache_size / 4) ? l1_cache_size / (n * 2)
                                                             : std::numeric_limits<size_t>::max();

        if (outer_size * inner_size <= max_elements)
        {
            for (size_t i = 0; i < outer_size; ++i)
                for (size_t j = 0; j < inner_size; ++j)
                    memcpy(dst + (j * full_outer_size + i) * n,
                           src + (i * full_inner_size + j) * n,
                           n);
            return;
        }

        if (outer_size >= inner_size)
        {
            size_t half = outer_size / 2;
            transposeBlockFixedString(src,                                  dst,                    full_outer_size, full_inner_size, half,              inner_size, n);
            transposeBlockFixedString(src + half * full_inner_size * n,     dst + half * n,         full_outer_size, full_inner_size, outer_size - half, inner_size, n);
        }
        else
        {
            size_t half = inner_size / 2;
            transposeBlockFixedString(src,                                  dst,                                 full_outer_size, full_inner_size, outer_size, half,              n);
            transposeBlockFixedString(src + half * n,                       dst + half * full_outer_size * n,    full_outer_size, full_inner_size, outer_size, inner_size - half, n);
        }
    }

    /// Fast path for numeric and decimal types: uses cache-oblivious block transpose on raw pointers.
    /// Selects ColumnDecimal for decimal/datetime types, ColumnVector otherwise.
    template <typename T>
    static bool executeNumber(
        const IColumn & src_data,
        const ColumnArray::Offsets & outer_offsets,
        const ColumnArray::Offsets & inner_offsets,
        IColumn & res_data,
        size_t input_rows_count)
    {
        using ColumnType = ColumnVectorOrDecimal<T>;

        const auto * src_col = checkAndGetColumn<ColumnType>(&src_data);
        if (!src_col)
            return false;

        const PaddedPODArray<T> & src_vec = src_col->getData();
        PaddedPODArray<T> & res_vec = typeid_cast<ColumnType &>(res_data).getData();
        res_vec.resize(src_vec.size());

        size_t result_offset = 0;

        for (size_t i = 0; i != input_rows_count; ++i)
        {
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];
            size_t outer_size = outer_end - outer_start;

            if (outer_size == 0)
                continue;

            size_t inner_size = inner_offsets[outer_start] - inner_offsets[outer_start - 1];

            if (inner_size == 0)
                continue;

            size_t src_offset = inner_offsets[outer_start - 1];

            transposeBlock<T>(
                src_vec.data() + src_offset,
                res_vec.data() + result_offset,
                outer_size, inner_size,
                outer_size, inner_size);

            result_offset += outer_size * inner_size;
        }

        return true;
    }

    /// Fast path for FixedString: uses cache-oblivious block transpose with memcpy per element.
    static bool executeFixedString(
        const IColumn & src_data,
        const ColumnArray::Offsets & outer_offsets,
        const ColumnArray::Offsets & inner_offsets,
        IColumn & res_data,
        size_t input_rows_count)
    {
        const auto * src_col = checkAndGetColumn<ColumnFixedString>(&src_data);
        if (!src_col)
            return false;

        const size_t n = src_col->getN();
        const ColumnFixedString::Chars & src_chars = src_col->getChars();
        ColumnFixedString::Chars & res_chars = typeid_cast<ColumnFixedString &>(res_data).getChars();
        res_chars.resize(src_chars.size());

        size_t result_offset = 0;

        for (size_t i = 0; i != input_rows_count; ++i)
        {
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];
            size_t outer_size = outer_end - outer_start;

            if (outer_size == 0)
                continue;

            size_t inner_size = inner_offsets[outer_start] - inner_offsets[outer_start - 1];

            if (inner_size == 0)
                continue;

            size_t src_offset = inner_offsets[outer_start - 1];

            transposeBlockFixedString(
                src_chars.data() + src_offset * n,
                res_chars.data() + result_offset * n,
                outer_size, inner_size,
                outer_size, inner_size, n);

            result_offset += outer_size * inner_size;
        }

        return true;
    }

    /// Fast path for String:
    /// cache-oblivious algorithm is not applicable due to variable-length elements,
    /// so we simply use out[j][k] = input[k][j] here as in executeGeneric(),
    /// but without using virtual function insertFrom().
    static bool executeString(
        const IColumn & src_data,
        const ColumnArray::Offsets & outer_offsets,
        const ColumnArray::Offsets & inner_offsets,
        IColumn & res_data,
        size_t input_rows_count)
    {
        const auto * src_col = checkAndGetColumn<ColumnString>(&src_data);
        if (!src_col)
            return false;

        const ColumnString::Chars & src_chars = src_col->getChars();
        const ColumnString::Offsets & src_string_offsets = src_col->getOffsets();

        ColumnString & res_col = typeid_cast<ColumnString &>(res_data);
        ColumnString::Chars & res_chars = res_col.getChars();
        ColumnString::Offsets & res_string_offsets = res_col.getOffsets();

        res_chars.resize(src_chars.size());
        res_string_offsets.resize(src_string_offsets.size());

        size_t result_string_idx = 0;
        size_t result_chars_offset = 0;

        for (size_t i = 0; i != input_rows_count; ++i)
        {
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];
            size_t outer_size = outer_end - outer_start;

            if (outer_size == 0)
                continue;

            size_t inner_size = inner_offsets[outer_start] - inner_offsets[outer_start - 1];

            if (inner_size == 0)
                continue;

            for (size_t j = 0; j < inner_size; ++j)
            {
                for (ColumnArray::Offset k = outer_start; k < outer_end; ++k)
                {
                    size_t string_idx = inner_offsets[k - 1] + j;
                    size_t src_start = src_string_offsets[string_idx - 1];
                    size_t string_size = src_string_offsets[string_idx] - src_start;

                    memcpySmallAllowReadWriteOverflow15(&res_chars[result_chars_offset], &src_chars[src_start], string_size);
                    result_chars_offset += string_size;
                    res_string_offsets[result_string_idx++] = result_chars_offset;
                }
            }
        }

        return true;
    }

    /// Generic algorithm: out[j][k] = input[k][j],
    /// with using virtual function insertFrom().
    static bool executeGeneric(
        const IColumn & src_data,
        const ColumnArray::Offsets & outer_offsets,
        const ColumnArray::Offsets & inner_offsets,
        IColumn & res_data,
        size_t input_rows_count)
    {
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            ColumnArray::Offset outer_start = outer_offsets[i - 1];
            ColumnArray::Offset outer_end = outer_offsets[i];
            size_t outer_size = outer_end - outer_start;

            if (outer_size == 0)
                continue;

            size_t inner_size = inner_offsets[outer_start] - inner_offsets[outer_start - 1];

            for (size_t j = 0; j < inner_size; ++j)
                for (ColumnArray::Offset k = outer_start; k < outer_end; ++k)
                    res_data.insertFrom(src_data, inner_offsets[k - 1] + j);
        }

        return true;
    }
};


REGISTER_FUNCTION(ArrayTranspose)
{
    FunctionDocumentation::Description description = R"(
Transposes a two-dimensional array.

All inner arrays must have the same length.
)";
    FunctionDocumentation::Syntax syntax = "arrayTranspose(arr)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "A two-dimensional array to transpose. All inner arrays must have the same length.", {"Array(Array(T))"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "A transposed two-dimensional array where element `[i][j]` of the result equals element `[j][i]` of the input.",
        {"Array(Array(T))"}
    };
    FunctionDocumentation::Examples examples = {
        {"Square matrix", "SELECT arrayTranspose([[1, 2], [3, 4]])", "[[1, 3], [2, 4]]"},
        {"Non-square matrix", "SELECT arrayTranspose([[1, 2, 3], [4, 5, 6]])", "[[1, 4], [2, 5], [3, 6]]"},
        {"String elements", "SELECT arrayTranspose([['a', 'b'], ['c', 'd']])", "[['a', 'c'], ['b', 'd']]"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayTranspose>(documentation);
}

}
