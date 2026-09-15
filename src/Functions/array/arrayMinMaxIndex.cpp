#include <base/defines.h>
#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Functions/array/FunctionArrayMapped.h>

#include <Common/NaNUtils.h>
#include <Common/TargetSpecific.h>
#include <Common/findExtreme.h>

#include <algorithm>
#include <array>
#include <optional>
#include <type_traits>

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

namespace DB
{

enum class ArrayMinMaxIndexStrategy : uint8_t
{
    Min,
    Max,
};

#if USE_MULTITARGET_CODE
DECLARE_X86_64_V3_SPECIFIC_CODE(

template <typename T>
size_t findFirstEqualSIMD(const T * data, size_t size, const T & value)
{
    static_assert(
        std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> || std::is_same_v<T, UInt32> || std::is_same_v<T, UInt64>
        || std::is_same_v<T, Int8> || std::is_same_v<T, Int16> || std::is_same_v<T, Int32> || std::is_same_v<T, Int64>
        || std::is_same_v<T, Float32> || std::is_same_v<T, Float64>);

    constexpr size_t vector_bytes = sizeof(__m256i);
    constexpr size_t lanes = vector_bytes / sizeof(T);
    constexpr size_t scalar_prefix = 4;
    const size_t prefix_size = std::min(size, scalar_prefix);

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (data[i] == value)
            return i;
    }

    const auto findInVector = [&](size_t offset)
    {
        unsigned mask;

        if constexpr (std::is_same_v<T, Float32>)
        {
            const auto values = _mm256_loadu_ps(data + offset);
            const auto target = _mm256_set1_ps(value);
            mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_cmp_ps(values, target, _CMP_EQ_OQ)));
        }
        else if constexpr (std::is_same_v<T, Float64>)
        {
            const auto values = _mm256_loadu_pd(data + offset);
            const auto target = _mm256_set1_pd(value);
            mask = static_cast<unsigned>(_mm256_movemask_pd(_mm256_cmp_pd(values, target, _CMP_EQ_OQ)));
        }
        else
        {
            const auto values = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + offset));
            __m256i equal;

            if constexpr (sizeof(T) == 1)
                equal = _mm256_cmpeq_epi8(values, _mm256_set1_epi8(static_cast<char>(value)));
            else if constexpr (sizeof(T) == 2)
                equal = _mm256_cmpeq_epi16(values, _mm256_set1_epi16(static_cast<short>(value)));
            else if constexpr (sizeof(T) == 4)
                equal = _mm256_cmpeq_epi32(values, _mm256_set1_epi32(static_cast<int>(value)));
            else
                equal = _mm256_cmpeq_epi64(values, _mm256_set1_epi64x(static_cast<long long>(value)));

            if constexpr (sizeof(T) == 1 || sizeof(T) == 2)
                mask = static_cast<unsigned>(_mm256_movemask_epi8(equal));
            else if constexpr (sizeof(T) == 4)
                mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_castsi256_ps(equal)));
            else
                mask = static_cast<unsigned>(_mm256_movemask_pd(_mm256_castsi256_pd(equal)));
        }

        if (!mask)
            return lanes;

        size_t lane = static_cast<size_t>(__builtin_ctz(mask));
        if constexpr (sizeof(T) == 2)
            lane >>= 1;
        return lane;
    };

    size_t i = prefix_size;
    for (; i + 2 * lanes <= size; i += 2 * lanes)
    {
        const size_t first = findInVector(i);
        if (first != lanes)
            return i + first;

        const size_t second = findInVector(i + lanes);
        if (second != lanes)
            return i + lanes + second;
    }

    for (; i + lanes <= size; i += lanes)
    {
        const size_t found = findInVector(i);
        if (found != lanes)
            return i + found;
    }

    for (; i < size; ++i)
    {
        if (data[i] == value)
            return i;
    }

    return size;
}

template <typename T>
requires(std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
size_t findFirstNaNSIMD(const T * data, size_t size)
{
    constexpr size_t lanes = sizeof(__m256i) / sizeof(T);
    constexpr size_t scalar_prefix = 4;
    const size_t prefix_size = std::min(size, scalar_prefix);

    for (size_t i = 0; i < prefix_size; ++i)
    {
        if (isNaN(data[i]))
            return i;
    }

    const auto findInVector = [&](size_t offset)
    {
        unsigned mask;
        if constexpr (std::is_same_v<T, Float32>)
        {
            const auto values = _mm256_loadu_ps(data + offset);
            mask = static_cast<unsigned>(_mm256_movemask_ps(_mm256_cmp_ps(values, values, _CMP_UNORD_Q)));
        }
        else
        {
            const auto values = _mm256_loadu_pd(data + offset);
            mask = static_cast<unsigned>(_mm256_movemask_pd(_mm256_cmp_pd(values, values, _CMP_UNORD_Q)));
        }

        if (!mask)
            return lanes;
        return static_cast<size_t>(__builtin_ctz(mask));
    };

    size_t i = prefix_size;
    for (; i + 2 * lanes <= size; i += 2 * lanes)
    {
        const size_t first = findInVector(i);
        if (first != lanes)
            return i + first;

        const size_t second = findInVector(i + lanes);
        if (second != lanes)
            return i + lanes + second;
    }

    for (; i + lanes <= size; i += lanes)
    {
        const size_t found = findInVector(i);
        if (found != lanes)
            return i + found;
    }

    for (; i < size; ++i)
    {
        if (isNaN(data[i]))
            return i;
    }

    return size;
}
)
#endif

namespace ArrayMinMaxIndexImpl
{

constexpr size_t small_tournament_limit = 48;
constexpr size_t medium_two_pass_limit = 256;
constexpr size_t record_block_limit_64_bit_integer = 16384;

template <typename T>
constexpr size_t mediumTwoPassMinSize()
{
    if constexpr (std::is_integral_v<T> && (sizeof(T) == 2 || sizeof(T) == 4))
        return small_tournament_limit;
    return 64;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
static bool isBetter(const T & lhs, const T & rhs)
{
    if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
        return lhs < rhs;
    else
        return lhs > rhs;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(has_find_extreme_implementation<T>)
static std::optional<T> findExtremeValue(const T * data, size_t begin, size_t end)
{
    if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
        return findExtremeMin(data, begin, end);
    else
        return findExtremeMax(data, begin, end);
}

static bool useAVX2()
{
#if USE_MULTITARGET_CODE
    return isArchSupported(TargetArch::x86_64_v3);
#else
    return false;
#endif
}

template <typename T>
static size_t findFirstEqual(const T * data, size_t size, const T & value, bool use_simd)
{
#if USE_MULTITARGET_CODE
    if (use_simd)
        return TargetSpecific::x86_64_v3::findFirstEqualSIMD(data, size, value);
#else
    (void)use_simd;
#endif

    for (size_t i = 0; i < size; ++i)
    {
        if (data[i] == value)
            return i;
    }
    return size;
}

template <typename T>
requires(std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
static size_t findFirstNaN(const T * data, size_t size, bool use_simd)
{
#if USE_MULTITARGET_CODE
    if (use_simd)
        return TargetSpecific::x86_64_v3::findFirstNaNSIMD(data, size);
#else
    (void)use_simd;
#endif

    for (size_t i = 0; i < size; ++i)
    {
        if (isNaN(data[i]))
            return i;
    }
    return size;
}

template <typename T>
static size_t findFirstSelectedValue(const T * data, size_t size, const T & value, bool use_simd)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if (isNaN(value))
            return findFirstNaN(data, size, use_simd);
    }
    return findFirstEqual(data, size, value, use_simd);
}

template <typename T>
requires(std::is_integral_v<T>)
struct IndexedValue
{
    T value;
    size_t index;
};

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_integral_v<T>)
static IndexedValue<T> selectIndexedValue(const IndexedValue<T> & lhs, const IndexedValue<T> & rhs)
{
    return isBetter<strategy>(rhs.value, lhs.value) ? rhs : lhs;
}

template <size_t count, ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_integral_v<T>)
static IndexedValue<T> selectIndexedBlock(const T * data, size_t offset)
{
    if constexpr (count == 1)
        return {data[offset], offset};
    else
        return selectIndexedValue<strategy>(
            selectIndexedBlock<count / 2, strategy>(data, offset),
            selectIndexedBlock<count / 2, strategy>(data, offset + count / 2));
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(std::is_integral_v<T>)
static size_t findIndexTournament(const T * data, size_t size)
{
    bool have_best = false;
    IndexedValue<T> best{};

    const auto add = [&](const IndexedValue<T> & value)
    {
        if (!have_best)
        {
            best = value;
            have_best = true;
        }
        else
        {
            best = selectIndexedValue<strategy>(best, value);
        }
    };

    size_t i = 0;
    for (; i + 16 <= size; i += 16)
        add(selectIndexedBlock<16, strategy>(data, i));
    if (i + 8 <= size)
    {
        add(selectIndexedBlock<8, strategy>(data, i));
        i += 8;
    }
    if (i + 4 <= size)
    {
        add(selectIndexedBlock<4, strategy>(data, i));
        i += 4;
    }
    if (i + 2 <= size)
    {
        add(selectIndexedBlock<2, strategy>(data, i));
        i += 2;
    }
    if (i < size)
        add({data[i], i});

    return best.index;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
static size_t findIndexOnePass(const T * data, size_t size)
{
    size_t index = 0;

    if constexpr (std::is_floating_point_v<T>)
    {
        while (index < size && isNaN(data[index]))
            ++index;

        if (index == size)
            return 0;
    }

    T best = data[index];

#pragma clang loop unroll_count(8)
    for (size_t i = index + 1; i < size; ++i)
    {
        if (isBetter<strategy>(data[i], best))
        {
            best = data[i];
            index = i;
        }
    }

    return index;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
static size_t findIndexSmallOrOnePass(const T * data, size_t size)
{
    if constexpr (std::is_integral_v<T>)
    {
        if (size <= small_tournament_limit)
            return findIndexTournament<strategy>(data, size);
    }
    return findIndexOnePass<strategy>(data, size);
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(has_find_extreme_implementation<T>)
static size_t findIndexRecordBlocks(const T * data, size_t size, bool use_simd)
{
    constexpr size_t block_size = 1024;

    T best{};
    size_t best_index = 0;
    [[maybe_unused]] size_t first_nan = size;
    bool have_numeric_value = !std::is_floating_point_v<T>;

    if constexpr (std::is_floating_point_v<T>)
    {
        if (isNaN(data[0]))
            first_nan = 0;
        else
        {
            best = data[0];
            have_numeric_value = true;
        }
    }
    else
    {
        best = data[0];
    }

    for (size_t block_begin = 0; block_begin < size; block_begin += block_size)
    {
        const size_t block_end = std::min(block_begin + block_size, size);
        const auto block_extreme = findExtremeValue<strategy>(data, block_begin, block_end);

        chassert(block_extreme.has_value());

        if constexpr (std::is_floating_point_v<T>)
        {
            if (isNaN(*block_extreme))
            {
                if (first_nan == size)
                {
                    const size_t nan_index = findFirstNaN(data + block_begin, block_end - block_begin, use_simd);
                    chassert(nan_index < block_end - block_begin);
                    first_nan = block_begin + nan_index;
                }
                continue;
            }
        }

        const bool record = !have_numeric_value || isBetter<strategy>(*block_extreme, best);

        if (record)
        {
            const size_t block_index = findFirstSelectedValue(
                data + block_begin, block_end - block_begin, *block_extreme, use_simd);
            chassert(block_index < block_end - block_begin);
            best = *block_extreme;
            best_index = block_begin + block_index;
            have_numeric_value = true;
        }
    }

    if constexpr (std::is_floating_point_v<T>)
    {
        if (!have_numeric_value)
        {
            chassert(first_nan < size);
            return first_nan;
        }
    }

    return best_index;
}

/// Wide integers do not have a SIMD findExtreme implementation. Keep them on a typed scan
/// instead of paying the generic compareAt dispatch for every element.
template <ArrayMinMaxIndexStrategy strategy, typename Element>
requires(is_big_int_v<Element>)
static bool executeWideNumeric(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & result_ptr)
{
    const auto * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);
    if (!column)
        return false;

    auto result_column = ColumnUInt32::create(offsets.size());
    auto & result = result_column->getData();
    const Element * data = column->getData().data();

    size_t begin = 0;
    for (size_t row = 0; row < offsets.size(); ++row)
    {
        const size_t end = offsets[row];
        const size_t size = end - begin;
        result[row] = size <= 1
            ? static_cast<UInt32>(size)
            : static_cast<UInt32>(findIndexOnePass<strategy>(data + begin, size) + 1);
        begin = end;
    }

    result_ptr = std::move(result_column);
    return true;
}

template <ArrayMinMaxIndexStrategy strategy, typename Element>
static bool executeNumeric(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & result_ptr)
{
    const auto * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);
    if (!column)
        return false;

    auto result_column = ColumnUInt32::create(offsets.size());
    auto & result = result_column->getData();
    const Element * data = column->getData().data();
    const bool use_simd = useAVX2();

    size_t begin = 0;
    for (size_t row = 0; row < offsets.size(); ++row)
    {
        const size_t end = offsets[row];
        const size_t size = end - begin;

        constexpr size_t medium_two_pass_min_size = mediumTwoPassMinSize<Element>();

        if (size <= 1)
        {
            result[row] = static_cast<UInt32>(size);
        }
        else if (size <= medium_two_pass_limit && size > medium_two_pass_min_size && use_simd)
        {
            std::array<Element, 4> extrema{};
            std::array<const Element *, 4> row_data{};
            std::array<size_t, 4> row_sizes{};
            size_t tile_rows = 0;
            size_t tile_begin = begin;

            while (tile_rows < extrema.size() && row + tile_rows < offsets.size())
            {
                const size_t tile_end = offsets[row + tile_rows];
                const size_t tile_size = tile_end - tile_begin;
                if (tile_size <= medium_two_pass_min_size || tile_size > medium_two_pass_limit)
                    break;

                row_data[tile_rows] = data + tile_begin;
                row_sizes[tile_rows] = tile_size;
                extrema[tile_rows] = *findExtremeValue<strategy>(data, tile_begin, tile_end);
                tile_begin = tile_end;
                ++tile_rows;
            }

            chassert(tile_rows != 0);
            for (size_t tile_row = 0; tile_row < tile_rows; ++tile_row)
            {
                const size_t index = findFirstSelectedValue(
                    row_data[tile_row], row_sizes[tile_row], extrema[tile_row], true);
                chassert(index < row_sizes[tile_row]);
                result[row + tile_row] = static_cast<UInt32>(index + 1);
            }
            row += tile_rows - 1;
            begin = tile_begin;
            continue;
        }
        else if (size <= 64 || !use_simd)
        {
            result[row] = static_cast<UInt32>(findIndexSmallOrOnePass<strategy>(data + begin, size) + 1);
        }
        else if (std::is_integral_v<Element> && sizeof(Element) == 8 && size < record_block_limit_64_bit_integer)
        {
            result[row] = static_cast<UInt32>(findIndexOnePass<strategy>(data + begin, size) + 1);
        }
        else
        {
            result[row] = static_cast<UInt32>(findIndexRecordBlocks<strategy>(data + begin, size, true) + 1);
        }

        begin = end;
    }

    result_ptr = std::move(result_column);
    return true;
}

template <ArrayMinMaxIndexStrategy strategy>
struct ArrayMinMaxIndexImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr &, const DataTypePtr &)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const auto & offsets = array.getOffsets();
        auto result = ColumnUInt32::create(offsets.size());
        auto & result_data = result->getData();

        if (checkAndGetColumn<ColumnConst>(&*mapped))
        {
            size_t begin = 0;
            for (size_t row = 0; row < offsets.size(); ++row)
            {
                const size_t end = offsets[row];
                result_data[row] = begin == end ? 0 : 1;
                begin = end;
            }
            return result;
        }

        ColumnPtr numeric_result;
        if (executeNumeric<strategy, UInt8>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt16>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt64>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int8>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int16>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int64>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, UInt128>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, UInt256>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, Int128>(mapped, offsets, numeric_result)
            || executeWideNumeric<strategy, Int256>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Float32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Float64>(mapped, offsets, numeric_result))
            return numeric_result;

        static constexpr int nan_null_direction_hint = strategy == ArrayMinMaxIndexStrategy::Min ? 1 : -1;

        size_t begin = 0;
        for (size_t row = 0; row < offsets.size(); ++row)
        {
            const size_t end = offsets[row];
            if (begin == end)
            {
                result_data[row] = 0;
                begin = end;
                continue;
            }

            size_t best = begin;
            for (size_t i = begin + 1; i < end; ++i)
            {
                const int comparison = mapped->compareAt(i, best, *mapped, nan_null_direction_hint);
                if (isBetter<strategy>(comparison, 0))
                    best = i;
            }

            result_data[row] = static_cast<UInt32>(best - begin + 1);
            begin = end;
        }

        return result;
    }
};

struct NameArrayMinIndex { static constexpr auto name = "arrayMinIndex"; };
using FunctionArrayMinIndex = FunctionArrayMapped<ArrayMinMaxIndexImpl<ArrayMinMaxIndexStrategy::Min>, NameArrayMinIndex>;

struct NameArrayMaxIndex { static constexpr auto name = "arrayMaxIndex"; };
using FunctionArrayMaxIndex = FunctionArrayMapped<ArrayMinMaxIndexImpl<ArrayMinMaxIndexStrategy::Max>, NameArrayMaxIndex>;

REGISTER_FUNCTION(ArrayMinMaxIndex)
{
    FunctionDocumentation::Description description_min = R"(
Returns the 1-based index of the minimum element in the source array, or `0` if the array is empty.

If a lambda function `func` is specified, returns the index of the element corresponding to the minimum lambda result. If multiple elements have the same minimum value, returns the index of the first one.
    )";
    FunctionDocumentation::Syntax syntax_min = "arrayMinIndex([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_min = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_min = {"Returns the 1-based index of the first minimum element, or `0` for an empty array.", {"UInt32"}};
    FunctionDocumentation::Examples examples_min = {
        {"Basic example", "SELECT arrayMinIndex([5, 3, 2, 7]);", "3"},
        {"First equal minimum", "SELECT arrayMinIndex([5, 3, 3, 7]);", "2"},
        {"Usage with lambda function", "SELECT arrayMinIndex(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "1"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_min = {26, 9};
    FunctionDocumentation::Category category_min = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_min = {description_min, syntax_min, arguments_min, {}, returned_value_min, examples_min, introduced_in_min, category_min};
    factory.registerFunction<FunctionArrayMinIndex>(documentation_min);

    FunctionDocumentation::Description description_max = R"(
Returns the 1-based index of the maximum element in the source array, or `0` if the array is empty.

If a lambda function `func` is specified, returns the index of the element corresponding to the maximum lambda result. If multiple elements have the same maximum value, returns the index of the first one.
    )";
    FunctionDocumentation::Syntax syntax_max = "arrayMaxIndex([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_max = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_max = {"Returns the 1-based index of the first maximum element, or `0` for an empty array.", {"UInt32"}};
    FunctionDocumentation::Examples examples_max = {
        {"Basic example", "SELECT arrayMaxIndex([5, 3, 2, 7]);", "4"},
        {"First equal maximum", "SELECT arrayMaxIndex([5, 7, 7, 3]);", "2"},
        {"Usage with lambda function", "SELECT arrayMaxIndex(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "3"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_max = {26, 9};
    FunctionDocumentation::Category category_max = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_max = {description_max, syntax_max, arguments_max, {}, returned_value_max, examples_max, introduced_in_max, category_max};
    factory.registerFunction<FunctionArrayMaxIndex>(documentation_max);
}

}
