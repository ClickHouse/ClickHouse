#include <Functions/FunctionsRollingHash.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/UTF8Helpers.h>
#include <Common/assert_cast.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Interpreters/Context_fwd.h>

#include <algorithm>
#include <array>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
constexpr UInt64 buzhashSplitmix64(UInt64 x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}
}

const UInt64 * BuzhashImpl::getTable()
{
    static const std::array<UInt64, 256> table = []
    {
        std::array<UInt64, 256> t{};
        for (size_t i = 0; i < 256; ++i)
            t[i] = buzhashSplitmix64(0x0123456789abcdefULL + i);
        return t;
    }();
    return table.data();
}

namespace RollingHashCDC
{
namespace
{
constexpr size_t MAX_CDC_CHUNK_SIZE = 256 * 1024 * 1024; /// 256 MiB safety cap
constexpr size_t DEFAULT_MIN_MAX_CHUNK = 262144; /// 256 KiB default max chunk floor
}

size_t maxChunkSizeForCdc(UInt64 reverse_probability)
{
    const uint64_t scaled = std::min<uint64_t>(reverse_probability * 64, static_cast<uint64_t>(MAX_CDC_CHUNK_SIZE));
    return static_cast<size_t>(std::max<uint64_t>(DEFAULT_MIN_MAX_CHUNK, scaled));
}

bool isUtf8ChunkBoundary(const UInt8 * data, size_t byte_pos, size_t data_size)
{
    if (byte_pos == 0 || byte_pos >= data_size)
        return true;
    return !UTF8::isContinuationOctet(data[byte_pos]);
}

size_t forceCutPositionBytes(const UInt8 * /*data*/, size_t data_size, size_t chunk_start, size_t max_chunk_size)
{
    return std::min(chunk_start + max_chunk_size, data_size);
}

size_t forceCutPositionUtf8(const UInt8 * data, size_t data_size, size_t chunk_start, size_t max_chunk_size)
{
    size_t tentative = std::min(chunk_start + max_chunk_size, data_size);
    if (tentative <= chunk_start)
        return tentative;
    if (tentative == data_size)
        return data_size;

    const UInt8 * p = data + tentative;
    UTF8::syncBackward(p, data + chunk_start);
    size_t cut_pos = static_cast<size_t>(p - data);
    if (cut_pos <= chunk_start)
    {
        p = data + chunk_start;
        UTF8::syncForward(p, data + data_size);
        cut_pos = std::min<size_t>(static_cast<size_t>(p - data), data_size);
    }
    return cut_pos;
}

}

namespace
{

void validateWindowSize(const char * func_name, size_t window_size)
{
    if (window_size < BuzhashImpl::MIN_WINDOW_SIZE || window_size > BuzhashImpl::MAX_WINDOW_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Argument 'window_size' of function {} must be in range [{}, {}], got {}",
            func_name,
            BuzhashImpl::MIN_WINDOW_SIZE,
            BuzhashImpl::MAX_WINDOW_SIZE,
            window_size);
}

size_t getWindowSize(const ColumnsWithTypeAndName & arguments, const char * func_name)
{
    if (!isUInt(arguments[1].type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument (window_size) of function {} must be unsigned integer, got {}",
            func_name,
            arguments[1].type->getName());

    if (!arguments[1].column || !isColumnConst(*arguments[1].column))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument (window_size) of function {} must be constant",
            func_name);

    size_t window_size = arguments[1].column->getUInt(0);
    validateWindowSize(func_name, window_size);
    return window_size;
}

UInt64 getReverseProbability(const ColumnsWithTypeAndName & arguments, const char * func_name)
{
    if (!isUInt(arguments[2].type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Third argument (reverse_probability) of function {} must be unsigned integer, got {}",
            func_name,
            arguments[2].type->getName());

    if (!arguments[2].column || !isColumnConst(*arguments[2].column))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Third argument (reverse_probability) of function {} must be constant",
            func_name);

    UInt64 p = arguments[2].column->getUInt(0);
    if (p < 2)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Third argument (reverse_probability) of function {} must be at least 2 (expected average chunk size in bytes), got {}",
            func_name,
            p);
    return p;
}

void contentDefinedCdcOneRow(
    ColumnString * out_strings,
    ColumnUInt64 * out_offsets,
    const UInt8 * data,
    size_t data_size,
    size_t window_size,
    UInt64 reverse_probability,
    bool utf8_boundaries,
    bool return_offsets)
{
    RollingHashCDC::forEachContentDefinedChunk(
        data,
        data_size,
        window_size,
        reverse_probability,
        utf8_boundaries,
        return_offsets,
        [&](const char * ptr, size_t len)
        {
            if (out_strings)
                out_strings->insertData(ptr, len);
        },
        [&](UInt64 o)
        {
            if (out_offsets)
                out_offsets->getData().push_back(o);
        });
}

void validateContentDefinedCdcArguments(const ColumnsWithTypeAndName & arguments, const char * func_name)
{
    if (arguments.size() != 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} expects 3 arguments (string, window_size, reverse_probability), got {}",
            func_name,
            arguments.size());

    if (!isString(arguments[0].type) && !isFixedString(arguments[0].type))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument of function {} must be String or FixedString, got {}",
            func_name,
            arguments[0].type->getName());

    getWindowSize(arguments, func_name);
    getReverseProbability(arguments, func_name);
}

}

DataTypePtr FunctionContentDefinedChunks::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    validateContentDefinedCdcArguments(arguments, name);
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
}

DataTypePtr FunctionContentDefinedChunksUTF8::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    validateContentDefinedCdcArguments(arguments, name);
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
}

DataTypePtr FunctionContentDefinedChunkOffsets::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    validateContentDefinedCdcArguments(arguments, name);
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
}

DataTypePtr FunctionContentDefinedChunkOffsetsUTF8::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    validateContentDefinedCdcArguments(arguments, name);
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
}

template <bool Utf8Boundaries, bool ReturnOffsets>
ColumnPtr executeContentDefinedCdcImpl(
    const ColumnsWithTypeAndName & arguments,
    const char * func_name,
    size_t input_rows_count)
{
    size_t window_size = getWindowSize(arguments, func_name);
    UInt64 reverse_probability = getReverseProbability(arguments, func_name);

    const ColumnPtr & column = arguments[0].column;

    if constexpr (ReturnOffsets)
    {
        auto col_res = ColumnArray::create(ColumnUInt64::create());
        ColumnUInt64 & res_values = assert_cast<ColumnUInt64 &>(col_res->getData());
        ColumnArray::Offsets & res_row_offsets = col_res->getOffsets();
        res_row_offsets.reserve(input_rows_count);

        if (const auto * col_string = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & data = col_string->getChars();
            const auto & offsets = col_string->getOffsets();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t data_size = offsets[i] - current_offset;
                contentDefinedCdcOneRow(
                    nullptr,
                    &res_values,
                    reinterpret_cast<const UInt8 *>(&data[current_offset]),
                    data_size,
                    window_size,
                    reverse_probability,
                    Utf8Boundaries,
                    true);
                res_row_offsets.push_back(res_values.size());
                current_offset = offsets[i];
            }
        }
        else if (const auto * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            size_t n = col_fixed->getN();
            const auto & data = col_fixed->getChars();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const UInt8 * row_data = reinterpret_cast<const UInt8 *>(&data[i * n]);
                contentDefinedCdcOneRow(
                    nullptr, &res_values, row_data, n, window_size, reverse_probability, Utf8Boundaries, true);
                res_row_offsets.push_back(res_values.size());
            }
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                column->getName(),
                func_name);
        }

        return col_res;
    }
    else
    {
        auto col_res = ColumnArray::create(ColumnString::create());
        ColumnString & res_strings = assert_cast<ColumnString &>(col_res->getData());
        ColumnArray::Offsets & res_row_offsets = col_res->getOffsets();
        res_row_offsets.reserve(input_rows_count);

        if (const auto * col_string = checkAndGetColumn<ColumnString>(column.get()))
        {
            const auto & data = col_string->getChars();
            const auto & offsets = col_string->getOffsets();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t data_size = offsets[i] - current_offset;
                contentDefinedCdcOneRow(
                    &res_strings,
                    nullptr,
                    reinterpret_cast<const UInt8 *>(&data[current_offset]),
                    data_size,
                    window_size,
                    reverse_probability,
                    Utf8Boundaries,
                    false);
                res_row_offsets.push_back(res_strings.size());
                current_offset = offsets[i];
            }
        }
        else if (const auto * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            size_t n = col_fixed->getN();
            const auto & data = col_fixed->getChars();

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const UInt8 * row_data = reinterpret_cast<const UInt8 *>(&data[i * n]);
                contentDefinedCdcOneRow(
                    &res_strings, nullptr, row_data, n, window_size, reverse_probability, Utf8Boundaries, false);
                res_row_offsets.push_back(res_strings.size());
            }
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                column->getName(),
                func_name);
        }

        return col_res;
    }
}

ColumnPtr FunctionContentDefinedChunks::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    return executeContentDefinedCdcImpl<false, false>(arguments, name, input_rows_count);
}

ColumnPtr FunctionContentDefinedChunksUTF8::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    return executeContentDefinedCdcImpl<true, false>(arguments, name, input_rows_count);
}

ColumnPtr FunctionContentDefinedChunkOffsets::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    return executeContentDefinedCdcImpl<false, true>(arguments, name, input_rows_count);
}

ColumnPtr FunctionContentDefinedChunkOffsetsUTF8::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    return executeContentDefinedCdcImpl<true, true>(arguments, name, input_rows_count);
}

REGISTER_FUNCTION(RollingHash)
{
    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category_cdc = FunctionDocumentation::Category::StringSplitting;

    /// Fields map to generated reference (docs website, `system.functions`): description, syntax line, args, notes, return type, examples, version, category.
    FunctionDocumentation::Description cdc_chunks_desc = R"(
Content-defined chunking: split input using Buzhash; cut when `(hash % reverse_probability) == 0`.
Min chunk length is `window_size`; max chunk size depends on `reverse_probability` (capped).
)";
    FunctionDocumentation::Arguments cdc_args = {
        {"string", "Input string or binary data.", {"String", "FixedString"}},
        {"window_size", "Sliding window size in bytes. Must be in range [1, 256].", {"UInt*"}},
        {"reverse_probability",
         "Unsigned divisor: cut when `(buzhash % reverse_probability) == 0`. Must be >= 2. Typical values: 256–65536.",
         {"UInt*"}},
    };
    FunctionDocumentation::ReturnedValue cdc_chunks_ret = {"Array of substrings covering the whole input (empty array for empty string).", {"Array(String)"}};
    FunctionDocumentation::Examples cdc_chunks_ex = {
        {"Chunk binary data",
         "SELECT contentDefinedChunks('abcdefghijklmnop', 4, 1000);",
         "Returns an array of chunks with boundaries robust to insertions/deletions elsewhere."},
    };
    FunctionDocumentation cdc_chunks_doc
        = {cdc_chunks_desc, "contentDefinedChunks(string, window_size, reverse_probability)", cdc_args, {}, cdc_chunks_ret, cdc_chunks_ex, introduced_in, category_cdc};
    factory.registerFunction<FunctionContentDefinedChunks>(cdc_chunks_doc);

    FunctionDocumentation::Description cdc_utf8_desc = R"(
Same as `contentDefinedChunks`, but cuts only at UTF-8 code point boundaries so chunks never split a multibyte character.
)";
    FunctionDocumentation cdc_utf8_doc = {
        cdc_utf8_desc,
        "contentDefinedChunksUTF8(string, window_size, reverse_probability)",
        cdc_args,
        {},
        cdc_chunks_ret,
        cdc_chunks_ex,
        introduced_in,
        category_cdc};
    factory.registerFunction<FunctionContentDefinedChunksUTF8>(cdc_utf8_doc);

    FunctionDocumentation::Description cdc_off_desc = R"(
Returns start byte offsets of each chunk (first offset is always 0 for non-empty strings). The concatenation of substrings taken at these offsets to the next offset (or string end) covers the input.
)";
    FunctionDocumentation::ReturnedValue cdc_off_ret = {"Array of UInt64 chunk start positions.", {"Array(UInt64)"}};
    FunctionDocumentation cdc_off_doc = {
        cdc_off_desc,
        "contentDefinedChunkOffsets(string, window_size, reverse_probability)",
        cdc_args,
        {},
        cdc_off_ret,
        cdc_chunks_ex,
        introduced_in,
        category_cdc};
    factory.registerFunction<FunctionContentDefinedChunkOffsets>(cdc_off_doc);

    FunctionDocumentation::Description cdc_off_utf8_desc = R"(
Same as `contentDefinedChunkOffsets`, but only allows boundaries at UTF-8 code point starts.
)";
    FunctionDocumentation cdc_off_utf8_doc = {
        cdc_off_utf8_desc,
        "contentDefinedChunkOffsetsUTF8(string, window_size, reverse_probability)",
        cdc_args,
        {},
        cdc_off_ret,
        cdc_chunks_ex,
        introduced_in,
        category_cdc};
    factory.registerFunction<FunctionContentDefinedChunkOffsetsUTF8>(cdc_off_utf8_doc);
}

}
