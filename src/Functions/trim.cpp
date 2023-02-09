#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <base/find_symbols.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

struct TrimModeLeft
{
    static constexpr auto name = "trimLeft";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = false;
};

struct TrimModeRight
{
    static constexpr auto name = "trimRight";
    static constexpr bool trim_left = false;
    static constexpr bool trim_right = true;
};

struct TrimModeBoth
{
    static constexpr auto name = "trimBoth";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = true;
};

template <typename Mode>
class FunctionTrimImpl
{
public:
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        const UInt8 * start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
        {
            execute(reinterpret_cast<const UInt8 *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);

            res_data.resize(res_data.size() + length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length + 1;
            res_data[res_offset - 1] = '\0';

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Functions trimLeft, trimRight and trimBoth cannot work with FixedString argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:
    static void execute(const UInt8 * data, size_t size, const UInt8 *& res_data, size_t & res_size)
    {
        const char * char_data = reinterpret_cast<const char *>(data);
        const char * char_end = char_data + size;

        if constexpr (Mode::trim_left)
        { // NOLINT
            const char * found = find_first_not_symbols<' '>(char_data, char_end);
            size_t num_chars = found - char_data;
            char_data += num_chars;
        }

        if constexpr (Mode::trim_right)
        { // NOLINT
            const char * found = find_last_not_symbols_or_null<' '>(char_data, char_end);
            if (found)
                char_end = found + 1;
            else
                char_end = char_data;
        }

        res_data = reinterpret_cast<const UInt8 *>(char_data);
        res_size = char_end - char_data;
    }
};

using FunctionTrimLeft = FunctionStringToString<FunctionTrimImpl<TrimModeLeft>, TrimModeLeft>;
using FunctionTrimRight = FunctionStringToString<FunctionTrimImpl<TrimModeRight>, TrimModeRight>;
using FunctionTrimBoth = FunctionStringToString<FunctionTrimImpl<TrimModeBoth>, TrimModeBoth>;

}

void registerFunctionTrim(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTrimLeft>();
    factory.registerFunction<FunctionTrimRight>();
    factory.registerFunction<FunctionTrimBoth>();
}
}
