#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include "Columns/IColumn.h"
#include "Functions/IFunction.h"
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <base/find_symbols.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

template <typename Mode>
class FunctionTrim : public IFunction
{
public:
    static constexpr auto name = Mode::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTrim<Mode>>(); }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"input_string", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}
        };

        FunctionArgumentDescriptors optional_args{
            {"trim_character", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::optional<SearchSymbols> custom_trim_characters;
        if (arguments.size() == 2 && input_rows_count > 0)
        {
            const ColumnConst * col_trim_characters_const = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
            const String & trim_characters_string = col_trim_characters_const->getDataAt(0).toString();
            custom_trim_characters = std::make_optional<SearchSymbols>(trim_characters_string);
        }

        ColumnPtr col_input_full;
        col_input_full = arguments[0].column->convertToFullColumnIfConst();

        auto col_res = ColumnString::create();
        if (const ColumnString * col_input_string = checkAndGetColumn<ColumnString>(col_input_full.get()))
        {
            vector(
                col_input_string->getChars(), col_input_string->getOffsets(),
                custom_trim_characters,
                col_res->getChars(), col_res->getOffsets(),
                input_rows_count);
        }
        else if (const ColumnFixedString * col_input_fixed_string = checkAndGetColumn<ColumnFixedString>(col_input_full.get()))
        {
            vectorFixed(
                col_input_fixed_string->getChars(), col_input_fixed_string->getN(),
                custom_trim_characters,
                col_res->getChars(),
                col_res->getOffsets(),
                input_rows_count);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
        }

        return col_res;
    }

    static void vector(
        const ColumnString::Chars & input_data,
        const ColumnString::Offsets & input_offsets,
        const std::optional<SearchSymbols> & custom_trim_characters,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_offsets.resize_exact(input_rows_count);
        res_data.reserve_exact(input_data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        const UInt8 * start;
        size_t length;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            execute(reinterpret_cast<const UInt8 *>(&input_data[prev_offset]), input_offsets[i] - prev_offset - 1, custom_trim_characters, start, length);

            res_data.resize(res_data.size() + length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length + 1;
            res_data[res_offset - 1] = '\0';

            res_offsets[i] = res_offset;
            prev_offset = input_offsets[i];
        }
    }

    static void vectorFixed(
        const ColumnString::Chars & input_data,
        size_t n,
        const std::optional<SearchSymbols> & custom_trim_characters,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_offsets.resize_exact(input_rows_count);
        res_data.reserve_exact(input_data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        const UInt8 * start;
        size_t length;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            execute(reinterpret_cast<const UInt8 *>(&input_data[prev_offset]), n, custom_trim_characters, start, length);

            res_data.resize(res_data.size() + length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length + 1;
            res_data[res_offset - 1] = '\0';

            res_offsets[i] = res_offset;
            prev_offset += n;
        }
    }

    static void execute(const UInt8 * data, size_t size, const std::optional<SearchSymbols> & custom_trim_characters, const UInt8 *& res_data, size_t & res_size)
    {
        const char * char_begin = reinterpret_cast<const char *>(data);
        const char * char_end = char_begin + size;

        if constexpr (Mode::trim_left)
        {
            const char * found = nullptr;
            if (!custom_trim_characters)
                found = find_first_not_symbols<' '>(char_begin, char_end);
            else
            {
                std::string_view input(char_begin, char_end);
                found = find_first_not_symbols(input, *custom_trim_characters);
            }
            size_t num_chars = found - char_begin;
            char_begin += num_chars;
        }
        if constexpr (Mode::trim_right)
        {
            const char * found = nullptr;
            if (!custom_trim_characters)
                found = find_last_not_symbols_or_null<' '>(char_begin, char_end);
            else
            {
                std::string_view input(char_begin, char_end);
                found = find_last_not_symbols_or_null(input, *custom_trim_characters);
            }
            if (found)
                char_end = found + 1;
            else
                char_end = char_begin;
        }

        res_data = reinterpret_cast<const UInt8 *>(char_begin);
        res_size = char_end - char_begin;
    }
};

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

using FunctionTrimLeft = FunctionTrim<TrimModeLeft>;
using FunctionTrimRight = FunctionTrim<TrimModeRight>;
using FunctionTrimBoth = FunctionTrim<TrimModeBoth>;

}

REGISTER_FUNCTION(Trim)
{
    factory.registerFunction<FunctionTrimLeft>();
    factory.registerFunction<FunctionTrimRight>();
    factory.registerFunction<FunctionTrimBoth>();
    factory.registerAlias("ltrim", FunctionTrimLeft::name);
    factory.registerAlias("rtrim", FunctionTrimRight::name);
    factory.registerAlias("trim", FunctionTrimBoth::name);
}
}
