#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool splitby_max_substrings_includes_remaining_string;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/** Functions that split strings into an array of strings or vice versa.
  *
  * splitByChar(sep, s[, max_substrings])
  * splitByString(sep, s[, max_substrings])
  * splitByRegexp(regexp, s[, max_substrings])
  *
  * splitByWhitespace(s[, max_substrings])      - split the string by whitespace characters
  * splitByNonAlpha(s[, max_substrings])        - split the string by whitespace and punctuation characters
  *
  * extractAll(s, regexp)     - select from the string the subsequences corresponding to the regexp.
  * - first subpattern, if regexp has subpattern;
  * - zero subpattern (the match part, otherwise);
  * - otherwise, an empty array
  *
  * alphaTokens(s[, max_substrings])            - select from the string subsequence `[a-zA-Z]+`.
  *
  * URL functions are located separately.
  */


/// A function that takes a string, and returns an array of substrings created by some generator.
template <typename Generator>
class FunctionTokens : public IFunction
{
private:
    using Pos = const char *;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = Generator::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTokens>(context); }

    explicit FunctionTokens<Generator>(ContextPtr context)
    {
        const Settings & settings = context->getSettingsRef();
        max_substrings_includes_remaining_string = settings[Setting::splitby_max_substrings_includes_remaining_string];
    }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool isVariadic() const override { return Generator::isVariadic(); }

    size_t getNumberOfArguments() const override { return Generator::getNumberOfArguments(); }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return Generator::getArgumentsThatAreAlwaysConstant(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        Generator::checkArguments(*this, arguments);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        Generator generator;
        generator.init(arguments, max_substrings_includes_remaining_string);

        const auto & array_argument = arguments[generator.strings_argument_position];

        const ColumnString * col_str = checkAndGetColumn<ColumnString>(array_argument.column.get());
        const ColumnConst * col_str_const = checkAndGetColumnConstStringOrFixedString(array_argument.column.get());

        auto col_res = ColumnArray::create(ColumnString::create());

        ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
        ColumnString::Chars & res_strings_chars = res_strings.getChars();
        ColumnString::Offsets & res_strings_offsets = res_strings.getOffsets();

        ColumnArray::Offsets & res_offsets = col_res->getOffsets();

        if (col_str)
        {
            const ColumnString::Chars & src_chars = col_str->getChars();
            const ColumnString::Offsets & src_offsets = col_str->getOffsets();

            res_offsets.reserve(input_rows_count);
            res_strings_offsets.reserve(input_rows_count * 5);    /// Constant 5 - at random.
            res_strings_chars.reserve(src_chars.size());

            Pos token_begin = nullptr;
            Pos token_end = nullptr;

            ColumnString::Offset current_src_offset = 0;
            ColumnArray::Offset current_dst_offset = 0;
            ColumnString::Offset current_dst_strings_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Pos pos = reinterpret_cast<Pos>(&src_chars[current_src_offset]);
                current_src_offset = src_offsets[i];
                Pos end = reinterpret_cast<Pos>(&src_chars[current_src_offset]) - 1;

                generator.set(pos, end);
                size_t j = 0;
                while (generator.get(token_begin, token_end))
                {
                    size_t token_size = token_end - token_begin;

                    res_strings_chars.resize(res_strings_chars.size() + token_size + 1);
                    memcpySmallAllowReadWriteOverflow15(&res_strings_chars[current_dst_strings_offset], token_begin, token_size);
                    res_strings_chars[current_dst_strings_offset + token_size] = 0;

                    current_dst_strings_offset += token_size + 1;
                    res_strings_offsets.push_back(current_dst_strings_offset);
                    ++j;
                }

                current_dst_offset += j;
                res_offsets.push_back(current_dst_offset);
            }

            return col_res;
        }
        else if (col_str_const)
        {
            String src = col_str_const->getValue<String>();
            Array dst;

            generator.set(src.data(), src.data() + src.size());
            Pos token_begin = nullptr;
            Pos token_end = nullptr;

            while (generator.get(token_begin, token_end))
                dst.push_back(String(token_begin, token_end - token_begin));

            return result_type->createColumnConst(col_str_const->size(), dst);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns {}, {} of arguments of function {}",
                    array_argument.column->getName(), array_argument.column->getName(), getName());
    }
};


/// Helper functions for implementations
static inline std::optional<size_t> extractMaxSplits(
    const ColumnsWithTypeAndName & arguments, size_t max_substrings_argument_position)
{
    if (max_substrings_argument_position >= arguments.size())
        return std::nullopt;

    if (const ColumnConst * column = checkAndGetColumn<ColumnConst>(arguments[max_substrings_argument_position].column.get()))
    {
        size_t res = column->getUInt(0);
        if (res)
            return res;
    }

    return std::nullopt;
}

static inline void checkArgumentsWithSeparatorAndOptionalMaxSubstrings(
    const IFunction & func, const ColumnsWithTypeAndName & arguments)
{
    FunctionArgumentDescriptors mandatory_args{
        {"separator", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "const String"},
        {"s", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
    };

    FunctionArgumentDescriptors optional_args{
        {"max_substrings", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), isColumnConst, "const Number"},
    };

    validateFunctionArguments(func, arguments, mandatory_args, optional_args);
}

static inline void checkArgumentsWithOptionalMaxSubstrings(const IFunction & func, const ColumnsWithTypeAndName & arguments)
{
    FunctionArgumentDescriptors mandatory_args{
        {"s", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
    };

    FunctionArgumentDescriptors optional_args{
        {"max_substrings", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), isColumnConst, "const Number"},
    };

    validateFunctionArguments(func, arguments, mandatory_args, optional_args);
}

}
