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
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <Functions/Regexps.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
  * arrayStringConcat(arr)
  * arrayStringConcat(arr, delimiter)
  * - join an array of strings into one string via a separator.
  *
  * alphaTokens(s[, max_substrings])            - select from the string subsequence `[a-zA-Z]+`.
  *
  * URL functions are located separately.
  */


using Pos = const char *;

std::optional<size_t> extractMaxSplits(const ColumnsWithTypeAndName & arguments, size_t max_substrings_argument_position);

/// Substring generators. All of them have a common interface.

class SplitByAlphaImpl
{
private:
    Pos pos;
    Pos end;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "alphaTokens";
    static String getName() { return name; }

    static bool isVariadic() { return true; }

    static size_t getNumberOfArguments() { return 0; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"s", &isString<IDataType>, nullptr, "String"},
        };

        FunctionArgumentDescriptors optional_args{
            {"max_substrings", &isNativeInteger<IDataType>, isColumnConst, "const Number"},
        };

        validateFunctionArgumentTypes(func, arguments, mandatory_args, optional_args);
    }

    static constexpr auto strings_argument_position = 0uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 1);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        /// Skip garbage
        while (pos < end && !isAlphaASCII(*pos))
            ++pos;

        if (pos == end)
            return false;

        token_begin = pos;

        if (max_splits)
        {
            if (max_substrings_includes_remaining_string)
            {
                if (splits == *max_splits - 1)
                {
                    token_end = end;
                    pos = end;
                    return true;
                }
            }
            else
                if (splits == *max_splits)
                    return false;
        }

        while (pos < end && isAlphaASCII(*pos))
            ++pos;

        token_end = pos;
        ++splits;

        return true;
    }
};

class SplitByNonAlphaImpl
{
private:
    Pos pos;
    Pos end;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    /// Get the name of the function.
    static constexpr auto name = "splitByNonAlpha";
    static String getName() { return name; }

    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        SplitByAlphaImpl::checkArguments(func, arguments);
    }

    static constexpr auto strings_argument_position = 0uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 1);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        /// Skip garbage
        while (pos < end && (isWhitespaceASCII(*pos) || isPunctuationASCII(*pos)))
            ++pos;

        if (pos == end)
            return false;

        token_begin = pos;

        if (max_splits)
        {
            if (max_substrings_includes_remaining_string)
            {
                if (splits == *max_splits - 1)
                {
                    token_end = end;
                    pos = end;
                    return true;
                }
            }
            else
                if (splits == *max_splits)
                    return false;
        }

        while (pos < end && !(isWhitespaceASCII(*pos) || isPunctuationASCII(*pos)))
            ++pos;

        token_end = pos;
        splits++;

        return true;
    }
};

class SplitByWhitespaceImpl
{
private:
    Pos pos;
    Pos end;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByWhitespace";
    static String getName() { return name; }

    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        return SplitByNonAlphaImpl::checkArguments(func, arguments);
    }

    static constexpr auto strings_argument_position = 0uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 1);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        /// Skip garbage
        while (pos < end && isWhitespaceASCII(*pos))
            ++pos;

        if (pos == end)
            return false;

        token_begin = pos;

        if (max_splits)
        {
            if (max_substrings_includes_remaining_string)
            {
                if (splits == *max_splits - 1)
                {
                    token_end = end;
                    pos = end;
                    return true;
                }
            }
            else
                if (splits == *max_splits)
                    return false;
        }

        while (pos < end && !isWhitespaceASCII(*pos))
            ++pos;

        token_end = pos;
        splits++;

        return true;
    }
};

class SplitByCharImpl
{
private:
    Pos pos;
    Pos end;
    char separator;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByChar";
    static String getName() { return name; }
    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"separator", &isString<IDataType>, isColumnConst, "const String"},
            {"s", &isString<IDataType>, nullptr, "String"}
        };

        FunctionArgumentDescriptors optional_args{
            {"max_substrings", &isNativeInteger<IDataType>, isColumnConst, "const Number"},
        };

        validateFunctionArgumentTypes(func, arguments, mandatory_args, optional_args);
    }

    static constexpr auto strings_argument_position = 1uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be constant string.", arguments[0].column->getName(), getName());

        String sep_str = col->getValue<String>();

        if (sep_str.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal separator for function {}. Must be exactly one byte.", getName());

        separator = sep_str[0];

        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 2);
    }

    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!pos)
            return false;

        token_begin = pos;

        if (max_splits)
        {
            if (max_substrings_includes_remaining_string)
            {
                if (splits == *max_splits - 1)
                {
                    token_end = end;
                    pos = nullptr;
                    return true;
                }
            }
            else
               if (splits == *max_splits)
                   return false;
        }

        pos = reinterpret_cast<Pos>(memchr(pos, separator, end - pos));
        if (pos)
        {
            token_end = pos;
            ++pos;
            ++splits;
        }
        else
            token_end = end;

        return true;
    }
};


class SplitByStringImpl
{
private:
    Pos pos;
    Pos end;
    String separator;
    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByString";
    static String getName() { return name; }
    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        SplitByCharImpl::checkArguments(func, arguments);
    }

    static constexpr auto strings_argument_position = 1uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be constant string.", arguments[0].column->getName(), getName());

        separator = col->getValue<String>();

        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 2);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (separator.empty())
        {
            if (pos == end)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = end;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            pos += 1;
            token_end = pos;
            ++splits;
        }
        else
        {
            if (!pos)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = nullptr;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            pos = reinterpret_cast<Pos>(memmem(pos, end - pos, separator.data(), separator.size()));
            if (pos)
            {
                token_end = pos;
                pos += separator.size();
                ++splits;
            }
            else
                token_end = end;
        }

        return true;
    }
};

class SplitByRegexpImpl
{
private:
    Regexps::RegexpPtr re;
    OptimizedRegularExpression::MatchVec matches;

    Pos pos;
    Pos end;

    std::optional<size_t> max_splits;
    size_t splits;
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = "splitByRegexp";
    static String getName() { return name; }

    static bool isVariadic() { return true; }
    static size_t getNumberOfArguments() { return 0; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        SplitByStringImpl::checkArguments(func, arguments);
    }

    static constexpr auto strings_argument_position = 1uz;

    void init(const ColumnsWithTypeAndName & arguments, bool max_substrings_includes_remaining_string_)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                            "Must be constant string.", arguments[0].column->getName(), getName());

        if (!col->getValue<String>().empty())
            re = std::make_shared<OptimizedRegularExpression>(Regexps::createRegexp<false, false, false>(col->getValue<String>()));

        max_substrings_includes_remaining_string = max_substrings_includes_remaining_string_;
        max_splits = extractMaxSplits(arguments, 2);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
        splits = 0;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!re)
        {
            if (pos == end)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = end;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            pos += 1;
            token_end = pos;
            ++splits;
        }
        else
        {
            if (!pos || pos > end)
                return false;

            token_begin = pos;

            if (max_splits)
            {
                if (max_substrings_includes_remaining_string)
                {
                    if (splits == *max_splits - 1)
                    {
                        token_end = end;
                        pos = nullptr;
                        return true;
                    }
                }
                else
                    if (splits == *max_splits)
                        return false;
            }

            if (!re->match(pos, end - pos, matches) || !matches[0].length)
            {
                token_end = end;
                pos = end + 1;
            }
            else
            {
                token_end = pos + matches[0].offset;
                pos = token_end + matches[0].length;
                ++splits;
            }
        }

        return true;
    }
};

class ExtractAllImpl
{
private:
    Regexps::RegexpPtr re;
    OptimizedRegularExpression::MatchVec matches;
    size_t capture;

    Pos pos;
    Pos end;
public:
    static constexpr auto name = "extractAll";
    static String getName() { return name; }
    static bool isVariadic() { return false; }
    static size_t getNumberOfArguments() { return 2; }

    static void checkArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
    {
        FunctionArgumentDescriptors mandatory_args{
            {"haystack", &isString<IDataType>, nullptr, "String"},
            {"pattern", &isString<IDataType>, isColumnConst, "const String"}
        };

        validateFunctionArgumentTypes(func, arguments, mandatory_args);
    }

    static constexpr auto strings_argument_position = 0uz;

    void init(const ColumnsWithTypeAndName & arguments, bool /*max_substrings_includes_remaining_string*/)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());

        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}. "
                "Must be constant string.", arguments[1].column->getName(), getName());

        re = std::make_shared<OptimizedRegularExpression>(Regexps::createRegexp<false, false, false>(col->getValue<String>()));
        capture = re->getNumberOfSubpatterns() > 0 ? 1 : 0;

        matches.resize(capture + 1);
    }

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
    }

    /// Get the next token, if any, or return false.
    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!pos || pos > end)
            return false;

        if (!re->match(pos, end - pos, matches) || !matches[0].length)
            return false;

        if (matches[capture].offset == std::string::npos)
        {
            /// Empty match.
            token_begin = pos;
            token_end = pos;
        }
        else
        {
            token_begin = pos + matches[capture].offset;
            token_end = token_begin + matches[capture].length;
        }

        pos += matches[0].offset + matches[0].length;

        return true;
    }
};

/// A function that takes a string, and returns an array of substrings created by some generator.
template <typename Generator>
class FunctionTokens : public IFunction
{
private:
    bool max_substrings_includes_remaining_string;

public:
    static constexpr auto name = Generator::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTokens>(context); }

    explicit FunctionTokens<Generator>(ContextPtr context)
    {
        const Settings & settings = context->getSettingsRef();
        max_substrings_includes_remaining_string = settings.splitby_max_substrings_includes_remaining_string;
    }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool isVariadic() const override { return Generator::isVariadic(); }

    size_t getNumberOfArguments() const override { return Generator::getNumberOfArguments(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        Generator::checkArguments(*this, arguments);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
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

            res_offsets.reserve(src_offsets.size());
            res_strings_offsets.reserve(src_offsets.size() * 5);    /// Constant 5 - at random.
            res_strings_chars.reserve(src_chars.size());

            Pos token_begin = nullptr;
            Pos token_end = nullptr;

            size_t size = src_offsets.size();
            ColumnString::Offset current_src_offset = 0;
            ColumnArray::Offset current_dst_offset = 0;
            ColumnString::Offset current_dst_strings_offset = 0;
            for (size_t i = 0; i < size; ++i)
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


/// Joins an array of type serializable to string into one string via a separator.
class FunctionArrayStringConcat : public IFunction
{
private:
    static void executeInternal(
        const ColumnString::Chars & src_chars,
        const ColumnString::Offsets & src_string_offsets,
        const ColumnArray::Offsets & src_array_offsets,
        const char * delimiter,
        const size_t delimiter_size,
        ColumnString::Chars & dst_chars,
        ColumnString::Offsets & dst_string_offsets,
        const char8_t * null_map)
    {
        size_t size = src_array_offsets.size();

        if (!size)
            return;

        /// With a small margin - as if the separator goes after the last string of the array.
        dst_chars.resize(
            src_chars.size()
            + delimiter_size * src_string_offsets.size()    /// Separators after each string...
            + src_array_offsets.size()                      /// Zero byte after each joined string
            - src_string_offsets.size());                   /// The former zero byte after each string of the array

        /// There will be as many strings as there were arrays.
        dst_string_offsets.resize(src_array_offsets.size());

        ColumnArray::Offset current_src_array_offset = 0;

        ColumnString::Offset current_dst_string_offset = 0;

        /// Loop through the array of strings.
        for (size_t i = 0; i < size; ++i)
        {
            bool first_non_null = true;
            /// Loop through the rows within the array. /// NOTE You can do everything in one copy, if the separator has a size of 1.
            for (auto next_src_array_offset = src_array_offsets[i]; current_src_array_offset < next_src_array_offset; ++current_src_array_offset)
            {
                if (null_map && null_map[current_src_array_offset]) [[unlikely]]
                    continue;

                if (!first_non_null)
                {
                    memcpy(&dst_chars[current_dst_string_offset], delimiter, delimiter_size);
                    current_dst_string_offset += delimiter_size;
                }
                first_non_null = false;

                const auto current_src_string_offset = current_src_array_offset ? src_string_offsets[current_src_array_offset - 1] : 0;
                size_t bytes_to_copy = src_string_offsets[current_src_array_offset] - current_src_string_offset - 1;

                memcpySmallAllowReadWriteOverflow15(
                    &dst_chars[current_dst_string_offset], &src_chars[current_src_string_offset], bytes_to_copy);

                current_dst_string_offset += bytes_to_copy;
            }

            dst_chars[current_dst_string_offset] = 0;
            ++current_dst_string_offset;

            dst_string_offsets[i] = current_dst_string_offset;
        }

        dst_chars.resize(dst_string_offsets.back());
    }

    static void executeInternal(
        const ColumnString & col_string,
        const ColumnArray & col_arr,
        const String & delimiter,
        ColumnString & col_res,
        const char8_t * null_map = nullptr)
    {
        executeInternal(
            col_string.getChars(),
            col_string.getOffsets(),
            col_arr.getOffsets(),
            delimiter.data(),
            delimiter.size(),
            col_res.getChars(),
            col_res.getOffsets(),
            null_map);
    }

    static ColumnPtr serializeNestedColumn(const ColumnArray & col_arr, const DataTypePtr & nested_type)
    {
        if (isString(nested_type))
        {
            return col_arr.getDataPtr();
        }
        else if (const ColumnNullable * col_nullable = checkAndGetColumn<ColumnNullable>(col_arr.getData());
                 col_nullable && isString(col_nullable->getNestedColumn().getDataType()))
        {
            return col_nullable->getNestedColumnPtr();
        }
        else
        {
            ColumnsWithTypeAndName cols;
            cols.emplace_back(col_arr.getDataPtr(), nested_type, "tmp");
            return ConvertImplGenericToString<ColumnString>::execute(cols, std::make_shared<DataTypeString>(), col_arr.size());
        }
    }

public:
    static constexpr auto name = "arrayStringConcat";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayStringConcat>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        String delimiter;
        if (arguments.size() == 2)
        {
            const ColumnConst * col_delim = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());
            if (!col_delim)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be constant string.", getName());

            delimiter = col_delim->getValue<String>();
        }

        const auto & nested_type = assert_cast<const DataTypeArray &>(*arguments[0].type).getNestedType();
        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get());
            col_const_arr && isString(nested_type))
        {
            Array src_arr = col_const_arr->getValue<Array>();
            String dst_str;
            bool first_non_null = true;
            for (size_t i = 0, size = src_arr.size(); i < size; ++i)
            {
                if (src_arr[i].isNull())
                    continue;
                if (!first_non_null)
                    dst_str += delimiter;
                first_non_null = false;
                dst_str += src_arr[i].get<const String &>();
            }

            return result_type->createColumnConst(col_const_arr->size(), dst_str);
        }

        ColumnPtr src_column = arguments[0].column->convertToFullColumnIfConst();
        const ColumnArray & col_arr = assert_cast<const ColumnArray &>(*src_column.get());

        ColumnPtr str_subcolumn = serializeNestedColumn(col_arr, nested_type);
        const ColumnString & col_string = assert_cast<const ColumnString &>(*str_subcolumn.get());

        auto col_res = ColumnString::create();
        if (const ColumnNullable * col_nullable = checkAndGetColumn<ColumnNullable>(col_arr.getData()))
            executeInternal(col_string, col_arr, delimiter, *col_res, col_nullable->getNullMapData().data());
        else
            executeInternal(col_string, col_arr, delimiter, *col_res);
        return col_res;
    }
};


using FunctionSplitByAlpha = FunctionTokens<SplitByAlphaImpl>;
using FunctionSplitByNonAlpha = FunctionTokens<SplitByNonAlphaImpl>;
using FunctionSplitByWhitespace = FunctionTokens<SplitByWhitespaceImpl>;
using FunctionSplitByChar = FunctionTokens<SplitByCharImpl>;
using FunctionSplitByString = FunctionTokens<SplitByStringImpl>;
using FunctionSplitByRegexp = FunctionTokens<SplitByRegexpImpl>;
using FunctionExtractAll = FunctionTokens<ExtractAllImpl>;

}
