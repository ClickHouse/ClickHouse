#pragma once

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/Regexps.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}


/** Functions that split strings into an array of strings or vice versa.
  *
  * splitByChar(sep, s)
  * splitByString(sep, s)
  * splitByRegexp(regexp, s)
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
  * alphaTokens(s)            - select from the string subsequence `[a-zA-Z]+`.
  *
  * URL functions are located separately.
  */


using Pos = const char *;


/// Substring generators. All of them have a common interface.

class AlphaTokensImpl
{
private:
    Pos pos;
    Pos end;

public:
    /// Get the name of the function.
    static constexpr auto name = "alphaTokens";
    static String getName() { return name; }

    static size_t getNumberOfArguments() { return 1; }

    /// Check the type of the function's arguments.
    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Initialize by the function arguments.
    void init(const ColumnsWithTypeAndName & /*arguments*/) {}

    /// Called for each next string.
    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
    }

    /// Returns the position of the argument, that is the column of strings
    size_t getStringsArgumentPosition()
    {
        return 0;
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

        while (pos < end && isAlphaASCII(*pos))
            ++pos;

        token_end = pos;

        return true;
    }
};


class SplitByCharImpl
{
private:
    Pos pos;
    Pos end;

    char sep;

public:
    static constexpr auto name = "splitByChar";
    static String getName() { return name; }
    static size_t getNumberOfArguments() { return 2; }

    static void checkArguments(const DataTypes & arguments)
    {
        if (!isString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void init(const ColumnsWithTypeAndName & arguments)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception("Illegal column " + arguments[0].column->getName()
                + " of first argument of function " + getName() + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

        String sep_str = col->getValue<String>();

        if (sep_str.size() != 1)
            throw Exception("Illegal separator for function " + getName() + ". Must be exactly one byte.", ErrorCodes::BAD_ARGUMENTS);

        sep = sep_str[0];
    }

    /// Returns the position of the argument, that is the column of strings
    size_t getStringsArgumentPosition()
    {
        return 1;
    }

    void set(Pos pos_, Pos end_)
    {
        pos = pos_;
        end = end_;
    }

    bool get(Pos & token_begin, Pos & token_end)
    {
        if (!pos)
            return false;

        token_begin = pos;
        pos = reinterpret_cast<Pos>(memchr(pos, sep, end - pos));

        if (pos)
        {
            token_end = pos;
            ++pos;
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

    String sep;

public:
    static constexpr auto name = "splitByString";
    static String getName() { return name; }
    static size_t getNumberOfArguments() { return 2; }

    static void checkArguments(const DataTypes & arguments)
    {
        SplitByCharImpl::checkArguments(arguments);
    }

    void init(const ColumnsWithTypeAndName & arguments)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[0].column.get());

        if (!col)
            throw Exception("Illegal column " + arguments[0].column->getName()
                + " of first argument of function " + getName() + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

        sep = col->getValue<String>();
    }

    /// Returns the position of the argument that is the column of strings
    size_t getStringsArgumentPosition()
    {
        return 1;
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
        if (sep.empty())
        {
            if (pos == end)
                return false;

            token_begin = pos;
            pos += 1;
            token_end = pos;
        }
        else
        {
            if (!pos)
                return false;

            token_begin = pos;

            pos = reinterpret_cast<Pos>(memmem(pos, end - pos, sep.data(), sep.size()));

            if (pos)
            {
                token_end = pos;
                pos += sep.size();
            }
            else
                token_end = end;
        }

        return true;
    }
};

class ExtractAllImpl
{
private:
    Regexps::Pool::Pointer re;
    OptimizedRegularExpression::MatchVec matches;
    size_t capture;

    Pos pos;
    Pos end;
public:
    static constexpr auto name = "extractAll";
    static String getName() { return name; }
    static size_t getNumberOfArguments() { return 2; }

    /// Check the type of function arguments.
    static void checkArguments(const DataTypes & arguments)
    {
        SplitByStringImpl::checkArguments(arguments);
    }

    /// Initialize by the function arguments.
    void init(const ColumnsWithTypeAndName & arguments)
    {
        const ColumnConst * col = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());

        if (!col)
            throw Exception("Illegal column " + arguments[1].column->getName()
                + " of first argument of function " + getName() + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

        re = Regexps::get<false, false>(col->getValue<String>());
        capture = re->getNumberOfSubpatterns() > 0 ? 1 : 0;

        matches.resize(capture + 1);
    }

    /// Returns the position of the argument that is the column of strings
    size_t getStringsArgumentPosition()
    {
        return 0;
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
public:
    static constexpr auto name = Generator::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTokens>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return Generator::getNumberOfArguments(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        Generator::checkArguments(arguments);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        Generator generator;
        generator.init(arguments);
        const auto & array_argument = arguments[generator.getStringsArgumentPosition()];

        const ColumnString * col_str = checkAndGetColumn<ColumnString>(array_argument.column.get());
        const ColumnConst * col_const_str =
                checkAndGetColumnConstStringOrFixedString(array_argument.column.get());

        auto col_res = ColumnArray::create(ColumnString::create());
        ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
        ColumnArray::Offsets & res_offsets = col_res->getOffsets();
        ColumnString::Chars & res_strings_chars = res_strings.getChars();
        ColumnString::Offsets & res_strings_offsets = res_strings.getOffsets();

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
        else if (col_const_str)
        {
            String src = col_const_str->getValue<String>();
            Array dst;

            generator.set(src.data(), src.data() + src.size());
            Pos token_begin = nullptr;
            Pos token_end = nullptr;

            while (generator.get(token_begin, token_end))
                dst.push_back(String(token_begin, token_end - token_begin));

            return result_type->createColumnConst(col_const_str->size(), dst);
        }
        else
            throw Exception("Illegal columns " + array_argument.column->getName()
                    + ", " + array_argument.column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Joins an array of strings into one string via a separator.
class FunctionArrayStringConcat : public IFunction
{
private:
    void executeInternal(
        const ColumnString::Chars & src_chars,
        const ColumnString::Offsets & src_string_offsets,
        const ColumnArray::Offsets & src_array_offsets,
        const char * delimiter, const size_t delimiter_size,
        ColumnString::Chars & dst_chars,
        ColumnString::Offsets & dst_string_offsets) const
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
        ColumnString::Offset current_src_string_offset = 0;

        ColumnString::Offset current_dst_string_offset = 0;

        /// Loop through the array of strings.
        for (size_t i = 0; i < size; ++i)
        {
            /// Loop through the rows within the array. /// NOTE You can do everything in one copy, if the separator has a size of 1.
            for (auto next_src_array_offset = src_array_offsets[i]; current_src_array_offset < next_src_array_offset; ++current_src_array_offset)
            {
                size_t bytes_to_copy = src_string_offsets[current_src_array_offset] - current_src_string_offset - 1;

                memcpySmallAllowReadWriteOverflow15(
                    &dst_chars[current_dst_string_offset], &src_chars[current_src_string_offset], bytes_to_copy);

                current_src_string_offset = src_string_offsets[current_src_array_offset];
                current_dst_string_offset += bytes_to_copy;

                if (current_src_array_offset + 1 != next_src_array_offset)
                {
                    memcpy(&dst_chars[current_dst_string_offset], delimiter, delimiter_size);
                    current_dst_string_offset += delimiter_size;
                }
            }

            dst_chars[current_dst_string_offset] = 0;
            ++current_dst_string_offset;

            dst_string_offsets[i] = current_dst_string_offset;
        }

        dst_chars.resize(dst_string_offsets.back());
    }

public:
    static constexpr auto name = "arrayStringConcat";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayStringConcat>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type || !isString(array_type->getNestedType()))
            throw Exception("First argument for function " + getName() + " must be array of strings.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2
            && !isString(arguments[1]))
            throw Exception("Second argument for function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        String delimiter;
        if (arguments.size() == 2)
        {
            const ColumnConst * col_delim = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());
            if (!col_delim)
                throw Exception("Second argument for function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);

            delimiter = col_delim->getValue<String>();
        }

        if (const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get()))
        {
            Array src_arr = col_const_arr->getValue<Array>();
            String dst_str;
            for (size_t i = 0, size = src_arr.size(); i < size; ++i)
            {
                if (i != 0)
                    dst_str += delimiter;
                dst_str += src_arr[i].get<const String &>();
            }

            return result_type->createColumnConst(col_const_arr->size(), dst_str);
        }
        else
        {
            const ColumnArray & col_arr = assert_cast<const ColumnArray &>(*arguments[0].column);
            const ColumnString & col_string = assert_cast<const ColumnString &>(col_arr.getData());

            auto col_res = ColumnString::create();

            executeInternal(
                col_string.getChars(), col_string.getOffsets(), col_arr.getOffsets(),
                delimiter.data(), delimiter.size(),
                col_res->getChars(), col_res->getOffsets());

            return col_res;
        }
    }
};


using FunctionAlphaTokens = FunctionTokens<AlphaTokensImpl>;
using FunctionSplitByChar = FunctionTokens<SplitByCharImpl>;
using FunctionSplitByString = FunctionTokens<SplitByStringImpl>;
using FunctionExtractAll = FunctionTokens<ExtractAllImpl>;

}
