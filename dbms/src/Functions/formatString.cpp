#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <ext/range.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

template <typename Impl, typename Name>
class FormatFunction : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FormatFunction>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                + ", should be at least 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > Impl::ArgumentThreshold)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                + ", should be at most " + std::to_string(Impl::ArgumentThreshold),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!isStringOrFixedString(arg))
                throw Exception("Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr c0 = block.getByPosition(arguments[0]).column;
        const ColumnConst * c0_const_string = typeid_cast<const ColumnConst *>(&*c0);

        if (!c0_const_string)
            throw Exception("First argument of function " + getName() + " must be constant string", ErrorCodes::ILLEGAL_COLUMN);

        String pattern = c0_const_string->getValue<String>();

        auto col_res = ColumnString::create();
        std::vector<const ColumnString::Chars *> data(arguments.size() - 1);
        std::vector<const ColumnString::Offsets *> offsets(arguments.size() - 1);
        std::vector<size_t> fixed_string_N(arguments.size() - 1);
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const ColumnPtr column = block.getByPosition(arguments[i]).column;
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
            {
                data[i - 1] = std::addressof(col->getChars());
                offsets[i - 1] = std::addressof(col->getOffsets());
            }
            else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
            {
                data[i - 1] = std::addressof(fixed_col->getChars());
                fixed_string_N[i - 1] = fixed_col->getN();
            }
            else
                throw Exception(
                    "Illegal column " + column->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
        Impl::vector(std::move(pattern), data, offsets, fixed_string_N, col_res->getChars(), col_res->getOffsets(), input_rows_count);

        block.getByPosition(result).column = std::move(col_res);
    }
};


struct FormatImpl
{

    static constexpr size_t ArgumentThreshold = 1024;
    static constexpr size_t RightPadding = 15;

    static void parseNumber(const String & description, UInt64 l, UInt64 r, UInt64 & res)
    {
        res = 0;
        for (UInt64 pos = l; pos < r; pos ++)
        {
            if (!isNumericASCII(description[pos]))
                throw Exception("Not a number in curly braces at position " + std::to_string(pos), ErrorCodes::LOGICAL_ERROR);
            res = res * 10 + description[pos] - '0';
            if (res >= ArgumentThreshold)
                throw Exception("Too big number for arguments, must be at most " + std::to_string(ArgumentThreshold), ErrorCodes::LOGICAL_ERROR);
        }
    }

    static void vector(
        String pattern,
        const std::vector<const ColumnString::Chars *> & data,
        const std::vector<const ColumnString::Offsets *> & offsets,
        const std::vector<size_t> & fixed_string_N,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {

        /// The subsequent indexes of strings we should use. e.g `Hello world {1} {3} {1} {0}` this array will be filled with [1, 3, 1, 0, ... (garbage)].
        UInt64 index_positions[ArgumentThreshold];
        UInt64 * index_positions_ptr = index_positions;

        /// Is current position is after open curly brace.
        bool is_open_curly = false;
        /// The position of last open token.
        size_t last_open = -1;

        /// Is formatting in a plain {} token.
        std::optional<bool> is_plain_numbering;
        UInt64 index_if_plain = 0;

        /// Vector of substrings of pattern that will be copied to the ans, not string view because of escaping and iterators invalidation.
        /// These are exactly what is between {} tokens, for `Hello {} world {}` we will have [`Hello `, ` world `, ``].
        std::vector<std::string> substrings;

        /// Left position of adding substrings, just to the closed brace position or the start of the string.
        /// Invariant --- the start of substring is in this position.
        size_t start_pos = 0;

        for (size_t i = 0; i < pattern.size(); ++i)
        {
            if (pattern[i] == '{')
            {
                /// Escaping handling
                /// It is safe to access because of null termination
                if (pattern[i + 1] == '{')
                {
                    ++i;
                    continue;
                }

                if (is_open_curly)
                    throw Exception("Two open curly braces without close one at position " + std::to_string(i), ErrorCodes::LOGICAL_ERROR);

                substrings.emplace_back(pattern.data() + start_pos, i - start_pos);

                is_open_curly = true;
                last_open = i + 1;
            }
            else if (pattern[i] == '}')
            {
                if (pattern[i + 1] == '}')
                {
                    ++i;
                    continue;
                }

                if (!is_open_curly)
                    throw Exception("Closed curly brace without open one at position " + std::to_string(i), ErrorCodes::LOGICAL_ERROR);

                is_open_curly = false;

                if (last_open == i)
                {
                    if (is_plain_numbering && !*is_plain_numbering)
                        throw Exception("Cannot switch from automatic field numbering to manual field specification", ErrorCodes::LOGICAL_ERROR);
                    is_plain_numbering = true;
                    if (index_if_plain >= offsets.size())
                        throw Exception("Argument is too big for formatting", ErrorCodes::LOGICAL_ERROR);
                    *index_positions_ptr = index_if_plain++;
                    ++index_positions_ptr;
                }
                else
                {
                    if (is_plain_numbering && *is_plain_numbering)
                        throw Exception("Cannot switch from automatic field numbering to manual field specification", ErrorCodes::LOGICAL_ERROR);
                    is_plain_numbering = false;

                    UInt64 arg;
                    parseNumber(pattern, last_open, i, arg);

                    if (arg >= offsets.size())
                        throw Exception("Argument is too big for formatting. Note that indexing starts from zero", ErrorCodes::LOGICAL_ERROR);

                    *index_positions_ptr = arg;
                    ++index_positions_ptr;
                }

                start_pos = i + 1;
            }
        }

        if (is_open_curly)
            throw Exception("Last open curly brace is not closed", ErrorCodes::LOGICAL_ERROR);

        substrings.emplace_back(pattern.data() + start_pos, pattern.size() - start_pos);


        UInt64 final_size = 0;

        for (std::string& str : substrings)
        {
            /// Handling double braces (escaping).
            size_t i = 0;
            bool should_delete = true;
            str.erase(std::remove_if(str.begin(), str.end(),
                [&i, &should_delete, &str](char)
                {
                    bool is_double_brace = (str[i] == '{' && str[i + 1] == '{') || (str[i] == '}' && str[i + 1] == '}');
                    ++i;
                    if (is_double_brace && should_delete)
                    {
                        should_delete = false;
                        return true;
                    }
                    should_delete = true;
                    return false;
                }), str.end());

            /// To use memcpySmallAllowReadWriteOverflow15 for substrings we should allocate a bit more to each string.
            /// That was chosen due to perfomance issues.
            if (!str.empty())
                str.reserve(str.size() + RightPadding);
            final_size += str.size();
        }

        /// The substring number is repeated input_rows_times.
        final_size *= input_rows_count;

        /// Strings without null termination.
        for (size_t i = 1; i < substrings.size(); ++i)
            final_size += data[index_positions[i - 1]]->size() - input_rows_count;

        /// Null termination characters.
        final_size += input_rows_count;

        res_data.resize(final_size);
        res_offsets.resize(input_rows_count);

        UInt64 offset = 0;
        for (UInt64 i = 0; i < input_rows_count; ++i)
        {
            memcpySmallAllowReadWriteOverflow15(res_data.data() + offset, substrings[0].data(), substrings[0].size());
            offset += substrings[0].size();
            for (size_t j = 1; j < substrings.size(); ++j)
            {
                UInt64 arg = index_positions[j - 1];
                auto offset_ptr = offsets[arg];
                UInt64 arg_offset;
                UInt64 size;
                if (offset_ptr)
                {
                    arg_offset = (*offset_ptr)[i - 1];
                    size = (*offset_ptr)[i] - arg_offset - 1;
                }
                else
                {
                    arg_offset = fixed_string_N[arg] * i;
                    size = fixed_string_N[arg];
                    std::cout << arg_offset << ' ' << arg << ' ' << std::string(reinterpret_cast<const char *>(data[arg]->data()), data[arg]->size()) << ' ' << data[arg]->size() << ' ' << size << std::endl;
                }


                memcpySmallAllowReadWriteOverflow15(res_data.data() + offset, data[arg]->data() + arg_offset, size);
                offset += size;
                memcpySmallAllowReadWriteOverflow15(res_data.data() + offset, substrings[j].data(), substrings[j].size());
                offset += substrings[j].size();
            }
            res_data[offset] = '\0';
            ++offset;
            res_offsets[i] = offset;
        }

        /*
         * Invariant of `offset == final_size` must be held.
         *
         * if (offset != final_size)
         *    abort();
         */
    }
};

struct NameFormat
{
    static constexpr auto name = "format";
};
using FunctionFormat = FormatFunction<FormatImpl, NameFormat>;

void registerFunctionFormat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormat>();
}

}
