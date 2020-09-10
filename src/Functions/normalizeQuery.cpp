#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Parsers/Lexer.h>
#include <common/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

struct Impl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        ColumnString::Offset prev_src_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset curr_src_offset = offsets[i];
            Lexer lexer(reinterpret_cast<const char *>(&data[prev_src_offset]), reinterpret_cast<const char *>(&data[curr_src_offset - 1]));
            prev_src_offset = offsets[i];

            /// Coalesce whitespace characters and comments to a single whitespace.
            bool prev_insignificant = false;

            /// Coalesce a list of comma separated literals to a single '?..' sequence.
            size_t num_literals_in_sequence = 0;
            bool prev_comma = false;
            bool prev_whitespace = false;

            while (true)
            {
                Token token = lexer.nextToken();

                if (!token.isSignificant())
                {
                    /// Replace a sequence of insignificant tokens with single whitespace.
                    if (!prev_insignificant)
                    {
                        if (0 == num_literals_in_sequence)
                            res_data.push_back(' ');
                        else
                            prev_whitespace = true;
                    }
                    prev_insignificant = true;
                    continue;
                }

                prev_insignificant = false;

                /// Literals.
                if (token.type == TokenType::Number || token.type == TokenType::StringLiteral)
                {
                    if (0 == num_literals_in_sequence)
                        res_data.push_back('?');
                    ++num_literals_in_sequence;
                    prev_whitespace = false;
                    prev_comma = false;
                    continue;
                }
                else if (token.type == TokenType::Comma)
                {
                    if (num_literals_in_sequence)
                    {
                        prev_comma = true;
                        continue;
                    }
                }
                else
                {
                    if (num_literals_in_sequence > 1)
                    {
                        res_data.push_back('.');
                        res_data.push_back('.');
                    }

                    if (prev_comma)
                        res_data.push_back(',');

                    if (prev_whitespace)
                        res_data.push_back(' ');

                    num_literals_in_sequence = 0;
                    prev_comma = false;
                    prev_whitespace = false;
                }

                /// Slightly normalize something that look like aliases - if they are complex, replace them to `?` placeholders.
                if (token.type == TokenType::QuotedIdentifier
                    /// Differentiate identifier from function (example: SHA224(x)).
                    /// By the way, there is padding in columns and pointer dereference is Ok.
                    || (token.type == TokenType::BareWord && *token.end != '('))
                {
                    /// Identifier is complex if it contains whitespace or more than two digits
                    /// or it's at least 36 bytes long (UUID for example).
                    size_t num_digits = 0;

                    const char * pos = token.begin;
                    if (token.size() < 36)
                    {
                        for (; pos != token.end; ++pos)
                        {
                            if (isWhitespaceASCII(*pos))
                                break;

                            if (isNumericASCII(*pos))
                            {
                                ++num_digits;
                                if (num_digits > 2)
                                    break;
                            }
                        }
                    }

                    if (pos == token.end)
                    {
                        res_data.insert(token.begin, token.end);
                    }
                    else
                    {
                        res_data.push_back('`');
                        res_data.push_back('?');
                        res_data.push_back('`');
                    }

                    continue;
                }

                if (token.isEnd() || token.isError())
                    break;

                res_data.insert(token.begin, token.end);
            }

            res_data.push_back(0);
            res_offsets[i] = res_data.size();
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Cannot apply function normalizeQuery to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct Name
{
    static constexpr auto name = "normalizeQuery";
};

}

void registerFunctionNormalizeQuery(FunctionFactory & factory)
{
    factory.registerFunction<FunctionStringToString<Impl, Name>>();
}

}

