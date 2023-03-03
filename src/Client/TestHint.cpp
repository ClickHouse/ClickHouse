#include "TestHint.h"

#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/Lexer.h>

namespace DB::ErrorCodes
{
extern const int CANNOT_PARSE_TEXT;
}

namespace
{

/// Parse error as number or as a string (name of the error code const)
DB::TestHint::error_vector parseErrorCode(DB::ReadBufferFromString & in)
{
    DB::TestHint::error_vector error_codes{};

    while (!in.eof())
    {
        int code = -1;
        String code_name;
        auto * pos = in.position();

        tryReadText(code, in);
        if (pos == in.position())
        {
            readStringUntilWhitespace(code_name, in);
            code = DB::ErrorCodes::getErrorCodeByName(code_name);
        }
        error_codes.push_back(code);

        if (in.eof())
            break;
        skipWhitespaceIfAny(in);
        if (in.eof())
            break;
        char c;
        in.readStrict(c);
        if (c != '|')
            throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Expected separator '|'. Got '{}'", c);
        skipWhitespaceIfAny(in);
    }

    return error_codes;
}

}

namespace DB
{

TestHint::TestHint(const String & query_)
    : query(query_)
{
    // Don't parse error hints in leading comments, because it feels weird.
    // Leading 'echo' hint is OK.
    bool is_leading_hint = true;

    Lexer lexer(query.data(), query.data() + query.size());

    for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
    {
        if (token.type != TokenType::Comment
            && token.type != TokenType::Whitespace)
        {
            is_leading_hint = false;
        }
        else if (token.type == TokenType::Comment)
        {
            String comment(token.begin, token.begin + token.size());

            if (!comment.empty())
            {
                size_t pos_start = comment.find('{', 0);
                if (pos_start != String::npos)
                {
                    size_t pos_end = comment.find('}', pos_start);
                    if (pos_end != String::npos)
                    {
                        String hint(comment.begin() + pos_start + 1, comment.begin() + pos_end);
                        parse(hint, is_leading_hint);
                    }
                }
            }
        }
    }
}

void TestHint::parse(const String & hint, bool is_leading_hint)
{
    ReadBufferFromString in(hint);
    String item;

    while (!in.eof())
    {
        readStringUntilWhitespace(item, in);
        if (in.eof())
            break;

        skipWhitespaceIfAny(in);

        if (!is_leading_hint)
        {
            if (item == "serverError")
                server_errors = parseErrorCode(in);
            else if (item == "clientError")
                client_errors = parseErrorCode(in);
        }

        if (item == "echo")
            echo.emplace(true);
        if (item == "echoOn")
            echo.emplace(true);
        if (item == "echoOff")
            echo.emplace(false);
    }
}

}
