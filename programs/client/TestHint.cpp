#include "TestHint.h"

#include <sstream>
#include <iostream>
#include <Common/Exception.h>
#include <Parsers/Lexer.h>

namespace DB
{

TestHint::TestHint(bool enabled_, const String & query_)
    : query(query_)
{
    if (!enabled_)
        return;

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
    std::stringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss << hint;
    String item;

    while (!ss.eof())
    {
        ss >> item;
        if (ss.eof())
            break;

        if (!is_leading_hint)
        {
            if (item == "serverError")
                ss >> server_error;
            else if (item == "clientError")
                ss >> client_error;
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
