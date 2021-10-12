#pragma once

#include <memory>
#include <sstream>
#include <iostream>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Parsers/Lexer.h>


namespace DB
{

/// Checks expected server and client error codes in testmode.
/// To enable it add special comment after the query: "-- { serverError 60 }" or "-- { clientError 20 }".
/// Also you can enable echoing all queries by writing "-- { echo }".
class TestHint
{
public:
    TestHint(bool enabled_, const String & query_) :
        query(query_)
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

    int serverError() const { return server_error; }
    int clientError() const { return client_error; }
    bool echoQueries() const { return echo; }

private:
    const String & query;
    int server_error = 0;
    int client_error = 0;
    bool echo = false;

    void parse(const String & hint, bool is_leading_hint)
    {
        std::stringstream ss;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
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
                echo = true;
        }
    }

    bool allErrorsExpected(int actual_server_error, int actual_client_error) const
    {
        return (server_error || client_error) && (server_error == actual_server_error) && (client_error == actual_client_error);
    }

    bool lostExpectedError(int actual_server_error, int actual_client_error) const
    {
        return (server_error && !actual_server_error) || (client_error && !actual_client_error);
    }
};

}
