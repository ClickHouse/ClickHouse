#pragma once

#include <memory>
#include <sstream>
#include <iostream>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Parsers/Lexer.h>


namespace DB
{

/// Checks expected server and client error codes in --testmode.
///
/// The following comment hints are supported:
///
/// - "-- { serverError 60 }" -- in case of you are expecting server error.
///
/// - "-- { clientError 20 }" -- in case of you are expecting client error.
///
///   Remember that the client parse the query first (not the server), so for
///   example if you are expecting syntax error, then you should use
///   clientError not serverError.
///
/// Examples:
///
/// - echo 'select / -- { clientError 62 }' | clickhouse-client --testmode -nm
///
//    Here the client parses the query but it is incorrect, so it expects
///   SYNTAX_ERROR (62).
///
/// - echo 'select foo -- { serverError 47 }' | clickhouse-client --testmode -nm
///
///   But here the query is correct, but there is no such column "foo", so it
///   is UNKNOWN_IDENTIFIER server error.
///
/// The following hints will control the query echo mode (i.e print each query):
///
/// - "-- { echo }"
/// - "-- { echoOn }"
/// - "-- { echoOff }"
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
    std::optional<bool> echoQueries() const { return echo; }

private:
    const String & query;
    int server_error = 0;
    int client_error = 0;
    std::optional<bool> echo;

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
                echo.emplace(true);
            if (item == "echoOn")
                echo.emplace(true);
            if (item == "echoOff")
                echo.emplace(false);
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
