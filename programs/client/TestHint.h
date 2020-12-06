#pragma once

#include <memory>
#include <sstream>
#include <iostream>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Parsers/Lexer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_ERROR_CODE;
}


/// Checks expected server and client error codes in testmode.
/// To enable it add special comment after the query: "-- { serverError 60 }" or "-- { clientError 20 }".
class TestHint
{
public:
    TestHint(bool enabled_, const String & query_)
    : enabled(enabled_)
    , query(query_)
    {
        if (!enabled_)
            return;

        Lexer lexer(query.data(), query.data() + query.size());

        for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        {
            if (token.type == TokenType::Comment)
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
                            parse(hint);
                        }
                    }
                }
            }
        }
    }

    /// @returns true if it's possible to continue without reconnect
    bool checkActual(int & actual_server_error, int & actual_client_error,
                     bool & got_exception, std::unique_ptr<Exception> & last_exception) const
    {
        if (!enabled)
            return true;

        if (allErrorsExpected(actual_server_error, actual_client_error))
        {
            got_exception = false;
            last_exception.reset();
            actual_server_error = 0;
            actual_client_error = 0;
            return false;
        }

        if (lostExpectedError(actual_server_error, actual_client_error))
        {
            std::cerr << "Success when error expected in query: " << query << "It expects server error "
                << server_error << ", client error " << client_error << "." << std::endl;
            got_exception = true;
            last_exception = std::make_unique<Exception>("Success when error expected", ErrorCodes::UNEXPECTED_ERROR_CODE); /// return error to OS
            return false;
        }

        return true;
    }

    int serverError() const { return server_error; }
    int clientError() const { return client_error; }

private:
    bool enabled = false;
    const String & query;
    int server_error = 0;
    int client_error = 0;

    void parse(const String & hint)
    {
        std::stringstream ss;       // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ss << hint;
        String item;

        while (!ss.eof())
        {
            ss >> item;
            if (ss.eof())
                break;

            if (item == "serverError")
                ss >> server_error;
            else if (item == "clientError")
                ss >> client_error;
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
