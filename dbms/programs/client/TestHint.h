#pragma once

#include <memory>
#include <sstream>
#include <iostream>
#include <Core/Types.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/// Checks expected server and client error codes in testmode.
/// To enable it add special comment after the query: "-- { serverError 60 }" or "-- { clientError 20 }".
class TestHint
{
public:
    TestHint(bool enabled_, const String & query)
    :   enabled(enabled_)
    {
        if (!enabled_)
            return;

        /// TODO: This is absolutely wrong. Fragment may be contained inside string literal.
        size_t pos = query.find("--");

        if (pos != String::npos && query.find("--", pos + 2) != String::npos)
            return; /// It's not last comment. Hint belongs to commented query. /// TODO Absolutely wrong: there maybe the following comment for the next query.

        if (pos != String::npos)
        {
            /// TODO: This is also wrong. Comment may already have ended by line break.
            pos = query.find('{', pos + 2);

            if (pos != String::npos)
            {
                String hint = query.substr(pos + 1);

                /// TODO: And this is wrong for the same reason.
                pos = hint.find('}');
                hint.resize(pos);
                parse(hint);
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
            std::cerr << "Success when error expected. It expects server error "
                << server_error << ", client error " << client_error << "." << std::endl;
            got_exception = true;
            last_exception = std::make_unique<Exception>("Success when error expected", ErrorCodes::LOGICAL_ERROR); /// return error to OS
            return false;
        }

        return true;
    }

    int serverError() const { return server_error; }
    int clientError() const { return client_error; }

private:
    bool enabled = false;
    int server_error = 0;
    int client_error = 0;

    void parse(const String & hint)
    {
        std::stringstream ss;
        ss << hint;
        while (!ss.eof())
        {
            String item;
            ss >> item;
            if (item.empty())
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
