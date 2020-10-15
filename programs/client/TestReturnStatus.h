#pragma once

#include <memory>
#include <sstream>
#include <iostream>
#include <Core/Types.h>
#include <Common/Exception.h>
#include <Parsers/Lexer.h>
#include <re2/re2.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNEXPECTED_ERROR_CODE;
    extern const int INCORRECT_QUERY;
}

struct ParsedTestErrorInfo
{
    /**
     * The error code and message (if supplied) are valid.
     * Examples of non-valid requests:
     * -- {serverError}
     * -- {clientError 48 a}
     * -- {serverError 123 "aaa"
     */
    bool valid;

    /**
     * If not valid, contains a message describing a message error.
     */
    const String error_message;

    int server_error = 0;
    int client_error = 0;

    String wanted_error_message = {};
};

/**
 * If enabled, checks that a given test returns some error code (optionally, with some error message).
 * Evaluated on queries like "-- {serverError 42}, "-- {clientError 88}", "{-- serverError 42 "DB::exception * \"}",
 * etc.
 */
class TestReturnStatus
{
public:
    TestReturnStatus(bool enabled_, const String & query_)
        : enabled(enabled_),
          query(query_),
          error_info(getErrorInfo()) {}

    bool mayContinueWithoutReconnect(
        int & actual_server_error, int & actual_client_error,
        bool & got_exception, std::unique_ptr<Exception> & last_exception) const
    {
        if (!enabled)
            return true;

        if (!error_info.valid)
        {
            got_exception = true;

            last_exception = std::make_unique<Exception>(
                "The supplied serverError or clientError comment is invalid: " + error_info.error_message,
                ErrorCodes::INCORRECT_QUERY);

            return false;
        }

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
            std::cerr << "Success when error expected in query: " << query
                      << "It expects server error " << error_info.server_error
                      << ", client error " << error_info.client_error
                      << "." << std::endl;

            got_exception = true;

            last_exception = std::make_unique<Exception>("Success when error expected",
                    ErrorCodes::UNEXPECTED_ERROR_CODE); /// return error to OS

            return false;
        }

        const bool exception_needed = !error_info.wanted_error_message.empty();

        if (exception_needed && !got_exception)
        {
            got_exception = true;

            last_exception = std::make_unique<Exception>(
                "The query didn't return an exception, but is was expected with message: " +
                    error_info.wanted_error_message,
                ErrorCodes::INCORRECT_QUERY);

            return false;
        }

        if (!errorMessageMatches(last_exception->message()))
        {
            got_exception = true;

            last_exception = std::make_unique<Exception>(
                "Different exception messages: wanted " + error_info.wanted_error_message +
                    ", got " + last_exception->message(),
                ErrorCodes::INCORRECT_QUERY);

            return false;
        }

        return true;
    }

    bool checkIfIsQueryParseSyntaxError(Exception & e) const
    {
        if (!valid())
            return false;

        if (serverError()) /// Syntax errors are considered as client errors
            return false;

        if (clientError() != e.code())
        {
            if (clientError())
                e.addMessage("\nExpected client error: " + std::to_string(clientError()));

            return false;
        }

        if (!errorMessageMatches(e.message()))
        {
            e.addMessage("\nExpected error message: " + errorMessage());
            return false;
        }

        return true;
    }

    constexpr int serverError() const { return error_info.server_error; }
    constexpr int clientError() const { return error_info.client_error; }
    constexpr bool valid() const {return error_info.valid; }

    const String& errorMessage() const { return error_info.error_message; }

    bool errorMessageMatches(const String& error) const
    {
        return error_info.wanted_error_message.empty() || re2::RE2::FullMatch(error, error_info.wanted_error_message);
    }

private:
    const bool enabled = false;
    const String & query;
    const ParsedTestErrorInfo error_info;

    ParsedTestErrorInfo getErrorInfo() const
    {
        if (!enabled)
            return {};

        Lexer lexer(query.data(), query.data() + query.size());

        for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        {
            if (token.type != TokenType::Comment)
                continue;

            const String comment(token.begin, token.begin + token.size());

            if (comment.empty())
                continue;

            const size_t pos_start = comment.find('{', 0);

            if (pos_start == String::npos)
                continue;

            const size_t pos_end = comment.find('}', pos_start);

            if (pos_end == String::npos)
                continue;

            const String hint(comment.begin() + pos_start + 1, comment.begin() + pos_end);
            return parse(hint);
        }

        return {};
    }

    static ParsedTestErrorInfo parse(const String& hint)
    {
        ParsedTestErrorInfo out;

        std::stringstream ss;
        ss << hint;

        String item;

        if (ss.eof()) return out;

        ss >> item;

        if (ss.eof()) return out;

        if (item == "serverError")
            ss >> out.server_error;
        else if (item == "clientError")
            ss >> out.client_error;
        else
            return out; // not an error info message TODO Maybe return .valid=false

        if (ss.eof()) return out; //the message was not supplied

        ss >> out.wanted_error_message;

        if (out.wanted_error_message.at(0) != '"' || *out.wanted_error_message.crbegin() != '"')
            return {.valid = false, .error_message = "Missing \" in wanted error start or end"};

        return out;
    }

    constexpr bool allErrorsExpected(int actual_server_error, int actual_client_error) const
    {
        return (error_info.server_error || error_info.client_error)
               && (error_info.server_error == actual_server_error)
               && (error_info.client_error == actual_client_error);
    }

    constexpr bool lostExpectedError(int actual_server_error, int actual_client_error) const
    {
        return (error_info.server_error && !actual_server_error)
               || (error_info.client_error && !actual_client_error);
    }
};
}
