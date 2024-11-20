#include <charconv>
#include <string_view>

#include <Client/TestHint.h>

#include <Parsers/Lexer.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int OK;
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
                        Lexer comment_lexer(comment.c_str() + pos_start + 1, comment.c_str() + pos_end, 0);
                        parse(comment_lexer, is_leading_hint);
                    }
                }
            }
        }
    }
}

bool TestHint::hasExpectedClientError(int error)
{
    return std::find(client_errors.begin(), client_errors.end(), error) != client_errors.end();
}

bool TestHint::hasExpectedServerError(int error)
{
    return std::find(server_errors.begin(), server_errors.end(), error) != server_errors.end();
}

bool TestHint::needRetry(const std::unique_ptr<Exception> & server_exception, size_t * retries_counter)
{
    chassert(retries_counter);
    if (max_retries <= *retries_counter)
        return false;

    ++*retries_counter;

    int error = ErrorCodes::OK;
    if (server_exception)
        error = server_exception->code();


    if (retry_until)
        return !hasExpectedServerError(error);  /// retry until we get the expected error
    return hasExpectedServerError(error); /// retry while we have the expected error
}

void TestHint::parse(Lexer & comment_lexer, bool is_leading_hint)
{
    std::unordered_set<std::string_view> commands{"echo", "echoOn", "echoOff", "retry"};

    std::unordered_set<std::string_view> command_errors{
        "serverError",
        "clientError",
    };

    for (Token token = comment_lexer.nextToken(); !token.isEnd(); token = comment_lexer.nextToken())
    {
        if (token.type == TokenType::Whitespace)
            continue;

        String item = String(token.begin, token.end);
        if (token.type == TokenType::BareWord && commands.contains(item))
        {
            if (item == "echo")
                echo.emplace(true);
            if (item == "echoOn")
                echo.emplace(true);
            if (item == "echoOff")
                echo.emplace(false);

            if (item == "retry")
            {
                token = comment_lexer.nextToken();
                while (token.type == TokenType::Whitespace)
                    token = comment_lexer.nextToken();

                if (token.type != TokenType::Number)
                    throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Could not parse the number of retries: {}",
                                        std::string_view(token.begin, token.end));

                max_retries = std::stoul(std::string(token.begin, token.end));

                token = comment_lexer.nextToken();
                while (token.type == TokenType::Whitespace)
                    token = comment_lexer.nextToken();

                if (token.type != TokenType::BareWord ||
                    (std::string_view(token.begin, token.end) != "until" &&
                    std::string_view(token.begin, token.end) != "while"))
                    throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Expected 'until' or 'while' after the number of retries, got: {}",
                                        std::string_view(token.begin, token.end));
                retry_until = std::string_view(token.begin, token.end) == "until";
            }
        }
        else if (!is_leading_hint && token.type == TokenType::BareWord && command_errors.contains(item))
        {
            /// Everything after this must be a list of errors separated by comma
            ErrorVector error_codes;
            while (!token.isEnd())
            {
                token = comment_lexer.nextToken();
                if (token.type == TokenType::Whitespace)
                    continue;
                if (token.type == TokenType::Number)
                {
                    int code;
                    auto [p, ec] = std::from_chars(token.begin, token.end, code);
                    if (p == token.begin)
                        throw DB::Exception(
                            DB::ErrorCodes::CANNOT_PARSE_TEXT,
                            "Could not parse integer number for errorcode: {}",
                            std::string_view(token.begin, token.end));
                    error_codes.push_back(code);
                }
                else if (token.type == TokenType::BareWord)
                {
                    int code = DB::ErrorCodes::getErrorCodeByName(std::string_view(token.begin, token.end));
                    error_codes.push_back(code);
                }
                else
                    throw DB::Exception(
                        DB::ErrorCodes::CANNOT_PARSE_TEXT,
                        "Could not parse error code in {}: {}",
                        getTokenName(token.type),
                        std::string_view(token.begin, token.end));
                do
                {
                    token = comment_lexer.nextToken();
                } while (!token.isEnd() && token.type == TokenType::Whitespace);

                if (!token.isEnd() && token.type != TokenType::Comma)
                    throw DB::Exception(
                        DB::ErrorCodes::CANNOT_PARSE_TEXT,
                        "Could not parse error code. Expected ','. Got '{}'",
                        std::string_view(token.begin, token.end));
            }

            if (item == "serverError")
                server_errors = error_codes;
            else
                client_errors = error_codes;
            break;
        }
    }

    if (max_retries && server_errors.size() != 1)
        throw DB::Exception(DB::ErrorCodes::CANNOT_PARSE_TEXT, "Expected one serverError after the 'retry N while|until' command");
}

}
