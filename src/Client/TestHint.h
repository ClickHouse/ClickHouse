#pragma once

#include <optional>
#include <vector>

#include <fmt/format.h>

#include <Core/Types.h>
#include <Common/Exception.h>


namespace DB
{

class Lexer;

/// Checks expected server and client error codes.
///
/// The following comment hints are supported:
///
/// - "-- { serverError 60 }" -- in case of you are expecting server error.
/// - "-- { serverError 16, 36 }" -- in case of you are expecting one of the 2 errors.
///
/// - "-- { clientError 20 }" -- in case of you are expecting client error.
/// - "-- { clientError 20, 60, 92 }" -- It's expected that the client will return one of the 3 errors.
///
/// - "-- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }" -- by error name.
/// - "-- { serverError NO_SUCH_COLUMN_IN_TABLE, BAD_ARGUMENTS }" -- by error name.
///
/// - "-- { clientError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }" -- by error name.
///
///   Remember that the client parse the query first (not the server), so for
///   example if you are expecting syntax error, then you should use
///   clientError not serverError.
///
/// Examples:
///
/// - echo 'select / -- { clientError 62 }' | clickhouse-client -nm
///
//    Here the client parses the query but it is incorrect, so it expects
///   SYNTAX_ERROR (62).
///
/// - echo 'select foo -- { serverError 47 }' | clickhouse-client -nm
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
    using ErrorVector = std::vector<int>;
    explicit TestHint(const String & query_);

    const auto & serverErrors() const { return server_errors; }
    const auto & clientErrors() const { return client_errors; }
    std::optional<bool> echoQueries() const { return echo; }

    bool hasClientErrors() { return !client_errors.empty(); }
    bool hasServerErrors() { return !server_errors.empty(); }

    bool hasExpectedClientError(int error);
    bool hasExpectedServerError(int error);

    bool needRetry(const std::unique_ptr<Exception> & server_exception, size_t * retries_counter);

private:
    const String & query;
    ErrorVector server_errors{};
    ErrorVector client_errors{};
    std::optional<bool> echo;

    size_t max_retries = 0;
    bool retry_until = false;

    void parse(Lexer & comment_lexer, bool is_leading_hint);

    bool allErrorsExpected(int actual_server_error, int actual_client_error) const
    {
        if (actual_server_error && std::find(server_errors.begin(), server_errors.end(), actual_server_error) == server_errors.end())
            return false;
        if (!actual_server_error && !server_errors.empty())
            return false;

        if (actual_client_error && std::find(client_errors.begin(), client_errors.end(), actual_client_error) == client_errors.end())
            return false;
        if (!actual_client_error && !client_errors.empty())
            return false;

        return true;
    }

    bool lostExpectedError(int actual_server_error, int actual_client_error) const
    {
        return (!server_errors.empty() && !actual_server_error) || (!client_errors.empty() && !actual_client_error);
    }
};

}

template <>
struct fmt::formatter<DB::TestHint::ErrorVector>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("Invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::TestHint::ErrorVector & ErrorVector, FormatContext & ctx) const
    {
        if (ErrorVector.empty())
            return fmt::format_to(ctx.out(), "{}", 0);
        if (ErrorVector.size() == 1)
            return fmt::format_to(ctx.out(), "{}", ErrorVector[0]);
        return fmt::format_to(ctx.out(), "[{}]", fmt::join(ErrorVector, ", "));
    }
};
