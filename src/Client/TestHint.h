#pragma once

#include <optional>
#include <Core/Types.h>


namespace DB
{

/// Checks expected server and client error codes.
///
/// The following comment hints are supported:
///
/// - "-- { serverError 60 }" -- in case of you are expecting server error.
///
/// - "-- { clientError 20 }" -- in case of you are expecting client error.
///
/// - "-- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }" -- by error name.
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
    TestHint(const String & query_);

    int serverError() const { return server_error; }
    int clientError() const { return client_error; }
    std::optional<bool> echoQueries() const { return echo; }

private:
    const String & query;
    int server_error = 0;
    int client_error = 0;
    std::optional<bool> echo;

    void parse(const String & hint, bool is_leading_hint);

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
