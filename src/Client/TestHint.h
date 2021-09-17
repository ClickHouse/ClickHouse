#pragma once

#include <optional>
#include <Core/Types.h>


namespace DB
{

struct ExpectedError
{
    int code{0};
    bool is_server{false};
    bool required{false};

    static ExpectedError makeServer(int code, bool required = true) { return ExpectedError{code, true, required}; }
    static ExpectedError makeClient(int code, bool required = true) { return ExpectedError{code, false, required}; }

    int serverError() const { return is_server ? code : 0; }
    int clientError() const { return is_server ? 0 : code; }
};

/// Checks expected server and client error codes in --testmode.
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
    TestHint(bool enabled_, const String & query_);

    int serverError() const { return expected_error.serverError(); }
    int clientError() const { return expected_error.clientError(); }
    bool errorRequired() const { return expected_error.required; }
    std::optional<bool> echoQueries() const { return echo; }

private:
    const String & query;
    ExpectedError expected_error;
    std::optional<bool> echo;

    void parse(const String & hint, bool is_leading_hint);

};

}
