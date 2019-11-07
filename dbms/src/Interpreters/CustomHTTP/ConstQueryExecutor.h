#pragma once

#include <Interpreters/CustomHTTP/QueryExecutorAndMatcher.h>

namespace DB
{

class QueryExecutorConst : public QueryExecutor
{
public:
    using HTTPServerRequest = Poco::Net::HTTPServerRequest;
    using HTTPServerResponse = Poco::Net::HTTPServerResponse;

    bool canBeParseRequestBody() const override { return can_be_parse_request_body; }

    bool isQueryParam(const String & param_name) const override { return query_params_name.count(param_name); }

    QueryExecutorConst(const Poco::Util::AbstractConfiguration & configuration, const String & config_key);

    void executeQueryImpl(
        Context & context, HTTPServerRequest & request, HTTPServerResponse & response,
        HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const override;

private:
    String execute_query;
    NameSet query_params_name;
    bool can_be_parse_request_body{false};
};

}
