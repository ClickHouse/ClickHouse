#pragma once

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/HTTPInputStreams.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Interpreters/CustomHTTP/QueryExecutorAndMatcher.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

class QueryExecutorDynamic : public QueryExecutor
{
public:
    using HTTPServerRequest = Poco::Net::HTTPServerRequest;
    using HTTPServerResponse = Poco::Net::HTTPServerResponse;
    using Configuration = Poco::Util::AbstractConfiguration;

    bool canBeParseRequestBody() const override { return false; }

    bool isQueryParam(const String & param_name) const override;

    QueryExecutorDynamic(const Configuration & configuration, const String & config_key);

    void executeQueryImpl(
        Context & context, HTTPServerRequest & request, HTTPServerResponse & response,
        HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const override;

private:
    String dynamic_param_name{"query"};

    String prepareQuery(Context & context, HTMLForm & params) const;
};

}
