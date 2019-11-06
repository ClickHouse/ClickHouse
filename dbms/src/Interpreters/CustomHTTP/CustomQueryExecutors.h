#pragma once

#include <Core/Types.h>
#include <Common/HTMLForm.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/CustomHTTP/HTTPInputStreams.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

class CustomQueryExecutor
{
public:
    virtual ~CustomQueryExecutor() = default;

    virtual bool isQueryParam(const String &) const = 0;
    virtual bool canBeParseRequestBody(Poco::Net::HTTPServerRequest &, HTMLForm &) const = 0;

    virtual void executeQueryImpl(
        Context & context, Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
        HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const = 0;
};

using CustomQueryExecutorPtr = std::shared_ptr<CustomQueryExecutor>;

class ExtractQueryParamCustomQueryExecutor : public CustomQueryExecutor
{
public:
    bool isQueryParam(const String & param_name) const override { return param_name == "query" || startsWith(param_name, "param_"); }

    bool canBeParseRequestBody(Poco::Net::HTTPServerRequest & /*request*/, HTMLForm & /*form*/) const override { return false; }

    void executeQueryImpl(
        Context & context, Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
        HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const override
    {
        const auto & execute_query = prepareQuery(context, params);
        ReadBufferPtr execute_query_buf = std::make_shared<ReadBufferFromString>(execute_query);

        ReadBufferPtr temp_query_buf;
        if (!startsWith(request.getContentType().data(), "multipart/form-data"))
        {
            temp_query_buf = execute_query_buf; /// we create a temporary reference for not to be destroyed
            execute_query_buf = std::make_unique<ConcatReadBuffer>(*temp_query_buf, *input_streams.in_maybe_internal_compressed);
        }

        executeQuery(
            *execute_query_buf, *output_streams.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
            [&response] (const String & content_type) { response.setContentType(content_type); },
            [&response] (const String & current_query_id) { response.add("X-ClickHouse-Query-Id", current_query_id); }
        );
    }

private:
    String prepareQuery(Context & context, HTMLForm & params) const
    {
        const static size_t prefix_size = strlen("param_");

        std::stringstream query_stream;
        for (const auto & param : params)
        {
            if (param.first == "query")
                query_stream << param.second;
            else if (startsWith(param.first, "param_"))
                context.setQueryParameter(param.first.substr(prefix_size), param.second);
        }

        query_stream << "\n";
        return query_stream.str();
    }
};

}
