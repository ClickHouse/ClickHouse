#pragma once

#include <Interpreters/CustomHTTP/CustomExecutor.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>


namespace DB
{

class CustomExecutorDefault : public CustomExecutor
{
public:
    bool match(HTTPServerRequest & /*request*/, HTMLForm & /*params*/) const override { return true; }

    bool canBeParseRequestBody(HTTPServerRequest & /*request*/, HTMLForm & /*params*/) override { return false; }

    bool isQueryParam(const String & param_name) const override
    {
        return param_name == "query" || startsWith(param_name, "param_");
    }

    QueryExecutors getQueryExecutor(Context & context, HTTPServerRequest & request, HTMLForm & params, const HTTPInputStreams & input_streams) const override
    {
        ReadBufferPtr in = prepareAndGetQueryInput(context, request, params, input_streams);

        return {[&, shared_in = in](HTTPOutputStreams & output, HTTPServerResponse & response)
        {
            executeQuery(
                *shared_in, *output.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
                [&response] (const String & content_type) { response.setContentType(content_type); },
                [&response] (const String & current_query_id) { response.add("X-ClickHouse-Query-Id", current_query_id); }
            );
        }};
    }

private:
    ReadBufferPtr prepareAndGetQueryInput(Context & context, HTTPServerRequest & request, HTMLForm & params, const HTTPInputStreams & input_streams) const
    {
        for (const auto & [key, value] : params)
        {

        }
    }
};

}
