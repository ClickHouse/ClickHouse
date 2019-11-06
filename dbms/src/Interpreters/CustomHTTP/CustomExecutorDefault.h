#pragma once

#include <Interpreters/CustomHTTP/CustomExecutor.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include "HTTPInputStreams.h"


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
        return {[&](HTTPOutputStreams & output, HTTPServerResponse & response)
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
                *execute_query_buf, *output.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
                [&response] (const String & content_type) { response.setContentType(content_type); },
                [&response] (const String & current_query_id) { response.add("X-ClickHouse-Query-Id", current_query_id); }
            );
        }};
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
