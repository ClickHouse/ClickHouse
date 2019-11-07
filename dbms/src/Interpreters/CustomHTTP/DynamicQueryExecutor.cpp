#include <Interpreters/CustomHTTP/DynamicQueryExecutor.h>

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>

namespace DB
{

bool QueryExecutorDynamic::isQueryParam(const String & param_name) const
{
    return param_name == dynamic_param_name || startsWith(param_name, "param_");
}

QueryExecutorDynamic::QueryExecutorDynamic(const Configuration & configuration, const String & config_key)
{
    dynamic_param_name = configuration.getString(config_key + "." + "param_name", "query");
}

void QueryExecutorDynamic::executeQueryImpl(
    Context & context, Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
    HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const
{
    ReadBufferPtr temp_query_buf;
    const auto & execute_query = prepareQuery(context, params);
    ReadBufferPtr execute_query_buf = std::make_shared<ReadBufferFromString>(execute_query);

    if (!startsWith(request.getContentType().data(), "multipart/form-data"))
    {
        temp_query_buf = execute_query_buf; /// we create a temporary reference for not to be destroyed
        execute_query_buf = std::make_unique<ConcatReadBuffer>(*temp_query_buf, *input_streams.in_maybe_internal_compressed);
    }

    executeQuery(
        *execute_query_buf, *output_streams.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false,
        context, [&response] (const String & content_type) { response.setContentType(content_type); },
        [&response] (const String & current_query_id) { response.add("X-ClickHouse-Query-Id", current_query_id); }
    );
}

String QueryExecutorDynamic::prepareQuery(Context & context, HTMLForm & params) const
{
    const static size_t prefix_size = strlen("param_");

    WriteBufferFromOwnString query_buffer;
    for (const auto & param : params)
    {
        if (param.first == dynamic_param_name)
            writeString(param.second, query_buffer);
        else if (startsWith(param.first, "param_"))
            context.setQueryParameter(param.first.substr(prefix_size), param.second);
    }

    if (query_buffer.offset())
        writeString("\n", query_buffer);

    return query_buffer.str();
}

}
