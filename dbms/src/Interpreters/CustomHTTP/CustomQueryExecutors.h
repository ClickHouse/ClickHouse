#pragma once

#include <Core/Types.h>
#include <Common/HTMLForm.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/QueryParameterVisitor.h>
#include <Interpreters/CustomHTTP/HTTPInputStreams.h>
#include <Interpreters/CustomHTTP/HTTPOutputStreams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTInsertQuery.h>

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

class ConstQueryCustomQueryExecutor : public CustomQueryExecutor
{
public:
    ConstQueryCustomQueryExecutor(const Poco::Util::AbstractConfiguration & configuration, const String & config_key)
    {
        execute_query = configuration.getString(config_key, "");

        const char * query_begin = execute_query.data();
        const char * query_end = execute_query.data() + execute_query.size();

        ParserQuery parser(query_end, false);
        ASTPtr extract_query_ast = parseQuery(parser, query_begin, query_end, "", 0);

        QueryParameterVisitor{query_params_name}.visit(extract_query_ast);
        can_be_parse_request_body = !extract_query_ast->as<ASTInsertQuery>();
    }

    bool isQueryParam(const String & param_name) const override { return query_params_name.count(param_name); }

    bool canBeParseRequestBody(Poco::Net::HTTPServerRequest & /*request*/, HTMLForm & /*form*/) const override { return can_be_parse_request_body; }

    void executeQueryImpl(
        Context & context, Poco::Net::HTTPServerRequest & /*request*/, Poco::Net::HTTPServerResponse & response,
        HTMLForm & params, const HTTPInputStreams & /*input_streams*/, const HTTPOutputStreams & output_streams) const override
    {
        prepareQueryParams(context, params);
        ReadBufferPtr execute_query_buf = std::make_shared<ReadBufferFromString>(execute_query);

        executeQuery(
            *execute_query_buf, *output_streams.out_maybe_delayed_and_compressed, /* allow_into_outfile = */ false, context,
            [&response] (const String & content_type) { response.setContentType(content_type); },
            [&response] (const String & current_query_id) { response.add("X-ClickHouse-Query-Id", current_query_id); }
        );
    }

private:
    String execute_query;
    NameSet query_params_name;
    bool can_be_parse_request_body{false};

    void prepareQueryParams(Context & context, HTMLForm & params) const
    {
        for (const auto & param : params)
            if (isQueryParam(param.first))
                context.setQueryParameter(param.first, param.second);
    }
};

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
