#include <Interpreters/CustomHTTP/ConstQueryExecutor.h>

#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/QueryParameterVisitor.h>
#include <Interpreters/CustomHTTP/ExtractorContextChange.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_CUSTOM_EXECUTOR_PARAM;
}

void prepareQueryParams(Context & context, HTMLForm & params, const NameSet & query_params_name)
{
    for (const auto & param : params)
        if (query_params_name.count(param.first))
            context.setQueryParameter(param.first, param.second);
}

QueryExecutorConst::QueryExecutorConst(const Poco::Util::AbstractConfiguration & configuration, const String & config_key)
{
    execute_query = configuration.getString(config_key);

    const char * query_begin = execute_query.data();
    const char * query_end = execute_query.data() + execute_query.size();

    ParserQuery parser(query_end, false);
    ASTPtr extract_query_ast = parseQuery(parser, query_begin, query_end, "", 0);

    QueryParameterVisitor{query_params_name}.visit(extract_query_ast);
    can_be_parse_request_body = !extract_query_ast->as<ASTInsertQuery>();

    const auto & reserved_params_name = ExtractorContextChange::getReservedParamNames();
    for (const auto & prepared_param_name : query_params_name)
    {
        if (Settings::findIndex(prepared_param_name) != Settings::npos || reserved_params_name.count(prepared_param_name))
            throw Exception(
                "Illegal custom executor query param name '" + prepared_param_name + "', Because it's a reserved name or Settings name",
                ErrorCodes::ILLEGAL_CUSTOM_EXECUTOR_PARAM);
    }
}

void QueryExecutorConst::executeQueryImpl(
    Context & context, Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
    HTMLForm & params, const HTTPInputStreams & input_streams, const HTTPOutputStreams & output_streams) const
{
    ReadBufferPtr temp_query_buf;
    prepareQueryParams(context, params, query_params_name);
    ReadBufferPtr execute_query_buf = std::make_shared<ReadBufferFromString>(execute_query);

    if (!canBeParseRequestBody() && !startsWith(request.getContentType().data(), "multipart/form-data"))
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

}
