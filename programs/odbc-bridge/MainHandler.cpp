#include "MainHandler.h"

#include "validateODBCConnectionString.h"
#include "ODBCBlockInputStream.h"
#include "ODBCBlockOutputStream.h"
#include "getIdentifierQuote.h"
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatFactory.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromIStream.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/ThreadPool.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Common/logger_useful.h>
#include <Server/HTTP/HTMLForm.h>
#include <Common/config.h>

#include <mutex>
#include <memory>


namespace DB
{

namespace
{
    std::unique_ptr<Block> parseColumns(std::string && column_string)
    {
        std::unique_ptr<Block> sample_block = std::make_unique<Block>();
        auto names_and_types = NamesAndTypesList::parse(column_string);
        for (const NameAndTypePair & column_data : names_and_types)
            sample_block->insert({column_data.type, column_data.name});
        return sample_block;
    }
}


void ODBCHandler::processError(HTTPServerResponse & response, const std::string & message)
{
    response.setStatusAndReason(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
    if (!response.sent())
        *response.send() << message << std::endl;
    LOG_WARNING(log, fmt::runtime(message));
}


void ODBCHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    HTMLForm params(getContext()->getSettingsRef(), request);
    LOG_TRACE(log, "Request URI: {}", request.getURI());

    if (mode == "read")
        params.read(request.getStream());

    if (mode == "read" && !params.has("query"))
    {
        processError(response, "No 'query' in request body");
        return;
    }


    if (!params.has("connection_string"))
    {
        processError(response, "No 'connection_string' in request URL");
        return;
    }

    if (!params.has("sample_block"))
    {
        processError(response, "No 'sample_block' in request URL");
        return;
    }

    std::string format = params.get("format", "RowBinary");
    std::string connection_string = params.get("connection_string");
    LOG_TRACE(log, "Connection string: '{}'", connection_string);

    UInt64 max_block_size = DEFAULT_BLOCK_SIZE;
    if (params.has("max_block_size"))
    {
        std::string max_block_size_str = params.get("max_block_size", "");
        if (max_block_size_str.empty())
        {
            processError(response, "Empty max_block_size specified");
            return;
        }
        max_block_size = parse<size_t>(max_block_size_str);
    }

    std::string sample_block_string = params.get("sample_block");
    std::unique_ptr<Block> sample_block;
    try
    {
        sample_block = parseColumns(std::move(sample_block_string));
    }
    catch (const Exception & ex)
    {
        processError(response, "Invalid 'sample_block' parameter in request body '" + ex.message() + "'");
        LOG_ERROR(log, fmt::runtime(ex.getStackTraceString()));
        return;
    }

    WriteBufferFromHTTPServerResponse out(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout);

    try
    {
        auto connection_handler = ODBCConnectionFactory::instance().get(
                validateODBCConnectionString(connection_string),
                getContext()->getSettingsRef().odbc_bridge_connection_pool_size);

        if (mode == "write")
        {
            if (!params.has("db_name"))
            {
                processError(response, "No 'db_name' in request URL");
                return;
            }
            if (!params.has("table_name"))
            {
                processError(response, "No 'table_name' in request URL");
                return;
            }
            std::string db_name = params.get("db_name");
            std::string table_name = params.get("table_name");
            LOG_TRACE(log, "DB name: '{}', table name: '{}'", db_name, table_name);

            auto quoting_style = IdentifierQuotingStyle::None;
#if USE_ODBC
            quoting_style = getQuotingStyle(connection_handler);
#endif
            auto & read_buf = request.getStream();
            auto input_format = getContext()->getInputFormat(format, read_buf, *sample_block, max_block_size);
            auto sink = std::make_shared<ODBCSink>(std::move(connection_handler), db_name, table_name, *sample_block, getContext(), quoting_style);

            QueryPipeline pipeline(std::move(input_format));
            pipeline.complete(std::move(sink));

            CompletedPipelineExecutor executor(pipeline);
            executor.execute();

            writeStringBinary("Ok.", out);
        }
        else
        {
            std::string query = params.get("query");
            LOG_TRACE(log, "Query: {}", query);

            auto writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, out, *sample_block, getContext());
            auto source = std::make_shared<ODBCSource>(std::move(connection_handler), query, *sample_block, max_block_size);

            QueryPipeline pipeline(std::move(source));
            pipeline.complete(std::move(writer));

            CompletedPipelineExecutor executor(pipeline);
            executor.execute();
        }
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(true);
        response.setStatusAndReason(
                Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR); // can't call process_error, because of too soon response sending

        try
        {
            writeStringBinary(message, out);
            out.finalize();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }

        tryLogCurrentException(log);
    }

    try
    {
        out.finalize();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

}
