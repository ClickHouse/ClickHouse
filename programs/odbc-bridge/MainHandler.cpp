#include "MainHandler.h"

#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatFactory.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/ThreadPool.h>
#include <Common/BridgeProtocolVersion.h>
#include <Common/logger_useful.h>
#include "ODBCSink.h"
#include "ODBCSource.h"
#include "config.h"
#include "getIdentifierQuote.h"
#include "validateODBCConnectionString.h"

#include <mutex>
#include <memory>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 odbc_bridge_connection_pool_size;
}

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
        *response.send() << message << '\n';
    LOG_WARNING(log, fmt::runtime(message));
}


void ODBCHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    HTMLForm params(getContext()->getSettingsRef(), request);
    LOG_TRACE(log, "Request URI: {}", request.getURI());

    size_t version;

    if (!params.has("version"))
        version = 0; /// assumed version for too old servers which do not send a version
    else
    {
        String version_str = params.get("version");
        if (!tryParse(version, version_str))
        {
            processError(response, "Unable to parse 'version' string in request URL: '" + version_str + "' Check if the server and library-bridge have the same version.");
            return;
        }
    }

    if (version != XDBC_BRIDGE_PROTOCOL_VERSION)
    {
        /// backwards compatibility is considered unnecessary for now, just let the user know that the server and the bridge must be upgraded together
        processError(response, "Server and library-bridge have different versions: '" + std::to_string(version) + "' vs. '" + std::to_string(LIBRARY_BRIDGE_PROTOCOL_VERSION) + "'");
        return;
    }


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
    bool use_connection_pooling = params.getParsed<bool>("use_connection_pooling", true);
    LOG_TRACE(log, "Connection string: '{}'", connection_string);
    LOG_TRACE(log, "Use pooling: {}", use_connection_pooling);

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

    WriteBufferFromHTTPServerResponse out(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD);

    try
    {
        nanodbc::ConnectionHolderPtr connection_handler;
        if (use_connection_pooling)
            connection_handler = ODBCPooledConnectionFactory::instance().get(
                validateODBCConnectionString(connection_string), getContext()->getSettingsRef()[Setting::odbc_bridge_connection_pool_size]);
        else
            connection_handler = std::make_shared<nanodbc::ConnectionHolder>(validateODBCConnectionString(connection_string));

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
