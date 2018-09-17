#include "MainHandler.h"

#include "validateODBCConnectionString.h"
#include <memory>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeFactory.h>
#include <Dictionaries/ODBCBlockInputStream.h>
#include <Formats/BinaryRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTMLForm.h>
#include <common/logger_useful.h>

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


ODBCHandler::PoolPtr ODBCHandler::getPool(const std::string & connection_str)
{
    std::lock_guard lock(mutex);
    if (!pool_map->count(connection_str))
    {
        pool_map->emplace(connection_str, createAndCheckResizePocoSessionPool([connection_str] {
            return std::make_shared<Poco::Data::SessionPool>("ODBC", validateODBCConnectionString(connection_str));
        }));
    }
    return pool_map->at(connection_str);
}

void ODBCHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Poco::Net::HTMLForm params(request, request.stream());
    LOG_TRACE(log, "Request URI: " + request.getURI());

    auto process_error = [&response, this](const std::string & message)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            response.send() << message << std::endl;
        LOG_WARNING(log, message);
    };

    if (!params.has("query"))
    {
        process_error("No 'query' in request body");
        return;
    }

    if (!params.has("columns"))
    {
        process_error("No 'columns' in request URL");
        return;
    }

    if (!params.has("connection_string"))
    {
        process_error("No 'connection_string' in request URL");
        return;
    }

    size_t max_block_size = DEFAULT_BLOCK_SIZE;
    if (params.has("max_block_size"))
    {
        std::string max_block_size_str = params.get("max_block_size", "");
        if (max_block_size_str.empty())
        {
            process_error("Empty max_block_size specified");
            return;
        }
        max_block_size = parse<size_t>(max_block_size_str);
    }

    std::string columns = params.get("columns");
    std::unique_ptr<Block> sample_block;
    try
    {
        sample_block = parseColumns(std::move(columns));
    }
    catch (const Exception & ex)
    {
        process_error("Invalid 'columns' parameter in request body '" + ex.message() + "'");
        LOG_WARNING(log, ex.getStackTrace().toString());
        return;
    }

    std::string format = params.get("format", "RowBinary");
    std::string query = params.get("query");
    LOG_TRACE(log, "Query: " << query);

    std::string connection_string = params.get("connection_string");
    LOG_TRACE(log, "Connection string: '" << connection_string << "'");

    WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);
    try
    {
        BlockOutputStreamPtr writer = FormatFactory::instance().getOutput(format, out, *sample_block, *context);
        auto pool = getPool(connection_string);
        ODBCBlockInputStream inp(pool->get(), query, *sample_block, max_block_size);
        copyData(inp, *writer);
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(true);
        response.setStatusAndReason(
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR); // can't call process_error, bacause of too soon response sending
        writeStringBinary(message, out);
        tryLogCurrentException(log);
    }
}
}
