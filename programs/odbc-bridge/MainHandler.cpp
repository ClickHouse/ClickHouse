#include "MainHandler.h"

#include "validateODBCConnectionString.h"
#include <memory>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeFactory.h>
#include "ODBCBlockInputStream.h"
#include "ODBCBlockOutputStream.h"
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTMLForm.h>
#include <common/logger_useful.h>
#include <mutex>
#include <Poco/ThreadPool.h>
#include <IO/ReadBufferFromIStream.h>
#include <Columns/ColumnsNumber.h>
#include "getIdentifierQuote.h"

#if USE_ODBC
#include <Poco/Data/ODBC/SessionImpl.h>
#define POCO_SQL_ODBC_CLASS Poco::Data::ODBC
#endif

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

using PocoSessionPoolConstructor = std::function<std::shared_ptr<Poco::Data::SessionPool>()>;
/** Is used to adjust max size of default Poco thread pool. See issue #750
  * Acquire the lock, resize pool and construct new Session.
  */
static std::shared_ptr<Poco::Data::SessionPool> createAndCheckResizePocoSessionPool(PocoSessionPoolConstructor pool_constr)
{
    static std::mutex mutex;

    Poco::ThreadPool & pool = Poco::ThreadPool::defaultPool();

    /// NOTE: The lock don't guarantee that external users of the pool don't change its capacity
    std::unique_lock lock(mutex);

    if (pool.available() == 0)
        pool.addCapacity(2 * std::max(pool.capacity(), 1));

    return pool_constr();
}

ODBCHandler::PoolPtr ODBCHandler::getPool(const std::string & connection_str)
{
    std::lock_guard lock(mutex);
    if (!pool_map->count(connection_str))
    {
        pool_map->emplace(connection_str, createAndCheckResizePocoSessionPool([connection_str]
        {
            return std::make_shared<Poco::Data::SessionPool>("ODBC", validateODBCConnectionString(connection_str));
        }));
    }
    return pool_map->at(connection_str);
}

void ODBCHandler::processError(Poco::Net::HTTPServerResponse & response, const std::string & message)
{
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
    if (!response.sent())
        response.send() << message << std::endl;
    LOG_WARNING(log, message);
}

void ODBCHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Poco::Net::HTMLForm params(request);
    if (mode == "read")
        params.read(request.stream());
    LOG_TRACE(log, "Request URI: {}", request.getURI());

    if (mode == "read" && !params.has("query"))
    {
        processError(response, "No 'query' in request body");
        return;
    }

    if (!params.has("columns"))
    {
        processError(response, "No 'columns' in request URL");
        return;
    }

    if (!params.has("connection_string"))
    {
        processError(response, "No 'connection_string' in request URL");
        return;
    }

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

    std::string columns = params.get("columns");
    std::unique_ptr<Block> sample_block;
    try
    {
        sample_block = parseColumns(std::move(columns));
    }
    catch (const Exception & ex)
    {
        processError(response, "Invalid 'columns' parameter in request body '" + ex.message() + "'");
        LOG_WARNING(log, ex.getStackTraceString());
        return;
    }

    std::string format = params.get("format", "RowBinary");

    std::string connection_string = params.get("connection_string");
    LOG_TRACE(log, "Connection string: '{}'", connection_string);

    WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);

    try
    {
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
            POCO_SQL_ODBC_CLASS::SessionImpl session(validateODBCConnectionString(connection_string), DBMS_DEFAULT_CONNECT_TIMEOUT_SEC);
            quoting_style = getQuotingStyle(session.dbc().handle());
#endif

            auto pool = getPool(connection_string);
            ReadBufferFromIStream read_buf(request.stream());
            BlockInputStreamPtr input_stream = FormatFactory::instance().getInput(format, read_buf, *sample_block,
                                                                                  context, max_block_size);
            ODBCBlockOutputStream output_stream(pool->get(), db_name, table_name, *sample_block, quoting_style);
            copyData(*input_stream, output_stream);
            writeStringBinary("Ok.", out);
        }
        else
        {
            std::string query = params.get("query");
            LOG_TRACE(log, "Query: {}", query);

            BlockOutputStreamPtr writer = FormatFactory::instance().getOutput(format, out, *sample_block, context);
            auto pool = getPool(connection_string);
            ODBCBlockInputStream inp(pool->get(), query, *sample_block, max_block_size);
            copyData(inp, *writer);
        }
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(true);
        response.setStatusAndReason(
                Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR); // can't call process_error, because of too soon response sending
        writeStringBinary(message, out);
        tryLogCurrentException(log);

    }
}

}
