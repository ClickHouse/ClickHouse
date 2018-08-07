#include "ODBCHandler.h"
#include <Common/HTMLForm.h>

#include <memory>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Dictionaries/ODBCBlockInputStream.h>
#include <Dictionaries/validateODBCConnectionString.h>
#include <Formats/BinaryRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <common/logger_useful.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}

namespace
{
    std::string buildConnectionString(const std::string & DSN, const std::string & database)
    {
        std::stringstream ss;
        ss << "DSN=" << DSN << ";DATABASE=" << database;
        return ss.str();
    }
    NamesAndTypesList parseColumns(std::string && column_string)
    {
        const auto & factory_instance = DataTypeFactory::instance();
        NamesAndTypesList result;
        static boost::char_separator<char> sep(",");
        boost::tokenizer<boost::char_separator<char>> tokens(column_string, sep);
        for (const std::string & name_and_type_str : tokens)
        {
            std::vector<std::string> name_and_type;
            boost::split(name_and_type, name_and_type_str, boost::is_any_of(":"));
            if (name_and_type.size() != 2)
                throw Exception("ODBCBridge: Invalid columns parameter '" + column_string + "'", ErrorCodes::BAD_REQUEST_PARAMETER);
            result.emplace_back(name_and_type[0], factory_instance.get(name_and_type[1]));
        }
        return result;
    }
}
void ODBCHTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Poco::Net::HTMLForm params(request, request.stream());
    LOG_TRACE(log, "Request URI: " + request.getURI());

    if (!params.has("query"))
        throw Exception("ODBCBridge: No 'query' in request body", ErrorCodes::BAD_REQUEST_PARAMETER);

    std::string query = params.get("query");

    if (!params.has("columns"))
        throw Exception("ODBCBridge: No 'columns' in request body", ErrorCodes::BAD_REQUEST_PARAMETER);

    std::string columns = params.get("columns");

    auto names_and_types = parseColumns(std::move(columns));
    Block sample_block;
    for (const NameAndTypePair & column_data : names_and_types)
    {
        sample_block.insert({column_data.type, column_data.name});
    }

    ODBCBlockInputStream inp(pool->get(), query, sample_block, max_block_size);

    WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);
    std::shared_ptr<IBlockOutputStream> writer = FormatFactory::instance().getOutput(format, out, sample_block, *context);

    writer->writePrefix();
    while (auto block = inp.read())
        writer->write(block);

    writer->writeSuffix();


    writer->flush();
}


void PingHandler::handleRequest(Poco::Net::HTTPServerRequest & /*request*/, Poco::Net::HTTPServerResponse & response)
{
    try
    {
        setResponseDefaultHeaders(response, keep_alive_timeout);
        const char * data = "Ok.\n";
        response.sendBuffer(data, strlen(data));
    }
    catch (...)
    {
        tryLogCurrentException("PingHandler");
    }
}

Poco::Net::HTTPRequestHandler * ODBCRequestHandlerFactory::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    const auto & uri = request.getURI();
    LOG_TRACE(log, "Request URI: " + uri);

    if (uri == "/ping" && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        return new PingHandler(keep_alive_timeout);

    HTMLForm params(request);
    std::string DSN = params.get("DSN");
    std::string database = params.get("database");
    std::string max_block_size = params.get("max_block_size", "");
    std::string format = params.get("format", "RowBinary");

    std::string connection_string = buildConnectionString(DSN, database);

    LOG_TRACE(log, "Connection string:" << connection_string);

    if (!pool_map.count(connection_string))
        pool_map[connection_string] = createAndCheckResizePocoSessionPool(
            [&] { return std::make_shared<Poco::Data::SessionPool>("ODBC", validateODBCConnectionString(connection_string)); });

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        return new ODBCHTTPHandler(pool_map.at(connection_string),
            format,
            max_block_size == "" ? DEFAULT_BLOCK_SIZE : parse<size_t>(max_block_size),
            keep_alive_timeout,
            context);

    return nullptr;
}
}
