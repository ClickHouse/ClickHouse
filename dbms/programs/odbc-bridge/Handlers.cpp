#include "Handlers.h"
#include <Common/HTMLForm.h>

#include <memory>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Dictionaries/ODBCBlockInputStream.h>
#include <Formats/BinaryRowInputStream.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
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
    std::optional<NamesAndTypesList> parseColumns(std::string && column_string, Poco::Logger * log)
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
                return std::nullopt;
            try
            {
                result.emplace_back(name_and_type[0], factory_instance.get(name_and_type[1]));
            }
            catch (...)
            {
                tryLogCurrentException(log);
                return std::nullopt;
            }
        }
        return result;
    }
}

void ODBCHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Poco::Net::HTMLForm params(request, request.stream());
    LOG_TRACE(log, "Request URI: " + request.getURI());

    auto process_error = [&response, this](const std::string & message) {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            response.send() << message << std::endl;
        LOG_WARNING(log, message);
    };

    if (pool == nullptr)
    {
        process_error("ODBCBridge: DSN or database in URL params is not provided or incorrect");
        return;
    }

    if (!params.has("query"))
    {
        process_error("ODBCBridge: No 'query' in request body");
        return;
    }

    if (!params.has("columns"))
    {
        process_error("ODBCBridge: No 'columns' in request body");
        return;
    }

    std::string query = params.get("query");
    std::string columns = params.get("columns");

    auto names_and_types = parseColumns(std::move(columns), log);

    if (!names_and_types)
    {
        process_error("ODBCBridge: Invalid 'columns' parameter in request body");
        return;
    }

    Block sample_block;
    for (const NameAndTypePair & column_data : *names_and_types)
        sample_block.insert({column_data.type, column_data.name});

    WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);

    std::shared_ptr<IBlockOutputStream> writer = FormatFactory::instance().getOutput(format, out, sample_block, *context);

    try
    {
        ODBCBlockInputStream inp(pool->get(), query, sample_block, max_block_size);

        writer->writePrefix();
        while (auto block = inp.read())
            writer->write(block);

        writer->writeSuffix();


        writer->flush();
    }
    catch (...)
    {
        auto message = "Communication with ODBC-driver failed: \n" + getCurrentExceptionMessage(true);
        response.setStatusAndReason(
            Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR); // can't call process_error, bacause of too soon response sending
        writeStringBinary(message, out);
        LOG_WARNING(log, message);
    }
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
}
