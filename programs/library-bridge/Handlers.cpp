#include "Handlers.h"

#include <DataStreams/copyData.h>
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

#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <common/logger_useful.h>
#include <Server/HTTP/HTMLForm.h>

#include <mutex>
#include <memory>


namespace DB
{
namespace
{
    std::shared_ptr<Block> parseColumns(std::string && column_string)
    {
        auto sample_block = std::make_shared<Block>();
        auto names_and_types = NamesAndTypesList::parse(column_string);

        for (const NameAndTypePair & column_data : names_and_types)
            sample_block->insert({column_data.type, column_data.name});

        return sample_block;
    }
}


void LibraryRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    LOG_TRACE(log, "Request URI: {}", request.getURI());

    HTMLForm params(request);
    params.read(request.getStream());

    if (!params.has("method"))
    {
        processError(response, "No 'method' in request URL");
        return;
    }

    std::string method = params.get("method");
    LOG_TRACE(log, "Library method: '{}'", method);

    WriteBufferFromHTTPServerResponse out(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout);
    try
    {
        if (method == "libNew")
        {
            if (!params.has("library_path"))
            {
                processError(response, "No 'library_path' in request URL");
                return;
            }

            if (!params.has("library_settings"))
            {
                processError(response, "No 'library_settings' in request URL");
                return;
            }

            std::string library_path = params.get("library_path");
            std::string library_settings = params.get("library_settings");

            LOG_TRACE(log, "Library path: '{}', library_settings: '{}'", library_path, library_settings);

            library_handler->libNew(library_path, library_settings);
            writeStringBinary("1", out);
        }
        else if (method == "libClone")
        {
            /// libClone was already called, only need to send the responce.
            writeStringBinary("1", out);
        }
        else if (method == "isModified")
        {
            auto res = library_handler->isModified();
            writeStringBinary(std::to_string(res), out);
        }
        else if (method == "supportsSelectiveLoad")
        {
            auto res = library_handler->supportsSelectiveLoad();
            writeStringBinary(std::to_string(res), out);
        }
        else if (method == "loadAll")
        {
            if (!params.has("attributes"))
            {
                processError(response, "No 'attributes' in request URL");
                return;
            }

            std::string attributes = params.get("attributes");
            std::string columns = params.get("columns");
            std::shared_ptr<Block> sample_block;

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

            auto input = library_handler->loadAll(attributes, *sample_block);
            BlockOutputStreamPtr output = FormatFactory::instance().getOutputStream("RowBinary", out, *sample_block, context);

            copyData(*input, *output);
        }
        else if (method == "loadIds")
        {
            if (!params.has("attributes"))
            {
                processError(response, "No 'attributes' in request URL");
                return;
            }

            if (!params.has("ids"))
            {
                processError(response, "No 'ids' in request URL");
                return;
            }

            std::string attributes = params.get("attributes");
            std::string ids = params.get("ids");

            std::string columns = params.get("columns");
            std::shared_ptr<Block> sample_block;

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

            auto input = library_handler->loadIds(attributes, ids, *sample_block);
            BlockOutputStreamPtr output = FormatFactory::instance().getOutputStream("RowBinary", out, *sample_block, context);

            copyData(*input, *output);
        }
    }
    catch (...)
    {
        LOG_TRACE(log, "KSSENII caught an error");

        auto message = getCurrentExceptionMessage(true);
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR); // can't call process_error, because of too soon response sending

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


void LibraryRequestHandler::processError(HTTPServerResponse & response, const std::string & message)
{
    response.setStatusAndReason(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

    if (!response.sent())
        *response.send() << message << std::endl;

    LOG_WARNING(log, message);
}


void LibraryErrorResponseHandler::handleRequest(HTTPServerRequest & /* request */, HTTPServerResponse & response)
{
    response.setStatusAndReason(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

    if (!response.sent())
        *response.send() << message << std::endl;

    LOG_ERROR(log, message);
}


void PingHandler::handleRequest(HTTPServerRequest & /* request */, HTTPServerResponse & response)
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
