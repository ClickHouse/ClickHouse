#include "Handlers.h"
#include "SharedLibraryHandlerFactory.h"

#include <DataStreams/copyData.h>
#include <Formats/FormatFactory.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/ThreadPool.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Server/HTTP/HTMLForm.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}

namespace
{
    void processError(HTTPServerResponse & response, const std::string & message)
    {
        response.setStatusAndReason(HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        if (!response.sent())
            *response.send() << message << std::endl;

        LOG_WARNING(&Poco::Logger::get("LibraryBridge"), message);
    }

    std::shared_ptr<Block> parseColumns(std::string && column_string)
    {
        auto sample_block = std::make_shared<Block>();
        auto names_and_types = NamesAndTypesList::parse(column_string);

        for (const NameAndTypePair & column_data : names_and_types)
            sample_block->insert({column_data.type, column_data.name});

        return sample_block;
    }

    std::vector<uint64_t> parseIdsFromBinary(ReadBuffer & buf)
    {
        std::vector<uint64_t> ids;
        readVectorBinary(ids, buf);
        return ids;
    }

    std::vector<std::string> parseNamesFromBinary(const std::string & names_string)
    {
        ReadBufferFromString buf(names_string);
        std::vector<std::string> names;
        readVectorBinary(names, buf);
        return names;
    }
}


void LibraryRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    LOG_TRACE(log, "Request URI: {}", request.getURI());
    HTMLForm params(getContext()->getSettingsRef(), request);

    if (!params.has("method"))
    {
        processError(response, "No 'method' in request URL");
        return;
    }

    if (!params.has("dictionary_id"))
    {
        processError(response, "No 'dictionary_id in request URL");
        return;
    }

    std::string method = params.get("method");
    std::string dictionary_id = params.get("dictionary_id");

    LOG_TRACE(log, "Library method: '{}', dictionary id: {}", method, dictionary_id);
    WriteBufferFromHTTPServerResponse out(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout);

    try
    {
        bool lib_new = (method == "libNew");
        if (method == "libClone")
        {
            if (!params.has("from_dictionary_id"))
            {
                processError(response, "No 'from_dictionary_id' in request URL");
                return;
            }

            std::string from_dictionary_id = params.get("from_dictionary_id");
            bool cloned = false;
            cloned = SharedLibraryHandlerFactory::instance().clone(from_dictionary_id, dictionary_id);

            if (cloned)
            {
                writeStringBinary("1", out);
            }
            else
            {
                LOG_TRACE(log, "Cannot clone from dictionary with id: {}, will call libNew instead");
                lib_new = true;
            }
        }
        if (lib_new)
        {
            auto & read_buf = request.getStream();
            params.read(read_buf);

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
            const auto & settings_string = params.get("library_settings");

            LOG_DEBUG(log, "Parsing library settings from binary string");
            std::vector<std::string> library_settings = parseNamesFromBinary(settings_string);

            /// Needed for library dictionary
            if (!params.has("attributes_names"))
            {
                processError(response, "No 'attributes_names' in request URL");
                return;
            }

            const auto & attributes_string = params.get("attributes_names");

            LOG_DEBUG(log, "Parsing attributes names from binary string");
            std::vector<std::string> attributes_names = parseNamesFromBinary(attributes_string);

            /// Needed to parse block from binary string format
            if (!params.has("sample_block"))
            {
                processError(response, "No 'sample_block' in request URL");
                return;
            }
            std::string sample_block_string = params.get("sample_block");

            std::shared_ptr<Block> sample_block;
            try
            {
                sample_block = parseColumns(std::move(sample_block_string));
            }
            catch (const Exception & ex)
            {
                processError(response, "Invalid 'sample_block' parameter in request body '" + ex.message() + "'");
                LOG_WARNING(log, ex.getStackTraceString());
                return;
            }

            if (!params.has("null_values"))
            {
                processError(response, "No 'null_values' in request URL");
                return;
            }

            ReadBufferFromString read_block_buf(params.get("null_values"));
            auto format = FormatFactory::instance().getInput(FORMAT, read_block_buf, *sample_block, getContext(), DEFAULT_BLOCK_SIZE);
            auto reader = std::make_shared<InputStreamFromInputFormat>(format);
            auto sample_block_with_nulls = reader->read();

            LOG_DEBUG(log, "Dictionary sample block with null values: {}", sample_block_with_nulls.dumpStructure());

            SharedLibraryHandlerFactory::instance().create(dictionary_id, library_path, library_settings, sample_block_with_nulls, attributes_names);
            writeStringBinary("1", out);
        }
        else if (method == "libDelete")
        {
            auto deleted = SharedLibraryHandlerFactory::instance().remove(dictionary_id);

            /// Do not throw, a warning is ok.
            if (!deleted)
                LOG_WARNING(log, "Cannot delete library for with dictionary id: {}, because such id was not found.", dictionary_id);

            writeStringBinary("1", out);
        }
        else if (method == "isModified")
        {
            auto library_handler = SharedLibraryHandlerFactory::instance().get(dictionary_id);
            if (!library_handler)
                throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Not found dictionary with id: {}", dictionary_id);

            bool res = library_handler->isModified();
            writeStringBinary(std::to_string(res), out);
        }
        else if (method == "supportsSelectiveLoad")
        {
            auto library_handler = SharedLibraryHandlerFactory::instance().get(dictionary_id);
            if (!library_handler)
                throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Not found dictionary with id: {}", dictionary_id);

            bool res = library_handler->supportsSelectiveLoad();
            writeStringBinary(std::to_string(res), out);
        }
        else if (method == "loadAll")
        {
            auto library_handler = SharedLibraryHandlerFactory::instance().get(dictionary_id);
            if (!library_handler)
                throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Not found dictionary with id: {}", dictionary_id);

            const auto & sample_block = library_handler->getSampleBlock();
            LOG_DEBUG(log, "Calling loadAll() for dictionary id: {}", dictionary_id);
            auto input = library_handler->loadAll();

            LOG_DEBUG(log, "Started sending result data for dictionary id: {}", dictionary_id);
            BlockOutputStreamPtr output = FormatFactory::instance().getOutputStream(FORMAT, out, sample_block, getContext());
            copyData(*input, *output);
        }
        else if (method == "loadIds")
        {
            LOG_DEBUG(log, "Getting diciontary ids for dictionary with id: {}", dictionary_id);
            String ids_string;
            std::vector<uint64_t> ids = parseIdsFromBinary(request.getStream());

            auto library_handler = SharedLibraryHandlerFactory::instance().get(dictionary_id);
            if (!library_handler)
                throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Not found dictionary with id: {}", dictionary_id);

            const auto & sample_block = library_handler->getSampleBlock();
            LOG_DEBUG(log, "Calling loadIds() for dictionary id: {}", dictionary_id);
            auto input = library_handler->loadIds(ids);

            LOG_DEBUG(log, "Started sending result data for dictionary id: {}", dictionary_id);
            BlockOutputStreamPtr output = FormatFactory::instance().getOutputStream(FORMAT, out, sample_block, getContext());
            copyData(*input, *output);
        }
        else if (method == "loadKeys")
        {
            if (!params.has("requested_block_sample"))
            {
                processError(response, "No 'requested_block_sample' in request URL");
                return;
            }

            std::string requested_block_string = params.get("requested_block_sample");

            std::shared_ptr<Block> requested_sample_block;
            try
            {
                requested_sample_block = parseColumns(std::move(requested_block_string));
            }
            catch (const Exception & ex)
            {
                processError(response, "Invalid 'requested_block' parameter in request body '" + ex.message() + "'");
                LOG_WARNING(log, ex.getStackTraceString());
                return;
            }

            auto & read_buf = request.getStream();
            auto format = FormatFactory::instance().getInput(FORMAT, read_buf, *requested_sample_block, getContext(), DEFAULT_BLOCK_SIZE);
            auto reader = std::make_shared<InputStreamFromInputFormat>(format);
            auto block = reader->read();

            auto library_handler = SharedLibraryHandlerFactory::instance().get(dictionary_id);
            if (!library_handler)
                throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Not found dictionary with id: {}", dictionary_id);

            const auto & sample_block = library_handler->getSampleBlock();
            LOG_DEBUG(log, "Calling loadKeys() for dictionary id: {}", dictionary_id);
            auto input = library_handler->loadKeys(block.getColumns());

            LOG_DEBUG(log, "Started sending result data for dictionary id: {}", dictionary_id);
            BlockOutputStreamPtr output = FormatFactory::instance().getOutputStream(FORMAT, out, sample_block, getContext());
            copyData(*input, *output);
        }
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(true);
        LOG_ERROR(log, "Failed to process request for dictionary_id: {}. Error: {}", dictionary_id, message);

        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, message); // can't call process_error, because of too soon response sending
        try
        {
            writeStringBinary(message, out);
            out.finalize();
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
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


void LibraryExistsHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        LOG_TRACE(log, "Request URI: {}", request.getURI());
        HTMLForm params(getContext()->getSettingsRef(), request);

        if (!params.has("dictionary_id"))
        {
            processError(response, "No 'dictionary_id' in request URL");
            return;
        }

        std::string dictionary_id = params.get("dictionary_id");
        auto library_handler = SharedLibraryHandlerFactory::instance().get(dictionary_id);
        String res;
        if (library_handler)
            res = "1";
        else
            res = "0";

        setResponseDefaultHeaders(response, keep_alive_timeout);
        LOG_TRACE(log, "Senging ping response: {} (dictionary id: {})", res, dictionary_id);
        response.sendBuffer(res.data(), res.size());
    }
    catch (...)
    {
        tryLogCurrentException("PingHandler");
    }
}


}
