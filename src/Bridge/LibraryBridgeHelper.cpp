#include "LibraryBridgeHelper.h"

#include <IO/ReadHelpers.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatFactory.h>
#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <common/logger_useful.h>
#include <ext/range.h>
#include <Core/Field.h>
#include <Common/escapeForFileName.h>


namespace DB
{

LibraryBridgeHelper::LibraryBridgeHelper(
        ContextPtr context_,
        const Block & sample_block_,
        const Field & dictionary_id_)
    : IBridgeHelper(context_->getGlobalContext())
    , log(&Poco::Logger::get("LibraryBridgeHelper"))
    , sample_block(sample_block_)
    , config(context_->getConfigRef())
    , http_timeout(context_->getGlobalContext()->getSettingsRef().http_receive_timeout.value)
    , dictionary_id(dictionary_id_)
{
    bridge_port = config.getUInt("library_bridge.port", DEFAULT_PORT);
    bridge_host = config.getString("library_bridge.host", DEFAULT_HOST);
}


Poco::URI LibraryBridgeHelper::createRequestURI(const String & method) const
{
    auto uri = getMainURI();
    uri.addQueryParameter("dictionary_id", toString(dictionary_id));
    uri.addQueryParameter("method", method);
    return uri;
}


Poco::URI LibraryBridgeHelper::createBaseURI() const
{
    Poco::URI uri;
    uri.setHost(bridge_host);
    uri.setPort(bridge_port);
    uri.setScheme("http");
    return uri;
}


void LibraryBridgeHelper::startBridge(std::unique_ptr<ShellCommand> cmd) const
{
    getContext()->addBridgeCommand(std::move(cmd));
}


bool LibraryBridgeHelper::initLibrary(const std::string & library_path, const std::string library_settings, const std::string attributes_names)
{
    startBridgeSync();
    auto uri = createRequestURI(LIB_NEW_METHOD);

    /// Sample block must contain null values
    WriteBufferFromOwnString out;
    auto output_stream = getContext()->getOutputStream(LibraryBridgeHelper::DEFAULT_FORMAT, out, sample_block);
    formatBlock(output_stream, sample_block);
    auto block_string = out.str();

    auto out_stream_callback = [library_path, library_settings, attributes_names, block_string, this](std::ostream & os)
    {
        os << "library_path=" << escapeForFileName(library_path) << "&";
        os << "library_settings=" << escapeForFileName(library_settings) << "&";
        os << "attributes_names=" << escapeForFileName(attributes_names) << "&";
        os << "sample_block=" << escapeForFileName(sample_block.getNamesAndTypesList().toString()) << "&";
        os << "null_values=" << escapeForFileName(block_string);
    };
    return executeRequest(uri, out_stream_callback);
}


bool LibraryBridgeHelper::cloneLibrary(const Field & other_dictionary_id)
{
    startBridgeSync();
    auto uri = createRequestURI(LIB_CLONE_METHOD);
    uri.addQueryParameter("from_dictionary_id", toString(other_dictionary_id));
    return executeRequest(uri);
}


bool LibraryBridgeHelper::removeLibrary()
{
    startBridgeSync();
    auto uri = createRequestURI(LIB_DELETE_METHOD);
    return executeRequest(uri);
}


bool LibraryBridgeHelper::isModified()
{
    startBridgeSync();
    auto uri = createRequestURI(IS_MODIFIED_METHOD);
    return executeRequest(uri);
}


bool LibraryBridgeHelper::supportsSelectiveLoad()
{
    startBridgeSync();
    auto uri = createRequestURI(SUPPORTS_SELECTIVE_LOAD_METHOD);
    return executeRequest(uri);
}


BlockInputStreamPtr LibraryBridgeHelper::loadAll()
{
    startBridgeSync();
    auto uri = createRequestURI(LOAD_ALL_METHOD);
    return loadBase(uri);
}


BlockInputStreamPtr LibraryBridgeHelper::loadIds(const std::string ids_string)
{
    startBridgeSync();
    auto uri = createRequestURI(LOAD_IDS_METHOD);
    return loadBase(uri, [ids_string](std::ostream & os) { os << "ids=" << ids_string; });
}


BlockInputStreamPtr LibraryBridgeHelper::loadKeys(const Block & requested_block)
{
    startBridgeSync();
    auto uri = createRequestURI(LOAD_KEYS_METHOD);
    /// Sample block to parse block from callback
    uri.addQueryParameter("requested_block_sample", requested_block.getNamesAndTypesList().toString());
    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [requested_block, this](std::ostream & os)
    {
        WriteBufferFromOStream out_buffer(os);
        auto output_stream = getContext()->getOutputStream(LibraryBridgeHelper::DEFAULT_FORMAT, out_buffer, sample_block);
        formatBlock(output_stream, requested_block);
    };
    return loadBase(uri, out_stream_callback);
}


bool LibraryBridgeHelper::executeRequest(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback)
{
    ReadWriteBufferFromHTTP buf(
        uri,
        Poco::Net::HTTPRequest::HTTP_POST,
        std::move(out_stream_callback),
        ConnectionTimeouts::getHTTPTimeouts(getContext()));

    bool res;
    readBoolText(res, buf);
    return res;
}


BlockInputStreamPtr LibraryBridgeHelper::loadBase(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback)
{
    auto read_buf_ptr = std::make_unique<ReadWriteBufferFromHTTP>(
        uri,
        Poco::Net::HTTPRequest::HTTP_POST,
        std::move(out_stream_callback),
        ConnectionTimeouts::getHTTPTimeouts(getContext()),
        0,
        Poco::Net::HTTPBasicCredentials{},
        DBMS_DEFAULT_BUFFER_SIZE,
        ReadWriteBufferFromHTTP::HTTPHeaderEntries{});

    auto input_stream = getContext()->getInputFormat(LibraryBridgeHelper::DEFAULT_FORMAT, *read_buf_ptr, sample_block, DEFAULT_BLOCK_SIZE);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(read_buf_ptr));
}

}
