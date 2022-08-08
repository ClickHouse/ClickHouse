#include "ExternalDictionaryLibraryBridgeHelper.h"

#include <Formats/formatBlock.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Formats/IInputFormat.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <base/range.h>
#include <Core/Field.h>
#include <Common/escapeForFileName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int LOGICAL_ERROR;
}

ExternalDictionaryLibraryBridgeHelper::ExternalDictionaryLibraryBridgeHelper(
        ContextPtr context_,
        const Block & sample_block_,
        const Field & dictionary_id_,
        const LibraryInitData & library_data_)
    : LibraryBridgeHelper(context_->getGlobalContext())
    , sample_block(sample_block_)
    , library_data(library_data_)
    , dictionary_id(dictionary_id_)
{
}


Poco::URI ExternalDictionaryLibraryBridgeHelper::getPingURI() const
{
    auto uri = createBaseURI();
    uri.setPath(PING_HANDLER);
    uri.addQueryParameter("dictionary_id", toString(dictionary_id));
    return uri;
}


Poco::URI ExternalDictionaryLibraryBridgeHelper::getMainURI() const
{
    auto uri = createBaseURI();
    uri.setPath(MAIN_HANDLER);
    return uri;
}


Poco::URI ExternalDictionaryLibraryBridgeHelper::createRequestURI(const String & method) const
{
    auto uri = getMainURI();
    uri.addQueryParameter("dictionary_id", toString(dictionary_id));
    uri.addQueryParameter("method", method);
    return uri;
}


bool ExternalDictionaryLibraryBridgeHelper::bridgeHandShake()
{
    String result;
    try
    {
        ReadWriteBufferFromHTTP buf(getPingURI(), Poco::Net::HTTPRequest::HTTP_GET, {}, http_timeouts, credentials);
        readString(result, buf);
    }
    catch (...)
    {
        tryLogCurrentException(log);
        return false;
    }

    /*
     * When pinging bridge we also pass current dicionary_id. The bridge will check if there is such
     * dictionary. It is possible that such dictionary_id is not present only in two cases:
     * 1. It is dictionary source creation and initialization of library handler on bridge side did not happen yet.
     * 2. Bridge crashed or restarted for some reason while server did not.
    **/
    if (result.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected message from library bridge: {}. Check that bridge and server have the same version.", result);

    UInt8 dictionary_id_exists;
    auto parsed = tryParse<UInt8>(dictionary_id_exists, result);
    if (!parsed || (dictionary_id_exists != 0 && dictionary_id_exists != 1))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected message from library bridge: {} ({}). Check that bridge and server have the same version.",
                        result, parsed ? toString(dictionary_id_exists) : "failed to parse");

    LOG_TRACE(log, "dictionary_id: {}, dictionary_id_exists on bridge side: {}, library confirmed to be initialized on server side: {}",
              toString(dictionary_id), toString(dictionary_id_exists), library_initialized);

    if (dictionary_id_exists && !library_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Library was not initialized, but bridge responded to already have dictionary id: {}", dictionary_id);

    /// Here we want to say bridge to recreate a new library handler for current dictionary,
    /// because it responded to have lost it, but we know that it has already been created. (It is a direct result of bridge crash).
    if (!dictionary_id_exists && library_initialized)
    {
        LOG_WARNING(log, "Library bridge does not have library handler with dictionaty id: {}. It will be reinitialized.", dictionary_id);
        bool reinitialized = false;
        try
        {
            auto uri = createRequestURI(EXT_DICT_LIB_NEW_METHOD);
            reinitialized = executeRequest(uri, getInitLibraryCallback());
        }
        catch (...)
        {
            tryLogCurrentException(log);
            return false;
        }

        if (!reinitialized)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR,
                            "Failed to reinitialize library handler on bridge side for dictionary with id: {}", dictionary_id);
    }

    return true;
}


ReadWriteBufferFromHTTP::OutStreamCallback ExternalDictionaryLibraryBridgeHelper::getInitLibraryCallback() const
{
    /// Sample block must contain null values
    WriteBufferFromOwnString out;
    auto output_format = getContext()->getOutputFormat(ExternalDictionaryLibraryBridgeHelper::DEFAULT_FORMAT, out, sample_block);
    formatBlock(output_format, sample_block);
    auto block_string = out.str();

    return [block_string, this](std::ostream & os)
    {
        os << "library_path=" << escapeForFileName(library_data.library_path) << "&";
        os << "library_settings=" << escapeForFileName(library_data.library_settings) << "&";
        os << "attributes_names=" << escapeForFileName(library_data.dict_attributes) << "&";
        os << "sample_block=" << escapeForFileName(sample_block.getNamesAndTypesList().toString()) << "&";
        os << "null_values=" << escapeForFileName(block_string);
    };
}


bool ExternalDictionaryLibraryBridgeHelper::initLibrary()
{
    startBridgeSync();
    auto uri = createRequestURI(EXT_DICT_LIB_NEW_METHOD);
    library_initialized = executeRequest(uri, getInitLibraryCallback());
    return library_initialized;
}


bool ExternalDictionaryLibraryBridgeHelper::cloneLibrary(const Field & other_dictionary_id)
{
    startBridgeSync();
    auto uri = createRequestURI(EXT_DICT_LIB_CLONE_METHOD);
    uri.addQueryParameter("from_dictionary_id", toString(other_dictionary_id));
    /// We also pass initialization settings in order to create a library handler
    /// in case from_dictionary_id does not exist in bridge side (possible in case of bridge crash).
    library_initialized = executeRequest(uri, getInitLibraryCallback());
    return library_initialized;
}


bool ExternalDictionaryLibraryBridgeHelper::removeLibrary()
{
    /// Do not force bridge restart if it is not running in case of removeLibrary
    /// because in this case after restart it will not have this dictionaty id in memory anyway.
    if (bridgeHandShake())
    {
        auto uri = createRequestURI(EXT_DICT_LIB_DELETE_METHOD);
        return executeRequest(uri);
    }
    return true;
}


bool ExternalDictionaryLibraryBridgeHelper::isModified()
{
    startBridgeSync();
    auto uri = createRequestURI(EXT_DICT_IS_MODIFIED_METHOD);
    return executeRequest(uri);
}


bool ExternalDictionaryLibraryBridgeHelper::supportsSelectiveLoad()
{
    startBridgeSync();
    auto uri = createRequestURI(EXT_DICT_SUPPORTS_SELECTIVE_LOAD_METHOD);
    return executeRequest(uri);
}


QueryPipeline ExternalDictionaryLibraryBridgeHelper::loadAll()
{
    startBridgeSync();
    auto uri = createRequestURI(EXT_DICT_LOAD_ALL_METHOD);
    return QueryPipeline(loadBase(uri));
}


static String getDictIdsString(const std::vector<UInt64> & ids)
{
    WriteBufferFromOwnString out;
    writeVectorBinary(ids, out);
    return out.str();
}


QueryPipeline ExternalDictionaryLibraryBridgeHelper::loadIds(const std::vector<uint64_t> & ids)
{
    startBridgeSync();
    auto uri = createRequestURI(EXT_DICT_LOAD_IDS_METHOD);
    uri.addQueryParameter("ids_num", toString(ids.size())); /// Not used parameter, but helpful
    auto ids_string = getDictIdsString(ids);
    return QueryPipeline(loadBase(uri, [ids_string](std::ostream & os) { os << ids_string; }));
}


QueryPipeline ExternalDictionaryLibraryBridgeHelper::loadKeys(const Block & requested_block)
{
    startBridgeSync();
    auto uri = createRequestURI(EXT_DICT_LOAD_KEYS_METHOD);
    /// Sample block to parse block from callback
    uri.addQueryParameter("requested_block_sample", requested_block.getNamesAndTypesList().toString());
    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [requested_block, this](std::ostream & os)
    {
        WriteBufferFromOStream out_buffer(os);
        auto output_format = getContext()->getOutputFormat(ExternalDictionaryLibraryBridgeHelper::DEFAULT_FORMAT, out_buffer, requested_block.cloneEmpty());
        formatBlock(output_format, requested_block);
    };
    return QueryPipeline(loadBase(uri, out_stream_callback));
}


bool ExternalDictionaryLibraryBridgeHelper::executeRequest(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback) const
{
    ReadWriteBufferFromHTTP buf(
        uri,
        Poco::Net::HTTPRequest::HTTP_POST,
        std::move(out_stream_callback),
        http_timeouts, credentials);

    bool res;
    readBoolText(res, buf);
    return res;
}


QueryPipeline ExternalDictionaryLibraryBridgeHelper::loadBase(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback)
{
    auto read_buf_ptr = std::make_unique<ReadWriteBufferFromHTTP>(
        uri,
        Poco::Net::HTTPRequest::HTTP_POST,
        std::move(out_stream_callback),
        http_timeouts,
        credentials,
        0,
        DBMS_DEFAULT_BUFFER_SIZE,
        getContext()->getReadSettings(),
        ReadWriteBufferFromHTTP::HTTPHeaderEntries{});

    auto source = FormatFactory::instance().getInput(ExternalDictionaryLibraryBridgeHelper::DEFAULT_FORMAT, *read_buf_ptr, sample_block, getContext(), DEFAULT_BLOCK_SIZE);
    source->addBuffer(std::move(read_buf_ptr));
    return QueryPipeline(std::move(source));
}

}
