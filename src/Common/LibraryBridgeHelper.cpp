#include <Common/LibraryBridgeHelper.h>

#include <sstream>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Poco/Path.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ShellCommand.h>
#include <common/logger_useful.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_SERVER_IS_NOT_RESPONDING;
}


LibraryBridgeHelper::LibraryBridgeHelper(
        const Context & context_,
        const std::string & dictionary_id_)
    : log(&Poco::Logger::get("LibraryBridgeHelper"))
    , context(context_)
    , dictionary_id(dictionary_id_)
{
    const auto & config = context.getConfigRef();
    bridge_port = config.getUInt("library_bridge.port", DEFAULT_PORT);
    bridge_host = config.getString("library_bridge.host", DEFAULT_HOST);
}


Poco::URI LibraryBridgeHelper::getPingURI() const
{
    auto uri = getBaseURI();
    uri.setPath(PING_HANDLER);
    return uri;
}


Poco::URI LibraryBridgeHelper::getDictionaryURI() const
{
    auto uri = getBaseURI();
    uri.setPath('/' + dictionary_id);
    return uri;
}


Poco::URI LibraryBridgeHelper::getBaseURI() const
{
    Poco::URI uri;
    uri.setHost(bridge_host);
    uri.setPort(bridge_port);
    uri.setScheme("http");
    return uri;
}


bool LibraryBridgeHelper::initLibrary(const std::string & library_path, const std::string library_settings)
{
    startLibraryBridgeSync();

    auto uri = getDictionaryURI();
    uri.addQueryParameter("method", LIB_NEW_METHOD);
    uri.addQueryParameter("library_path", library_path);
    uri.addQueryParameter("library_settings", library_settings);

    ReadWriteBufferFromHTTP buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(context));
    bool res;
    readBoolText(res, buf);
    return res;
}


bool LibraryBridgeHelper::deleteLibrary()
{
    startLibraryBridgeSync();

    auto uri = getDictionaryURI();
    uri.addQueryParameter("method", LIB_DELETE_METHOD);

    ReadWriteBufferFromHTTP buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(context));
    bool res;
    readBoolText(res, buf);
    return res;
}


bool LibraryBridgeHelper::isModified()
{
    startLibraryBridgeSync();

    auto uri = getDictionaryURI();
    uri.addQueryParameter("method", IS_MODIFIED_METHOD);

    ReadWriteBufferFromHTTP buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(context));
    bool res;
    readBoolText(res, buf);
    return res;
}


bool LibraryBridgeHelper::supportsSelectiveLoad()
{
    startLibraryBridgeSync();

    auto uri = getDictionaryURI();
    uri.addQueryParameter("method", SUPPORTS_SELECTIVE_LOAD_METHOD);

    ReadWriteBufferFromHTTP buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(context));
    bool res;
    readBoolText(res, buf);
    return res;
}


BlockInputStreamPtr LibraryBridgeHelper::loadAll(const std::string attributes_string, const Block & sample_block)
{
    startLibraryBridgeSync();

    auto uri = getDictionaryURI();

    uri.addQueryParameter("method", LOAD_ALL_METHOD);
    uri.addQueryParameter("attributes", attributes_string);
    uri.addQueryParameter("columns", sample_block.getNamesAndTypesList().toString());

    /// TODO: timeouts?

    ReadWriteBufferFromHTTP read_buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, {});

    auto format = FormatFactory::instance().getInput(LibraryBridgeHelper::DEFAULT_FORMAT, read_buf, sample_block, context, DEFAULT_BLOCK_SIZE);
    auto reader = std::make_shared<InputStreamFromInputFormat>(format);
    auto block = reader->read();

    return std::make_shared<OneBlockInputStream>(block);
}


BlockInputStreamPtr LibraryBridgeHelper::loadIds(const std::string attributes_string, const std::string ids_string, const Block & sample_block)
{
    startLibraryBridgeSync();

    auto uri = getDictionaryURI();

    uri.addQueryParameter("method", LOAD_IDS_METHOD);
    uri.addQueryParameter("attributes", attributes_string);
    uri.addQueryParameter("ids", ids_string);
    uri.addQueryParameter("columns", sample_block.getNamesAndTypesList().toString());

    /// TODO: timeouts?

    ReadWriteBufferFromHTTP read_buf(uri, Poco::Net::HTTPRequest::HTTP_POST, {}, {});

    auto format = FormatFactory::instance().getInput(LibraryBridgeHelper::DEFAULT_FORMAT, read_buf, sample_block, context, DEFAULT_BLOCK_SIZE);
    auto reader = std::make_shared<InputStreamFromInputFormat>(format);
    auto block = reader->read();

    return std::make_shared<OneBlockInputStream>(block);
}


bool LibraryBridgeHelper::isLibraryBridgeRunning() const
{
    try
    {
        ReadWriteBufferFromHTTP buf(getPingURI(), Poco::Net::HTTPRequest::HTTP_GET, {}, ConnectionTimeouts::getHTTPTimeouts(context));
        return checkString(PING_OK_ANSWER, buf);
    }
    catch (...)
    {
        return false;
    }
}


void LibraryBridgeHelper::startLibraryBridgeSync() const
{
    if (!isLibraryBridgeRunning())
    {
        LOG_TRACE(log, "clickhouse-library-bridge is not running, will try to start it");
        startLibraryBridge();

        bool started = false;
        uint64_t milliseconds_to_wait = 10; /// Exponential backoff
        uint64_t counter = 0;

        while (milliseconds_to_wait < 10000)
        {
            ++counter;
            LOG_TRACE(log, "Checking clickhouse-library-bridge is running, try {}", counter);

            if (isLibraryBridgeRunning())
            {
                started = true;
                break;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds_to_wait));
            milliseconds_to_wait *= 2;
        }

        if (!started)
            throw Exception("LibraryBridgeHelper: clickhouse-library-bridge is not responding", ErrorCodes::EXTERNAL_SERVER_IS_NOT_RESPONDING);
    }
}


void LibraryBridgeHelper::startLibraryBridge() const
{
    const auto & config = context.getConfigRef();
    const auto & settings = context.getSettingsRef();

    /// Path to executable folder
    Poco::Path path{config.getString("application.dir", "/usr/bin")};

    std::vector<std::string> cmd_args;
    path.setFileName("clickhouse-library-bridge");

    cmd_args.push_back("--http-port");
    cmd_args.push_back(std::to_string(config.getUInt("library_bridge.port", DEFAULT_PORT)));

    cmd_args.push_back("--listen-host");
    cmd_args.push_back(config.getString("librray_bridge.listen_host", DEFAULT_HOST));

    cmd_args.push_back("--http-timeout");
    cmd_args.push_back(std::to_string(settings.http_receive_timeout.value.totalSeconds()));

    if (config.has("logger.library_bridge_log"))
    {
        cmd_args.push_back("--log-path");
        cmd_args.push_back(config.getString("logger.library_bridge_log"));
    }

    if (config.has("logger.library_bridge_errlog"))
    {
        cmd_args.push_back("--err-log-path");
        cmd_args.push_back(config.getString("logger.library_bridge_errlog"));
    }

    if (config.has("logger.library_bridge_stdout"))
    {
        cmd_args.push_back("--stdout-path");
        cmd_args.push_back(config.getString("logger.library_bridge_stdout"));
    }

    if (config.has("logger.library_bridge_stderr"))
    {
        cmd_args.push_back("--stderr-path");
        cmd_args.push_back(config.getString("logger.library_bridge_stderr"));
    }

    if (config.has("logger.library_level"))
    {
        cmd_args.push_back("--log-level");
        cmd_args.push_back(config.getString("logger.library_bridge_level"));
    }

    LOG_TRACE(log, "Starting clickhouse-library-bridge");

    auto cmd =ShellCommand::executeDirect(path.toString(), cmd_args, true);
    cmd->wait();
}

}
