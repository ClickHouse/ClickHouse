#pragma once

#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <BridgeHelper/IBridgeHelper.h>
#include <QueryPipeline/QueryPipeline.h>


namespace DB
{

class Pipe;

class LibraryBridgeHelper : public IBridgeHelper
{

public:
    struct LibraryInitData
    {
        String library_path;
        String library_settings;
        String dict_attributes;
    };

    static constexpr inline size_t DEFAULT_PORT = 9012;

    LibraryBridgeHelper(ContextPtr context_, const Block & sample_block, const Field & dictionary_id_, const LibraryInitData & library_data_);

    bool initLibrary();

    bool cloneLibrary(const Field & other_dictionary_id);

    bool removeLibrary();

    bool isModified();

    bool supportsSelectiveLoad();

    QueryPipeline loadAll();

    QueryPipeline loadIds(const std::vector<uint64_t> & ids);

    QueryPipeline loadKeys(const Block & requested_block);

    LibraryInitData getLibraryData() const { return library_data; }

protected:
    bool bridgeHandShake() override;

    void startBridge(std::unique_ptr<ShellCommand> cmd) const override;

    String serviceAlias() const override { return "clickhouse-library-bridge"; }

    String serviceFileName() const override { return serviceAlias(); }

    size_t getDefaultPort() const override { return DEFAULT_PORT; }

    bool startBridgeManually() const override { return false; }

    String configPrefix() const override { return "library_bridge"; }

    const Poco::Util::AbstractConfiguration & getConfig() const override { return config; }

    Poco::Logger * getLog() const override { return log; }

    Poco::Timespan getHTTPTimeout() const override { return http_timeout; }

    Poco::URI createBaseURI() const override;

    QueryPipeline loadBase(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {});

    bool executeRequest(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {}) const;

    ReadWriteBufferFromHTTP::OutStreamCallback getInitLibraryCallback() const;

private:
    static constexpr inline auto EXT_DICT_LIB_NEW_METHOD = "extDict_libNew";
    static constexpr inline auto EXT_DICT_LIB_CLONE_METHOD = "extDict_libClone";
    static constexpr inline auto EXT_DICT_LIB_DELETE_METHOD = "extDict_libDelete";
    static constexpr inline auto EXT_DICT_LOAD_ALL_METHOD = "extDict_loadAll";
    static constexpr inline auto EXT_DICT_LOAD_IDS_METHOD = "extDict_loadIds";
    static constexpr inline auto EXT_DICT_LOAD_KEYS_METHOD = "extDict_loadKeys";
    static constexpr inline auto EXT_DICT_IS_MODIFIED_METHOD = "extDict_isModified";
    static constexpr inline auto PING = "ping";
    static constexpr inline auto EXT_DICT_SUPPORTS_SELECTIVE_LOAD_METHOD = "extDict_supportsSelectiveLoad";

    Poco::URI createRequestURI(const String & method) const;

    static String getDictIdsString(const std::vector<UInt64> & ids);

    Poco::Logger * log;
    const Block sample_block;
    const Poco::Util::AbstractConfiguration & config;
    const Poco::Timespan http_timeout;

    LibraryInitData library_data;
    Field dictionary_id;
    std::string bridge_host;
    size_t bridge_port;
    bool library_initialized = false;
    ConnectionTimeouts http_timeouts;
    Poco::Net::HTTPBasicCredentials credentials{};
};

}
