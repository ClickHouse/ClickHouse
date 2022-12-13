#pragma once

#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <Bridge/IBridgeHelper.h>


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

    Pipe loadAll();

    Pipe loadIds(const std::vector<uint64_t> & ids);

    Pipe loadKeys(const Block & requested_block);

    Pipe loadBase(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {});

    bool executeRequest(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {}) const;

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

    ReadWriteBufferFromHTTP::OutStreamCallback getInitLibraryCallback() const;

private:
    static constexpr inline auto LIB_NEW_METHOD = "libNew";
    static constexpr inline auto LIB_CLONE_METHOD = "libClone";
    static constexpr inline auto LIB_DELETE_METHOD = "libDelete";
    static constexpr inline auto LOAD_ALL_METHOD = "loadAll";
    static constexpr inline auto LOAD_IDS_METHOD = "loadIds";
    static constexpr inline auto LOAD_KEYS_METHOD = "loadKeys";
    static constexpr inline auto IS_MODIFIED_METHOD = "isModified";
    static constexpr inline auto PING = "ping";
    static constexpr inline auto SUPPORTS_SELECTIVE_LOAD_METHOD = "supportsSelectiveLoad";

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
