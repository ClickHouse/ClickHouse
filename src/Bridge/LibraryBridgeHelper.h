#pragma once

#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>
#include <Bridge/IBridgeHelper.h>


namespace DB
{

class LibraryBridgeHelper : public IBridgeHelper
{

public:
    static constexpr inline size_t DEFAULT_PORT = 9012;

    LibraryBridgeHelper(ContextPtr context_, const Block & sample_block, const Field & dictionary_id_);

    bool initLibrary(const std::string & library_path, std::string library_settings, std::string attributes_names);

    bool cloneLibrary(const Field & other_dictionary_id);

    bool removeLibrary();

    bool isModified();

    bool supportsSelectiveLoad();

    BlockInputStreamPtr loadAll();

    BlockInputStreamPtr loadIds(std::string ids_string);

    BlockInputStreamPtr loadKeys(const Block & requested_block);

    BlockInputStreamPtr loadBase(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {});

    bool executeRequest(const Poco::URI & uri, ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = {});


protected:
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

private:
    static constexpr inline auto LIB_NEW_METHOD = "libNew";
    static constexpr inline auto LIB_CLONE_METHOD = "libClone";
    static constexpr inline auto LIB_DELETE_METHOD = "libDelete";
    static constexpr inline auto LOAD_ALL_METHOD = "loadAll";
    static constexpr inline auto LOAD_IDS_METHOD = "loadIds";
    static constexpr inline auto LOAD_KEYS_METHOD = "loadKeys";
    static constexpr inline auto IS_MODIFIED_METHOD = "isModified";
    static constexpr inline auto SUPPORTS_SELECTIVE_LOAD_METHOD = "supportsSelectiveLoad";

    Poco::URI createRequestURI(const String & method) const;

    Poco::Logger * log;
    const Block sample_block;
    const Poco::Util::AbstractConfiguration & config;
    const Poco::Timespan http_timeout;

    Field dictionary_id;
    std::string bridge_host;
    size_t bridge_port;
};

}
