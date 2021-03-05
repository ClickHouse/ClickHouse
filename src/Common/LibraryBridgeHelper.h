#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_EXECUTABLE_NOT_FOUND;
}


class LibraryBridgeHelper
{

public:
    LibraryBridgeHelper(
        const Context & context_,
        const std::string & dictionary_id_);

    std::vector<std::pair<std::string, std::string>> getURLParams(const NamesAndTypesList & cols, size_t max_block_size) const;

    bool initLibrary(const std::string & library_path, const std::string librray_settings);

    bool deleteLibrary();

    BlockInputStreamPtr loadAll(const std::string attributes_string, const Block & sample_block);

    BlockInputStreamPtr loadIds(const std::string attributes_string, const std::string ids_string, const Block & sample_block);

    bool isModified();

    bool supportsSelectiveLoad();

    static constexpr inline size_t DEFAULT_PORT = 9018;
    static constexpr inline auto DEFAULT_HOST = "127.0.0.1";

    static constexpr inline auto PING_HANDLER = "/ping";

    static constexpr inline auto LIB_NEW_METHOD = "libNew";
    static constexpr inline auto LIB_DELETE_METHOD = "libDelete";
    static constexpr inline auto LOAD_ALL_METHOD = "loadAll";
    static constexpr inline auto LOAD_IDS_METHOD = "loadIds";
    static constexpr inline auto IS_MODIFIED_METHOD = "isModified";
    static constexpr inline auto SUPPORTS_SELECTIVE_LOAD_METHOD = "supportsSelectiveLoad";

    static constexpr inline auto DEFAULT_FORMAT = "RowBinary";
    static constexpr inline auto PING_OK_ANSWER = "Ok.";

    static const inline std::string PING_METHOD = Poco::Net::HTTPRequest::HTTP_GET;
    static const inline std::string MAIN_METHOD = Poco::Net::HTTPRequest::HTTP_POST;

    bool isLibraryBridgeRunning() const;

    void startLibraryBridge() const;

    void startLibraryBridgeSync() const;

private:
    Poco::URI getDictionaryURI() const;

    Poco::URI getPingURI() const;

    Poco::URI getBaseURI() const;

    Poco::Logger * log;
    const Context & context;

    const std::string dictionary_id;

    std::string bridge_host;
    size_t bridge_port;

};
}
