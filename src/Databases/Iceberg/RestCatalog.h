#pragma once
#include <Databases/Iceberg/ICatalog.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>
#include <Poco/JSON/Object.h>

namespace DB
{
class ReadBuffer;
}

namespace Iceberg
{

class RestCatalog final : public ICatalog, private DB::WithContext
{
public:
    explicit RestCatalog(
        const std::string & warehouse_,
        const std::string & base_url_,
        const std::string & catalog_credential_,
        const DB::HTTPHeaderEntries & headers_,
        DB::ContextPtr context_);

    ~RestCatalog() override = default;

    bool empty() const override;

    Tables getTables() const override;

    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;

    void getTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    bool tryGetTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override;

private:
    LoggerPtr log;

    struct Config
    {
        std::filesystem::path prefix;
        std::string default_base_location;

        std::string toString() const;
    };

    const std::filesystem::path base_url;
    DB::HTTPHeaderEntries headers;
    std::string client_id;
    std::string client_secret;
    Config config;

    Poco::Net::HTTPBasicCredentials credentials{};


    DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
        const std::string & endpoint,
        const Poco::URI::QueryParameters & params = {}) const;

    Poco::URI::QueryParameters createParentNamespaceParams(const std::string & base_namespace) const;

    using StopCondition = std::function<bool(const std::string & namespace_name)>;
    void getNamespacesRecursive(const std::string & base_namespace, Namespaces & result, StopCondition stop_condition) const;

    Namespaces getNamespaces(const std::string & base_namespace) const;

    Namespaces parseNamespaces(DB::ReadBuffer & buf, const std::string & base_namespace) const;

    Tables getTables(const std::string & base_namespace, size_t limit = 0) const;

    Tables parseTables(DB::ReadBuffer & buf, const std::string & base_namespace, size_t limit) const;

    bool getTableMetadataImpl(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const;

    Config loadConfig();
    static void parseConfig(const Poco::JSON::Object::Ptr & object, Config & result);
};

}
