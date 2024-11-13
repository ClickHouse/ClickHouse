#pragma once
#include <Databases/Iceberg/ICatalog.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>

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
        const std::string & catalog_name_,
        const std::string & base_url_,
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

private:
    const std::filesystem::path base_url;
    Poco::Net::HTTPBasicCredentials credentials{};
    LoggerPtr log;

    DB::ReadWriteBufferFromHTTPPtr createReadBuffer(const std::string & endpoint, const Poco::URI::QueryParameters & params = {}) const;

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
};

}
