#pragma once
#include "config.h"

#if USE_AVRO

#include <Databases/Iceberg/ICatalog.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPHeaderEntries.h>
#include <Interpreters/Context_fwd.h>
#include <filesystem>
#include <Poco/JSON/Object.h>

namespace DeltaLake
{

using namespace Iceberg;

class UnityCatalog final : public ICatalog, private DB::WithContext
{
public:
    UnityCatalog(
        const std::string & catalog_,
        const std::string & base_url_,
        const std::string & catalog_credential_,
        DB::ContextPtr context_);

    ~UnityCatalog() override = default;

    bool empty() const override;

    DB::Names getTables() const override;

    bool existsTable(const std::string & schema_name, const std::string & table_name) const override;

    void getTableMetadata(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    bool tryGetTableMetadata(
        const std::string & schema_name,
        const std::string & table_name,
        TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override { return std::nullopt; }

private:
    const std::filesystem::path base_url;
    const LoggerPtr log;

    DB::HTTPHeaderEntry auth_header;

    Poco::Net::HTTPBasicCredentials credentials{};

    DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
        const std::string & endpoint,
        const Poco::URI::QueryParameters & params = {},
        const DB::HTTPHeaderEntries & headers = {},
        const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
        std::function<void(std::ostream &)> out_stream_callaback = {}
    ) const;

    Iceberg::ICatalog::Namespaces getSchemas(const std::string & base_prefix, size_t limit = 0) const;

    DB::Names getTablesForSchema(const std::string & schema, size_t limit = 0) const;
    void getCredentials(const std::string & table_id, TableMetadata & metadata) const;

    bool getTableMetadataImpl(
        const std::string & namespace_name,
        const std::string & table_name,
        TableMetadata & result) const;
};
   
}

#endif
