#pragma once
#include "config.h"

#if USE_AVRO
#include <filesystem>
#include <optional>
#include <unordered_map>
#include <vector>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Databases/DataLake/ICatalog.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPBasicCredentials.h>

namespace DataLake
{
namespace Paimon
{
static constexpr Int32 LIST_MAX_RESULTS = 100;

/// http endpoints
static constexpr auto API_VERSION = "v1";
static constexpr auto CONFIG_ENDPOINT = "config";
static constexpr auto DATABASES_ENDPOINT = "databases";
static constexpr auto TABLES_ENDPOINT = "tables";
static constexpr auto TABLE_TOKEN_ENDPOINT = "token";

/// headers
static constexpr auto DLF_AUTHORIZATION_HEADER_KEY = "Authorization";
static constexpr auto DLF_CONTENT_MD5_HEADER_KEY = "Content-MD5";
static constexpr auto DLF_CONTENT_TYPE_KEY = "Content-Type";
static constexpr auto DLF_DATE_HEADER_KEY = "x-dlf-date";
static constexpr auto DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token";
static constexpr auto DLF_AUTH_VERSION_HEADER_KEY = "x-dlf-version";
static constexpr auto DLF_CONTENT_SHA56_HEADER_KEY = "x-dlf-content-sha256";
static constexpr auto DLF_CONTENT_SHA56_VALUE = "UNSIGNED-PAYLOAD";
static constexpr auto AUTH_DATE_TIME_FORMATTER = "{:%Y%m%dT%H%M%SZ}";
static constexpr auto DLF_NEW_LINE = "\n";

/// DLF auth signature
static constexpr auto VERSION = "v1";
static constexpr auto SIGNATURE_ALGORITHM = "DLF4-HMAC-SHA256";
static constexpr auto PRODUCT = "DlfNext";
static constexpr auto HMAC_SHA256 = "HmacSHA256";
static constexpr auto REQUEST_TYPE = "aliyun_v4_request";
static constexpr auto SIGNATURE_KEY = "Signature";
static constexpr auto NEW_LINE = "\n";

}


struct PaimonToken
{
    const String token_provider;
    const String bearer_token;
    const String dlf_access_key_id;
    const String dlf_access_key_secret;
    mutable String dlf_generated_authorization;

    explicit PaimonToken(const String & bearer_token_)
        : token_provider("bearer")
        , bearer_token(bearer_token_)
    {
    }

    PaimonToken(const String & access_key_id_, const String & access_key_secret_)
        : token_provider("dlf")
        , dlf_access_key_id(access_key_id_)
        , dlf_access_key_secret(access_key_secret_)
    {
    }
};

class PaimonRestCatalog final : public ICatalog, private DB::WithContext
{
public:
    explicit PaimonRestCatalog(
        const String & warehouse_, const String & base_url_, const PaimonToken & token_, const String & region, DB::ContextPtr context_);

    ~PaimonRestCatalog() override = default;

    bool empty() const override;

    DB::Names getTables() const override;

    bool existsTable(const String & database_name, const String & table_name) const override;

    void getTableMetadata(const String & database_name, const String & table_name, TableMetadata & result) const override;

    bool tryGetTableMetadata(const String & database_name, const String & table_name, TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override { return storage_type; }

    DB::DatabaseDataLakeCatalogType getCatalogType() const override { return DB::DatabaseDataLakeCatalogType::PAIMON_REST; }

private:
    using StopCondition = std::function<bool(const String &)>;
    using ExecuteFunc = std::function<void(const String &)>;
    const static std::vector<String> SIGNED_HEADERS;
    const std::filesystem::path base_url;
    std::optional<PaimonToken> token;
    String region;
    String prefix;
    String warehouse_root_path;
    std::optional<StorageType> storage_type;
    const LoggerPtr log;
    Poco::Net::HTTPBasicCredentials credentials{};

    // void listDatabases();
    void createAuthHeaders(
        DB::HTTPHeaderEntries & current_headers,
        const String & resource_path,
        const std::unordered_map<String, String> & query_params,
        const String & method,
        const std::optional<String> & data = std::nullopt) const;

    DB::ReadWriteBufferFromHTTPPtr createReadBuffer(
        const String & endpoint,
        const String & method,
        const Poco::URI::QueryParameters & params = {},
        const DB::HTTPHeaderEntries & headers = {}) const;

    void loadConfig();

    void forEachDatabase(DB::Strings & databases, StopCondition stop_condition = {}, ExecuteFunc execute_func = {}) const;

    void forEachTables(const String & database, DB::Names & tables, StopCondition stop_condition = {}, ExecuteFunc execute_func = {}) const;

    Poco::JSON::Object::Ptr requestRest(
        const String & endpoint,
        const String & method,
        const Poco::URI::QueryParameters & params = {},
        const DB::HTTPHeaderEntries & headers = {}) const;
};
}
#endif
