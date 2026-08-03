
#include "config.h"

#if USE_AVRO

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <Core/Names.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Databases/DataLake/PaimonRestCatalog.h>
#include <Databases/DataLake/StorageCredentials.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonTableSchema.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <base/types.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Logger.h>
#include <Poco/MD5Engine.h>
#include <Poco/String.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/logger_useful.h>


namespace DB::ErrorCodes
{
    extern const int DATALAKE_DATABASE_ERROR;
    extern const int BAD_ARGUMENTS;
}


namespace DataLake
{
using namespace DataLake::Paimon;

String md5(const String & input)
{
    Poco::MD5Engine md5;
    md5.update(input);
    return DB::base64Encode(String(reinterpret_cast<const char *>(md5.digest().data()), md5.digestLength()));
}

String bytesToHex(const String & bytes)
{
    const char hex_digits[] = "0123456789abcdef";
    DB::WriteBufferFromOwnString hex_str;
    for (const auto byte : bytes)
    {
        DB::writeChar(hex_digits[(byte >> 4) & 0x0F], hex_str);
        DB::writeChar(hex_digits[byte & 0x0F], hex_str);
    }
    return hex_str.str();
}


const std::vector<String> PaimonRestCatalog::SIGNED_HEADERS
    = {Poco::toLower(String(DLF_CONTENT_MD5_HEADER_KEY)),
       Poco::toLower(String(DLF_CONTENT_TYPE_KEY)),
       Poco::toLower(String(DLF_CONTENT_SHA56_HEADER_KEY)),
       Poco::toLower(String(DLF_DATE_HEADER_KEY)),
       Poco::toLower(String(DLF_AUTH_VERSION_HEADER_KEY)),
       Poco::toLower(String(DLF_SECURITY_TOKEN_HEADER_KEY))};

PaimonRestCatalog::PaimonRestCatalog(
    const String & warehouse_, const String & base_url_, const PaimonToken & token_, const String & region_, DB::ContextPtr context_)
    : ICatalog(warehouse_)
    , DB::WithContext(context_)
    , base_url(base_url_)
    , token(token_)
    , region(region_)
    , log(getLogger("PaimonRestCatalog(" + warehouse_ + ")"))
{
    loadConfig();
}

void PaimonRestCatalog::loadConfig()
{
    Poco::URI::QueryParameters params = {{"warehouse", warehouse}};
    auto object = requestRest(std::filesystem::path(API_VERSION) / CONFIG_ENDPOINT, "GET", params);
    auto set_config = [this](const Poco::JSON::Object::Ptr & object_)
    {
        if (!object_)
            return;
        if (object_->has("prefix"))
            this->prefix = object_->get("prefix").extract<String>();
        if (object_->has("warehouse"))
        {
            this->warehouse_root_path = object_->get("warehouse").extract<String>();
        }
    };

    const auto & default_config = object->getObject("defaults");
    set_config(default_config);
    const auto & override_config = object->getObject("overrides");
    set_config(override_config);


    if (prefix.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Could not found prefix in catalog configuration.");
    if (warehouse.empty())
    {
        LOG_WARNING(log, "Could not found warehouse in catalog configuration.");
    }
    else
    {
        storage_type = parseStorageTypeFromLocation(warehouse_root_path);
    }
}

void PaimonRestCatalog::createAuthHeaders(
    DB::HTTPHeaderEntries & current_headers,
    const String & resource_path,
    const std::unordered_map<String, String> & query_params,
    const String & method,
    const std::optional<String> & data) const
{
    if (!token.has_value())
    {
        return;
    }
    if (token->token_provider == "bearer")
    {
        current_headers.emplace_back("Authorization", fmt::format("Bearer {}", token->bearer_token));
        return;
    }
    else if (token->token_provider == "dlf")
    {
        std::unordered_map<String, String> headers_map;
        for (const auto & entry : current_headers)
        {
            headers_map.emplace(entry.name, entry.value);
        }
        auto get_or_default = [](const std::unordered_map<String, String> & map, const String & key_, const String & value_)
        {
            auto it = map.find(key_);
            return it == map.end() ? value_ : it->second;
        };
        auto generate_sign_headers = [&headers_map](std::optional<String> data_, String date_time_, std::optional<String> security_token_)
        {
            headers_map.emplace(DLF_DATE_HEADER_KEY, date_time_);
            headers_map.emplace(DLF_CONTENT_SHA56_HEADER_KEY, DLF_CONTENT_SHA56_VALUE);
            headers_map.emplace(DLF_AUTH_VERSION_HEADER_KEY, "v1");
            if (data_.has_value() && !data_.value().empty())
            {
                headers_map.emplace(DLF_CONTENT_TYPE_KEY, "application/json");
                headers_map.emplace(DLF_CONTENT_MD5_HEADER_KEY, md5(data_.value()));
            }
            if (security_token_.has_value())
            {
                headers_map.emplace(DLF_SECURITY_TOKEN_HEADER_KEY, security_token_.value());
            }
            else
            {
                headers_map.emplace(DLF_SECURITY_TOKEN_HEADER_KEY, "");
            }
        };
        auto get_authorization
            = [&headers_map, &query_params, &resource_path, &method, get_or_default, this](String date, String date_time) -> String
        {
            auto get_canonical_request = [&]()
            {
                /// query params
                std::map<String, String> params_ordered_map(query_params.begin(), query_params.end());
                DB::WriteBufferFromOwnString canonical_part;
                String sep;
                for (const auto & entry : params_ordered_map)
                {
                    DB::writeString(sep, canonical_part);
                    DB::writeString(trim(entry.first), canonical_part);
                    DB::writeString("=", canonical_part);
                    if (!entry.second.empty())
                    {
                        DB::writeString(trim(entry.second), canonical_part);
                    }
                    sep = "&";
                }

                /// signed headers
                std::map<String, String> headers_ordered_map;
                for (const auto & entry : headers_map)
                {
                    String key = Poco::toLower(entry.first);
                    if (std::find(SIGNED_HEADERS.begin(), SIGNED_HEADERS.end(), key) != SIGNED_HEADERS.end())
                    {
                        headers_ordered_map.emplace(key, trim(entry.second));
                    }
                }

                String canonical_request_resource_path = resource_path.starts_with("/") ? resource_path : fmt::format("/{}", resource_path);
                std::vector<String> join_vec{method, canonical_request_resource_path, canonical_part.str()};
                for (const auto & entry : headers_ordered_map)
                {
                    join_vec.emplace_back(fmt::format("{}:{}", entry.first, entry.second));
                }
                String content_sha56 = get_or_default(headers_map, DLF_CONTENT_SHA56_HEADER_KEY, DLF_CONTENT_SHA56_VALUE);
                join_vec.emplace_back(content_sha56);
                return fmt::to_string(fmt::join(join_vec, DLF_NEW_LINE));
            };
            String canonical_request = get_canonical_request();
            LOG_TEST(log, "canonical_request: {}", canonical_request);
            String string_to_sign = fmt::to_string(fmt::join(
                {String(SIGNATURE_ALGORITHM),
                 date_time,
                 fmt::format("{}/{}/{}/{}", date, region, PRODUCT, REQUEST_TYPE),
                 bytesToHex(DB::encodeSHA256(canonical_request))},
                DLF_NEW_LINE));

            String key_secret = fmt::format("aliyun_v4{}", token->dlf_access_key_secret);
            std::vector<uint8_t> key_secret_byte(key_secret.data(), key_secret.data() + key_secret.length());
            auto date_key = DB::hmacSHA256(key_secret_byte, date);
            auto date_region_key = DB::hmacSHA256(date_key, region);
            auto date_region_service_key = DB::hmacSHA256(date_region_key, PRODUCT);
            auto signing_key = DB::hmacSHA256(date_region_service_key, REQUEST_TYPE);
            auto result = DB::hmacSHA256(signing_key, string_to_sign);
            String signature = bytesToHex(String(reinterpret_cast<const char *>(result.data()), result.size()));
            return fmt::to_string(fmt::join(
                {fmt::format(
                     "{} Credential={}/{}/{}/{}/{}", SIGNATURE_ALGORITHM, token->dlf_access_key_id, date, region, PRODUCT, REQUEST_TYPE),
                 fmt::format("{}={}", SIGNATURE_KEY, signature)},
                ","));
        };
        DB::HTTPHeaderEntries result;
        auto now = std::chrono::system_clock::now();
        std::time_t now_time_t = std::chrono::system_clock::to_time_t(now);
        thread_local struct tm utc_tm_buf;
        struct tm * utc_tm = gmtime_r(&now_time_t, &utc_tm_buf);
        String date_time = get_or_default(headers_map, DLF_DATE_HEADER_KEY, fmt::format(AUTH_DATE_TIME_FORMATTER, *utc_tm));
        String date = date_time.substr(0, 8);
        generate_sign_headers(data, date_time, std::nullopt);
        String authorization
            = token->dlf_generated_authorization.empty() ? get_authorization(date, date_time) : token->dlf_generated_authorization;
        token->dlf_generated_authorization = authorization;
        headers_map.emplace(DLF_AUTHORIZATION_HEADER_KEY, authorization);
        current_headers.clear();
        for (const auto & entry : headers_map)
        {
            current_headers.emplace_back(entry.first, entry.second);
        }
        return;
    }
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown token provider: {}", token->token_provider);
}

DB::ReadWriteBufferFromHTTPPtr PaimonRestCatalog::createReadBuffer(
    const String & endpoint, const String & method, const Poco::URI::QueryParameters & params, const DB::HTTPHeaderEntries & headers) const
{
    const auto & context = getContext();
    Poco::URI url(base_url / endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    auto create_buffer = [&, this]()
    {
        std::unordered_map<String, String> query_parameters_map;
        for (const auto & entry : params)
        {
            query_parameters_map.emplace(entry.first, entry.second);
        }
        DB::HTTPHeaderEntries request_headers(headers);
        createAuthHeaders(request_headers, endpoint, query_parameters_map, method);


        DB::WriteBufferFromOwnString headers_string;
        headers_string << "{";
        for (const auto & entry : request_headers)
        {
            headers_string << entry.name << ": " << entry.value;
        }
        headers_string << "}";
        LOG_TRACE(log, "Request headers: {}", headers_string.str());

        return DB::BuilderRWBufferFromHTTP(url)
            .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
            .withSettings(getContext()->getReadSettings())
            .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
            .withHostFilter(&getContext()->getRemoteHostFilter())
            .withHeaders(request_headers)
            .withDelayInit(false)
            .withSkipNotFound(false)
            .create(credentials);
    };

    bool refresh_token = true;
    LOG_TRACE(log, "Requesting endpoint: {}", endpoint);
    try
    {
        return create_buffer();
    }
    catch (DB::HTTPException & e)
    {
        if (e.code() == Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED && refresh_token && token->token_provider == "dlf")
        {
            refresh_token = false;
            token->dlf_generated_authorization = "";
            return create_buffer();
        }
        throw;
    }
}

void PaimonRestCatalog::forEachDatabase(DB::Strings & databases, StopCondition stop_condition, ExecuteFunc execute_func) const
{
    auto json_ptr = requestRest(
        std::filesystem::path(API_VERSION) / prefix / DATABASES_ENDPOINT, "GET", {{"maxResults", fmt::to_string(LIST_MAX_RESULTS)}});
    auto databases_array = json_ptr->getArray("databases");
    String next_page_token;
    if (json_ptr->has("nextPageToken") && !json_ptr->isNull("nextPageToken"))
    {
        next_page_token = json_ptr->getValue<String>("nextPageToken");
    }
    bool first_iteration = true;
    bool stop = false;

    while (first_iteration || !next_page_token.empty())
    {
        first_iteration = false;
        for (unsigned int i = 0; i < databases_array->size(); ++i)
        {
            const String & database_name = databases_array->getElement<String>(i);
            databases.emplace_back(database_name);
            LOG_TRACE(log, "execute for database: {}", database_name);
            if (execute_func)
            {
                execute_func(database_name);
            }
            if (stop_condition && stop_condition(database_name))
            {
                stop = true;
                break;
            }
        }
        if (stop)
        {
            break;
        }
        Poco::URI::QueryParameters params = {
            {"maxResults", fmt::to_string(LIST_MAX_RESULTS)},
        };
        if (!next_page_token.empty())
        {
            params.emplace_back("pageToken", next_page_token);
        }
        json_ptr = requestRest(std::filesystem::path(API_VERSION) / prefix / DATABASES_ENDPOINT, "GET", params);
        databases_array = json_ptr->getArray("databases");
        if (json_ptr->has("nextPageToken") && !json_ptr->isNull("nextPageToken"))
        {
            next_page_token = json_ptr->getValue<String>("nextPageToken");
        }
    }
}

void PaimonRestCatalog::forEachTables(
    const String & database, DB::Names & tables, StopCondition stop_condition, ExecuteFunc execute_func) const
{
    auto json_ptr = requestRest(
        std::filesystem::path(API_VERSION) / prefix / DATABASES_ENDPOINT / database / TABLES_ENDPOINT,
        "GET",
        {{"maxResults", fmt::to_string(LIST_MAX_RESULTS)}});
    auto tables_array = json_ptr->getArray("tables");
    String next_page_token;
    if (json_ptr->has("nextPageToken") && !json_ptr->isNull("nextPageToken"))
    {
        next_page_token = json_ptr->getValue<String>("nextPageToken");
    }
    bool first_iteration = true;
    bool stop = false;

    while (first_iteration || !next_page_token.empty())
    {
        first_iteration = false;
        for (unsigned int i = 0; i < tables_array->size(); ++i)
        {
            String table_name = tables_array->getElement<String>(i);
            table_name = database + "." + table_name;
            tables.emplace_back(table_name);
            if (execute_func)
            {
                execute_func(table_name);
            }
            if (stop_condition && stop_condition(table_name))
            {
                stop = true;
                break;
            }
        }
        if (stop)
        {
            break;
        }
        Poco::URI::QueryParameters params = {
            {"maxResults", fmt::to_string(LIST_MAX_RESULTS)},
        };
        if (!next_page_token.empty())
        {
            params.emplace_back("pageToken", next_page_token);
        }
        json_ptr
            = requestRest(std::filesystem::path(API_VERSION) / prefix / DATABASES_ENDPOINT / database / TABLES_ENDPOINT, "GET", params);
        tables_array = json_ptr->getArray("tables");
        if (json_ptr->has("nextPageToken") && !json_ptr->isNull("nextPageToken"))
        {
            next_page_token = json_ptr->getValue<String>("nextPageToken");
        }
    }
}


bool PaimonRestCatalog::empty() const
{
    DB::Strings databases;
    DB::Names tables;
    auto list_database_stop_condition = [this, &tables](const String & database_name)
    {
        /// stop when get any table
        forEachTables(database_name, tables, [](const String &) { return true; });
        return !tables.empty();
    };
    forEachDatabase(databases, list_database_stop_condition);
    return tables.empty();
}

DB::Names PaimonRestCatalog::getTables() const
{
    DB::Strings databases;
    DB::Names tables;
    auto list_tables = [this, &tables](const String & database_name) { forEachTables(database_name, tables, {}); };
    forEachDatabase(databases, {}, list_tables);
    return tables;
}

bool PaimonRestCatalog::existsTable(const String & database_name, const String & table_name) const
{
    try
    {
        createReadBuffer(
            std::filesystem::path(API_VERSION) / prefix / DATABASES_ENDPOINT / database_name / TABLES_ENDPOINT / table_name, "GET");
    }
    catch (const DB::HTTPException & e)
    {
        if (e.code() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
        {
            return false;
        }
        throw;
    }
    return true;
}

bool PaimonRestCatalog::tryGetTableMetadata(const String & database_name, const String & table_name, TableMetadata & result) const
{
    try
    {
        auto table_json_ptr = requestRest(
            std::filesystem::path(API_VERSION) / prefix / DATABASES_ENDPOINT / database_name / TABLES_ENDPOINT / table_name, "GET");
        if (result.requiresLocation())
        {
            if (table_json_ptr->has("path"))
            {
                String path = table_json_ptr->getValue<String>("path");
                /// handle file location
                auto pos = path.find("://");
                if (pos == std::string::npos && path.starts_with("file:/"))
                {
                    path.insert(std::strlen("file:/"), "//");
                }
                result.setLocation(path);
                LOG_TRACE(log, "Location for table {}: {}", table_name, result.getLocation());
            }
            else
            {
                std::ostringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
                table_json_ptr->stringify(ss);
                result.setTableIsNotReadable(
                    fmt::format("Cannot read table {}, because no 'location' in response: {}", table_name, ss.str()));
            }
        }

        if (result.requiresSchema())
        {
            ::Paimon::PaimonTableSchema table_schema;
            auto schema_json_ptr = table_json_ptr->getObject("schema");
            const auto & json_array = schema_json_ptr->getArray("fields");
            table_schema.fields.reserve(json_array->size());
            for (uint32_t i = 0; i < json_array->size(); ++i)
            {
                table_schema.fields.emplace_back(json_array->getObject(i));
                table_schema.fields_by_name_indexes.emplace(table_schema.fields.back().name, table_schema.fields.size() - 1);
            }
            DB::NamesAndTypesList names_types_list;
            for (const auto & field : table_schema.fields)
            {
                names_types_list.emplace_back(field.name, field.type.clickhouse_data_type);
            }
            result.setSchema(names_types_list);
        }

        if (result.isDefaultReadableTable() && result.requiresCredentials())
        {
            auto table_token_json_ptr = requestRest(
                std::filesystem::path(API_VERSION) / prefix / DATABASES_ENDPOINT / database_name / TABLES_ENDPOINT / table_name
                    / TABLE_TOKEN_ENDPOINT,
                "GET");
            StorageType table_storage_type = parseStorageTypeFromLocation(result.getLocation());
            switch (table_storage_type)
            {
                case StorageType::S3: {
                    static constexpr std::array<String, 3> access_key_id_strs
                        = {"fs.s3a.access-key", "fs.s3a.access.key", "fs.oss.accessKeyId"};
                    static constexpr std::array<String, 3> secret_access_key_strs
                        = {"fs.s3a.secret-key", "fs.s3a.secret.key", "fs.oss.accessKeySecret"};
                    static constexpr std::array<String, 1> session_token_strs = {"fs.oss.securityToken"};

                    std::string access_key_id;
                    std::string secret_access_key;
                    std::string session_token;

                    for (const auto & access_key_id_str : access_key_id_strs)
                    {
                        if (table_token_json_ptr->has(access_key_id_str))
                        {
                            access_key_id = table_token_json_ptr->get(access_key_id_str).extract<String>();
                            break;
                        }
                    }
                    for (const auto & secret_access_key_str : secret_access_key_strs)
                    {
                        if (table_token_json_ptr->has(secret_access_key_str))
                        {
                            secret_access_key = table_token_json_ptr->get(secret_access_key_str).extract<String>();
                            break;
                        }
                    }
                    for (const auto & session_token_str : session_token_strs)
                    {
                        if (table_token_json_ptr->has(session_token_str))
                        {
                            session_token = table_token_json_ptr->get(session_token_str).extract<String>();
                            break;
                        }
                    }

                    result.setStorageCredentials(std::make_shared<S3Credentials>(access_key_id, secret_access_key, ""));
                    break;
                }
                default: {
                    LOG_WARNING(log, "Unsupported storage type {} for table {} to get table token", table_storage_type, table_name);
                    break;
                }
            }
        }
        return true;
    }
    catch (const DB::HTTPException & e)
    {
        if (e.code() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
        {
            return false;
        }
        throw;
    }
}

Poco::JSON::Object::Ptr PaimonRestCatalog::requestRest(
    const String & endpoint, const String & method, const Poco::URI::QueryParameters & params, const DB::HTTPHeaderEntries & headers) const
{
    auto buf = createReadBuffer(endpoint, method, params, headers);
    std::string json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    LOG_TRACE(log, "Request endpoint: {}, response: {}", endpoint, json_str);
    return json.extract<Poco::JSON::Object::Ptr>();
}

void PaimonRestCatalog::getTableMetadata(const String & database_name, const String & table_name, TableMetadata & result) const
{
    if (!tryGetTableMetadata(database_name, table_name, result))
    {
        throw DB::Exception(DB::ErrorCodes::DATALAKE_DATABASE_ERROR, "No response from paimon rest catalog");
    }
}

}
#endif
