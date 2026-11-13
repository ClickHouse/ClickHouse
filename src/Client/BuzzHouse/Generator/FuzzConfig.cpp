#include <Client/BuzzHouse/Generator/FuzzConfig.h>
#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <ranges>
#include <IO/copyData.h>
#include <fmt/ranges.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BUZZHOUSE;
extern const int BUZZHOUSE_ORACLE;
extern const int NETWORK_ERROR;
}
}

namespace BuzzHouse
{

const DB::Strings compressionMethods
    = {"auto", "none", "gz", "gzip", "deflate", "brotli", "br", "xz", "zst", "zstd", "lzma", "lz4", "bz2", "snappy"};

const DB::Strings codecs
    = {"LZ4",
       "LZ4HC",
       "ZSTD",
       "ZXC",
       "GDeflate",
       "Delta",
       "DoubleDelta",
       "Gorilla",
       "T64",
       "FPC",
       "GCD",
       "ALP",
       "PCO",
       "AES_128_GCM_SIV",
       "AES_256_GCM_SIV",
       "NONE"};

String escapeSQLString(const String & s, const char escape_char, const bool escape_backslash)
{
    String out;
    out.reserve(s.size());
    for (const char c : s)
    {
        /// Both the quote and the backslash are escaped by doubling them
        if (c == escape_char || (escape_backslash && c == '\\'))
            out += c;
        out += c;
    }
    return out;
}

String urlEncodeQueryParam(const String & s)
{
    String out;
    out.reserve(s.size() * 3);
    for (const unsigned char c : s)
    {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~')
        {
            out += static_cast<char>(c);
        }
        else if (c == ' ')
        {
            out += '+';
        }
        else
        {
            static constexpr const char hex[] = "0123456789ABCDEF";
            out += '%';
            out += hex[c >> 4];
            out += hex[c & 0xF];
        }
    }
    return out;
}

void SystemTable::setName(ExprSchemaTable * est) const
{
    est->mutable_database()->set_value(schema_name);
    est->mutable_table()->set_value(table_name);
}

/// A configuration value with its key, read with type and range checks instead of as 0, false or "" when mistyped
struct JSONValue
{
    const String & key;
    const JSONObjectType & value;

    [[noreturn]] void throwBadValue(const String & expected) const
    {
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "BuzzHouse option {} must be {}", key, expected);
    }

    uint64_t getUInt(const uint64_t max = std::numeric_limits<uint64_t>::max()) const
    {
        /// Integers that fit an Int64 are parsed as one, only larger ones are UInt64
        if (value.isInt64() && value.getInt64() >= 0 && static_cast<uint64_t>(value.getInt64()) <= max)
        {
            return static_cast<uint64_t>(value.getInt64());
        }
        if (value.isUInt64() && value.getUInt64() <= max)
        {
            return value.getUInt64();
        }
        throwBadValue(fmt::format("an unsigned integer up to {}", max));
    }

    uint32_t getUInt32(const uint32_t max = std::numeric_limits<uint32_t>::max()) const { return static_cast<uint32_t>(getUInt(max)); }

    uint32_t getPort() const { return getUInt32(65535); }

    bool getBool() const
    {
        if (!value.isBool())
        {
            throwBadValue("a boolean");
        }
        return value.getBool();
    }

    String getString() const
    {
        if (!value.isString())
        {
            throwBadValue("a string");
        }
        return String(value.getString());
    }

    const JSONObjectType & getObject() const
    {
        if (!value.isObject())
        {
            throwBadValue("an object");
        }
        return value;
    }

    DB::Strings getStrings() const
    {
        DB::Strings res;

        if (!value.isArray())
        {
            throwBadValue("an array of strings");
        }
        for (const auto entry : value.getArray())
        {
            res.emplace_back(JSONValue{key, entry}.getString());
        }
        return res;
    }
};

using SettingEntries = std::unordered_map<String, std::function<void(const JSONValue &)>>;

static std::optional<Catalog> loadCatalog(const JSONParserImpl::Element & jobj, const String & default_region, const uint32_t default_port)
{
    String client_hostname = "localhost";
    String server_hostname = "localhost";
    String path;
    String region = default_region;
    String warehouse = "data";
    uint32_t port = default_port;

    const SettingEntries configEntries
        = {{"client_hostname", [&](const JSONValue & v) { client_hostname = v.getString(); }},
           {"server_hostname", [&](const JSONValue & v) { server_hostname = v.getString(); }},
           {"path", [&](const JSONValue & v) { path = v.getString(); }},
           {"region", [&](const JSONValue & v) { region = v.getString(); }},
           {"warehouse", [&](const JSONValue & v) { warehouse = v.getString(); }},
           {"port", [&](const JSONValue & v) { port = v.getPort(); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (!configEntries.contains(nkey))
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown catalog option: {}", nkey);
        }
        configEntries.at(nkey)(JSONValue{nkey, value});
    }

    return std::optional<Catalog>(Catalog(client_hostname, server_hostname, path, region, warehouse, port));
}

static std::optional<ServerCredentials> loadServerCredentials(
    const JSONParserImpl::Element & jobj, const String & sname, const uint32_t default_port, const uint32_t default_mysql_port = 0)
{
    uint32_t port = default_port;
    uint32_t mysql_port = default_mysql_port;
    String client_hostname = "localhost";
    String server_hostname = "localhost";
    String container;
    String unix_socket;
    String user = "test";
    String password;
    String secret;
    String database = "test";
    String named_collection;
    std::filesystem::path user_files_dir = std::filesystem::temp_directory_path();
    std::filesystem::path query_log_file = std::filesystem::temp_directory_path() / (sname + ".sql");
    std::optional<Catalog> glue_catalog;
    std::optional<Catalog> hive_catalog;
    std::optional<Catalog> rest_catalog;
    std::optional<Catalog> unity_catalog;

    const SettingEntries configEntries
        = {{"client_hostname", [&](const JSONValue & v) { client_hostname = v.getString(); }},
           {"server_hostname", [&](const JSONValue & v) { server_hostname = v.getString(); }},
           {"container", [&](const JSONValue & v) { container = v.getString(); }},
           {"port", [&](const JSONValue & v) { port = v.getPort(); }},
           {"mysql_port", [&](const JSONValue & v) { mysql_port = v.getPort(); }},
           {"unix_socket", [&](const JSONValue & v) { unix_socket = v.getString(); }},
           {"user", [&](const JSONValue & v) { user = v.getString(); }},
           {"password", [&](const JSONValue & v) { password = v.getString(); }},
           {"secret", [&](const JSONValue & v) { secret = v.getString(); }},
           {"database", [&](const JSONValue & v) { database = v.getString(); }},
           {"named_collection", [&](const JSONValue & v) { named_collection = v.getString(); }},
           {"user_files_dir", [&](const JSONValue & v) { user_files_dir = std::filesystem::path(v.getString()); }},
           {"query_log_file", [&](const JSONValue & v) { query_log_file = std::filesystem::path(v.getString()); }},
           {"glue", [&](const JSONValue & v) { glue_catalog = loadCatalog(v.getObject(), "us-east-1", 3000); }},
           {"hive", [&](const JSONValue & v) { hive_catalog = loadCatalog(v.getObject(), "", 9083); }},
           {"rest", [&](const JSONValue & v) { rest_catalog = loadCatalog(v.getObject(), "", 8181); }},
           {"unity", [&](const JSONValue & v) { unity_catalog = loadCatalog(v.getObject(), "", 8181); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (!configEntries.contains(nkey))
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown server option: {}", nkey);

        configEntries.at(nkey)(JSONValue{nkey, value});
    }

    return std::optional<ServerCredentials>(ServerCredentials(
        client_hostname,
        server_hostname,
        container,
        port,
        mysql_port,
        unix_socket,
        user,
        password,
        secret,
        database,
        named_collection,
        user_files_dir,
        query_log_file,
        glue_catalog,
        hive_catalog,
        rest_catalog,
        unity_catalog));
}

static PerformanceMetric
loadPerformanceMetric(const JSONParserImpl::Element & jobj, const uint32_t default_threshold, const uint32_t default_minimum)
{
    bool enabled = false;
    uint32_t threshold = default_threshold;
    uint32_t minimum = default_minimum;

    const SettingEntries metricEntries
        = {{"enabled", [&](const JSONValue & v) { enabled = v.getBool(); }},
           {"threshold", [&](const JSONValue & v) { threshold = v.getUInt32(); }},
           {"minimum", [&](const JSONValue & v) { minimum = v.getUInt32(); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (!metricEntries.contains(nkey))
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown metric option: {}", nkey);

        metricEntries.at(nkey)(JSONValue{nkey, value});
    }

    return PerformanceMetric(std::move(enabled), std::move(threshold), std::move(minimum));
}

/// Names the option from the value, capturing nothing that could dangle
static std::function<void(const JSONValue &)>
parseDisabledOptions(uint64_t & res, const std::unordered_map<std::string_view, uint64_t> & entries)
{
    return [&res, &entries](const JSONValue & v)
    {
        using std::operator""sv;
        constexpr auto delim{","sv};
        String input = v.getString();
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);

        for (auto word : std::views::split(input, delim))
        {
            const std::string_view entry(word.begin(), word.end());

            if (!entries.contains(entry))
            {
                throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown type option for {}: {}", v.key, String(entry));
            }
            res &= (~entries.at(entry));
        }
    };
}

static std::function<void(const JSONValue &)> parseErrorCodes(std::unordered_set<uint32_t> & res)
{
    return [&res](const JSONValue & v)
    {
        using std::operator""sv;
        constexpr auto delim{","sv};
        const String input = v.getString();

        for (auto word : std::views::split(input, delim))
        {
            uint32_t result = {};
            const std::string_view sv(word.begin(), word.end());
            const auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result);

            if (ec == std::errc::invalid_argument)
            {
                throw std::invalid_argument("Not a valid number for an error code");
            }
            else if (ec == std::errc::result_out_of_range)
            {
                throw std::out_of_range("Number out of range for uint32_t");
            }
            else if (ptr != sv.data() + sv.size())
            {
                throw std::invalid_argument("Invalid characters in input");
            }
            res.insert(result);
        }
    };
}

FuzzConfig::FuzzConfig(DB::ClientBase * c, const String & path)
    : cb(c)
    , log(getLogger("BuzzHouse"))
{
    JSONParserImpl parser;
    JSONObjectType object;
    DB::ReadBufferFromFile in(path);
    DB::WriteBufferFromOwnString out;

    DB::copyData(in, out);
    if (!parser.parse(out.str(), object))
    {
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Could not parse BuzzHouse JSON configuration file");
    }
    else if (!object.isObject())
    {
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Parsed BuzzHouse JSON configuration file is not an object");
    }

    static const std::unordered_map<std::string_view, uint64_t> type_entries
        = {{"bool", allow_bool},
           {"uint", allow_unsigned_int},
           {"int8", allow_int8},
           {"int16", allow_int16},
           {"int64", allow_int64},
           {"int128", allow_int128},
           {"bfloat16", allow_bfloat16},
           {"float32", allow_float32},
           {"float64", allow_float64},
           {"date", allow_dates},
           {"date32", allow_date32},
           {"time", allow_time},
           {"time64", allow_time64},
           {"datetime", allow_datetimes},
           {"datetime64", allow_datetime64},
           {"string", allow_strings},
           {"decimal", allow_decimals},
           {"uuid", allow_uuid},
           {"enum", allow_enum},
           {"dynamic", allow_dynamic},
           {"json", allow_JSON},
           {"nullable", allow_nullable},
           {"lcard", allow_low_cardinality},
           {"array", allow_array},
           {"map", allow_map},
           {"tuple", allow_tuple},
           {"variant", allow_variant},
           {"nested", allow_nested},
           {"ipv4", allow_ipv4},
           {"ipv6", allow_ipv6},
           {"geo", allow_geo},
           {"fixedstring", allow_fixed_strings},
           {"qbit", allow_qbit},
           {"aggregate", allow_aggregate},
           {"simpleaggregate", allow_simple_aggregate}};

    static const std::unordered_map<std::string_view, uint64_t> engine_entries
        = {{"replacingmergetree", allow_replacing_mergetree},
           {"coalescingmergetree", allow_coalescing_mergetree},
           {"summingmergetree", allow_summing_mergetree},
           {"aggregatingmergetree", allow_aggregating_mergetree},
           {"collapsingmergetree", allow_collapsing_mergetree},
           {"versionedcollapsingmergetree", allow_versioned_collapsing_mergetree},
           {"file", allow_file},
           {"null", allow_null},
           {"set", allow_setengine},
           {"join", allow_join},
           {"memory", allow_memory},
           {"stripelog", allow_stripelog},
           {"log", allow_log},
           {"tinylog", allow_tinylog},
           {"embeddedrocksdb", allow_embedded_rocksdb},
           {"buffer", allow_buffer},
           {"mysql", allow_mysql},
           {"postgresql", allow_postgresql},
           {"sqlite", allow_sqlite},
           {"mongodb", allow_mongodb},
           {"redis", allow_redis},
           {"s3", allow_S3},
           {"s3queue", allow_S3queue},
           {"hudi", allow_hudi},
           {"deltalakes3", allow_deltalakeS3},
           {"deltalakeazure", allow_deltalakeAzure},
           {"deltalakelocal", allow_deltalakelocal},
           {"icebergs3", allow_icebergS3},
           {"icebergazure", allow_icebergAzure},
           {"iceberglocal", allow_icebergLocal},
           {"paimon", allow_paimon},
           {"paimons3", allow_paimonS3},
           {"paimonazure", allow_paimonAzure},
           {"paimonlocal", allow_paimonLocal},
           {"merge", allow_merge},
           {"distributed", allow_distributed},
           {"remote", allow_remote},
           {"dictionary", allow_dictionary},
           {"generaterandom", allow_generaterandom},
           {"azureblobstorage", allow_AzureBlobStorage},
           {"azurequeue", allow_AzureQueue},
           {"url", allow_URL},
           {"keepermap", allow_keepermap},
           {"externaldistributed", allow_external_distributed},
           {"materializedpostgresql", allow_materialized_postgresql},
           {"replicated", allow_replicated},
           {"shared", allow_shared},
           {"datalakecatalog", allow_datalakecatalog},
           {"arrowflight", allow_arrowflight},
           {"alias", allow_alias},
           {"kafka", allow_kafka},
           {"backup", allow_backup},
           {"clusterdatabase", allow_cluster_database},
           {"remotedatabase", allow_remote_database},
           {"overlay", allow_overlay_database},
           {"timeseries", allow_time_series},
           {"filelog", allow_filelog},
           {"nats", allow_nats},
           {"rabbitmq", allow_rabbitmq}};

    const SettingEntries configEntries = {
        {"client_file_path", [&](const JSONValue & v) { client_file_path = std::filesystem::path(v.getString()); }},
        {"server_file_path", [&](const JSONValue & v) { server_file_path = std::filesystem::path(v.getString()); }},
        {"fuzzer_out_file", [&](const JSONValue & v) { fuzzer_out_file = std::filesystem::path(v.getString()); }},
        {"lakes_path", [&](const JSONValue & v) { lakes_path = std::filesystem::path(v.getString()); }},
        {"log_path", [&](const JSONValue & v) { log_path = std::filesystem::path(v.getString()); }},
        {"read_log", [&](const JSONValue & v) { read_log = v.getBool(); }},
        {"seed", [&](const JSONValue & v) { seed = v.getUInt(); }},
        {"host", [&](const JSONValue & v) { host = v.getString(); }},
        {"keeper_map_path_prefix", [&](const JSONValue & v) { keeper_map_path_prefix = v.getString(); }},
        {"port", [&](const JSONValue & v) { port = v.getPort(); }},
        {"secure_port", [&](const JSONValue & v) { secure_port = v.getPort(); }},
        {"http_port", [&](const JSONValue & v) { http_port = v.getPort(); }},
        {"http_secure_port", [&](const JSONValue & v) { http_secure_port = v.getPort(); }},
        {"min_insert_rows", [&](const JSONValue & v) { min_insert_rows = std::max(UINT64_C(1), v.getUInt()); }},
        {"max_insert_rows", [&](const JSONValue & v) { max_insert_rows = std::max(UINT64_C(1), v.getUInt()); }},
        {"min_nested_rows", [&](const JSONValue & v) { min_nested_rows = v.getUInt(); }},
        {"max_nested_rows", [&](const JSONValue & v) { max_nested_rows = v.getUInt(); }},
        {"min_string_length", [&](const JSONValue & v) { min_string_length = v.getUInt32(); }},
        {"max_string_length", [&](const JSONValue & v) { max_string_length = v.getUInt32(); }},
        {"max_depth", [&](const JSONValue & v) { max_depth = std::max(UINT32_C(1), v.getUInt32()); }},
        {"max_width", [&](const JSONValue & v) { max_width = std::max(UINT32_C(1), v.getUInt32()); }},
        {"max_columns",
         [&](const JSONValue & v) { max_columns = std::max(UINT32_C(1), v.getUInt32()); }},
        {"max_databases", [&](const JSONValue & v) { max_databases = v.getUInt32(); }},
        {"max_functions", [&](const JSONValue & v) { max_functions = v.getUInt32(); }},
        {"max_policies", [&](const JSONValue & v) { max_policies = v.getUInt32(); }},
        {"max_hypothetical_indexes",
         [&](const JSONValue & v) { max_hypothetical_indexes = v.getUInt32(); }},
        {"max_hypothetical_projections",
         [&](const JSONValue & v) { max_hypothetical_projections = v.getUInt32(); }},
        {"max_tables", [&](const JSONValue & v) { max_tables = v.getUInt32(); }},
        {"max_views", [&](const JSONValue & v) { max_views = v.getUInt32(); }},
        {"max_dictionaries", [&](const JSONValue & v) { max_dictionaries = v.getUInt32(); }},
        {"max_parallel_queries",
         [&](const JSONValue & v) { max_parallel_queries = std::max(UINT32_C(1), v.getUInt32()); }},
        {"max_number_alters",
         [&](const JSONValue & v) { max_number_alters = std::max(UINT32_C(1), v.getUInt32()); }},
        {"deterministic_prob", [&](const JSONValue & v) { deterministic_prob = v.getUInt32(); }},
        {"query_time", [&](const JSONValue & v) { metrics.insert({{"query_time", loadPerformanceMetric(v.getObject(), 10, 2000)}}); }},
        {"query_memory", [&](const JSONValue & v) { metrics.insert({{"query_memory", loadPerformanceMetric(v.getObject(), 10, 2000)}}); }},
        {"query_bytes_read",
         [&](const JSONValue & v) { metrics.insert({{"query_bytes_read", loadPerformanceMetric(v.getObject(), 10, 2000)}}); }},
        {"flush_log_wait_time", [&](const JSONValue & v) { flush_log_wait_time = v.getUInt(); }},
        {"time_to_run", [&](const JSONValue & v) { time_to_run = v.getUInt32(); }},
        {"fuzz_floating_points", [&](const JSONValue & v) { fuzz_floating_points = v.getBool(); }},
        {"test_with_fill", [&](const JSONValue & v) { test_with_fill = v.getBool(); }},
        {"use_dump_table_oracle", [&](const JSONValue & v) { use_dump_table_oracle = v.getUInt32(); }},
        {"compare_success_results", [&](const JSONValue & v) { compare_success_results = v.getBool(); }},
        {"allow_infinite_tables", [&](const JSONValue & v) { allow_infinite_tables = v.getBool(); }},
        {"compare_explains", [&](const JSONValue & v) { compare_explains = v.getBool(); }},
        {"allow_hardcoded_inserts", [&](const JSONValue & v) { allow_hardcoded_inserts = v.getBool(); }},
        {"allow_async_requests", [&](const JSONValue & v) { allow_async_requests = v.getBool(); }},
        {"allow_memory_tables", [&](const JSONValue & v) { allow_memory_tables = v.getBool(); }},
        {"allow_client_restarts", [&](const JSONValue & v) { allow_client_restarts = v.getBool(); }},
        {"set_smt_disk", [&](const JSONValue & v) { set_smt_disk = v.getBool(); }},
        {"has_distributed_cache", [&](const JSONValue & v) { has_distributed_cache = v.getBool(); }},
        {"allow_query_oracles", [&](const JSONValue & v) { allow_query_oracles = v.getBool(); }},
        {"allow_health_check", [&](const JSONValue & v) { allow_health_check = v.getBool(); }},
        {"enable_compatibility_settings", [&](const JSONValue & v) { enable_compatibility_settings = v.getBool(); }},
        {"max_reconnection_attempts",
         [&](const JSONValue & v)
         { max_reconnection_attempts = std::max(UINT32_C(1), v.getUInt32()); }},
        {"time_to_sleep_between_reconnects",
         [&](const JSONValue & v)
         { time_to_sleep_between_reconnects = std::max(UINT32_C(1000), v.getUInt32()); }},
        {"enable_fault_injection_settings", [&](const JSONValue & v) { enable_fault_injection_settings = v.getBool(); }},
        {"enable_force_settings", [&](const JSONValue & v) { enable_force_settings = v.getBool(); }},
        {"enable_time_settings", [&](const JSONValue & v) { enable_time_settings = v.getBool(); }},
        {"enable_overflow_settings", [&](const JSONValue & v) { enable_overflow_settings = v.getBool(); }},
        {"enable_memory_settings", [&](const JSONValue & v) { enable_memory_settings = v.getBool(); }},
        {"enable_sync_settings", [&](const JSONValue & v) { enable_sync_settings = v.getBool(); }},
        {"enable_backups", [&](const JSONValue & v) { enable_backups = v.getBool(); }},
        {"enable_renames", [&](const JSONValue & v) { enable_renames = v.getBool(); }},
        {"enable_failpoints", [&](const JSONValue & v) { enable_failpoints = v.getBool(); }},
        {"dolor_spark_tables", [&](const JSONValue & v) { dolor_spark_tables = v.getBool(); }},
        {"allow_nasty_identifiers", [&](const JSONValue & v) { allow_nasty_identifiers = v.getBool(); }},
        {"random_limited_values", [&](const JSONValue & v) { random_limited_values = v.getBool(); }},
        {"truncate_output", [&](const JSONValue & v) { truncate_output = v.getBool(); }},
        {"allow_transactions", [&](const JSONValue & v) { allow_transactions = v.getBool(); }},
        {"clickhouse", [&](const JSONValue & v) { clickhouse_server = loadServerCredentials(v.getObject(), "clickhouse", 9004, 9005); }},
        {"mysql", [&](const JSONValue & v) { mysql_server = loadServerCredentials(v.getObject(), "mysql", 3306, 3306); }},
        {"postgresql", [&](const JSONValue & v) { postgresql_server = loadServerCredentials(v.getObject(), "postgresql", 5432); }},
        {"sqlite", [&](const JSONValue & v) { sqlite_server = loadServerCredentials(v.getObject(), "sqlite", 0); }},
        {"mongodb", [&](const JSONValue & v) { mongodb_server = loadServerCredentials(v.getObject(), "mongodb", 27017); }},
        {"redis", [&](const JSONValue & v) { redis_server = loadServerCredentials(v.getObject(), "redis", 6379); }},
        {"minio", [&](const JSONValue & v) { minio_server = loadServerCredentials(v.getObject(), "minio", 9000); }},
        {"http", [&](const JSONValue & v) { http_server = loadServerCredentials(v.getObject(), "http", 80); }},
        {"azurite", [&](const JSONValue & v) { azurite_server = loadServerCredentials(v.getObject(), "azurite", 0); }},
        {"kafka", [&](const JSONValue & v) { kafka_server = loadServerCredentials(v.getObject(), "kafka", 19092); }},
        {"nats", [&](const JSONValue & v) { nats_server = loadServerCredentials(v.getObject(), "nats", 4444); }},
        {"rabbitmq", [&](const JSONValue & v) { rabbitmq_server = loadServerCredentials(v.getObject(), "rabbitmq", 5672); }},
        {"dolor", [&](const JSONValue & v) { dolor_server = loadServerCredentials(v.getObject(), "dolor", 8080); }},
        {"remote_servers", [&](const JSONValue & v) { remote_servers = v.getStrings(); }},
        {"remote_secure_servers", [&](const JSONValue & v) { remote_secure_servers = v.getStrings(); }},
        {"http_servers", [&](const JSONValue & v) { http_servers = v.getStrings(); }},
        {"https_servers", [&](const JSONValue & v) { https_servers = v.getStrings(); }},
        {"arrow_flight_servers", [&](const JSONValue & v) { arrow_flight_servers = v.getStrings(); }},
        {"hot_settings", [&](const JSONValue & v) { hot_settings = v.getStrings(); }},
        {"disallowed_settings", [&](const JSONValue & v) { disallowed_settings = v.getStrings(); }},
        {"hot_table_settings", [&](const JSONValue & v) { hot_table_settings = v.getStrings(); }},
        {"hot_formats", [&](const JSONValue & v) { hot_formats = v.getStrings(); }},
        {"disabled_types", parseDisabledOptions(type_mask, type_entries)},
        {"disabled_engines", parseDisabledOptions(engine_mask, engine_entries)},
        {"disallowed_error_codes", parseErrorCodes(disallowed_error_codes)},
        {"oracle_ignore_error_codes", parseErrorCodes(oracle_ignore_error_codes)}};

    for (const auto [key, value] : object.getObject())
    {
        const String & nkey = String(key);

        if (!configEntries.contains(nkey))
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown BuzzHouse option: {}", nkey);
        }
        configEntries.at(nkey)(JSONValue{nkey, value});
    }
    if (min_insert_rows > max_insert_rows)
    {
        throw DB::Exception(
            DB::ErrorCodes::BUZZHOUSE,
            "min_insert_rows value ({}) is higher than max_insert_rows value ({})",
            min_insert_rows,
            max_insert_rows);
    }
    if (min_nested_rows > max_nested_rows)
    {
        throw DB::Exception(
            DB::ErrorCodes::BUZZHOUSE,
            "min_nested_rows value ({}) is higher than max_nested_rows value ({})",
            min_nested_rows,
            max_nested_rows);
    }
    if (min_string_length > max_string_length)
    {
        throw DB::Exception(
            DB::ErrorCodes::BUZZHOUSE,
            "min_string_length value ({}) is higher than max_string_length value ({})",
            min_string_length,
            max_string_length);
    }
    if (max_columns == 0)
    {
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "max_columns must be at least 1");
    }
    if (deterministic_prob > 100)
    {
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Deterministic table probability must be 100 at most");
    }
    for (const auto & entry : std::views::values(metrics))
    {
        measure_performance |= entry.enabled;
    }
    if (!read_log)
    {
        outf = std::ofstream(log_path, std::ios::out | std::ios::trunc);
    }
}

bool FuzzConfig::processServerQuery(const bool outlog, const String & query)
{
    bool res = true;
    bool caught_exception = false;

    try
    {
        if (outlog)
        {
            outf << query << std::endl;
        }
        res &= this->cb->processTextAsSingleQuery(query);
    }
    catch (const std::exception &)
    {
        LOG_ERROR(log, "Error on processing query '{}': {}\n", query, DB::getCurrentExceptionMessage(false));
        res = false;
        caught_exception = true;
    }
    if (!res)
    {
        if (!caught_exception)
            LOG_ERROR(log, "Unknown error on processing query '{}'\n", query);
        if (!this->cb->tryToReconnect(max_reconnection_attempts, time_to_sleep_between_reconnects))
            throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "Couldn't not reconnect to the server");
    }
    return res;
}

std::optional<DB::Strings> FuzzConfig::queryLines(const String & query, const bool outlog, const String & format)
{
    String buf;
    DB::Strings res;

    if (!processServerQuery(
            outlog, fmt::format("{} INTO OUTFILE '{}' TRUNCATE FORMAT {};", query, fuzzer_out_file.generic_string(), format)))
    {
        return std::nullopt;
    }
    std::ifstream infile(fuzzer_out_file, std::ios::in);
    while (std::getline(infile, buf))
    {
        if (!buf.empty() && buf.back() == '\r')
        {
            buf.pop_back();
        }
        if (buf.empty())
        {
            break;
        }
        res.emplace_back(std::move(buf));
        buf.clear();
    }
    return res;
}

String FuzzConfig::queryFirstLine(const String & query, const bool outlog, const String & format)
{
    const std::optional<DB::Strings> lines = queryLines(query, outlog, format);

    return lines.has_value() && !lines->empty() ? lines->front() : "";
}

/// The `"database" = '<database>' AND ` filter of a system table query, nothing for an empty name
static String dbClause(const String & database)
{
    return database.empty() ? "" : (R"("database" = ')" + escapeSQLString(database) + "' AND ");
}

/// Splits a TabSeparated row into `n` fields, the last one takes the rest of the row. Empty when the row has fewer
static DB::Strings splitTabs(const String & row, const size_t n)
{
    DB::Strings res;
    size_t start = 0;

    for (size_t i = 1; i < n; i++)
    {
        const size_t tab = row.find('\t', start);

        if (tab == String::npos)
        {
            return {};
        }
        res.emplace_back(row.substr(start, tab - start));
        start = tab + 1;
    }
    res.emplace_back(row.substr(start));
    return res;
}

template <typename T, typename ParseFunc>
void FuzzConfig::loadServerSettings(std::vector<T> & out, const String & desc, const String & query, ParseFunc parse)
{
    const std::optional<DB::Strings> lines = queryLines(query, false);

    if (lines.has_value())
    {
        out.clear();
        for (const auto & line : lines.value())
        {
            out.push_back(parse(line));
        }
    }
    LOG_INFO(log, "Found {} entries for {}", lines.has_value() ? lines->size() : 0, desc);
}

void FuzzConfig::loadFunctions()
{
    static const String query = R"sql(WITH
args_by_function AS
(
    SELECT
        name,
        min(args_min) AS min_arguments,
        if (max(args_variadic) = 1, NULL, max(args_max)) AS max_arguments,
        max(lambda_syntax) AS lambda_syntax
    FROM
    (
        SELECT
            name,
            syntax_line,
            replaceRegexpOne(syntax_line, '^[^\\(]*\\((.*)\\)[^)]*$', '\\1') AS args_raw,
            match(args_raw, '(\\.\\.\\.|…)') AS args_variadic,
            multiIf(match(trim(args_raw), '^(func|λ)'), 2, 0) AS lambda_syntax,
            replaceRegexpAll(args_raw, '\\[[^\\]]*\\]', '') AS args_required_raw,
            replaceRegexpAll(args_raw, '[\\[\\]]', '') AS args_all_raw,
            if (
                trim(args_required_raw) = '',
                0,
                length(arrayFilter(
                    x -> trim(x) != '' AND NOT match(trim(x), '^(\\.\\.\\.|…)$') AND position(x, '->') = 0,
                    splitByChar(',', args_required_raw)))
            ) AS args_min,
            if (
                args_variadic, NULL,
                if (
                    trim(args_all_raw) = '',
                    0,
                    length(arrayFilter(
                        x -> trim(x) != '' AND NOT match(trim(x), '^(\\.\\.\\.|…)$') AND position(x, '->') = 0,
                        splitByChar(',', args_all_raw)))
                )
            ) AS args_max
        FROM system.functions
        ARRAY JOIN splitByRegexp('\\n+', syntax) AS syntax_line
        WHERE syntax != '' AND match(syntax_line, '\\(.*\\)')
    )
    GROUP BY name
),
params_by_function AS
(
    SELECT
        name,
        min(params_min) AS min_parameters,
        if (max(params_variadic) = 1, NULL, max(params_max)) AS max_parameters
    FROM
    (
        SELECT
            name,
            parameter_line,
            match(parameter_line, '(\\.\\.\\.|…)') AS params_variadic,
            replaceRegexpAll(parameter_line, '\\[[^\\]]*\\]', '') AS params_required_raw,
            replaceRegexpAll(parameter_line, '[\\[\\]]', '') AS params_all_raw,
            if (
                trim(params_required_raw) = '',
                0,
                length(arrayFilter(
                    x -> trim(x) != '' AND NOT match(trim(x), '^(\\.\\.\\.|…)$'),
                    splitByChar(',', params_required_raw)))
            ) AS params_min,
            if (
                params_variadic, NULL,
                if (
                    trim(params_all_raw) = '',
                    0,
                    length(arrayFilter(
                        x -> trim(x) != '' AND NOT match(trim(x), '^(\\.\\.\\.|…)$'),
                        splitByChar(',', params_all_raw)))
                )
            ) AS params_max
        FROM system.functions
        ARRAY JOIN if (parameters = '', [''], splitByRegexp('\\n+', parameters)) AS parameter_line
    )
    GROUP BY name
)
SELECT
    f.name,
    f.is_aggregate,
    f.deterministic,
    multiIf(
        coalesce(a.lambda_syntax, pa.lambda_syntax, 0) = 2, 2,
        coalesce(a.lambda_syntax, pa.lambda_syntax, 0) = 1, 1,
        f.higher_order = 1, 1,
        0) AS lambda_kind,
    coalesce(a.min_arguments, pa.min_arguments, 0),
    coalesce(a.max_arguments, pa.max_arguments),
    coalesce(p.min_parameters, pp.min_parameters, 0),
    coalesce(p.max_parameters, pp.max_parameters)
FROM system.functions AS f
LEFT JOIN args_by_function AS a ON f.name = a.name
LEFT JOIN args_by_function AS pa ON f.alias_to != '' AND f.alias_to = pa.name
LEFT JOIN params_by_function AS p ON f.name = p.name
LEFT JOIN params_by_function AS pp ON f.alias_to != '' AND f.alias_to = pp.name
ORDER BY f.name)sql";

    static const std::unordered_set<String> nulls_clause_funcs = {"any", "anyLast", "first_value", "last_value"};
    static const std::unordered_set<String> common_func_names = {"if", "materialize", "toNullable", "toLowCardinality"};
    static const std::unordered_set<String> two_arg_aggrs
        = {"analysisOfVariance",
           "approx_top_count",
           "approx_top_k",
           "approx_top_sum",
           "argMax",
           "argMin",
           "avgWeighted",
           "boundingRatio",
           "contingency",
           "corr",
           "corrStable",
           "covarPop",
           "covarPopStable",
           "covarSamp",
           "covarSampStable",
           "cramersV",
           "cramersVBiasCorrected",
           "deltaSumTimestamp",
           "exponentialMovingAverage",
           "exponentialTimeDecayedAvg",
           "exponentialTimeDecayedMax",
           "exponentialTimeDecayedSum",
           "groupArrayInsertAt",
           "intervalLengthSum",
           "kolmogorovSmirnovTest",
           "largestTriangleThreeBuckets",
           "mannWhitneyUTest",
           "maxIntersections",
           "maxIntersectionsPosition",
           "meanZTest",
           "medianDeterministic",
           "medianExactWeighted",
           "medianTDigestWeighted",
           "medianTimingWeighted",
           "quantileDeterministic",
           "quantileExactWeighted",
           "quantileExactWeightedInterpolated",
           "quantileInterpolatedWeighted",
           "quantileTDigestWeighted",
           "quantileTimingWeighted",
           "quantilesDeterministic",
           "quantilesExactWeighted",
           "quantilesExactWeightedInterpolated",
           "quantilesInterpolatedWeighted",
           "rankCorr",
           "regr_avgx",
           "regr_avgy",
           "regr_count",
           "regr_intercept",
           "regr_r2",
           "regr_slope",
           "regr_sxx",
           "regr_sxy",
           "regr_syy",
           "sequenceNextNode",
           "simpleLinearRegression",
           "sparkBar",
           "studentTTest",
           "sumMapFiltered",
           "sumMapFilteredWithOverflow",
           "theilsU",
           "topKWeighted",
           "welchTTest"};

    if (const std::optional<DB::Strings> lines = queryLines(query, false))
    {
        uint64_t found = 0;

        det_funcs.clear();
        nondet_funcs.clear();
        common_funcs.clear();
        det_aggrs.clear();
        simple_det_aggrs.clear();
        nondet_aggrs.clear();
        for (const auto & line : lines.value())
        {
            /// Fields: name, is_aggregate, deterministic, lambda_kind, min_args, max_args, min_params, max_params
            const DB::Strings fields = splitTabs(line, 8);

            if (fields.empty())
            {
                continue;
            }
            const String & name = fields[0];
            const bool is_aggregate = fields[1] == "1";
            const String & det_str = fields[2];
            const String & ho_str = fields[3];
            const String & min_args_str = fields[4];
            const String & max_args_str = fields[5];
            const String & min_params_str = fields[6];
            const String & max_params_str = fields[7];

            found++;

            const bool is_deterministic = (det_str != "0");
            const uint8_t lk_val = (ho_str != "\\N") ? static_cast<uint8_t>(std::stoul(ho_str)) : 0;
            const LambdaKind lambda_kind = lk_val <= 2 ? static_cast<LambdaKind>(lk_val) : LambdaKind::None;
            const uint32_t min_args = min_args_str == "\\N" ? 0 : static_cast<uint32_t>(std::stoul(min_args_str));
            const uint32_t max_args = max_args_str == "\\N" ? ulimited_params : static_cast<uint32_t>(std::stoul(max_args_str));
            const uint32_t min_params = min_params_str == "\\N" ? 0 : static_cast<uint32_t>(std::stoul(min_params_str));
            const uint32_t max_params = max_params_str == "\\N" ? ulimited_params : static_cast<uint32_t>(std::stoul(max_params_str));

            if (is_aggregate)
            {
                const bool snc = nulls_clause_funcs.contains(name);
                uint32_t aggr_min_args = 1;
                uint32_t aggr_max_args = 1;
                if (name == "count")
                {
                    aggr_min_args = 0;
                }
                else if (two_arg_aggrs.contains(name))
                {
                    aggr_min_args = 2;
                    aggr_max_args = 2;
                }
                CHAggregate agg(name, min_params, max_params, aggr_min_args, aggr_max_args, snc);
                if (is_deterministic)
                    det_aggrs.push_back(agg);
                else
                    nondet_aggrs.push_back(agg);
            }
            else
            {
                CHFunction func(name, lambda_kind, min_args, max_args);
                if (common_func_names.contains(name))
                    common_funcs.push_back(func);
                if (is_deterministic)
                    det_funcs.push_back(func);
                else
                    nondet_funcs.push_back(func);
            }
        }

        if (found > 0)
        {
            LOG_INFO(
                log,
                "Loaded {} functions from system.functions: {} det funcs, {} nondet funcs, {} common funcs, {} det aggrs, {} nondet aggrs",
                found,
                det_funcs.size(),
                nondet_funcs.size(),
                common_funcs.size(),
                det_aggrs.size(),
                nondet_aggrs.size());
        }
        else
        {
            LOG_WARNING(log, "No functions loaded from system.functions out of {} found", found);
        }
    }
    else
    {
        LOG_WARNING(log, "Failed to load functions from system.functions, keeping previous catalog");
    }
    static const std::unordered_set<String> simple_aggregate_allowlist = {
        "any",
        "any_respect_nulls",
        "anyLast",
        "anyLast_respect_nulls",
        "min",
        "max",
        "sum",
        "sumWithOverflow",
        "groupBitAnd",
        "groupBitOr",
        "groupBitXor",
        "sumMap",
        "minMap",
        "maxMap",
        "groupArrayArray",
        "groupArrayLastArray",
        "groupUniqArrayArray",
        "groupUniqArrayArrayMap",
        "sumMappedArrays",
        "minMappedArrays",
        "maxMappedArrays",
    };
    simple_det_aggrs.clear();
    for (const auto & agg : det_aggrs)
    {
        if (simple_aggregate_allowlist.contains(agg.fname))
            simple_det_aggrs.push_back(agg);
    }

    if (det_funcs.empty())
    {
        det_funcs.emplace_back("abs", LambdaKind::None, 1, 1);
        det_funcs.emplace_back("plus", LambdaKind::None, 2, 2);
    }
    if (det_aggrs.empty())
        det_aggrs.emplace_back("count", 0, 0, 0, 1, false);
    if (simple_det_aggrs.empty())
        simple_det_aggrs.emplace_back("any", 0, 0, 1, 1, false);
}

void FuzzConfig::loadServerConfigurations()
{
    loadServerSettings<String>(this->collations, "collations", R"(SELECT "name" FROM "system"."collations")");
    loadServerSettings<String>(this->in_formats, "input formats", R"(SELECT "name" FROM "system"."formats" WHERE "is_input" = 1)");
    loadServerSettings<String>(this->out_formats, "output formats", R"(SELECT "name" FROM "system"."formats" WHERE "is_output" = 1)");
    loadServerSettings<String>(
        this->in_out_formats,
        "input and output formats",
        R"(SELECT "name" FROM "system"."formats" WHERE "is_input" = 1 AND "is_output" = 1)");
    if (this->in_formats.empty() || this->out_formats.empty() || this->in_out_formats.empty())
    {
        throw DB::Exception(
            DB::ErrorCodes::BUZZHOUSE,
            "No {} formats were loaded from the server; cannot continue fuzzing",
            this->in_formats.empty() ? "input" : (this->out_formats.empty() ? "output" : "input and output"));
    }
    for (const auto & entry : this->hot_formats)
    {
        if (std::ranges::find(this->in_formats, entry) == this->in_formats.end()
            && std::ranges::find(this->out_formats, entry) == this->out_formats.end())
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown format: {}", entry);
        }
    }
    loadServerSettings<String>(
        this->storage_policies, "storage policies", R"(SELECT DISTINCT "policy_name" FROM "system"."storage_policies")");
    loadServerSettings<String>(this->volumes, "volumes", R"(SELECT DISTINCT "volume_name" FROM "system"."storage_policies")");
    loadServerSettings<String>(
        this->keeper_disks, "keeper disks", R"(SELECT DISTINCT "name" FROM "system"."disks" WHERE metadata_type = 'Keeper')");
    loadServerSettings<DiskInfo>(
        this->disks,
        "disks",
        R"(SELECT "name", "type", "path", "object_storage_type", "metadata_type", "is_encrypted", "cache_path" != '' FROM "system"."disks")",
        [](const String & row) -> DiskInfo
        {
            const DB::Strings f = splitTabs(row, 7);

            if (f.empty())
            {
                throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unexpected system.disks row: {}", row);
            }
            return {f[0], f[1], f[2], f[3], f[4], f[5] == "1", f[6] == "1"};
        });
    loadServerSettings<String>(this->timezones, "timezones", R"(SELECT "time_zone" FROM "system"."time_zones")");
    loadServerSettings<String>(this->clusters, "clusters", R"(SELECT DISTINCT "cluster" FROM "system"."clusters")");
    loadServerSettings<String>(this->caches, "caches", "SHOW FILESYSTEM CACHES");
    if (enable_failpoints)
    {
        loadServerSettings<String>(
            this->failpoints,
            "failpoints",
            "SELECT \"name\" FROM \"system\".\"fail_points\""
            " WHERE \"type\" NOT IN ('pauseable', 'pauseable_once') ORDER BY rand() LIMIT 10");
    }
    loadServerSettings<String>(this->tokenizers, "tokenizers", R"(SELECT "name" FROM "system"."tokenizers")");
    loadFunctions();
}

String FuzzConfig::getConnectionHostAndPort(const bool secure) const
{
    return fmt::format("{}:{}", this->host, secure ? this->secure_port : this->port);
}

String FuzzConfig::getHTTPURL(const bool secure) const
{
    return fmt::format("http{}://{}:{}", secure ? "s" : "", this->host, secure ? this->http_secure_port : this->http_port);
}

void FuzzConfig::loadSystemTables(std::vector<SystemTable> & tables)
{
    String current_schema;
    String current_table;
    DB::Strings next_cols;
    static constexpr std::array<std::string_view, 3> infinite_prefixes{"numbers", "zeros", "primes"};
    const auto flush_table = [&]()
    {
        if (!next_cols.empty() && current_table != "stack_trace"
            && (allow_infinite_tables
                || std::ranges::none_of(infinite_prefixes, [&](std::string_view p) { return current_table.starts_with(p); })))
        {
            tables.emplace_back(SystemTable(current_schema, current_table, next_cols));
        }
        next_cols.clear();
    };

    tables.clear();
    if (const std::optional<DB::Strings> lines = queryLines(
            "SELECT c.database, c.table, c.name from system.columns c WHERE c.database IN ('system', 'INFORMATION_SCHEMA', "
            "'information_schema')",
            false))
    {
        for (const auto & line : lines.value())
        {
            const DB::Strings fields = splitTabs(line, 3);

            if (fields.empty())
            {
                throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unexpected system.columns row: {}", line);
            }
            if (fields[0] != current_schema || fields[1] != current_table)
            {
                flush_table();
                current_schema = fields[0];
                current_table = fields[1];
            }
            next_cols.emplace_back(fields[2]);
        }
        /// Emit the last table group that was never flushed by the loop
        flush_table();
    }
}

bool FuzzConfig::tableHasPartitions(const bool detached, const String & database, const String & table)
{
    const String res = queryFirstLine(
        fmt::format(
            R"(SELECT count() FROM "system"."{}" WHERE {}"table" = '{}' AND "partition_id" != 'all')",
            detached ? "detached_parts" : "parts",
            dbClause(database),
            escapeSQLString(table)),
        true);

    return !res.empty() && res[0] != '0';
}

String FuzzConfig::getRandomMutation(const uint64_t rand_val)
{
    /// The system.mutations table doesn't support sampling, so pick up a random part with a window function
    return queryFirstLine(
        fmt::format(
            "SELECT z.y FROM (SELECT (row_number() OVER () - 1) AS x, \"mutation_id\" AS y FROM \"system\".\"mutations\") as z "
            "WHERE z.x = (SELECT {} % max2(count(), 1) FROM \"system\".\"mutations\")",
            rand_val),
        false);
}

String FuzzConfig::getRandomIcebergHistoryValue(const String & property)
{
    /// Can't use sampling here either
    const String res
        = queryFirstLine(fmt::format(R"(SELECT {} FROM "system"."iceberg_history" ORDER BY rand() LIMIT 1)", property), false);

    /// Empty history (e.g. a table with no snapshots yet). Return the DEFAULT keyword so a
    /// `SET iceberg_snapshot_id/iceberg_timestamp_ms = DEFAULT` resets the pin and clears its
    /// `changed` flag, instead of pinning the never-valid -1 snapshot and poisoning the session.
    return res.empty() ? "DEFAULT" : res;
}

String FuzzConfig::getRandomFileSystemCacheValue()
{
    /// Can't use sampling here either
    return queryFirstLine(R"(SELECT "cache_name" FROM "system"."filesystem_cache_settings" ORDER BY rand() LIMIT 1)", false);
}

String FuzzConfig::getRandomFuzzedPartName(const uint64_t rand_val)
{
    static const DB::Strings fuzzedPartNames = {"all_1_1_0", "all_0_0_0", "20000101_1_1_0", "invalid_part"};

    return fuzzedPartNames[rand_val % fuzzedPartNames.size()];
}

String FuzzConfig::getRandomFuzzedPartitionValue(const uint64_t rand_val)
{
    static const DB::Strings fuzzedPartitionValues
        = {"tuple()", "0", "-1", "202101", "20000101", "'x'", "(202101, 'x')", "(0, 'a', NULL)"};

    return fuzzedPartitionValues[rand_val % fuzzedPartitionValues.size()];
}

String FuzzConfig::getRandomFuzzedPartitionId(const uint64_t rand_val)
{
    static const DB::Strings fuzzedPartitionIds
        = {"all", "0", "202101", "20000101", "5-7", "8a4f3b2c1d0e9f8a7b6c5d4e3f2a1b0c", "invalid partition", ""};

    return fuzzedPartitionIds[rand_val % fuzzedPartitionIds.size()];
}

String FuzzConfig::tableGetRandomPartitionOrPart(
    const uint64_t rand_val, const bool detached, const bool partition, const String & database, const String & table)
{
    const String & detached_tbl = detached ? "detached_parts" : "parts";
    const String db_clause = dbClause(database);

    /// The system.parts table doesn't support sampling, so pick up a random part with a window function
    return queryFirstLine(
        fmt::format(
            "SELECT z.y FROM (SELECT (row_number() OVER () - 1) AS x, \"{}\" AS y FROM \"system\".\"{}\" WHERE {}\"table\" = '{}' AND "
            "\"partition_id\" != 'all') AS z WHERE z.x = (SELECT {} % max2(count(), 1) FROM \"system\".\"{}\" WHERE "
            "{}\"table\" = '{}')",
            partition ? "partition_id" : "name",
            detached_tbl,
            db_clause,
            escapeSQLString(table),
            rand_val,
            detached_tbl,
            db_clause,
            escapeSQLString(table)),
        true);
}

/// The value of `key` in a random row of the table, as a literal for `PARTITION <expr>`
String FuzzConfig::tableGetRandomKeyValue(const String & database, const String & table, const String & key)
{
    /// The Values format writes the row as a parenthesized tuple of SQL literals
    return queryFirstLine(
        fmt::format("SELECT {} FROM {}.{} ORDER BY rand() LIMIT 1", key, DB::backQuote(database), DB::backQuote(table)), true, "Values");
}

String FuzzConfig::tableGetRandomPartitionValue(const uint64_t rand_val, const String & database, const String & table)
{
    const String db_clause = dbClause(database);
    /// base64-encode the partition value so raw bytes survive the TabSeparated OUTFILE unescaped.
    const String encoded = queryFirstLine(
        fmt::format(
            "SELECT base64Encode(z.y) FROM (SELECT (row_number() OVER () - 1) AS x, \"partition\" AS y FROM \"system\".\"parts\" "
            "WHERE {}\"table\" = '{}' AND \"partition_id\" != 'all') AS z WHERE z.x = (SELECT {} % max2(count(), 1) FROM "
            "\"system\".\"parts\" WHERE {}\"table\" = '{}' AND \"partition_id\" != 'all')",
            db_clause,
            escapeSQLString(table),
            rand_val,
            db_clause,
            escapeSQLString(table)),
        true);
    String res = encoded.empty() ? "" : DB::base64Decode(encoded);

    /// Only emit values re-parseable as a bare `PARTITION <expr>`: the quoted tuple form
    /// `(202101, 'x')` or a bare integer. Unquoted single-column string keys fall back to PARTITION ID.
    const bool is_tuple = res.size() > 1 && res.front() == '(' && res.back() == ')';
    bool is_integer = !res.empty();
    for (size_t i = (!res.empty() && res.front() == '-') ? 1 : 0; is_integer && i < res.size(); i++)
    {
        is_integer &= res[i] >= '0' && res[i] <= '9';
    }
    is_integer &= !(res.size() == 1 && res.front() == '-'); /// reject a lone "-"
    if (!is_tuple && !is_integer)
    {
        res.clear();
    }
    return res;
}

uint32_t FuzzConfig::tableCountSystemRows(const String & system_table, const String & database, const String & table)
{
    const String res = queryFirstLine(
        fmt::format(
            R"(SELECT count() FROM "system"."{}" WHERE {}"table" = '{}')", system_table, dbClause(database), escapeSQLString(table)),
        false);

    return res.empty() ? 0 : static_cast<uint32_t>(std::stoul(res));
}

String
FuzzConfig::tableGetRandomSystemName(const uint64_t rand_val, const String & system_table, const String & database, const String & table)
{
    const String db_clause = dbClause(database);

    /// These system tables don't support sampling, so pick a random row with a window function
    return queryFirstLine(
        fmt::format(
            "SELECT z.y FROM (SELECT (row_number() OVER () - 1) AS x, \"name\" AS y FROM \"system\".\"{}\" WHERE "
            "{} \"table\" = '{}') AS z WHERE z.x = (SELECT {} % max2(count(), 1) FROM \"system\".\"{}\" WHERE "
            "{} \"table\" = '{}')",
            system_table,
            db_clause,
            escapeSQLString(table),
            rand_val,
            system_table,
            db_clause,
            escapeSQLString(table)),
        false);
}

uint32_t FuzzConfig::tableCountIndexes(const String & database, const String & table)
{
    return tableCountSystemRows("data_skipping_indices", database, table);
}

String FuzzConfig::tableGetRandomIndex(const uint64_t rand_val, const String & database, const String & table)
{
    return tableGetRandomSystemName(rand_val, "data_skipping_indices", database, table);
}

uint32_t FuzzConfig::tableCountProjections(const String & database, const String & table)
{
    return tableCountSystemRows("projections", database, table);
}

String FuzzConfig::tableGetRandomProjection(const uint64_t rand_val, const String & database, const String & table)
{
    return tableGetRandomSystemName(rand_val, "projections", database, table);
}

uint32_t FuzzConfig::tableCountConstraints(const String & database, const String & table)
{
    return tableCountSystemRows("constraints", database, table);
}

String FuzzConfig::tableGetRandomConstraint(const uint64_t rand_val, const String & database, const String & table)
{
    return tableGetRandomSystemName(rand_val, "constraints", database, table);
}

String FuzzConfig::pickFormat(RandomGenerator & rg, const DB::Strings & formats) const
{
    if (!this->hot_formats.empty() && rg.nextSmallNumber() < 4)
    {
        DB::Strings hot;

        std::ranges::copy_if(
            this->hot_formats, std::back_inserter(hot), [&](const String & f) { return std::ranges::find(formats, f) != formats.end(); });
        if (!hot.empty())
        {
            return rg.pickRandomly(hot);
        }
    }
    return rg.pickRandomly(formats);
}

String FuzzConfig::getRandomDistributedCacheServer(const uint64_t rand_val)
{
    return queryFirstLine(
        fmt::format(
            "SELECT \"distributed_cache_server\" FROM \"system\".\"distributed_cache_registry\" ORDER BY "
            "\"distributed_cache_server\" LIMIT 1 OFFSET {} % max2((SELECT count() FROM \"system\".\"distributed_cache_registry\"), 1)",
            rand_val),
        false);
}

void FuzzConfig::validateClickHouseHealth()
{
    /// Errors caused by the data, not the server, e.g. a mutation stuck on `MODIFY COLUMN` to a non-Nullable type
    /// over NULLs retries forever. `CANNOT_PARSE_*`, `CANNOT_CONVERT_TYPE`, out of range, `CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN`
    static const String data_conversion_errors = "(6, 25, 26, 27, 38, 41, 70, 72, 321, 349, 376, 407, 441, 467, 675, 676)";

    if (processServerQuery(
            false,
            fmt::format(
                "SELECT x FROM ("
                "(SELECT count() x, 1 y FROM \"system\".\"detached_parts\" WHERE startsWith(\"name\", 'broken'))"
                " UNION ALL "
                "(SELECT ifNull(sum(\"lost_part_count\"), 0) x, 2 y FROM \"system\".\"replicas\")"
                " UNION ALL "
                /// Single scan of text_log for all pattern-based checks (3, 8, 10, 11, 12).
                /// arrayZip + arrayJoin emits one row per pattern while reading text_log only once.
                "(SELECT t.1 x, t.2 y FROM ("
                "SELECT arrayJoin(arrayZip("
                "[countIf(msg ILIKE concat('%','POTENTIALLY','_BROKEN','_DATA','_PART','%')),"
                " countIf(msg ILIKE concat('%','REPLICA','_ALREADY','_EXISTS','%')),"
                " countIf(msg ILIKE concat('%','LOGICAL','_ERROR','%')),"
                " countIf(msg ILIKE concat('%','CORRUPTED','_DATA','%')),"
                " countIf(msg ILIKE concat('%','CHECKSUM','_DOESNT','_MATCH','%')),"
                " countIf(msg ILIKE concat('%','DATA','_AFTER','_MERGE','_DIFF','_FROM','_EXPECTED','%'))],"
                "[toUInt64(3),toUInt64(8),toUInt64(10),toUInt64(11),toUInt64(12),toUInt64(13)])) AS t"
                /// Match the server's own words: it echoes the statement at Debug/Trace and quotes it after `(in query:`.
                " FROM (SELECT splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] AS msg"
                " FROM \"system\".\"text_log\""
                " WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information')) tlog)"
                " UNION ALL "
                "(SELECT count() x, 4 y FROM clusterAllReplicas(default, \"system\".\"clusters\")"
                " WHERE is_shared_catalog_cluster = true AND is_local = true AND recovery_time > 10000)"
                " UNION ALL "
                /// Aggregated like every other check: unaggregated it emits one row per replica and shifts all later checks.
                "(SELECT greatest(sum(\"value\"), 0)::UInt64 x, 5 y FROM clusterAllReplicas(default, \"system\".\"metrics\") WHERE \"name\" = "
                "'SharedCatalogDropDetachLocalTablesErrors')"
                " UNION ALL "
                "(SELECT count() x, 6 y FROM clusterAllReplicas(default, \"system\".\"replicas\") WHERE readonly_start_time IS NOT NULL)"
                " UNION ALL "
                "(SELECT count() x, 7 y FROM (SELECT part_name FROM clusterAllReplicas(default, \"system\".\"part_log\")"
                " WHERE exception != '' AND error NOT IN {} AND event_time > (now() - toIntervalSecond(60))"
                " GROUP BY part_name HAVING count() > 10) tx)"
                " UNION ALL "
                "(SELECT count() x, 9 y FROM \"system\".\"replication_queue\" WHERE \"last_exception\" != '')"
                " UNION ALL "
                "(SELECT count() x, 14 y FROM \"system\".\"replication_queue\""
                " WHERE \"last_exception\" != '' AND \"num_tries\" > 5)"
                ") tx ORDER BY y SETTINGS use_query_cache = 0, use_query_condition_cache = 0 INTO OUTFILE '{}' TRUNCATE FORMAT "
                "TabSeparated;",
                data_conversion_errors,
                fuzzer_out_file.generic_string())))
    {
        String buf;
        size_t i = 0;
        std::ifstream infile(fuzzer_out_file, std::ios::in);
        static const DB::Strings health_errors
            = {"broken detached part(s)",
               "broken replica(s)",
               "broken data part(s)",
               "shared catalog replica(s) needing recovery",
               "shared catalog drop/detach error(s)",
               "readonly replica(s)",
               "part(s) with excessive errors",
               "replica(s) with REPLICA_ALREADY_EXISTS errors",
               "replication queue exception(s)",
               "LOGICAL_ERROR(s) in text_log",
               "CORRUPTED_DATA(s) in text_log",
               "CHECKSUM_DOESNT_MATCH error(s) in text_log",
               "DATA_AFTER_MERGE_DIFF_FROM_EXPECTED error(s) in text_log",
               "stuck replication queue task(s) (>5 retries)"};
        static const DB::Strings detail_queries = {
            R"(SELECT "database", "table", "name" FROM "system"."detached_parts" WHERE startsWith("name", 'broken') LIMIT 3)",
            R"(SELECT "database", "table", "lost_part_count" FROM "system"."replicas" WHERE "lost_part_count" > 0 LIMIT 3)",
            R"(SELECT "message" FROM "system"."text_log" WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'POTENTIALLY', '_BROKEN', '_DATA', '_PART', '%') ORDER BY event_time DESC LIMIT 3)",
            "",
            "",
            R"(SELECT "database", "table", "last_exception" FROM "system"."replicas" WHERE readonly_start_time IS NOT NULL LIMIT 3)",
            R"(SELECT "database", "table", "part_name", "exception" FROM "system"."part_log" WHERE exception != '' AND error NOT IN )"
                + data_conversion_errors + R"( AND event_time > (now() - toIntervalSecond(60)) ORDER BY event_time DESC LIMIT 3)",
            R"(SELECT "message" FROM "system"."text_log" WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'REPLICA', '_ALREADY', '_EXISTS', '%') ORDER BY event_time DESC LIMIT 3)",
            R"(SELECT "database", "table", "last_exception" FROM "system"."replication_queue" WHERE "last_exception" != '' LIMIT 3)",
            R"(SELECT "message" FROM "system"."text_log" WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'LOGICAL', '_ERROR', '%') ORDER BY event_time DESC LIMIT 3)",
            R"(SELECT "message" FROM "system"."text_log" WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'CORRUPTED', '_DATA', '%') ORDER BY event_time DESC LIMIT 3)",
            R"(SELECT "message" FROM "system"."text_log" WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'CHECKSUM', '_DOESNT', '_MATCH', '%') ORDER BY event_time DESC LIMIT 3)",
            R"(SELECT "message" FROM "system"."text_log" WHERE event_time >= now() - toIntervalSecond(60) AND level <= 'Information' AND splitByString(' in scope ', splitByString('(query: ', splitByString('(in query: ', message)[1])[1])[1] ILIKE concat('%', 'DATA', '_AFTER', '_MERGE', '_DIFF', '_FROM', '_EXPECTED', '%') ORDER BY event_time DESC LIMIT 3)",
            R"(SELECT "database", "table", "type", "last_exception", "num_tries" FROM "system"."replication_queue" WHERE "last_exception" != '' AND "num_tries" > 5 ORDER BY "num_tries" DESC LIMIT 3)"};

        while (std::getline(infile, buf) && !buf.empty() && i < health_errors.size())
        {
            buf.erase(std::find_if(buf.rbegin(), buf.rend(), [](unsigned char c) { return !std::isspace(c); }).base(), buf.end());
            const uint32_t val = static_cast<uint32_t>(std::stoul(buf));
            if (val != 0)
            {
                String details;
                if (i < detail_queries.size() && !detail_queries[i].empty()
                    && processServerQuery(
                        false,
                        fmt::format(
                            "{} INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;", detail_queries[i], fuzzer_out_file.generic_string())))
                {
                    String dbuf;
                    std::ifstream detail_file(fuzzer_out_file, std::ios::in);
                    while (std::getline(detail_file, dbuf))
                    {
                        if (!dbuf.empty())
                            details += "\n  " + dbuf;
                    }
                }
                throw DB::Exception(
                    DB::ErrorCodes::BUZZHOUSE_ORACLE,
                    "ClickHouse health check on {}:{}: found {} {}{}",
                    host,
                    port,
                    val,
                    health_errors[i],
                    details.empty() ? "" : "\nDetails:" + details);
            }
            i++;
            buf.resize(0);
        }
    }
}

void FuzzConfig::comparePerformanceResults(const String & oracle_name, PerformanceResult & server, PerformanceResult & peer) const
{
    server.result_strings.clear();
    peer.result_strings.clear();
    server.result_strings.insert(
        {{"query_time", formatReadableTime(static_cast<double>(server.metrics.at("query_time") * 1000000))},
         {"query_memory", formatReadableSizeWithBinarySuffix(static_cast<double>(server.metrics.at("query_memory")))},
         {"query_bytes_read", formatReadableSizeWithBinarySuffix(static_cast<double>(server.metrics.at("query_bytes_read")))}});
    peer.result_strings.insert(
        {{"query_time", formatReadableTime(static_cast<double>(peer.metrics.at("query_time") * 1000000))},
         {"query_memory", formatReadableSizeWithBinarySuffix(static_cast<double>(peer.metrics.at("query_memory")))},
         {"query_bytes_read", formatReadableSizeWithBinarySuffix(static_cast<double>(peer.metrics.at("query_bytes_read")))}});

    if (this->measure_performance)
    {
        for (const auto & [key, val] : metrics)
        {
            if (val.enabled)
            {
                if (val.minimum < server.metrics.at(key)
                    && server.metrics.at(key) > static_cast<uint64_t>(
                           static_cast<double>(peer.metrics.at(key)) * (1 + (static_cast<double>(val.threshold) / 100.0))))
                {
                    throw DB::Exception(
                        DB::ErrorCodes::BUZZHOUSE_ORACLE,
                        "{}: ClickHouse peer server {}: {} was less than the target server: {}",
                        oracle_name,
                        key,
                        peer.result_strings.at(key),
                        server.result_strings.at(key));
                }
            }
        }
    }
    for (const auto & [key, val] : metrics)
    {
        LOG_INFO(
            log, "{}: server {}: {} vs peer {}: {}", oracle_name, key, server.result_strings.at(key), key, peer.result_strings.at(key));
    }
}

}
