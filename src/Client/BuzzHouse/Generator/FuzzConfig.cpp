#include <Client/BuzzHouse/Generator/FuzzConfig.h>

#include <ranges>
#include <IO/copyData.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BUZZHOUSE;
}
}

namespace BuzzHouse
{

const DB::Strings compressionMethods
    = {"auto", "none", "gz", "gzip", "deflate", "brotli", "br", "xz", "zst", "zstd", "lzma", "lz4", "bz2", "snappy"};

using SettingEntries = std::unordered_map<String, std::function<void(const JSONObjectType &)>>;

static std::optional<Catalog> loadCatalog(const JSONParserImpl::Element & jobj, const String & default_region, const uint32_t default_port)
{
    String client_hostname = "localhost";
    String server_hostname = "localhost";
    String path;
    String region = default_region;
    String warehouse = "data";
    uint32_t port = default_port;

    static const SettingEntries configEntries
        = {{"client_hostname", [&](const JSONObjectType & value) { client_hostname = String(value.getString()); }},
           {"server_hostname", [&](const JSONObjectType & value) { server_hostname = String(value.getString()); }},
           {"path", [&](const JSONObjectType & value) { path = String(value.getString()); }},
           {"region", [&](const JSONObjectType & value) { region = String(value.getString()); }},
           {"warehouse", [&](const JSONObjectType & value) { warehouse = String(value.getString()); }},
           {"port", [&](const JSONObjectType & value) { port = static_cast<uint32_t>(value.getUInt64()); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (!configEntries.contains(nkey))
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown catalog option: {}", nkey);
        }
        configEntries.at(nkey)(value);
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

    static const SettingEntries configEntries
        = {{"client_hostname", [&](const JSONObjectType & value) { client_hostname = String(value.getString()); }},
           {"server_hostname", [&](const JSONObjectType & value) { server_hostname = String(value.getString()); }},
           {"container", [&](const JSONObjectType & value) { container = String(value.getString()); }},
           {"port", [&](const JSONObjectType & value) { port = static_cast<uint32_t>(value.getUInt64()); }},
           {"mysql_port", [&](const JSONObjectType & value) { mysql_port = static_cast<uint32_t>(value.getUInt64()); }},
           {"unix_socket", [&](const JSONObjectType & value) { unix_socket = String(value.getString()); }},
           {"user", [&](const JSONObjectType & value) { user = String(value.getString()); }},
           {"password", [&](const JSONObjectType & value) { password = String(value.getString()); }},
           {"secret", [&](const JSONObjectType & value) { secret = String(value.getString()); }},
           {"database", [&](const JSONObjectType & value) { database = String(value.getString()); }},
           {"named_collection", [&](const JSONObjectType & value) { named_collection = String(value.getString()); }},
           {"user_files_dir", [&](const JSONObjectType & value) { user_files_dir = std::filesystem::path(String(value.getString())); }},
           {"query_log_file", [&](const JSONObjectType & value) { query_log_file = std::filesystem::path(String(value.getString())); }},
           {"glue", [&](const JSONObjectType & value) { glue_catalog = loadCatalog(value, "us-east-1", 3000); }},
           {"hive", [&](const JSONObjectType & value) { hive_catalog = loadCatalog(value, "", 9083); }},
           {"rest", [&](const JSONObjectType & value) { rest_catalog = loadCatalog(value, "", 8181); }},
           {"unity", [&](const JSONObjectType & value) { unity_catalog = loadCatalog(value, "", 8181); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (!configEntries.contains(nkey))
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown server option: {}", nkey);

        configEntries.at(nkey)(value);
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
    uint32_t threshold = default_minimum;
    uint32_t minimum = default_threshold;

    static const SettingEntries metricEntries
        = {{"enabled", [&](const JSONObjectType & value) { enabled = value.getBool(); }},
           {"threshold", [&](const JSONObjectType & value) { threshold = static_cast<uint32_t>(value.getUInt64()); }},
           {"minimum", [&](const JSONObjectType & value) { minimum = static_cast<uint32_t>(value.getUInt64()); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (!metricEntries.contains(nkey))
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown metric option: {}", nkey);

        metricEntries.at(nkey)(value);
    }

    return PerformanceMetric(std::move(enabled), std::move(threshold), std::move(minimum));
}

static std::function<void(const JSONObjectType &)>
parseDisabledOptions(uint64_t & res, const String & text, const std::unordered_map<std::string_view, uint64_t> & entries)
{
    return [&](const JSONObjectType & value)
    {
        using std::operator""sv;
        constexpr auto delim{","sv};
        String input = String(value.getString());
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);

        for (auto word : std::views::split(input, delim))
        {
            const std::string_view entry(word.begin(), word.end());

            if (!entries.contains(entry))
            {
                throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown type option for {}: {}", text, String(entry));
            }
            res &= (~entries.at(entry));
        }
    };
}

static DB::Strings loadArray(const JSONObjectType & value)
{
    DB::Strings res;

    for (const auto entry : value.getArray())
    {
        res.emplace_back(String(entry.getString()));
    }
    return res;
}

static std::function<void(const JSONObjectType &)> parseErrorCodes(std::unordered_set<uint32_t> & res)
{
    return [&](const JSONObjectType & value)
    {
        using std::operator""sv;
        constexpr auto delim{","sv};

        for (auto word : std::views::split(String(value.getString()), delim))
        {
            uint32_t result;
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
           {"merge", allow_merge},
           {"distributed", allow_distributed},
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
           {"kafka", allow_kafka}};

    static const SettingEntries configEntries = {
        {"client_file_path",
         [&](const JSONObjectType & value)
         {
             client_file_path = std::filesystem::path(String(value.getString()));
             fuzz_client_out = client_file_path / "fuzz.data";
         }},
        {"server_file_path",
         [&](const JSONObjectType & value)
         {
             server_file_path = std::filesystem::path(String(value.getString()));
             fuzz_server_out = client_file_path / "fuzz.data";
         }},
        {"lakes_path", [&](const JSONObjectType & value) { lakes_path = std::filesystem::path(String(value.getString())); }},
        {"log_path", [&](const JSONObjectType & value) { log_path = std::filesystem::path(String(value.getString())); }},
        {"read_log", [&](const JSONObjectType & value) { read_log = value.getBool(); }},
        {"seed", [&](const JSONObjectType & value) { seed = value.getUInt64(); }},
        {"host", [&](const JSONObjectType & value) { host = String(value.getString()); }},
        {"keeper_map_path_prefix", [&](const JSONObjectType & value) { keeper_map_path_prefix = String(value.getString()); }},
        {"port", [&](const JSONObjectType & value) { port = static_cast<uint32_t>(value.getUInt64()); }},
        {"secure_port", [&](const JSONObjectType & value) { secure_port = static_cast<uint32_t>(value.getUInt64()); }},
        {"http_port", [&](const JSONObjectType & value) { http_port = static_cast<uint32_t>(value.getUInt64()); }},
        {"http_secure_port", [&](const JSONObjectType & value) { http_secure_port = static_cast<uint32_t>(value.getUInt64()); }},
        {"min_insert_rows", [&](const JSONObjectType & value) { min_insert_rows = std::max(UINT64_C(1), value.getUInt64()); }},
        {"max_insert_rows", [&](const JSONObjectType & value) { max_insert_rows = std::max(UINT64_C(1), value.getUInt64()); }},
        {"min_nested_rows", [&](const JSONObjectType & value) { min_nested_rows = value.getUInt64(); }},
        {"max_nested_rows", [&](const JSONObjectType & value) { max_nested_rows = value.getUInt64(); }},
        {"min_string_length", [&](const JSONObjectType & value) { min_string_length = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_string_length", [&](const JSONObjectType & value) { max_string_length = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_depth", [&](const JSONObjectType & value) { max_depth = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"max_width", [&](const JSONObjectType & value) { max_width = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"max_columns",
         [&](const JSONObjectType & value) { max_columns = static_cast<uint32_t>(std::max(UINT64_C(1), value.getUInt64())); }},
        {"max_databases", [&](const JSONObjectType & value) { max_databases = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_functions", [&](const JSONObjectType & value) { max_functions = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_tables", [&](const JSONObjectType & value) { max_tables = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_views", [&](const JSONObjectType & value) { max_views = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_dictionaries", [&](const JSONObjectType & value) { max_dictionaries = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_parallel_queries",
         [&](const JSONObjectType & value) { max_parallel_queries = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"max_number_alters",
         [&](const JSONObjectType & value) { max_number_alters = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"deterministic_prob", [&](const JSONObjectType & value) { deterministic_prob = static_cast<uint32_t>(value.getUInt64()); }},
        {"query_time", [&](const JSONObjectType & value) { metrics.insert({{"query_time", loadPerformanceMetric(value, 10, 2000)}}); }},
        {"query_memory", [&](const JSONObjectType & value) { metrics.insert({{"query_memory", loadPerformanceMetric(value, 10, 2000)}}); }},
        {"query_bytes_read",
         [&](const JSONObjectType & value) { metrics.insert({{"query_bytes_read", loadPerformanceMetric(value, 10, 2000)}}); }},
        {"flush_log_wait_time", [&](const JSONObjectType & value) { flush_log_wait_time = value.getUInt64(); }},
        {"time_to_run", [&](const JSONObjectType & value) { time_to_run = static_cast<uint32_t>(value.getUInt64()); }},
        {"fuzz_floating_points", [&](const JSONObjectType & value) { fuzz_floating_points = value.getBool(); }},
        {"test_with_fill", [&](const JSONObjectType & value) { test_with_fill = value.getBool(); }},
        {"use_dump_table_oracle", [&](const JSONObjectType & value) { use_dump_table_oracle = static_cast<uint32_t>(value.getUInt64()); }},
        {"compare_success_results", [&](const JSONObjectType & value) { compare_success_results = value.getBool(); }},
        {"allow_infinite_tables", [&](const JSONObjectType & value) { allow_infinite_tables = value.getBool(); }},
        {"compare_explains", [&](const JSONObjectType & value) { compare_explains = value.getBool(); }},
        {"allow_hardcoded_inserts", [&](const JSONObjectType & value) { allow_hardcoded_inserts = value.getBool(); }},
        {"allow_async_requests", [&](const JSONObjectType & value) { allow_async_requests = value.getBool(); }},
        {"allow_memory_tables", [&](const JSONObjectType & value) { allow_memory_tables = value.getBool(); }},
        {"allow_client_restarts", [&](const JSONObjectType & value) { allow_client_restarts = value.getBool(); }},
        {"set_smt_disk", [&](const JSONObjectType & value) { set_smt_disk = value.getBool(); }},
        {"allow_query_oracles", [&](const JSONObjectType & value) { allow_query_oracles = value.getBool(); }},
        {"allow_health_check", [&](const JSONObjectType & value) { allow_health_check = value.getBool(); }},
        {"enable_compatibility_settings", [&](const JSONObjectType & value) { enable_compatibility_settings = value.getBool(); }},
        {"max_reconnection_attempts",
         [&](const JSONObjectType & value)
         { max_reconnection_attempts = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"time_to_sleep_between_reconnects",
         [&](const JSONObjectType & value)
         { time_to_sleep_between_reconnects = std::max(UINT32_C(1000), static_cast<uint32_t>(value.getUInt64())); }},
        {"enable_fault_injection_settings", [&](const JSONObjectType & value) { enable_fault_injection_settings = value.getBool(); }},
        {"enable_force_settings", [&](const JSONObjectType & value) { enable_force_settings = value.getBool(); }},
        {"enable_overflow_settings", [&](const JSONObjectType & value) { enable_overflow_settings = value.getBool(); }},
        {"enable_memory_settings", [&](const JSONObjectType & value) { enable_memory_settings = value.getBool(); }},
        {"enable_backups", [&](const JSONObjectType & value) { enable_backups = value.getBool(); }},
        {"enable_renames", [&](const JSONObjectType & value) { enable_renames = value.getBool(); }},
        {"random_limited_values", [&](const JSONObjectType & value) { random_limited_values = value.getBool(); }},
        {"truncate_output", [&](const JSONObjectType & value) { truncate_output = value.getBool(); }},
        {"allow_transactions", [&](const JSONObjectType & value) { allow_transactions = value.getBool(); }},
        {"clickhouse", [&](const JSONObjectType & value) { clickhouse_server = loadServerCredentials(value, "clickhouse", 9004, 9005); }},
        {"mysql", [&](const JSONObjectType & value) { mysql_server = loadServerCredentials(value, "mysql", 3306, 3306); }},
        {"postgresql", [&](const JSONObjectType & value) { postgresql_server = loadServerCredentials(value, "postgresql", 5432); }},
        {"sqlite", [&](const JSONObjectType & value) { sqlite_server = loadServerCredentials(value, "sqlite", 0); }},
        {"mongodb", [&](const JSONObjectType & value) { mongodb_server = loadServerCredentials(value, "mongodb", 27017); }},
        {"redis", [&](const JSONObjectType & value) { redis_server = loadServerCredentials(value, "redis", 6379); }},
        {"minio", [&](const JSONObjectType & value) { minio_server = loadServerCredentials(value, "minio", 9000); }},
        {"http", [&](const JSONObjectType & value) { http_server = loadServerCredentials(value, "http", 80); }},
        {"azurite", [&](const JSONObjectType & value) { azurite_server = loadServerCredentials(value, "azurite", 0); }},
        {"kafka", [&](const JSONObjectType & value) { kafka_server = loadServerCredentials(value, "kafka", 19092); }},
        {"dolor", [&](const JSONObjectType & value) { dolor_server = loadServerCredentials(value, "dolor", 8080); }},
        {"remote_servers", [&](const JSONObjectType & value) { remote_servers = loadArray(value); }},
        {"remote_secure_servers", [&](const JSONObjectType & value) { remote_secure_servers = loadArray(value); }},
        {"http_servers", [&](const JSONObjectType & value) { http_servers = loadArray(value); }},
        {"https_servers", [&](const JSONObjectType & value) { https_servers = loadArray(value); }},
        {"arrow_flight_servers", [&](const JSONObjectType & value) { arrow_flight_servers = loadArray(value); }},
        {"hot_settings", [&](const JSONObjectType & value) { hot_settings = loadArray(value); }},
        {"disallowed_settings", [&](const JSONObjectType & value) { disallowed_settings = loadArray(value); }},
        {"hot_table_settings", [&](const JSONObjectType & value) { hot_table_settings = loadArray(value); }},
        {"disabled_types", parseDisabledOptions(type_mask, "disabled_types", type_entries)},
        {"disabled_engines", parseDisabledOptions(engine_mask, "disabled_engines", engine_entries)},
        {"disallowed_error_codes", parseErrorCodes(disallowed_error_codes)},
        {"oracle_ignore_error_codes", parseErrorCodes(oracle_ignore_error_codes)}};

    for (const auto [key, value] : object.getObject())
    {
        const String & nkey = String(key);

        if (!configEntries.contains(nkey))
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown BuzzHouse option: {}", nkey);
        }
        configEntries.at(nkey)(value);
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

    try
    {
        if (outlog)
        {
            outf << query << std::endl;
        }
        res &= this->cb->processTextAsSingleQuery(query);
    }
    catch (...)
    {
        res = false;
    }
    if (!res)
    {
        fmt::print(stderr, "Error on processing query '{}'\n", query);
        if (!this->cb->tryToReconnect(max_reconnection_attempts, time_to_sleep_between_reconnects))
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Couldn't not reconnect to the server");
        }
    }
    return res;
}

template <typename T>
void FuzzConfig::loadServerSettings(std::vector<T> & out, const String & desc, const String & query)
{
    String buf;
    uint64_t found = 0;

    if (processServerQuery(
            false, fmt::format(R"({} INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;)", query, fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out);
        out.clear();
        while (std::getline(infile, buf) && !buf.empty())
        {
            out.push_back(buf);
            buf.resize(0);
            found++;
        }
    }
    LOG_INFO(log, "Found {} entries for {}", found, desc);
}

void FuzzConfig::loadServerConfigurations()
{
    loadServerSettings<String>(this->collations, "collations", R"(SELECT "name" FROM "system"."collations")");
    loadServerSettings<String>(
        this->storage_policies, "storage policies", R"(SELECT DISTINCT "policy_name" FROM "system"."storage_policies")");
    loadServerSettings<String>(this->disks, "disks", R"(SELECT DISTINCT "name" FROM "system"."disks")");
    loadServerSettings<String>(
        this->keeper_disks, "keeper disks", R"(SELECT DISTINCT "name" FROM "system"."disks" WHERE metadata_type = 'Keeper')");
    loadServerSettings<String>(this->timezones, "timezones", R"(SELECT "time_zone" FROM "system"."time_zones")");
    loadServerSettings<String>(this->clusters, "clusters", R"(SELECT DISTINCT "cluster" FROM "system"."clusters")");
    loadServerSettings<String>(this->caches, "caches", "SHOW FILESYSTEM CACHES");
    /// keeper_leader_sets_invalid_digest, libcxx_hardening_out_of_bounds_assertion - The server aborts legitimately, can't be used
    /// terminate_with_exception, terminate_with_std_exception - Terminates the server
    loadServerSettings<String>(
        this->failpoints,
        "failpoints",
        "SELECT \"name\" FROM \"system\".\"fail_points\""
        " WHERE \"name\" NOT IN ('keeper_leader_sets_invalid_digest', 'terminate_with_exception', "
        "'terminate_with_std_exception', 'libcxx_hardening_out_of_bounds_assertion')");
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
    String buf;
    String current_schema;
    String current_table;
    DB::Strings next_cols;

    tables.clear();
    if (processServerQuery(
            false,
            fmt::format(
                "SELECT c.database, c.table, c.name from system.columns c WHERE c.database IN ('system', 'INFORMATION_SCHEMA', "
                "'information_schema') INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;",
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out);
        while (std::getline(infile, buf) && buf.size() > 1)
        {
            if (buf[buf.size() - 1] == '\r')
            {
                buf.pop_back();
            }
            const size_t pos1 = buf.find('\t');
            const String nschema = buf.substr(0, pos1);
            const size_t pos2 = buf.find('\t', pos1 + 1);
            const String ntable = buf.substr(pos1 + 1, pos2 - pos1 - 1);
            const String ncol = buf.substr(pos2 + 1);

            if (nschema != current_schema || ntable != current_table)
            {
                if (!next_cols.empty() && current_table != "stack_trace"
                    && (allow_infinite_tables || (!current_table.starts_with("numbers") && !current_table.starts_with("zeros"))))
                {
                    tables.emplace_back(SystemTable(current_schema, current_table, next_cols));
                }
                next_cols.clear();
                current_schema = nschema;
                current_table = ntable;
            }
            next_cols.emplace_back(ncol);
            buf.resize(0);
        }
    }
}

bool FuzzConfig::tableHasPartitions(const bool detached, const String & database, const String & table)
{
    String buf;
    const String & detached_tbl = detached ? "detached_parts" : "parts";
    const String & db_clause = database.empty() ? "" : (R"("database" = ')" + database + "' AND ");

    if (processServerQuery(
            true,
            fmt::format(
                R"(SELECT count() FROM "system"."{}" WHERE {}"table" = '{}' AND "partition_id" != 'all' INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;)",
                detached_tbl,
                db_clause,
                table,
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out);
        if (std::getline(infile, buf))
        {
            return !buf.empty() && buf[0] != '0';
        }
    }
    return false;
}

bool FuzzConfig::hasMutations()
{
    String buf;

    if (processServerQuery(
            false,
            fmt::format(
                R"(SELECT count() FROM "system"."mutations" INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;)",
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out);
        if (std::getline(infile, buf))
        {
            return !buf.empty() && buf[0] != '0';
        }
    }
    return false;
}

String FuzzConfig::getRandomMutation(const uint64_t rand_val)
{
    String res;

    /// The system.mutations table doesn't support sampling, so pick up a random part with a window function
    if (processServerQuery(
            false,
            fmt::format(
                "SELECT z.y FROM (SELECT (row_number() OVER () - 1) AS x, \"mutation_id\" AS y FROM \"system\".\"mutations\") as z "
                "WHERE z.x = (SELECT {} % max2(count(), 1) FROM \"system\".\"mutations\") INTO OUTFILE '{}' TRUNCATE "
                "FORMAT TabSeparated;",
                rand_val,
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out, std::ios::in);
        std::getline(infile, res);
    }
    return res;
}

String FuzzConfig::getRandomIcebergHistoryValue(const String & property)
{
    String res;

    /// Can't use sampling here either
    if (processServerQuery(
            false,
            fmt::format(
                R"(SELECT {} FROM "system"."iceberg_history" ORDER BY rand() LIMIT 1 INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;)",
                property,
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out, std::ios::in);
        std::getline(infile, res);
    }
    return res.empty() ? "-1" : res;
}

String FuzzConfig::getRandomFileSystemCacheValue()
{
    String res;

    /// Can't use sampling here either
    if (processServerQuery(
            false,
            fmt::format(
                R"(SELECT "cache_name" FROM "system"."filesystem_cache_settings" ORDER BY rand() LIMIT 1 INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;)",
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out, std::ios::in);
        std::getline(infile, res);
    }
    return res;
}

String FuzzConfig::tableGetRandomPartitionOrPart(
    const uint64_t rand_val, const bool detached, const bool partition, const String & database, const String & table)
{
    String res;
    const String & detached_tbl = detached ? "detached_parts" : "parts";
    const String & db_clause = database.empty() ? "" : (R"("database" = ')" + database + "' AND ");

    /// The system.parts table doesn't support sampling, so pick up a random part with a window function
    if (processServerQuery(
            true,
            fmt::format(
                "SELECT z.y FROM (SELECT (row_number() OVER () - 1) AS x, \"{}\" AS y FROM \"system\".\"{}\" WHERE {}\"table\" = '{}' AND "
                "\"partition_id\" != 'all') AS z WHERE z.x = (SELECT {} % max2(count(), 1) FROM \"system\".\"{}\" WHERE "
                "{}\"table\" = '{}') INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;",
                partition ? "partition_id" : "name",
                detached_tbl,
                db_clause,
                table,
                rand_val,
                detached_tbl,
                db_clause,
                table,
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out, std::ios::in);
        std::getline(infile, res);
    }
    return res;
}

void FuzzConfig::validateClickHouseHealth()
{
    if (processServerQuery(
            false,
            fmt::format(
                "SELECT x FROM ("
                "(SELECT count() x, 1 y FROM \"system\".\"detached_parts\" WHERE startsWith(\"name\", 'broken'))"
                " UNION ALL "
                "(SELECT ifNull(sum(\"lost_part_count\"), 0) x, 2 y FROM \"system\".\"replicas\")"
                " UNION ALL "
                "(SELECT count() x, 3 y FROM \"system\".\"text_log\" WHERE event_time >= now() - toIntervalSecond(30) AND message ILIKE "
                "'%POTENTIALLY_BROKEN_DATA_PART%' AND message NOT ILIKE '%UNION ALL%')"
                " UNION ALL "
                "(SELECT count() x, 4 y FROM clusterAllReplicas(default, \"system\".\"clusters\")"
                " WHERE is_shared_catalog_cluster = true AND is_local = true AND recovery_time > 5)"
                " UNION ALL "
                "(SELECT value::UInt64 x, 5 y FROM clusterAllReplicas(default, \"system\".\"metrics\") WHERE name = "
                "'SharedCatalogDropDetachLocalTablesErrors')"
                " UNION ALL "
                "(SELECT count() x, 6 y FROM clusterAllReplicas(default, \"system\".\"replicas\") WHERE readonly_start_time IS NOT NULL)"
                " UNION ALL "
                "(SELECT count() x, 7 y FROM (SELECT part_name FROM clusterAllReplicas(default, \"system\".\"part_log\")"
                " WHERE exception != '' AND event_time > (now() - toIntervalSecond(30)) GROUP BY part_name HAVING count() > 5) tx)"
                " UNION ALL "
                "(SELECT count() x, 8 y FROM \"system\".\"text_log\" WHERE event_time >= now() - toIntervalSecond(30) AND message ILIKE "
                "'%REPLICA_ALREADY_EXISTS%' AND message NOT ILIKE '%UNION ALL%')"
                ") tx ORDER BY y INTO OUTFILE '{}' TRUNCATE FORMAT TabSeparated;",
                fuzz_server_out.generic_string())))
    {
        String buf;
        size_t i = 0;
        std::ifstream infile(fuzz_client_out, std::ios::in);
        static const DB::Strings & health_errors
            = {"broken detached part(s)",
               "broken replica(s)",
               "broken data part(s)",
               "shared catalog replica(s) needing recovery",
               "shared catalog drop/detach error(s)",
               "readonly replica(s)",
               "part(s) with excessive errors",
               "replica(s) with REPLICA_ALREADY_EXISTS errors"};

        while (std::getline(infile, buf) && !buf.empty() && i < health_errors.size())
        {
            buf.erase(std::find_if(buf.rbegin(), buf.rend(), [](unsigned char c) { return !std::isspace(c); }).base(), buf.end());
            const uint32_t val = static_cast<uint32_t>(std::stoul(buf));
            if (val != 0)
            {
                throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "ClickHouse health check: found {} {}", val, health_errors[i]);
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
                           static_cast<double>(peer.metrics.at(key)) * (1 + (static_cast<double>(val.threshold) / 100.0f))))
                {
                    throw DB::Exception(
                        DB::ErrorCodes::BUZZHOUSE,
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
