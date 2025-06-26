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

using SettingEntries = std::unordered_map<String, std::function<void(const JSONObjectType &)>>;

static std::optional<ServerCredentials> loadServerCredentials(
    const JSONParserImpl::Element & jobj, const String & sname, const uint32_t default_port, const uint32_t default_mysql_port = 0)
{
    uint32_t port = default_port;
    uint32_t mysql_port = default_mysql_port;
    String hostname = "localhost";
    String container;
    String unix_socket;
    String user = "test";
    String password;
    String database = "test";
    std::filesystem::path user_files_dir = std::filesystem::temp_directory_path();
    std::filesystem::path query_log_file = std::filesystem::temp_directory_path() / (sname + ".sql");

    static const SettingEntries configEntries
        = {{"hostname", [&](const JSONObjectType & value) { hostname = String(value.getString()); }},
           {"container", [&](const JSONObjectType & value) { container = String(value.getString()); }},
           {"port", [&](const JSONObjectType & value) { port = static_cast<uint32_t>(value.getUInt64()); }},
           {"mysql_port", [&](const JSONObjectType & value) { mysql_port = static_cast<uint32_t>(value.getUInt64()); }},
           {"unix_socket", [&](const JSONObjectType & value) { unix_socket = String(value.getString()); }},
           {"user", [&](const JSONObjectType & value) { user = String(value.getString()); }},
           {"password", [&](const JSONObjectType & value) { password = String(value.getString()); }},
           {"database", [&](const JSONObjectType & value) { database = String(value.getString()); }},
           {"user_files_dir", [&](const JSONObjectType & value) { user_files_dir = std::filesystem::path(String(value.getString())); }},
           {"query_log_file", [&](const JSONObjectType & value) { query_log_file = std::filesystem::path(String(value.getString())); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (configEntries.find(nkey) == configEntries.end())
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown server option: {}", nkey);
        }
        configEntries.at(nkey)(value);
    }

    return std::optional<ServerCredentials>(
        ServerCredentials(hostname, container, port, mysql_port, unix_socket, user, password, database, user_files_dir, query_log_file));
}

static PerformanceMetric
loadPerformanceMetric(const JSONParserImpl::Element & jobj, const uint32_t default_threshold, const uint32_t default_minimum)
{
    bool enabled = false;
    uint32_t threshold = default_minimum;
    uint32_t minimum = default_threshold;

    static const SettingEntries metricEntries
        = {{"enabled", [&](const JSONObjectType & value) { enabled = value.getBool(); }},
           {"threshold", [&](const JSONObjectType & value) { threshold = value.getUInt64(); }},
           {"minimum", [&](const JSONObjectType & value) { minimum = value.getUInt64(); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const String & nkey = String(key);

        if (metricEntries.find(nkey) == metricEntries.end())
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown metric option: {}", nkey);
        }
        metricEntries.at(nkey)(value);
    }

    return PerformanceMetric(enabled, threshold, minimum);
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
        {"max_string_length", [&](const JSONObjectType & value) { max_string_length = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_depth", [&](const JSONObjectType & value) { max_depth = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"max_width", [&](const JSONObjectType & value) { max_width = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"max_columns", [&](const JSONObjectType & value) { max_columns = std::max(UINT64_C(1), value.getUInt64()); }},
        {"max_databases", [&](const JSONObjectType & value) { max_databases = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_functions", [&](const JSONObjectType & value) { max_functions = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_tables", [&](const JSONObjectType & value) { max_tables = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_views", [&](const JSONObjectType & value) { max_views = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_dictionaries", [&](const JSONObjectType & value) { max_dictionaries = static_cast<uint32_t>(value.getUInt64()); }},
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
        {"allow_memory_tables", [&](const JSONObjectType & value) { allow_memory_tables = value.getBool(); }},
        {"allow_client_restarts", [&](const JSONObjectType & value) { allow_client_restarts = value.getBool(); }},
        {"max_reconnection_attempts",
         [&](const JSONObjectType & value)
         { max_reconnection_attempts = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"time_to_sleep_between_reconnects",
         [&](const JSONObjectType & value)
         { time_to_sleep_between_reconnects = std::max(UINT32_C(1000), static_cast<uint32_t>(value.getUInt64())); }},
        {"clickhouse", [&](const JSONObjectType & value) { clickhouse_server = loadServerCredentials(value, "clickhouse", 9004, 9005); }},
        {"mysql", [&](const JSONObjectType & value) { mysql_server = loadServerCredentials(value, "mysql", 3306, 3306); }},
        {"postgresql", [&](const JSONObjectType & value) { postgresql_server = loadServerCredentials(value, "postgresql", 5432); }},
        {"sqlite", [&](const JSONObjectType & value) { sqlite_server = loadServerCredentials(value, "sqlite", 0); }},
        {"mongodb", [&](const JSONObjectType & value) { mongodb_server = loadServerCredentials(value, "mongodb", 27017); }},
        {"redis", [&](const JSONObjectType & value) { redis_server = loadServerCredentials(value, "redis", 6379); }},
        {"minio", [&](const JSONObjectType & value) { minio_server = loadServerCredentials(value, "minio", 9000); }},
        {"http", [&](const JSONObjectType & value) { http_server = loadServerCredentials(value, "http", 80); }},
        {"azurite", [&](const JSONObjectType & value) { azurite_server = loadServerCredentials(value, "azurite", 0); }},
        {"disabled_types",
         [&](const JSONObjectType & value)
         {
             using std::operator""sv;
             constexpr auto delim{","sv};
             String input = String(value.getString());
             std::transform(input.begin(), input.end(), input.begin(), ::tolower);

             static const std::unordered_map<std::string_view, uint32_t> type_entries
                 = {{"bool", allow_bool},
                    {"uint", allow_unsigned_int},
                    {"int8", allow_int8},
                    {"int64", allow_int64},
                    {"int128", allow_int128},
                    {"float", allow_floating_points},
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
                    {"uuid", allow_uuid},
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
                    {"geo", allow_geo}};

             for (const auto word : std::views::split(input, delim))
             {
                 const auto & entry = std::string_view(word);

                 if (type_entries.find(entry) == type_entries.end())
                 {
                     throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown type option for disabled_types: {}", String(entry));
                 }
                 type_mask &= (~type_entries.at(entry));
             }
         }},
        {"disallowed_error_codes",
         [&](const JSONObjectType & value)
         {
             using std::operator""sv;
             constexpr auto delim{","sv};

             for (const auto word : std::views::split(String(value.getString()), delim))
             {
                 uint32_t result;
                 const auto & sv = std::string_view(word);
                 auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result);

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
                 disallowed_error_codes.insert(result);
             }
         }}};

    for (const auto [key, value] : object.getObject())
    {
        const String & nkey = String(key);

        if (configEntries.find(nkey) == configEntries.end())
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

void FuzzConfig::loadServerSettings(DB::Strings & out, const String & desc, const String & query)
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
    loadServerSettings(this->collations, "collations", R"(SELECT "name" FROM "system"."collations")");
    loadServerSettings(this->storage_policies, "storage policies", R"(SELECT DISTINCT "policy_name" FROM "system"."storage_policies")");
    loadServerSettings(this->disks, "disks", R"(SELECT DISTINCT "name" FROM "system"."disks")");
    loadServerSettings(
        this->keeper_disks, "keeper disks", R"(SELECT DISTINCT "name" FROM "system"."disks" WHERE metadata_type = 'Keeper')");
    loadServerSettings(this->timezones, "timezones", R"(SELECT "time_zone" FROM "system"."time_zones")");
    loadServerSettings(this->clusters, "clusters", R"(SELECT DISTINCT "cluster" FROM "system"."clusters")");
    loadServerSettings(this->caches, "caches", "SHOW FILESYSTEM CACHES");
}

String FuzzConfig::getConnectionHostAndPort(const bool secure) const
{
    return fmt::format("{}:{}", this->host, secure ? this->secure_port : this->port);
}

String FuzzConfig::getHTTPURL(const bool secure) const
{
    return fmt::format("http{}://{}:{}", secure ? "s" : "", this->host, secure ? this->http_secure_port : this->http_port);
}

void FuzzConfig::loadSystemTables(std::unordered_map<String, DB::Strings> & tables)
{
    String buf;
    String current_table;
    DB::Strings next_cols;

    if (processServerQuery(
            false,
            fmt::format(
                "SELECT t.name, c.name from system.tables t JOIN system.columns c ON t.name = c.table WHERE t.database = 'system' AND "
                "c.database = 'system' INTO OUTFILE "
                "'{}' TRUNCATE FORMAT TabSeparated;",
                fuzz_server_out.generic_string())))
    {
        std::ifstream infile(fuzz_client_out);
        while (std::getline(infile, buf) && buf.size() > 1)
        {
            if (buf[buf.size() - 1] == '\r')
            {
                buf.pop_back();
            }
            const auto tabchar = buf.find('\t');
            const auto ntable = buf.substr(0, tabchar);
            const auto ncol = buf.substr(tabchar + 1);

            if (ntable != current_table && !next_cols.empty())
            {
                if (current_table != "stack_trace"
                    && (allow_infinite_tables || (!current_table.starts_with("numbers") && !current_table.starts_with("zeros"))))
                {
                    tables[current_table] = next_cols;
                }
                next_cols.clear();
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
                R"(SELECT count() FROM "system"."{}" WHERE {}"table" = '{}' AND "partition_id" != 'all' INTO OUTFILE '{}' TRUNCATE FORMAT CSV;)",
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
                R"(SELECT count() FROM "system"."mutations" INTO OUTFILE '{}' TRUNCATE FORMAT CSV;)", fuzz_server_out.generic_string())))
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
                "FORMAT RawBlob;",
                rand_val,
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
                "{}\"table\" "
                "= "
                "'{}') INTO OUTFILE '{}' TRUNCATE FORMAT RawBlob;",
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
                    && server.metrics.at(key)
                        > static_cast<uint64_t>(peer.metrics.at(key) * (1 + (static_cast<double>(val.threshold) / 100.0f))))
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
