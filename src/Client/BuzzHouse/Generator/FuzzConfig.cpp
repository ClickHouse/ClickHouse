#include <Client/BuzzHouse/Generator/FuzzConfig.h>

namespace BuzzHouse
{

static inline std::vector<std::string> splitString(const std::string & input, const char delimiter)
{
    std::vector<std::string> result;
    const char * str = input.c_str();

    do
    {
        const char * begin = str;

        while (*str && *str != delimiter)
        {
            str++;
        }
        result.push_back(std::string(begin, str));
    } while (*str++);

    return result;
}

static std::optional<ServerCredentials> loadServerCredentials(
    const JSONParserImpl::Element & jobj, const std::string & sname, const uint32_t default_port, const uint32_t default_mysql_port = 0)
{
    uint32_t port = default_port;
    uint32_t mysql_port = default_mysql_port;
    std::string hostname = "localhost";
    std::string unix_socket;
    std::string user = "test";
    std::string password;
    std::string database = "test";
    std::filesystem::path query_log_file = std::filesystem::temp_directory_path() / (sname + ".sql");

    std::unordered_map<std::string, std::function<void(const JSONObjectType &)>> config_entries = {
        {"hostname", [&](const JSONObjectType & value) { hostname = std::string(value.getString()); }},
        {"port", [&](const JSONObjectType & value) { port = static_cast<uint32_t>(value.getUInt64()); }},
        {"mysql_port", [&](const JSONObjectType & value) { mysql_port = static_cast<uint32_t>(value.getUInt64()); }},
        {"unix_socket", [&](const JSONObjectType & value) { unix_socket = std::string(value.getString()); }},
        {"user", [&](const JSONObjectType & value) { user = std::string(value.getString()); }},
        {"password", [&](const JSONObjectType & value) { password = std::string(value.getString()); }},
        {"database", [&](const JSONObjectType & value) { database = std::string(value.getString()); }},
        {"query_log_file", [&](const JSONObjectType & value) { query_log_file = std::filesystem::path(std::string(value.getString())); }}};

    for (const auto [key, value] : jobj.getObject())
    {
        const std::string & nkey = std::string(key);

        if (config_entries.find(nkey) == config_entries.end())
        {
            throw std::runtime_error("Unknown Server option: " + nkey);
        }
        config_entries.at(nkey)(value);
    }

    return std::optional<ServerCredentials>(
        ServerCredentials(hostname, port, mysql_port, unix_socket, user, password, database, query_log_file));
}

FuzzConfig::FuzzConfig(DB::ClientBase * c, const std::string & path) : cb(c), log(getLogger("BuzzHouse"))
{
    JSONParserImpl parser;
    JSONObjectType object;
    std::ifstream inputFile(path);
    std::string fileContent;

    buf.reserve(512);
    while (std::getline(inputFile, buf))
    {
        fileContent += buf;
    }
    inputFile.close();
    if (!parser.parse(fileContent, object))
    {
        throw std::runtime_error("Could not parse BuzzHouse JSON configuration file");
    }
    else if (!object.isObject())
    {
        throw std::runtime_error("Parsed JSON value is not an object");
    }

    std::unordered_map<std::string, std::function<void(const JSONObjectType &)>> config_entries = {
        {"db_file_path",
         [&](const JSONObjectType & value)
         {
             db_file_path = std::filesystem::path(std::string(value.getString()));
             fuzz_out = db_file_path / "fuzz.data";
         }},
        {"log_path", [&](const JSONObjectType & value) { log_path = std::filesystem::path(std::string(value.getString())); }},
        {"read_log", [&](const JSONObjectType & value) { read_log = value.getBool(); }},
        {"seed", [&](const JSONObjectType & value) { seed = value.getUInt64(); }},
        {"min_insert_rows", [&](const JSONObjectType & value) { min_insert_rows = std::max(UINT64_C(1), value.getUInt64()); }},
        {"max_insert_rows", [&](const JSONObjectType & value) { max_insert_rows = std::max(UINT64_C(1), value.getUInt64()); }},
        {"min_nested_rows", [&](const JSONObjectType & value) { min_nested_rows = value.getUInt64(); }},
        {"max_nested_rows", [&](const JSONObjectType & value) { max_nested_rows = value.getUInt64(); }},
        {"max_depth", [&](const JSONObjectType & value) { max_depth = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"max_width", [&](const JSONObjectType & value) { max_width = std::max(UINT32_C(1), static_cast<uint32_t>(value.getUInt64())); }},
        {"max_databases", [&](const JSONObjectType & value) { max_databases = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_functions", [&](const JSONObjectType & value) { max_functions = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_tables", [&](const JSONObjectType & value) { max_tables = static_cast<uint32_t>(value.getUInt64()); }},
        {"max_views", [&](const JSONObjectType & value) { max_views = static_cast<uint32_t>(value.getUInt64()); }},
        {"time_to_run", [&](const JSONObjectType & value) { time_to_run = static_cast<uint32_t>(value.getUInt64()); }},
        {"fuzz_floating_points", [&](const JSONObjectType & value) { fuzz_floating_points = value.getBool(); }},
        {"test_with_fill", [&](const JSONObjectType & value) { test_with_fill = value.getBool(); }},
        {"use_dump_table_oracle", [&](const JSONObjectType & value) { use_dump_table_oracle = value.getBool(); }},
        {"clickhouse", [&](const JSONObjectType & value) { clickhouse_server = loadServerCredentials(value, "clickhouse", 9004, 9005); }},
        {"mysql", [&](const JSONObjectType & value) { mysql_server = loadServerCredentials(value, "mysql", 3306, 3306); }},
        {"postgresql", [&](const JSONObjectType & value) { postgresql_server = loadServerCredentials(value, "postgresql", 5432); }},
        {"sqlite", [&](const JSONObjectType & value) { sqlite_server = loadServerCredentials(value, "sqlite", 0); }},
        {"mongodb", [&](const JSONObjectType & value) { mongodb_server = loadServerCredentials(value, "mongodb", 27017); }},
        {"redis", [&](const JSONObjectType & value) { redis_server = loadServerCredentials(value, "redis", 6379); }},
        {"minio", [&](const JSONObjectType & value) { minio_server = loadServerCredentials(value, "minio", 9000); }},
        {"disabled_types",
         [&](const JSONObjectType & value)
         {
             std::string input = std::string(value.getString());
             std::transform(input.begin(), input.end(), input.begin(), ::tolower);
             const auto & split = splitString(input, ',');

             std::unordered_map<std::string, uint32_t> type_entries
                 = {{"bool", allow_bool},
                    {"uint", allow_unsigned_int},
                    {"int8", allow_int8},
                    {"int64", allow_int64},
                    {"int128", allow_int128},
                    {"float", allow_floating_points},
                    {"date", allow_dates},
                    {"date32", allow_date32},
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

             for (const auto & entry : split)
             {
                 if (type_entries.find(entry) == type_entries.end())
                 {
                     throw std::runtime_error("Unknown type optiom: " + entry);
                 }
                 type_mask &= (~type_entries.at(entry));
             }
         }}};

    for (const auto [key, value] : object.getObject())
    {
        const std::string & nkey = std::string(key);

        if (config_entries.find(nkey) == config_entries.end())
        {
            throw std::runtime_error("Unknown BuzzHouse option: " + nkey);
        }
        config_entries.at(nkey)(value);
    }
    if (min_insert_rows > max_insert_rows)
    {
        throw std::runtime_error(
            "min_insert_rows value (" + std::to_string(min_insert_rows) + ") is higher than max_insert_rows value ("
            + std::to_string(max_insert_rows) + ")");
    }
    if (min_nested_rows > max_nested_rows)
    {
        throw std::runtime_error(
            "min_nested_rows value (" + std::to_string(min_nested_rows) + ") is higher than max_nested_rows value ("
            + std::to_string(max_nested_rows) + ")");
    }
}

bool FuzzConfig::processServerQuery(const std::string & input) const
{
    try
    {
        this->cb->processTextAsSingleQuery(input);
    }
    catch (...)
    {
        return false;
    }
    return true;
}

void FuzzConfig::loadServerSettings(std::vector<std::string> & out, const std::string & table, const std::string & col)
{
    uint64_t found = 0;

    buf.resize(0);
    buf += "SELECT \"";
    buf += col;
    buf += R"(" FROM "system".")";
    buf += table;
    buf += "\" INTO OUTFILE '";
    buf += fuzz_out.generic_string();
    buf += "' TRUNCATE FORMAT TabSeparated;";
    this->processServerQuery(buf);

    std::ifstream infile(fuzz_out);
    buf.resize(0);
    out.clear();
    while (std::getline(infile, buf))
    {
        out.push_back(buf);
        buf.resize(0);
        found++;
    }
    LOG_INFO(log, "Found {} entries from {} table", found, table);
}

void FuzzConfig::loadServerConfigurations()
{
    loadServerSettings(this->collations, "collations", "name");
    loadServerSettings(this->storage_policies, "storage_policies", "policy_name");
    loadServerSettings(this->disks, "disks", "name");
    loadServerSettings(this->timezones, "time_zones", "time_zone");
}

}
