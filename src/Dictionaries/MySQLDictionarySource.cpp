#include <Dictionaries/MySQLDictionarySource.h>


#if USE_MYSQL
#    include <mysqlxx/PoolFactory.h>
#endif

#include <Poco/Util/AbstractConfiguration.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/registerDictionaries.h>
#include <Core/Settings.h>
#include <Common/DateLUTImpl.h>
#include <Common/RemoteHostFilter.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/LocalDateTime.h>
#include <Common/parseRemoteDescription.h>
#include <Common/logger_useful.h>
#include <Dictionaries/readInvalidateQuery.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 external_storage_connect_timeout_sec;
    extern const SettingsUInt64 external_storage_rw_timeout_sec;
    extern const SettingsUInt64 glob_expansion_max_elements;
}

namespace MySQLSetting
{
    extern const MySQLSettingsUInt64 connect_timeout;
    extern const MySQLSettingsUInt64 read_write_timeout;
}

[[maybe_unused]]
static const size_t default_num_tries_on_connection_loss = 3;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_METHOD;
}

static const ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> dictionary_allowed_keys = {
    "host", "port", "user", "password",
    "db", "database", "table", "schema",
    "update_field", "invalidate_query", "priority",
    "update_lag",
    "dont_check_update_time" /* obsolete */,
    "query", "where", "name" /* name_collection */, "socket",
    "share_connection", "fail_on_connection_loss", "close_connection",
    "ssl_ca", "ssl_cert", "ssl_key",
    "ssl_ca_pem", "ssl_cert_pem", "ssl_key_pem",
    "enable_local_infile", "opt_reconnect", "enable_compression",
    "connect_timeout", "mysql_connect_timeout",
    "mysql_rw_timeout", "rw_timeout"};

#if USE_MYSQL
/// The source configuration of a dictionary created with a DDL query comes from the query itself, so
/// it may not name files for the server to open: the server reads them with its own privileges, and a
/// user who cannot read a certificate and key must not be able to authenticate with them. The
/// contents can be passed in `ssl_ca_pem`, `ssl_cert_pem` and `ssl_key_pem` instead.
/// Dictionaries defined in server configuration files are written by an operator and keep using paths.
static void checkNoSSLPaths(const Poco::Util::AbstractConfiguration & config, const std::string & prefix)
{
    static const std::initializer_list<std::pair<std::string_view, std::string_view>> keys
        = {{"ssl_ca", "ssl_ca_pem"}, {"ssl_cert", "ssl_cert_pem"}, {"ssl_key", "ssl_key_pem"}};

    for (const auto & [key, contents_key] : keys)
    {
        if (config.has(prefix + "." + std::string(key)))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`{}` cannot be specified in a dictionary created with a DDL query. "
                "Pass the contents of the file in `{}` instead",
                key, contents_key);
    }
}
#endif

void registerDictionarySourceMysql(DictionarySourceFactory & factory);
void registerDictionarySourceMysql(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                   [[maybe_unused]] const DictionaryStructure & dict_struct,
                                   [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                                   [[maybe_unused]] const std::string & config_prefix,
                                   [[maybe_unused]] Block & sample_block,
                                   [[maybe_unused]] ContextPtr global_context,
                                   const std::string & /* default_database */,
                                   [[maybe_unused]] bool created_from_ddl) -> DictionarySourcePtr {
#if USE_MYSQL
        MySQLStreamSettings mysql_input_stream_settings(
            global_context->getSettingsRef(),
            config.getBool(config_prefix + ".mysql.close_connection", false) || config.getBool(config_prefix + ".mysql.share_connection", false),
            false,
            config.getBool(config_prefix + ".mysql.fail_on_connection_loss", false) ? 1 : default_num_tries_on_connection_loss);

        auto settings_config_prefix = config_prefix + ".mysql";
        std::shared_ptr<mysqlxx::PoolWithFailover> pool;
        MySQLSettings mysql_settings;

        /// Every key here comes from the `CREATE DICTIONARY` query, including the keys that override a
        /// named collection, so this covers both of the branches below.
        if (created_from_ddl)
            checkNoSSLPaths(config, settings_config_prefix);

        std::optional<MySQLDictionarySource::Configuration> dictionary_configuration;
        auto named_collection = created_from_ddl ? tryGetNamedCollectionWithOverrides(config, settings_config_prefix, global_context) : nullptr;
        if (named_collection)
        {
            auto allowed_arguments{dictionary_allowed_keys};
            auto setting_names = mysql_settings.getAllRegisteredNames();
            for (const auto & name : setting_names)
                allowed_arguments.insert(name);
            validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(*named_collection, {}, allowed_arguments);

            StorageMySQL::Configuration::Addresses addresses;
            const auto addresses_expr = named_collection->getOrDefault<String>("addresses_expr", "");
            if (addresses_expr.empty())
            {
                const auto host = named_collection->getAnyOrDefault<String>({"host", "hostname"}, "");
                const auto port = static_cast<UInt16>(named_collection->get<UInt64>("port"));
                addresses = {std::make_pair(host, port)};
            }
            else
            {
                size_t max_addresses = global_context->getSettingsRef()[Setting::glob_expansion_max_elements];
                addresses = parseRemoteDescriptionForExternalDatabase(addresses_expr, max_addresses, 3306);
            }

            for (auto & address : addresses)
                global_context->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));

            dictionary_configuration.emplace(MySQLDictionarySource::Configuration{
                .db = named_collection->getAnyOrDefault<String>({"database", "db"}, ""),
                .table = named_collection->getOrDefault<String>("table", ""),
                .query = named_collection->getOrDefault<String>("query", ""),
                .where = named_collection->getOrDefault<String>("where", ""),
                .invalidate_query = named_collection->getOrDefault<String>("invalidate_query", ""),
                .update_field = named_collection->getOrDefault<String>("update_field", ""),
                .update_lag = named_collection->getOrDefault<UInt64>("update_lag", 1),
                .bg_reconnect = named_collection->getOrDefault<bool>("background_reconnect", false),
            });

            const auto & settings = global_context->getSettingsRef();
            if (!mysql_settings[MySQLSetting::connect_timeout].changed)
                mysql_settings[MySQLSetting::connect_timeout] = settings[Setting::external_storage_connect_timeout_sec];
            if (!mysql_settings[MySQLSetting::read_write_timeout].changed)
                mysql_settings[MySQLSetting::read_write_timeout] = settings[Setting::external_storage_rw_timeout_sec];

            mysql_settings.loadFromNamedCollection(*named_collection);

            pool = std::make_shared<mysqlxx::PoolWithFailover>(
                createMySQLPoolWithFailover(
                    dictionary_configuration->db,
                    addresses,
                    named_collection->getAnyOrDefault<String>({"user", "username"}, ""),
                    named_collection->getOrDefault<String>("password", ""),
                    StorageMySQL::getSSLParams(*named_collection),
                    mysql_settings));
        }
        else
        {
            dictionary_configuration.emplace(MySQLDictionarySource::Configuration{
                .db = config.getString(settings_config_prefix + ".db", ""),
                .table = config.getString(settings_config_prefix + ".table", ""),
                .query = config.getString(settings_config_prefix + ".query", ""),
                .where = config.getString(settings_config_prefix + ".where", ""),
                .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
                .update_field = config.getString(settings_config_prefix + ".update_field", ""),
                .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
                .bg_reconnect = config.getBool(settings_config_prefix + ".background_reconnect", false),
            });

            if (created_from_ddl)
            {
                if (config.has(settings_config_prefix + ".replica"))
                {
                    Poco::Util::AbstractConfiguration::Keys replica_keys;
                    config.keys(settings_config_prefix, replica_keys);
                    for (const auto & replica_key : replica_keys)
                    {
                        if (replica_key.starts_with("replica"))
                        {
                            const auto replica_prefix = settings_config_prefix + "." + replica_key;
                            checkNoSSLPaths(config, replica_prefix);
                            global_context->getRemoteHostFilter().checkHostAndPort(
                                config.getString(replica_prefix + ".host"),
                                toString(config.getInt(replica_prefix + ".port", 3306)));
                        }
                    }
                }
                else
                {
                    global_context->getRemoteHostFilter().checkHostAndPort(
                        config.getString(settings_config_prefix + ".host"),
                        toString(config.getInt(settings_config_prefix + ".port", 3306)));
                }
            }

            pool = std::make_shared<mysqlxx::PoolWithFailover>(
                mysqlxx::PoolFactory::instance().get(config, settings_config_prefix));
        }

        if (dictionary_configuration->query.empty() && dictionary_configuration->table.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MySQL dictionary source configuration must contain table or query field");

        return std::make_unique<MySQLDictionarySource>(dict_struct, *dictionary_configuration, std::move(pool), sample_block, mysql_input_stream_settings);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `mysql` is disabled because ClickHouse was built without mysql support.");
#endif
    };

    factory.registerSource("mysql", create_table_source, Documentation{
        .description = R"DOCS_MD(
# MySQL dictionary source

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
SOURCE(MYSQL(
    port 3306
    user 'clickhouse'
    password 'qwerty'
    replica(host 'example01-1' priority 1)
    replica(host 'example01-2' priority 1)
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    enable_compression 1
))
```

</Tab>
<Tab title="Configuration file">

```xml
<source>
  <mysql>
      <port>3306</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <replica>
          <host>example01-1</host>
          <priority>1</priority>
      </replica>
      <replica>
          <host>example01-2</host>
          <priority>1</priority>
      </replica>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      <enable_compression>1</enable_compression>
  </mysql>
</source>
```

</Tab>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `port` | The port on the MySQL server. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `user` | Name of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `password` | Password of the MySQL user. You can specify it for all replicas, or for each one individually (inside `<replica>`). |
| `replica` | Section of replica configurations. There can be multiple sections. |
| `replica/host` | The MySQL host. |
| `replica/priority` | The replica priority. When attempting to connect, ClickHouse traverses the replicas in order of priority. The lower the number, the higher the priority. |
| `db` | Name of the database. |
| `table` | Name of the table. |
| `where` | The selection criteria. The syntax for conditions is the same as for `WHERE` clause in MySQL, for example, `id > 10 AND id < 20`. Optional. |
| `invalidate_query` | Query for checking the dictionary status. Optional. Read more in the section [Refreshing dictionary data using LIFETIME](/reference/statements/create/dictionary/lifetime). |
| `fail_on_connection_loss` | Controls behavior of the server on connection loss. If `true`, an exception is thrown immediately if the connection between client and server was lost. If `false`, the server retries to fetch data at least three times before reporting an error. Note that retrying leads to increased response times. Default value: `false`. |
| `query` | The custom query. Optional. |
| `enable_compression` | Enables zlib compression for the MySQL protocol connection. When set to `1`, ClickHouse requests protocol-level compression from the MySQL server. Can also be set per-replica inside `<replica>`. Default value: `0`. |
| `ssl_ca_pem` | Contents of the CA certificate that the MySQL server certificate is verified against. Optional. |
| `ssl_cert_pem` | Contents of the client certificate, for certificate-based authentication. Optional. |
| `ssl_key_pem` | Contents of the private key belonging to `ssl_cert_pem`. Optional. |
| `ssl_ca`, `ssl_cert`, `ssl_key` | The same credentials as paths to files on the server. Only allowed for a dictionary defined in a server configuration file, or through a named collection defined there, see below. Optional. |

<Note>
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
</Note>

<Note>
`ssl_ca`, `ssl_cert` and `ssl_key` name files that the server opens with its own privileges, so they are only accepted for a dictionary defined in a server configuration file, or through a named collection defined there. A `CREATE DICTIONARY` query that specifies the TLS credentials directly must pass their contents instead, in `ssl_ca_pem`, `ssl_cert_pem` and `ssl_key_pem`. Those values are masked in logs and in `SHOW` queries, the same way passwords are.
</Note>

<Note>
There is no explicit parameter `secure`. When establishing an SSL-connection security is mandatory.
</Note>

MySQL can be connected to on a local host via sockets. To do this, set `host` and `socket`.

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
SOURCE(MYSQL(
    host 'localhost'
    socket '/path/to/socket/file.sock'
    user 'clickhouse'
    password 'qwerty'
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

</Tab>
<Tab title="Configuration file">

```xml
<source>
  <mysql>
      <host>localhost</host>
      <socket>/path/to/socket/file.sock</socket>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

</Tab>
</Tabs>
)DOCS_MD"
#if !USE_MYSQL
            "\n\nCurrently unavailable, because this ClickHouse build does not include MySQL support."
#endif
        ,
        .syntax = "SOURCE(MYSQL(host 'host' port 3306 user 'user' password '' db 'db' table 'table'))",
        .related = {"clickhouse", "postgresql"}});
}

}


#if USE_MYSQL

namespace DB
{


MySQLDictionarySource::MySQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    mysqlxx::PoolWithFailoverPtr pool_,
    const Block & sample_block_,
    const MySQLStreamSettings & settings_)
    : log(getLogger("MySQLDictionarySource"))
    , update_time(std::chrono::system_clock::from_time_t(0))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , pool(std::move(pool_))
    , sample_block(sample_block_)
    , query_builder(dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks)
    , load_all_query(query_builder.composeLoadAllQuery())
    , settings(settings_)
{
}

/// copy-constructor is provided in order to support cloneability
MySQLDictionarySource::MySQLDictionarySource(const MySQLDictionarySource & other)
    : log(getLogger("MySQLDictionarySource"))
    , update_time(other.update_time)
    , dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , pool(other.pool)
    , sample_block(other.sample_block)
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks}
    , load_all_query{other.load_all_query}
    , invalidate_query_response{other.invalidate_query_response}
    , settings(other.settings)
{
}

std::string MySQLDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
        std::string str_time = DateLUT::instance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(configuration.update_field, str_time);
    }

    update_time = std::chrono::system_clock::now();
    return load_all_query;
}

QueryPipeline MySQLDictionarySource::loadFromQuery(const String & query)
{
    return QueryPipeline(std::make_shared<MySQLWithFailoverSource>(
            pool, query, sample_block, settings));
}

BlockIO MySQLDictionarySource::loadAll()
{
    LOG_TRACE(log, fmt::runtime(load_all_query));
    BlockIO io;
    io.pipeline = loadFromQuery(load_all_query);
    return io;
}

BlockIO MySQLDictionarySource::loadUpdatedAll()
{
    std::string load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, fmt::runtime(load_update_query));
    BlockIO io;
    io.pipeline = loadFromQuery(load_update_query);
    return io;
}

BlockIO MySQLDictionarySource::loadIds(const VectorWithMemoryTracking<UInt64> & ids)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.
    const auto query = query_builder.composeLoadIdsQuery(ids);
    BlockIO io;
    io.pipeline = loadFromQuery(query);
    return io;
}

BlockIO MySQLDictionarySource::loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    BlockIO io;
    io.pipeline = loadFromQuery(query);
    return io;
}

bool MySQLDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        LOG_TRACE(log, "Executing invalidate query: {}", configuration.invalidate_query);
        auto response = doInvalidateQuery(configuration.invalidate_query);
        return invalidate_query_response.updateAndCheckModified(response);
    }

    return true;
}

bool MySQLDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool MySQLDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}

DictionarySourcePtr MySQLDictionarySource::clone() const
{
    return std::make_shared<MySQLDictionarySource>(*this);
}

std::string MySQLDictionarySource::toString() const
{
    const auto & where = configuration.where;
    return "MySQL: " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
}

std::string MySQLDictionarySource::quoteForLike(const std::string & value)
{
    std::string tmp;
    tmp.reserve(value.size());

    for (auto c : value)
    {
        if (c == '%' || c == '_' || c == '\\')
            tmp.push_back('\\');
        tmp.push_back(c);
    }

    WriteBufferFromOwnString out;
    writeQuoted(tmp, out);
    return out.str();
}

std::string MySQLDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));

    QueryPipeline pipeline(std::make_unique<MySQLSource>(pool->get(), request, invalidate_sample_block, settings));
    return readInvalidateQuery(pipeline);
}

}

#endif
