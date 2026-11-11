#include <Dictionaries/CassandraDictionarySource.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/RemoteHostFilter.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <IO/WriteHelpers.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <ranges>


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
}

void registerDictionarySourceCassandra(DictionarySourceFactory & factory);
void registerDictionarySourceCassandra(DictionarySourceFactory & factory)
{
    auto create_table_source = [=]([[maybe_unused]] const String & name,
                                   [[maybe_unused]] const DictionaryStructure & dict_struct,
                                   [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                                   [[maybe_unused]] const std::string & config_prefix,
                                   [[maybe_unused]] Block & sample_block,
                                   [[maybe_unused]] ContextPtr global_context,
                                                    const std::string & /* default_database */,
                                                    bool /*created_from_ddl*/) -> DictionarySourcePtr
    {
#if USE_CASSANDRA
    auto source_config_prefix = config_prefix + ".cassandra";
    const String port = toString(config.getUInt(source_config_prefix + ".port", CassandraDictionarySource::DEFAULT_PORT));
    for (const auto & host : CassandraDictionarySource::splitHosts(config.getString(source_config_prefix + ".host")))
        global_context->getRemoteHostFilter().checkHostAndPort(host, port);

    return std::make_unique<CassandraDictionarySource>(dict_struct, config, source_config_prefix, sample_block);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "Dictionary source of type `cassandra` is disabled because ClickHouse was built without cassandra support.");
#endif
    };
    factory.registerSource("cassandra", create_table_source, SecretArgumentsSpec{.secret_keys = {"password"}}, Documentation{
        .description = R"DOCS_MD(
# Cassandra dictionary source

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
SOURCE(CASSANDRA(
    host 'localhost'
    port 9042
    user 'username'
    password 'qwerty123'
    secure 1
    compression 'lz4'
    keyspace 'database_name'
    column_family 'table_name'
    allow_filtering 1
    partition_key_prefix 1
    consistency 'One'
    where '"SomeColumn" = 42'
    max_threads 8
    query 'SELECT id, value_1, value_2 FROM database_name.table_name'
))
```

</Tab>
<Tab title="Configuration file">

```xml
<source>
    <cassandra>
        <host>localhost</host>
        <port>9042</port>
        <user>username</user>
        <password>qwerty123</password>
        <secure>1</secure>
        <compression>lz4</compression>
        <keyspace>database_name</keyspace>
        <column_family>table_name</column_family>
        <allow_filtering>1</allow_filtering>
        <partition_key_prefix>1</partition_key_prefix>
        <consistency>One</consistency>
        <where>"SomeColumn" = 42</where>
        <max_threads>8</max_threads>
        <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
    </cassandra>
</source>
```

</Tab>
</Tabs>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The Cassandra host or comma-separated list of hosts. |
| `port` | The port on the Cassandra servers. If not specified, default port `9042` is used. |
| `user` | Name of the Cassandra user. |
| `password` | Password of the Cassandra user. |
| `secure` | Use TLS for the connection. The TLS client settings, such as the certificate verification mode, are taken from the `openSSL.client` section of the server configuration. Default value is `0`. |
| `compression` | Compression of the transferred data: `none`, `lz4`, or `snappy`. Default value is `none`. |
| `keyspace` | Name of the keyspace (database). |
| `column_family` | Name of the column family (table). |
| `allow_filtering` | Flag to allow or not potentially expensive conditions on clustering key columns. Default value is `1`. |
| `partition_key_prefix` | Number of partition key columns in primary key of the Cassandra table. Required for compose key dictionaries. Order of key columns in the dictionary definition must be the same as in Cassandra. Default value is `1` (the first key column is a partition key and other key columns are clustering key). |
| `consistency` | Consistency level. Possible values: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. Default value is `One`. |
| `where` | Optional selection criteria. |
| `max_threads` | The maximum number of idle connections to keep open for loading data, for example, from multiple partitions in compose key dictionaries. Default value is `8`. |
| `query` | The custom query. Optional. For dictionaries with complex keys and the `cache` or `direct` layouts, it must contain the `{condition}` placeholder, which is replaced with the condition on the requested keys, because CQL does not support subqueries. The placeholder must follow `WHERE` or `AND`, because it is removed together with them when the whole dictionary is loaded. |

<Note>
The `column_family` or `where` fields cannot be used together with the `query` field. And either one of the `column_family` or `query` fields must be declared.
</Note>
)DOCS_MD"
#if !USE_CASSANDRA
            "\n\nCurrently unavailable, because this ClickHouse build does not include Cassandra support."
#endif
        ,
        .syntax = "SOURCE(CASSANDRA(host 'host' port 9042 keyspace 'keyspace' column_family 'table'))",
        .related = {}});
}

}

#if USE_CASSANDRA

#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Databases/removeWhereConditionPlaceholder.h>
#include <Common/StringUtils.h>
#include <Dictionaries/CassandraSource.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int UNSUPPORTED_METHOD;
}

CassandraDictionarySource::Configuration::Configuration(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
    : host(config.getString(config_prefix + ".host"))
    , port(static_cast<UInt16>(config.getUInt(config_prefix + ".port", DEFAULT_PORT)))
    , user(config.getString(config_prefix + ".user", ""))
    , password(config.getString(config_prefix + ".password", ""))
    , secure(config.getBool(config_prefix + ".secure", false))
    , compression(Cassandra::parseCompression(config.getString(config_prefix + ".compression", "none")))
    , db(config.getString(config_prefix + ".keyspace", ""))
    , table(config.getString(config_prefix + ".column_family", ""))
    , query(config.getString(config_prefix + ".query", ""))
    , allow_filtering(config.getBool(config_prefix + ".allow_filtering", false))
    , partition_key_prefix(config.getUInt(config_prefix + ".partition_key_prefix", 1))
    , max_threads(config.getUInt(config_prefix + ".max_threads", 8))
    , where(config.getString(config_prefix + ".where", ""))
{
    setConsistency(config.getString(config_prefix + ".consistency", "One"));

    if (!query.empty())
    {
        /// `where` would be silently ignored, because a custom query is used as is.
        if (!table.empty() || !where.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "The `column_family` and `where` settings of a Cassandra dictionary source cannot be used together with `query`");
    }
    else if (db.empty() || table.empty())
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "A Cassandra dictionary source requires either `query` or both `keyspace` and `column_family` settings");
    }
}

void CassandraDictionarySource::Configuration::setConsistency(const String & config_str)
{
    using Cassandra::Consistency;
    if (config_str == "One")
        consistency = Consistency::One;
    else if (config_str == "Two")
        consistency = Consistency::Two;
    else if (config_str == "Three")
        consistency = Consistency::Three;
    else if (config_str == "All")
        consistency = Consistency::All;
    else if (config_str == "EachQuorum")
        consistency = Consistency::EachQuorum;
    else if (config_str == "Quorum")
        consistency = Consistency::Quorum;
    else if (config_str == "LocalQuorum")
        consistency = Consistency::LocalQuorum;
    else if (config_str == "LocalOne")
        consistency = Consistency::LocalOne;
    else if (config_str == "Serial")
        consistency = Consistency::Serial;
    else if (config_str == "LocalSerial")
        consistency = Consistency::LocalSerial;
    else    /// `Any` is only valid for writes
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unsupported consistency level: {}", config_str);
}

static const size_t max_block_size = 8192;

VectorWithMemoryTracking<String> CassandraDictionarySource::splitHosts(const String & hosts)
{
    VectorWithMemoryTracking<String> res;
    for (auto host : std::views::split(hosts, ','))
    {
        String trimmed(std::string_view(host.begin(), host.end()));
        boost::algorithm::trim(trimmed);
        if (trimmed.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Empty host in the list of Cassandra hosts: '{}'", hosts);
        res.push_back(std::move(trimmed));
    }
    return res;
}

/// For a full load, the `{condition}` placeholder of a custom query has to be removed:
/// replacing it with a tautology like `ExternalQueryBuilder` does is not possible, because CQL has no such expressions.
/// The placeholder is removed together with the preceding `AND`, or with `WHERE` if it is the only condition.
static String removeConditionPlaceholderFromCQL(const String & query)
{
    auto condition_position = query.find(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);
    if (condition_position == std::string::npos)
        return query;

    String prefix = query.substr(0, condition_position);
    String suffix = query.substr(condition_position + CONDITION_PLACEHOLDER_TO_REPLACE_VALUE.size());
    boost::algorithm::trim_right(prefix);
    boost::algorithm::trim_left(suffix);

    auto ends_with_word = [](const String & str, std::string_view word)
    {
        return boost::algorithm::iends_with(str, word)
            && (str.size() == word.size() || isWhitespaceASCII(str[str.size() - word.size() - 1]));
    };

    auto starts_with_word = [](const String & str, std::string_view word)
    {
        return boost::algorithm::istarts_with(str, word)
            && (str.size() == word.size() || isWhitespaceASCII(str[word.size()]));
    };

    if (ends_with_word(prefix, "AND"))
    {
        prefix.resize(prefix.size() - 3);
    }
    else if (ends_with_word(prefix, "WHERE"))
    {
        /// `WHERE {condition} AND ...` - keep `WHERE` and remove the following `AND`.
        if (starts_with_word(suffix, "AND"))
            suffix.erase(0, 3);
        else
            prefix.resize(prefix.size() - 5);
    }
    else
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "The {} placeholder in a custom query of a Cassandra dictionary source must follow WHERE or AND: '{}'",
            CONDITION_PLACEHOLDER_TO_REPLACE_VALUE, query);
    }

    boost::algorithm::trim_right(prefix);
    boost::algorithm::trim_left(suffix);
    if (suffix.empty() || suffix.starts_with(';'))
        return prefix + suffix;
    return prefix + ' ' + suffix;
}

CassandraDictionarySource::CassandraDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    const Block & sample_block_)
    : log(getLogger("CassandraDictionarySource"))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(std::make_shared<const Block>(sample_block_))
    , query_builder(dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::DoubleQuotes)
    , pool(std::make_shared<Cassandra::ConnectionPool>(
        splitHosts(configuration.host),
        configuration.port,
        Cassandra::ConnectionSettings{
            .user = configuration.user,
            .password = configuration.password,
            .secure = configuration.secure,
            .compression = configuration.compression},
        configuration.consistency,
        configuration.max_threads))
{
    /// The partition key is a prefix of the dictionary key columns, see `loadKeys`.
    if (dict_struct.key && (configuration.partition_key_prefix == 0 || configuration.partition_key_prefix > dict_struct.key->size()))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
            "The `partition_key_prefix` setting of a Cassandra dictionary source must be between 1 and the number of key columns ({}), got {}",
            dict_struct.key->size(), configuration.partition_key_prefix);
}

CassandraDictionarySource::CassandraDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    Block & sample_block_)
    : CassandraDictionarySource(
        dict_struct_,
        Configuration(config, config_prefix),
        sample_block_)
{
}

void CassandraDictionarySource::maybeAllowFiltering(String & query) const
{
    if (!configuration.allow_filtering)
        return;
    /// The generated queries end with a semicolon, a custom query may not.
    boost::algorithm::trim_right(query);
    if (query.ends_with(';'))
        query.pop_back();
    query += " ALLOW FILTERING;";
}

BlockIO CassandraDictionarySource::loadAll()
{
    String query = configuration.query.empty() ? query_builder.composeLoadAllQuery() : removeConditionPlaceholderFromCQL(configuration.query);
    maybeAllowFiltering(query);
    LOG_INFO(log, "Loading all using query: {}", query);
    BlockIO io;
    io.pipeline = QueryPipeline(std::make_shared<CassandraSource>(pool, query, sample_block, max_block_size));
    return io;
}

std::string CassandraDictionarySource::toString() const
{
    return "Cassandra: " + configuration.db + '.' + configuration.table;
}

BlockIO CassandraDictionarySource::loadIds(const VectorWithMemoryTracking<UInt64> & ids)
{
    String query = query_builder.composeLoadIdsQuery(ids);
    maybeAllowFiltering(query);
    LOG_INFO(log, "Loading ids using query: {}", query);

    BlockIO io;
    io.pipeline = QueryPipeline(std::make_shared<CassandraSource>(pool, query, sample_block, max_block_size));
    return io;
}

BlockIO CassandraDictionarySource::loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows)
{
    if (requested_rows.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No rows requested");

    /// Without the placeholder, `ExternalQueryBuilder` wraps the custom query in a subquery, which is not supported by CQL.
    if (!configuration.query.empty() && !configuration.query.contains(CONDITION_PLACEHOLDER_TO_REPLACE_VALUE))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Loading keys from a Cassandra dictionary source with a custom query requires the {} placeholder in the query",
            CONDITION_PLACEHOLDER_TO_REPLACE_VALUE);

    /// TODO is there a better way to load data by complex keys?
    UnorderedMapWithMemoryTracking<UInt64, VectorWithMemoryTracking<size_t>> partitions;
    for (const auto & row : requested_rows)
    {
        SipHash partition_key;
        for (size_t i = 0; i < configuration.partition_key_prefix; ++i)
            key_columns[i]->updateHashWithValue(row, partition_key);
        partitions[partition_key.get64()].push_back(row);
    }

    Pipes pipes;
    for (const auto & partition : partitions)
    {
        String query = query_builder.composeLoadKeysQuery(key_columns, partition.second, ExternalQueryBuilder::CASSANDRA_SEPARATE_PARTITION_KEY, configuration.partition_key_prefix);
        maybeAllowFiltering(query);
        LOG_INFO(log, "Loading keys for partition hash {} using query: {}", partition.first, query);
        pipes.push_back(Pipe(std::make_shared<CassandraSource>(pool, query, sample_block, max_block_size)));
    }

    BlockIO io;
    io.pipeline = QueryPipeline(Pipe::unitePipes(std::move(pipes)));
    return io;
}

BlockIO CassandraDictionarySource::loadUpdatedAll()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for CassandraDictionarySource");
}

}

#endif
