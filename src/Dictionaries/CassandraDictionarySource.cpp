#include "CassandraDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int NOT_IMPLEMENTED;
}

void registerDictionarySourceCassandra(DictionarySourceFactory & factory)
{
    auto create_table_source = [=]([[maybe_unused]] const DictionaryStructure & dict_struct,
                                   [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                                   [[maybe_unused]] const std::string & config_prefix,
                                   [[maybe_unused]] Block & sample_block,
                                                    ContextPtr /* context */,
                                                    const std::string & /* default_database */,
                                                    bool /*created_from_ddl*/) -> DictionarySourcePtr
    {
#if USE_CASSANDRA
    setupCassandraDriverLibraryLogging(CASS_LOG_INFO);
    return std::make_unique<CassandraDictionarySource>(dict_struct, config, config_prefix + ".cassandra", sample_block);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "Dictionary source of type `cassandra` is disabled because ClickHouse was built without cassandra support.");
#endif
    };
    factory.registerSource("cassandra", create_table_source);
}

}

#if USE_CASSANDRA

#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include "CassandraBlockInputStream.h"
#include <common/logger_useful.h>
#include <DataStreams/UnionBlockInputStream.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
}

CassandraSettings::CassandraSettings(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
    : host(config.getString(config_prefix + ".host"))
    , port(config.getUInt(config_prefix + ".port", 0))
    , user(config.getString(config_prefix + ".user", ""))
    , password(config.getString(config_prefix + ".password", ""))
    , db(config.getString(config_prefix + ".keyspace"))
    , table(config.getString(config_prefix + ".column_family"))
    , allow_filtering(config.getBool(config_prefix + ".allow_filtering", false))
    , partition_key_prefix(config.getUInt(config_prefix + ".partition_key_prefix", 1))
    , max_threads(config.getUInt(config_prefix + ".max_threads", 8))
    , where(config.getString(config_prefix + ".where", ""))
{
    setConsistency(config.getString(config_prefix + ".consistency", "One"));
}

void CassandraSettings::setConsistency(const String & config_str)
{
    if (config_str == "One")
        consistency = CASS_CONSISTENCY_ONE;
    else if (config_str == "Two")
        consistency = CASS_CONSISTENCY_TWO;
    else if (config_str == "Three")
        consistency = CASS_CONSISTENCY_THREE;
    else if (config_str == "All")
        consistency = CASS_CONSISTENCY_ALL;
    else if (config_str == "EachQuorum")
        consistency = CASS_CONSISTENCY_EACH_QUORUM;
    else if (config_str == "Quorum")
        consistency = CASS_CONSISTENCY_QUORUM;
    else if (config_str == "LocalQuorum")
        consistency = CASS_CONSISTENCY_LOCAL_QUORUM;
    else if (config_str == "LocalOne")
        consistency = CASS_CONSISTENCY_LOCAL_ONE;
    else if (config_str == "Serial")
        consistency = CASS_CONSISTENCY_SERIAL;
    else if (config_str == "LocalSerial")
        consistency = CASS_CONSISTENCY_LOCAL_SERIAL;
    else    /// CASS_CONSISTENCY_ANY is only valid for writes
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unsupported consistency level: {}", config_str);
}

static const size_t max_block_size = 8192;

CassandraDictionarySource::CassandraDictionarySource(
    const DictionaryStructure & dict_struct_,
    const CassandraSettings & settings_,
    const Block & sample_block_)
    : log(&Poco::Logger::get("CassandraDictionarySource"))
    , dict_struct(dict_struct_)
    , settings(settings_)
    , sample_block(sample_block_)
    , query_builder(dict_struct, settings.db, "", settings.table, settings.where, IdentifierQuotingStyle::DoubleQuotes)
{
    cassandraCheck(cass_cluster_set_contact_points(cluster, settings.host.c_str()));
    if (settings.port)
        cassandraCheck(cass_cluster_set_port(cluster, settings.port));
    cass_cluster_set_credentials(cluster, settings.user.c_str(), settings.password.c_str());
    cassandraCheck(cass_cluster_set_consistency(cluster, settings.consistency));
}

CassandraDictionarySource::CassandraDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    Block & sample_block_)
    : CassandraDictionarySource(
        dict_struct_,
        CassandraSettings(config, config_prefix),
        sample_block_)
{
}

void CassandraDictionarySource::maybeAllowFiltering(String & query) const
{
    if (!settings.allow_filtering)
        return;
    query.pop_back();   /// remove semicolon
    query += " ALLOW FILTERING;";
}

BlockInputStreamPtr CassandraDictionarySource::loadAll()
{
    String query = query_builder.composeLoadAllQuery();
    maybeAllowFiltering(query);
    LOG_INFO(log, "Loading all using query: {}", query);
    return std::make_shared<CassandraBlockInputStream>(getSession(), query, sample_block, max_block_size);
}

std::string CassandraDictionarySource::toString() const
{
    return "Cassandra: " + settings.db + '.' + settings.table;
}

BlockInputStreamPtr CassandraDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    String query = query_builder.composeLoadIdsQuery(ids);
    maybeAllowFiltering(query);
    LOG_INFO(log, "Loading ids using query: {}", query);
    return std::make_shared<CassandraBlockInputStream>(getSession(), query, sample_block, max_block_size);
}

BlockInputStreamPtr CassandraDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    if (requested_rows.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No rows requested");

    /// TODO is there a better way to load data by complex keys?
    std::unordered_map<UInt64, std::vector<size_t>> partitions;
    for (const auto & row : requested_rows)
    {
        SipHash partition_key;
        for (size_t i = 0; i < settings.partition_key_prefix; ++i)
            key_columns[i]->updateHashWithValue(row, partition_key);
        partitions[partition_key.get64()].push_back(row);
    }

    BlockInputStreams streams;
    for (const auto & partition : partitions)
    {
        String query = query_builder.composeLoadKeysQuery(key_columns, partition.second, ExternalQueryBuilder::CASSANDRA_SEPARATE_PARTITION_KEY, settings.partition_key_prefix);
        maybeAllowFiltering(query);
        LOG_INFO(log, "Loading keys for partition hash {} using query: {}", partition.first, query);
        streams.push_back(std::make_shared<CassandraBlockInputStream>(getSession(), query, sample_block, max_block_size));
    }

    if (streams.size() == 1)
        return streams.front();

    return std::make_shared<UnionBlockInputStream>(streams, nullptr, settings.max_threads);
}

BlockInputStreamPtr CassandraDictionarySource::loadUpdatedAll()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for CassandraDictionarySource");
}

CassSessionShared CassandraDictionarySource::getSession()
{
    /// Reuse connection if exists, create new one if not
    auto session = maybe_session.lock();
    if (session)
        return session;

    std::lock_guard lock(connect_mutex);
    session = maybe_session.lock();
    if (session)
        return session;

    session = std::make_shared<CassSessionPtr>();
    CassFuturePtr future = cass_session_connect(*session, cluster);
    cassandraWaitAndCheck(future);
    maybe_session = session;
    return session;
}

}

#endif
