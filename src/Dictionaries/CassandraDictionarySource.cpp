#include "CassandraDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "ExternalQueryBuilder.h"
#include <common/logger_useful.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int SUPPORT_IS_DISABLED;
    }

    void registerDictionarySourceCassandra(DictionarySourceFactory & factory)
    {
        auto create_table_source = [=]([[maybe_unused]] const DictionaryStructure & dict_struct,
                                       [[maybe_unused]] const Poco::Util::AbstractConfiguration & config,
                                       [[maybe_unused]] const std::string & config_prefix,
                                       [[maybe_unused]] Block & sample_block,
                                                        const Context & /* context */,
                                                        bool /*check_config*/) -> DictionarySourcePtr
        {
#if USE_CASSANDRA
        return std::make_unique<CassandraDictionarySource>(dict_struct, config, config_prefix + ".cassandra", sample_block);
#else
        throw Exception{"Dictionary source of type `cassandra` is disabled because library was built without cassandra support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        };
        factory.registerSource("cassandra", create_table_source);
    }

}

#if USE_CASSANDRA

#include <IO/WriteHelpers.h>
#include "CassandraBlockInputStream.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int WRONG_PASSWORD;
}

static const size_t max_block_size = 8192;

CassandraDictionarySource::CassandraDictionarySource(
    const DB::DictionaryStructure & dict_struct_,
    const String & host_,
    UInt16 port_,
    const String & user_,
    const String & password_,
    const String & db_,
    const String & table_,
    const DB::Block & sample_block_)
    : log(&Poco::Logger::get("CassandraDictionarySource"))
    , dict_struct(dict_struct_)
    , host(host_)
    , port(port_)
    , user(user_)
    , password(password_)
    , db(db_)
    , table(table_)
    , sample_block(sample_block_)
{
    cassandraCheck(cass_cluster_set_contact_points(cluster, host.c_str()));
    if (port)
        cassandraCheck(cass_cluster_set_port(cluster, port));
    cass_cluster_set_credentials(cluster, user.c_str(), password.c_str());
}

CassandraDictionarySource::CassandraDictionarySource(
    const DB::DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    DB::Block & sample_block_)
    : CassandraDictionarySource(
        dict_struct_,
        config.getString(config_prefix + ".host"),
        config.getUInt(config_prefix + ".port", 0),
        config.getString(config_prefix + ".user", ""),
        config.getString(config_prefix + ".password", ""),
        config.getString(config_prefix + ".keyspace", ""),
        config.getString(config_prefix + ".column_family"),
        sample_block_)
{
}

CassandraDictionarySource::CassandraDictionarySource(const CassandraDictionarySource & other)
    : CassandraDictionarySource{other.dict_struct,
                                other.host,
                                other.port,
                                other.user,
                                other.password,
                                other.db,
                                other.table,
                                other.sample_block}
{
}

BlockInputStreamPtr CassandraDictionarySource::loadAll()
{
    ExternalQueryBuilder builder{dict_struct, db, table, "", IdentifierQuotingStyle::DoubleQuotes};
    String query = builder.composeLoadAllQuery();
    query.pop_back();
    query += " ALLOW FILTERING;";
    LOG_INFO(log, "Loading all using query: ", query);
    return std::make_shared<CassandraBlockInputStream>(cluster, query, sample_block, max_block_size);
}

std::string CassandraDictionarySource::toString() const {
    return "Cassandra: " + db + '.' + table;
}

BlockInputStreamPtr CassandraDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    ExternalQueryBuilder builder{dict_struct, db, table, "", IdentifierQuotingStyle::DoubleQuotes};
    String query = builder.composeLoadIdsQuery(ids);
    query.pop_back();
    query += " ALLOW FILTERING;";
    LOG_INFO(log, "Loading ids using query: ", query);
    return std::make_shared<CassandraBlockInputStream>(cluster, query, sample_block, max_block_size);
}

BlockInputStreamPtr CassandraDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    //FIXME split conditions on partition key and clustering key
    ExternalQueryBuilder builder{dict_struct, db, table, "", IdentifierQuotingStyle::DoubleQuotes};
    String query = builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES);
    query.pop_back();
    query += " ALLOW FILTERING;";
    LOG_INFO(log, "Loading keys using query: ", query);
    return std::make_shared<CassandraBlockInputStream>(cluster, query, sample_block, max_block_size);
}


}

#endif
