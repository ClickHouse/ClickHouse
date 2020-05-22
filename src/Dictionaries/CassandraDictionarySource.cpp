#include "CassandraDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"

namespace DB
{
    namespace ErrorCodes
    {
        extern const int SUPPORT_IS_DISABLED;
    }

    void registerDictionarySourceCassandra(DictionarySourceFactory & factory)
    {
        auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                     const Poco::Util::AbstractConfiguration & config,
                                     const std::string & config_prefix,
                                     Block & sample_block,
                                     const Context & /* context */,
                                     bool /*check_config*/) -> DictionarySourcePtr {
#if USE_CASSANDRA
        return std::make_unique<CassandraDictionarySource>(dict_struct, config, config_prefix + ".cassandra", sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        throw Exception{"Dictionary source of type `cassandra` is disabled because library was built without cassandra support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        };
        factory.registerSource("cassandra", create_table_source);
    }

}

#if USE_CASSANDRA

#    include <cassandra.h>
#    include <IO/WriteHelpers.h>
#    include "CassandraBlockInputStream.h"

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
    const std::string & host_,
    UInt16 port_,
    const std::string & user_,
    const std::string & password_,
    const std::string & method_,
    const std::string & db_,
    const DB::Block & sample_block_)
    : dict_struct(dict_struct_)
    , host(host_)
    , port(port_)
    , user(user_)
    , password(password_)
    , method(method_)
    , db(db_)
    , sample_block(sample_block_)
    , cluster(cass_cluster_new())
    , session(cass_session_new())
{
    cass_cluster_set_contact_points(cluster, toConnectionString(host, port).c_str());
}

CassandraDictionarySource::CassandraDictionarySource(
    const DB::DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    DB::Block & sample_block_)
    : CassandraDictionarySource(
        dict_struct_,
        config.getString(config_prefix + ".host"),
        config.getUInt(config_prefix + ".port"),
        config.getString(config_prefix + ".user", ""),
        config.getString(config_prefix + ".password", ""),
        config.getString(config_prefix + ".method", ""),
        config.getString(config_prefix + ".db", ""),
        sample_block_)
{
}

CassandraDictionarySource::CassandraDictionarySource(const CassandraDictionarySource & other)
    : CassandraDictionarySource{other.dict_struct,
                                other.host,
                                other.port,
                                other.user,
                                other.password,
                                other.method,
                                other.db,
                                other.sample_block}
{
}

CassandraDictionarySource::~CassandraDictionarySource() {
    cass_session_free(session);
    cass_cluster_free(cluster);
}

std::string CassandraDictionarySource::toConnectionString(const std::string &host, const UInt16 port) {
    return host + (port != 0 ? ":" + std::to_string(port) : "");
}

BlockInputStreamPtr CassandraDictionarySource::loadAll() {
    return std::make_shared<CassandraBlockInputStream>(nullptr, "", sample_block, max_block_size);
}

std::string CassandraDictionarySource::toString() const {
    return "Cassandra: " + /*db + '.' + collection + ',' + (user.empty() ? " " : " " + user + '@') + */ host + ':' + DB::toString(port);
}


}

#endif
