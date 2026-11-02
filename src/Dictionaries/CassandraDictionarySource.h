#pragma once

#include "config.h"

#if USE_CASSANDRA

#include <Dictionaries/CassandraClient.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <QueryPipeline/BlockIO.h>

namespace DB
{

class CassandraDictionarySource final : public IDictionarySource
{
public:
    static constexpr UInt16 DEFAULT_PORT = 9042;

    /// Splits a comma-separated list of hosts.
    static VectorWithMemoryTracking<String> splitHosts(const String & hosts);

    struct Configuration
    {
        /// A comma-separated list of hosts.
        String host;
        UInt16 port;
        String user;
        String password;
        bool secure;
        Cassandra::Compression compression;
        String db;
        String table;
        String query;

        Cassandra::Consistency consistency = Cassandra::Consistency::One;
        bool allow_filtering;
        size_t partition_key_prefix;
        /// The maximum number of idle connections to keep.
        size_t max_threads;
        String where;

        Configuration(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

        void setConsistency(const String & config_str);
    };

    CassandraDictionarySource(
        const DictionaryStructure & dict_struct,
        const Configuration & configuration,
        const Block & sample_block);

    CassandraDictionarySource(
            const DictionaryStructure & dict_struct,
            const Poco::Util::AbstractConfiguration & config,
            const String & config_prefix,
            Block & sample_block);

    BlockIO loadAll() override;

    bool supportsSelectiveLoad() const override { return true; }

    bool isModified() const override { return true; }

    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override
    {
        return std::make_shared<CassandraDictionarySource>(dict_struct, configuration, *sample_block);
    }

    BlockIO loadIds(const VectorWithMemoryTracking<UInt64> & ids) override;

    BlockIO loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows) override;

    BlockIO loadUpdatedAll() override;

    String toString() const override;

private:
    void maybeAllowFiltering(String & query) const;

    LoggerPtr log;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    SharedHeader sample_block;
    ExternalQueryBuilder query_builder;
    Cassandra::ConnectionPoolPtr pool;
};
}

#endif
