#pragma once

#include <Dictionaries/CassandraHelpers.h>
#include <QueryPipeline/BlockIO.h>

#if USE_CASSANDRA

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Interpreters/Context_fwd.h>
#include <mutex>

namespace DB
{

class CassandraDictionarySource final : public IDictionarySource
{
public:

    struct Configuration
    {
        String host;
        UInt16 port;
        String user;
        String password;
        String db;
        String table;
        String query;

        CassConsistency consistency;
        bool allow_filtering;
        /// TODO get information about key from the driver
        size_t partition_key_prefix;
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
    CassSessionShared getSession();

    LoggerPtr log;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    SharedHeader sample_block;
    ExternalQueryBuilder query_builder;

    CassClusterPtr cluster;

    std::mutex connect_mutex;
    CassSessionWeak maybe_session TSA_GUARDED_BY(connect_mutex);
};
}

#endif
