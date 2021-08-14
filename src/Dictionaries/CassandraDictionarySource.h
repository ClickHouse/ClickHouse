#pragma once

#include <Dictionaries/CassandraHelpers.h>

#if USE_CASSANDRA

#include "DictionaryStructure.h"
#include "IDictionarySource.h"
#include "ExternalQueryBuilder.h"
#include <Core/Block.h>
#include <Poco/Logger.h>
#include <mutex>

namespace DB
{

struct CassandraSettings
{
    String host;
    UInt16 port;
    String user;
    String password;
    String db;
    String table;

    CassConsistency consistency;
    bool allow_filtering;
    /// TODO get information about key from the driver
    size_t partition_key_prefix;
    size_t max_threads;
    String where;

    CassandraSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

    void setConsistency(const String & config_str);
};

class CassandraDictionarySource final : public IDictionarySource
{
public:
    CassandraDictionarySource(
        const DictionaryStructure & dict_struct,
        const CassandraSettings & settings_,
        const Block & sample_block);

    CassandraDictionarySource(
            const DictionaryStructure & dict_struct,
            const Poco::Util::AbstractConfiguration & config,
            const String & config_prefix,
            Block & sample_block);

    BlockInputStreamPtr loadAll() override;

    bool supportsSelectiveLoad() const override { return true; }

    bool isModified() const override { return true; }

    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override
    {
        return std::make_unique<CassandraDictionarySource>(dict_struct, settings, sample_block);
    }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    BlockInputStreamPtr loadUpdatedAll() override;

    String toString() const override;

private:
    void maybeAllowFiltering(String & query) const;
    CassSessionShared getSession();

    Poco::Logger * log;
    const DictionaryStructure dict_struct;
    const CassandraSettings settings;
    Block sample_block;
    ExternalQueryBuilder query_builder;

    std::mutex connect_mutex;
    CassClusterPtr cluster;
    CassSessionWeak maybe_session;
};
}

#endif
