#pragma once

#include <Common/config.h>

#if USE_CASSANDRA

#    include "DictionaryStructure.h"
#    include "IDictionarySource.h"
#    include <cassandra.h>

namespace DB
{
class CassandraDictionarySource final : public IDictionarySource {
    CassandraDictionarySource(
        const DictionaryStructure & dict_struct,
        const std::string & host,
        UInt16 port,
        const std::string & user,
        const std::string & password,
        const std::string & method,
        const std::string & db,
        const Block & sample_block);

public:
    CassandraDictionarySource(
            const DictionaryStructure & dict_struct,
            const Poco::Util::AbstractConfiguration & config,
            const std::string & config_prefix,
            Block & sample_block);

    CassandraDictionarySource(const CassandraDictionarySource & other);

    ~CassandraDictionarySource() override;

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

private:
    static std::string toConnectionString(const std::string& host, const UInt16 port);

    const DictionaryStructure dict_struct;
    const std::string host;
    const UInt16 port;
    const std::string user;
    const std::string password;
    const std::string method;
    const std::string db;
    Block sample_block;

    CassCluster * cluster;
    CassSession * session;
};
}

#endif
