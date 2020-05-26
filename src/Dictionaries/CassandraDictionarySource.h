#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_CASSANDRA

#include "DictionaryStructure.h"
#include "IDictionarySource.h"
#include <Core/Block.h>
#include <Poco/Logger.h>
#include <cassandra.h>

namespace DB
{
class CassandraDictionarySource final : public IDictionarySource {
    CassandraDictionarySource(
        const DictionaryStructure & dict_struct,
        const String & host,
        UInt16 port,
        const String & user,
        const String & password,
        //const std::string & method,
        const String & db,
        const String & table,
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

    bool supportsSelectiveLoad() const override { return true; }

    bool isModified() const override { return true; }

    ///Not yet supported
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_unique<CassandraDictionarySource>(*this); }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;
    //{
    //    throw Exception{"Method loadIds is not implemented yet", ErrorCodes::NOT_IMPLEMENTED};
    //}

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;
    //{
    //    throw Exception{"Method loadKeys is not implemented yet", ErrorCodes::NOT_IMPLEMENTED};
    //}
        
    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for CassandraDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    std::string toString() const override;

private:
    //static std::string toConnectionString(const std::string & host, const UInt16 port);

    Poco::Logger * log;
    const DictionaryStructure dict_struct;
    const String host;
    const UInt16 port;
    const String user;
    const String password;
    //const std::string method;
    const String db;
    const String table;
    Block sample_block;

    CassCluster * cluster;
    CassSession * session;
};
}

#endif
