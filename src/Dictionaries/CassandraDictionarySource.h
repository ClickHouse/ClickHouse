#pragma once

#include <Dictionaries/CassandraHelpers.h>

#if USE_CASSANDRA

#include "DictionaryStructure.h"
#include "IDictionarySource.h"
#include <Core/Block.h>
#include <Poco/Logger.h>

namespace DB
{

class CassandraDictionarySource final : public IDictionarySource {
    CassandraDictionarySource(
        const DictionaryStructure & dict_struct,
        const String & host,
        UInt16 port,
        const String & user,
        const String & password,
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

    BlockInputStreamPtr loadAll() override;

    bool supportsSelectiveLoad() const override { return true; }

    bool isModified() const override { return true; }

    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_unique<CassandraDictionarySource>(*this); }

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for CassandraDictionarySource", ErrorCodes::NOT_IMPLEMENTED};
    }

    std::string toString() const override;

private:
    Poco::Logger * log;
    const DictionaryStructure dict_struct;
    const String host;
    const UInt16 port;
    const String user;
    const String password;
    const String db;
    const String table;
    Block sample_block;

    CassClusterPtr cluster;
};
}

#endif
