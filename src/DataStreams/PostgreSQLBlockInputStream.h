#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>
#include <Core/Field.h>
#include <pqxx/pqxx>
#include <Storages/PostgreSQL/insertPostgreSQLValue.h>


namespace DB
{

class PostgreSQLBlockInputStream : public IBlockInputStream
{
public:
    PostgreSQLBlockInputStream(
        std::unique_ptr<pqxx::work> tx_,
        const std::string & query_str,
        const Block & sample_block,
        const UInt64 max_block_size_);

    String getName() const override { return "PostgreSQL"; }
    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    void readPrefix() override;
    Block readImpl() override;
    void readSuffix() override;

    String query_str;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    std::unique_ptr<pqxx::work> tx;
    std::unique_ptr<pqxx::stream_from> stream;

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
};

}

#endif
