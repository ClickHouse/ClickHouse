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
#include <Storages/PostgreSQL/PostgreSQLConnection.h>


namespace DB
{

template<typename T>
class PostgreSQLBlockInputStream : public IBlockInputStream
{
public:
    PostgreSQLBlockInputStream(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        const Block & sample_block,
        const UInt64 max_block_size_,
        bool auto_commit_ = true);

    String getName() const override { return "PostgreSQL"; }
    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    void readPrefix() override;
    Block readImpl() override;
    void readSuffix() override;

    String query_str;
    const UInt64 max_block_size;
    const bool auto_commit;
    ExternalResultDescription description;

    PostgreSQLConnection::ConnectionPtr connection;
    std::shared_ptr<T> tx;
    std::unique_ptr<pqxx::stream_from> stream;

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
};

}

#endif
