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
#include <Core/PostgreSQL/insertPostgreSQLValue.h>
#include <Core/PostgreSQL/PostgreSQLConnection.h>


namespace DB
{

template <typename T>
class PostgreSQLBlockInputStream : public IBlockInputStream
{

public:
    PostgreSQLBlockInputStream(
        postgres::ConnectionHolderPtr connection_,
        const std::string & query_str_,
        const Block & sample_block,
        const UInt64 max_block_size_);

    PostgreSQLBlockInputStream(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        const Block & sample_block,
        const UInt64 max_block_size_,
        bool auto_commit_);

    String getName() const override { return "PostgreSQL"; }
    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    void readPrefix() override;

protected:
    String query_str;
    std::shared_ptr<T> tx;
    std::unique_ptr<pqxx::stream_from> stream;

private:
    Block readImpl() override;
    void readSuffix() override;

    void init(const Block & sample_block);

    const UInt64 max_block_size;
    bool auto_commit = true;
    ExternalResultDescription description;

    postgres::ConnectionHolderPtr connection;

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
};


/// Passes transaction object into PostgreSQLBlockInputStream and does not close transaction after read if finished.
template <typename T>
class PostgreSQLTransactionBlockInputStream : public PostgreSQLBlockInputStream<T>
{

public:
    using Base = PostgreSQLBlockInputStream<pqxx::ReplicationTransaction>;

    PostgreSQLTransactionBlockInputStream(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        const Block & sample_block_,
        const UInt64 max_block_size_)
        : PostgreSQLBlockInputStream<pqxx::ReplicationTransaction>(tx_, query_str_, sample_block_, max_block_size_, false) {}

    void readPrefix() override
    {
        Base::stream = std::make_unique<pqxx::stream_from>(*Base::tx, pqxx::from_query, std::string_view(Base::query_str));
    }
};

}

#endif
