#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Core/Block.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Core/ExternalResultDescription.h>
#include <Core/Field.h>
#include <Core/PostgreSQL/insertPostgreSQLValue.h>
#include <Core/PostgreSQL/ConnectionHolder.h>
#include <Core/PostgreSQL/Utils.h>


namespace DB
{

template <typename T = pqxx::ReadTransaction>
class PostgreSQLSource : public SourceWithProgress
{

public:
    PostgreSQLSource(
        postgres::ConnectionHolderPtr connection_holder_,
        const String & query_str_,
        const Block & sample_block,
        const UInt64 max_block_size_);

    String getName() const override { return "PostgreSQL"; }

protected:
    PostgreSQLSource(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        const Block & sample_block,
        const UInt64 max_block_size_,
        bool auto_commit_);

    String query_str;
    std::shared_ptr<T> tx;
    std::unique_ptr<pqxx::stream_from> stream;

    Status prepare() override;

private:
    void onStart();
    Chunk generate() override;
    void onFinish();

    void init(const Block & sample_block);

    const UInt64 max_block_size;
    bool auto_commit = true;
    ExternalResultDescription description;

    bool started = false;

    postgres::ConnectionHolderPtr connection_holder;

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
};


/// Passes transaction object into PostgreSQLBlockInputStream and does not close transaction after read is finished.
template <typename T>
class PostgreSQLTransactionBlockInputStream : public PostgreSQLBlockInputStream<T>
{
public:
    using Base = PostgreSQLBlockInputStream<T>;

    PostgreSQLTransactionBlockInputStream(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        const Block & sample_block_,
        const UInt64 max_block_size_)
        : PostgreSQLBlockInputStream<T>(tx_, query_str_, sample_block_, max_block_size_, false) {}

    void readPrefix() override
    {
        Base::stream = std::make_unique<pqxx::stream_from>(*Base::tx, pqxx::from_query, std::string_view(Base::query_str));
    }
};

}

#endif