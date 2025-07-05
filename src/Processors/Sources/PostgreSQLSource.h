#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Processors/ISource.h>
#include <Processors/Port.h>
#include <Core/ExternalResultDescription.h>
#include <Core/PostgreSQL/insertPostgreSQLValue.h>
#include <Core/PostgreSQL/ConnectionHolder.h>
#include <Core/PostgreSQL/Utils.h>


namespace DB
{

template <typename T = pqxx::ReadTransaction>
class PostgreSQLSource : public ISource
{

public:
    PostgreSQLSource(
        postgres::ConnectionHolderPtr connection_holder_,
        const String & query_str_,
        const Block & sample_block,
        UInt64 max_block_size_);

    String getName() const override { return "PostgreSQL"; }

    ~PostgreSQLSource() override;

protected:
    PostgreSQLSource(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        const Block & sample_block,
        UInt64 max_block_size_,
        bool auto_commit_);

    Status prepare() override;

    Chunk generate() override;

    void onStart();

private:
    void init(const Block & sample_block);

    const UInt64 max_block_size;
    bool auto_commit = true;
    ExternalResultDescription description;

    bool started = false;
    bool is_completed = false;

    postgres::ConnectionHolderPtr connection_holder;

    std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;

protected:
    String query_str;
    /// tx and stream must be destroyed before connection_holder.
    std::shared_ptr<T> tx;
    std::unique_ptr<pqxx::stream_from> stream;
};


/// Passes transaction object into PostgreSQLSource and does not close transaction after read is finished.
template <typename T>
class PostgreSQLTransactionSource : public PostgreSQLSource<T>
{
public:
    using Base = PostgreSQLSource<T>;

    PostgreSQLTransactionSource(
        std::shared_ptr<T> tx_,
        const std::string & query_str_,
        const Block & sample_block_,
        const UInt64 max_block_size_)
        : PostgreSQLSource<T>(tx_, query_str_, sample_block_, max_block_size_, false) {}
};

}

#endif
