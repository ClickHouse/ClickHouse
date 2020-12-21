#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <ext/shared_ptr_helper.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>
#include "pqxx/pqxx"

namespace DB
{

class PGConnection;
using PGConnectionPtr = std::shared_ptr<PGConnection>;
using ConnectionPtr = std::shared_ptr<pqxx::connection>;

class StoragePostgreSQL final : public ext::shared_ptr_helper<StoragePostgreSQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StoragePostgreSQL>;
public:
    StoragePostgreSQL(
        const StorageID & table_id_,
        const std::string & remote_table_name_,
        PGConnectionPtr connection_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const Context & context_);

    String getName() const override { return "PostgreSQL"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

private:
    friend class PostgreSQLBlockOutputStream;

    String remote_table_name;
    Context global_context;
    PGConnectionPtr connection;
};


class PostgreSQLBlockOutputStream : public IBlockOutputStream
{
public:
    explicit PostgreSQLBlockOutputStream(
        const StorageMetadataPtr & metadata_snapshot_,
        ConnectionPtr connection_,
        const std::string & remote_table_name_)
        : metadata_snapshot(metadata_snapshot_)
        , connection(connection_)
        , remote_table_name(remote_table_name_)
    {
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageMetadataPtr metadata_snapshot;
    ConnectionPtr connection;
    std::string remote_table_name;

    std::unique_ptr<pqxx::work> work;
    std::unique_ptr<pqxx::stream_to> stream_inserter;
};


/// Tiny connection class to make it more convenient to use.
class PGConnection
{
public:
    PGConnection(std::string & connection_str_) : connection_str(connection_str_) {}
    PGConnection(const PGConnection &) = delete;
    PGConnection operator =(const PGConnection &) = delete;

    ConnectionPtr conn()
    {
        checkUpdateConnection();
        return connection;
    }

    std::string & conn_str() { return connection_str; }

private:
    ConnectionPtr connection;
    std::string connection_str;

    void checkUpdateConnection()
    {
        if (!connection || !connection->is_open())
            connection = std::make_unique<pqxx::connection>(connection_str);
    }
};

}

#endif
