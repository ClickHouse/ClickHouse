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

class PostgreSQLConnection;
using PostgreSQLConnectionPtr = std::shared_ptr<PostgreSQLConnection>;
using ConnectionPtr = std::shared_ptr<pqxx::connection>;

class StoragePostgreSQL final : public ext::shared_ptr_helper<StoragePostgreSQL>, public IStorage
{
    friend struct ext::shared_ptr_helper<StoragePostgreSQL>;
public:
    StoragePostgreSQL(
        const StorageID & table_id_,
        const std::string & remote_table_name_,
        PostgreSQLConnectionPtr connection_,
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
    PostgreSQLConnectionPtr connection;
};


/// Tiny connection class to make it more convenient to use.
/// Connection is not made until actually used.
class PostgreSQLConnection
{
public:
    PostgreSQLConnection(const std::string & connection_str_) : connection_str(connection_str_) {}
    PostgreSQLConnection(const PostgreSQLConnection &) = delete;
    PostgreSQLConnection operator =(const PostgreSQLConnection &) = delete;

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
