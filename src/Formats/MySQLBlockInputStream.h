#pragma once

#include <string>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <mysqlxx/PoolWithFailover.h>
#include <mysqlxx/Query.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{

/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLBlockInputStream : public IBlockInputStream
{
public:
    MySQLBlockInputStream(
        mysqlxx::PoolWithFailover::Entry entry,
        const std::string & query_str,
        const Block & sample_block,
        UInt64 max_block_size_,
        bool fetch_by_name_ = false);

    String getName() const override { return "MySQL"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

protected:
    MySQLBlockInputStream(const Block & sample_block_, UInt64 max_block_size_, bool fetch_by_name_);

    Block readImpl() override;
    void initPositionMappingFromQueryResultStructure();

    struct Connection
    {
        Connection(mysqlxx::PoolWithFailover::Entry entry_, const std::string & query_str);

        mysqlxx::PoolWithFailover::Entry entry;
        mysqlxx::Query query;
        mysqlxx::UseQueryResult result;
    };

    Poco::Logger * log;
    std::unique_ptr<Connection> connection;

    const UInt64 max_block_size;
    const bool fetch_by_name;
    std::vector<size_t> position_mapping;
    ExternalResultDescription description;
};

/// Like MySQLBlockInputStream, but allocates connection only when reading is starting.
/// It allows to create a lot of stream objects without occupation of all connection pool.
/// Also makes attempts to reconnect in case of connection failures.
class MySQLLazyBlockInputStream final : public MySQLBlockInputStream
{
public:

    MySQLLazyBlockInputStream(
        mysqlxx::PoolPtr pool_,
        const std::string & query_str_,
        const Block & sample_block_,
        UInt64 max_block_size_,
        bool fetch_by_name_ = false);

private:
    void readPrefix() override;

    mysqlxx::PoolPtr pool;
    std::string query_str;
};

}
