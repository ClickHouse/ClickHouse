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
        const mysqlxx::PoolWithFailover::Entry & entry,
        const std::string & query_str,
        const Block & sample_block,
        const UInt64 max_block_size_,
        const bool auto_close_ = false,
        const bool fetch_by_name_ = false);

    String getName() const override { return "MySQL"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

protected:
    MySQLBlockInputStream(const Block & sample_block_, UInt64 max_block_size_, bool auto_close_, bool fetch_by_name_);
    Block readImpl() override;
    void initPositionMappingFromQueryResultStructure();

    struct Connection
    {
        Connection(const mysqlxx::PoolWithFailover::Entry & entry_, const std::string & query_str);

        mysqlxx::PoolWithFailover::Entry entry;
        mysqlxx::Query query;
        mysqlxx::UseQueryResult result;
    };

    Poco::Logger * log;
    std::unique_ptr<Connection> connection;

    const UInt64 max_block_size;
    const bool auto_close;
    const bool fetch_by_name;
    std::vector<size_t> position_mapping;
    ExternalResultDescription description;
};

/// Like MySQLBlockInputStream, but allocates connection only when reading is starting.
/// It allows to create a lot of stream objects without occupation of all connection pool.
/// Also makes attempts to reconnect in case of connection failures.
class MySQLWithFailoverBlockInputStream final : public MySQLBlockInputStream
{
public:
    static constexpr inline auto MAX_TRIES_MYSQL_CONNECT = 5;

    MySQLWithFailoverBlockInputStream(
        mysqlxx::PoolWithFailoverPtr pool_,
        const std::string & query_str_,
        const Block & sample_block_,
        const UInt64 max_block_size_,
        const bool auto_close_ = false,
        const bool fetch_by_name_ = false,
        const size_t max_tries_ = MAX_TRIES_MYSQL_CONNECT);

private:
    void readPrefix() override;

    mysqlxx::PoolWithFailoverPtr pool;
    std::string query_str;
    size_t max_tries;
};

}
