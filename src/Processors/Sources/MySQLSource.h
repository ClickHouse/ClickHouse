#pragma once

#include <string>
#include <Core/Block.h>
#include <Processors/ISource.h>
#include <mysqlxx/PoolWithFailover.h>
#include <mysqlxx/Query.h>
#include <Core/ExternalResultDescription.h>
#include <Core/Settings.h>

namespace DB
{

struct StreamSettings
{
    /// Check if setting is enabled, otherwise use common `max_block_size` setting.
    size_t max_read_mysql_row_nums;
    size_t max_read_mysql_bytes_size;
    bool auto_close;
    bool fetch_by_name;
    size_t default_num_tries_on_connection_loss;

    StreamSettings(const Settings & settings, bool auto_close_ = false, bool fetch_by_name_ = false, size_t max_retry_ = 5);

};

/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLSource : public ISource
{
public:
    MySQLSource(
        const mysqlxx::PoolWithFailover::Entry & entry,
        const std::string & query_str,
        const Block & sample_block,
        const StreamSettings & settings_);

    String getName() const override { return "MySQL"; }

protected:
    MySQLSource(const Block & sample_block_, const StreamSettings & settings);
    Chunk generate() override;
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

    const std::unique_ptr<StreamSettings> settings;
    std::vector<size_t> position_mapping;
    ExternalResultDescription description;
};

/// Like MySQLSource, but allocates connection only when reading is starting.
/// It allows to create a lot of stream objects without occupation of all connection pool.
/// Also makes attempts to reconnect in case of connection failures.
class MySQLWithFailoverSource final : public MySQLSource
{
public:

    MySQLWithFailoverSource(
        mysqlxx::PoolWithFailoverPtr pool_,
        const std::string & query_str_,
        const Block & sample_block_,
        const StreamSettings & settings_);

    Chunk generate() override;

private:
    void onStart();

    mysqlxx::PoolWithFailoverPtr pool;
    std::string query_str;
    bool is_initialized = false;
};

}
