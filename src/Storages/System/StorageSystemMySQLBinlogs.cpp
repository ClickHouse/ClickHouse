#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/MySQL/MySQLBinlogClientFactory.h>
#include <Processors/ISource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/System/StorageSystemMySQLBinlogs.h>


namespace DB
{

NamesAndTypesList StorageSystemMySQLBinlogs::getNamesAndTypes()
{
    return {
        {"binlog_client_name", std::make_shared<DataTypeString>()},
        {"name", std::make_shared<DataTypeString>()},
        {"mysql_binlog_name", std::make_shared<DataTypeString>()},
        {"mysql_binlog_pos", std::make_shared<DataTypeUInt64>()},
        {"mysql_binlog_timestamp", std::make_shared<DataTypeUInt64>()},
        {"mysql_binlog_executed_gtid_set", std::make_shared<DataTypeString>()},
        {"dispatcher_name", std::make_shared<DataTypeString>()},
        {"dispatcher_mysql_binlog_name", std::make_shared<DataTypeString>()},
        {"dispatcher_mysql_binlog_pos", std::make_shared<DataTypeUInt64>()},
        {"dispatcher_mysql_binlog_timestamp", std::make_shared<DataTypeUInt64>()},
        {"dispatcher_mysql_binlog_executed_gtid_set", std::make_shared<DataTypeString>()},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"bytes", std::make_shared<DataTypeUInt64>()},
        {"max_bytes", std::make_shared<DataTypeUInt64>()},
        {"max_waiting_ms", std::make_shared<DataTypeUInt64>()},
        {"dispatcher_events_read_per_sec", std::make_shared<DataTypeFloat32>()},
        {"dispatcher_bytes_read_per_sec", std::make_shared<DataTypeFloat32>()},
        {"dispatcher_events_flush_per_sec", std::make_shared<DataTypeFloat32>()},
        {"dispatcher_bytes_flush_per_sec", std::make_shared<DataTypeFloat32>()},
    };
}

StorageSystemMySQLBinlogs::StorageSystemMySQLBinlogs(const StorageID & storage_id_)
    : IStorage(storage_id_)
{
    StorageInMemoryMetadata storage_metadata;
    ColumnsDescription columns(getNamesAndTypes());
    storage_metadata.setColumns(columns);
    setInMemoryMetadata(storage_metadata);
}

class MetadataSource : public ISource
{
public:
    using DispatcherMetadata = MySQLReplication::BinlogEventsDispatcher::DispatcherMetadata;
    using BinlogMetadata = MySQLReplication::BinlogEventsDispatcher::BinlogMetadata;

    MetadataSource(Block block_header_, const std::vector<MySQLReplication::BinlogClient::Metadata> & clients_)
        : ISource(block_header_)
        , block_to_fill(std::move(block_header_))
        , clients(clients_)
        {}

    String getName() const override { return "MySQLBinlogClient"; }

protected:
    Chunk generate() override
    {
        if (clients.empty())
            return {};

        Columns columns;
        columns.reserve(block_to_fill.columns());

        size_t total_size = 0;
        auto create_column = [&](auto && column, const std::function<Field(const String & n, const DispatcherMetadata & d, const BinlogMetadata & b)> & field)
        {
            size_t size = 0;
            for (const auto & client : clients)
            {
                for (const auto & d : client.dispatchers)
                {
                    for (const auto & b : d.binlogs)
                    {
                        column->insert(field(client.binlog_client_name, d, b));
                        ++size;
                    }
                }
            }
            if (!total_size)
                total_size = size;
            return std::forward<decltype(column)>(column);
        };

        for (const auto & elem : block_to_fill)
        {
            if (elem.name == "binlog_client_name")
                columns.emplace_back(create_column(ColumnString::create(), [](auto n, auto, auto) { return Field(n); }));
            else if (elem.name == "name")
                columns.emplace_back(create_column(ColumnString::create(), [](auto, auto, auto b) { return Field(b.name); }));
            else if (elem.name == "mysql_binlog_name")
                columns.emplace_back(create_column(ColumnString::create(), [](auto, auto, auto b) { return Field(b.position_read.binlog_name); }));
            else if (elem.name == "mysql_binlog_pos")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto, auto b) { return Field(b.position_read.binlog_pos); }));
            else if (elem.name == "mysql_binlog_timestamp")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto, auto b) { return Field(b.position_read.timestamp); }));
            else if (elem.name == "mysql_binlog_executed_gtid_set")
                columns.emplace_back(create_column(ColumnString::create(), [](auto, auto, auto b) { return Field(b.position_read.gtid_sets.toString()); }));
            else if (elem.name == "dispatcher_name")
                columns.emplace_back(create_column(ColumnString::create(), [](auto, auto d, auto) { return Field(d.name); }));
            else if (elem.name == "dispatcher_mysql_binlog_name")
                columns.emplace_back(create_column(ColumnString::create(), [](auto, auto d, auto) { return Field(d.position.binlog_name); }));
            else if (elem.name == "dispatcher_mysql_binlog_pos")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto d, auto) { return Field(d.position.binlog_pos); }));
            else if (elem.name == "dispatcher_mysql_binlog_timestamp")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto d, auto) { return Field(d.position.timestamp); }));
            else if (elem.name == "dispatcher_mysql_binlog_executed_gtid_set")
                columns.emplace_back(create_column(ColumnString::create(), [](auto, auto d, auto) { return Field(d.position.gtid_sets.toString()); }));
            else if (elem.name == "size")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto, auto b) { return Field(b.size); }));
            else if (elem.name == "bytes")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto, auto b) { return Field(b.bytes); }));
            else if (elem.name == "max_bytes")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto, auto b) { return Field(b.max_bytes); }));
            else if (elem.name == "max_waiting_ms")
                columns.emplace_back(create_column(ColumnUInt64::create(), [](auto, auto, auto b) { return Field(b.max_waiting_ms); }));
            else if (elem.name == "dispatcher_events_read_per_sec")
                columns.emplace_back(create_column(ColumnFloat32::create(), [](auto, auto d, auto) { return Field(d.events_read_per_sec); }));
            else if (elem.name == "dispatcher_bytes_read_per_sec")
                columns.emplace_back(create_column(ColumnFloat32::create(), [](auto, auto d, auto) { return Field(d.bytes_read_per_sec); }));
            else if (elem.name == "dispatcher_events_flush_per_sec")
                columns.emplace_back(create_column(ColumnFloat32::create(), [](auto, auto d, auto) { return Field(d.events_flush_per_sec); }));
            else if (elem.name == "dispatcher_bytes_flush_per_sec")
                columns.emplace_back(create_column(ColumnFloat32::create(), [](auto, auto d, auto) { return Field(d.bytes_flush_per_sec); }));
        }

        clients.clear();
        return {std::move(columns), total_size};
    }

private:
    Block block_to_fill;
    std::vector<MySQLReplication::BinlogClient::Metadata> clients;
};

Pipe StorageSystemMySQLBinlogs::read(
    const Names & column_names_,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /* query_info_ */,
    ContextPtr /*context_ */,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names_);
    const ColumnsDescription & our_columns = storage_snapshot->getDescriptionForColumns(column_names_);
    Block block_header;
    for (const auto & name : column_names_)
    {
        const auto & name_type = our_columns.get(name);
        MutableColumnPtr column = name_type.type->createColumn();
        block_header.insert({std::move(column), name_type.type, name_type.name});
    }

    return Pipe{std::make_shared<MetadataSource>(block_header, MySQLReplication::BinlogClientFactory::instance().getMetadata())};
}

}
