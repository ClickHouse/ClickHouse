#include <Storages/MySQLReplication/StorageMySQLReplica.h>

#include <Parsers/ASTInsertQuery.h>

#include <Core/MySQLReplication.h>

#include <string>
#include <vector>

using namespace MySQLReplica;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace MySQLReplica
{
// https://github.com/mysql/mysql-server/blob/ea7d2e2d16ac03afdd9cb72a972a95981107bf51/sql/rpl_binlog_sender.cc#L825
// https://github.com/mysql/mysql-server/blob/7d10c82196c8e45554f27c00681474a9fb86d137/libbinlogevents/include/binlog_event.h#L83
const UInt32 BIN_LOG_HEADER_SIZE = 4U;

struct MySQLClickHouseEvent {
    MySQLEventType type;
    std::vector<Field> select_rows;
    std::vector<Field> insert_rows;
}

Pipes read(
    const Names & column_names,
    const SelectQueryInfo & query_info, 
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    return storage.read(
        column_names,
        query_info,
        context,
        processed_stage,
        max_block_size,
        num_streams);

void StorageMySQLReplica::ReadBinlogStream() {
    slave_client.connect();

    if (gtid_sets.empty()) {
        slave_client.startBinlogDump(slave_id, replicate_db, "", BIN_LOG_HEADER_SIZE);
    } else {
        slave_client.startBinlogDumpGTID(slave_id, replicate_db, gtid_sets);
    }

    while (true) { // TODO: replcate to deactivateAndSchedule
        auto event = slave_client.readOneBinlogEvent();
        if (event->table != replicate_table) {
           continue;
        }

        MySQLClickHouseEvent ch_event;
        ch_event.type = event->type();
        switch (event->type()) {
            case MYSQL_WRITE_ROWS_EVENT:
                auto write_event = std::static_pointer_cast<WriteRowsEvent>(event);

                ch_event.insert_rows.reserve(write_event->rows.size());
                for (auto row : write_event->rows) {
                    ch_event.insert_rows.push_back(row);
                }
                break;
            case MYSQL_UPDATE_ROWS_EVENT:
                auto update_event = std::static_pointer_cast<UpdateRowsEvent>(event);

                ch_event.select_rows.reserve(update_event->rows.size() / 2);
                ch_event.insert_rows.reserve(update_event->rows.size() / 2);
                int is_select_row = 1;
                for (auto row : update_event->rows) {
                    if (is_select_row) {
                        ch_event.select_rows.push_back(row);
                    } else {
                        ch_event.insert_rows.push_back(row);
                    }
                    is_select_row ^= 1;
                }
                break;
            case MYSQL_DELETE_ROWS_EVENT:
                auto delete_event = std::static_pointer_cast<DeleteRowsEvent>(event);

                ch_event.select_rows.reserve(delete_event->rows.size());
                for (auto row : delete_event->rows) {
                    ch_event.select_rows.push_back(row);
                }
                break;
            default:
                break;
        }

        BinlogEvents.push_back(ch_event);
    }
}

Block StorageMySQLReplica::RowsToBlock(StorageMetadataPtr & metadata, const std::vector<Field> & rows) {
    ColumnsWithTypeAndName columns(metadata->getColumns().size());
    size_t idx = 0;
    for (auto& column : metadata->getColumns()) {
        columns[i].name = column.name;
        columns[i].type = column.type;
    }
    // TODO: use table_map
    for (const auto& row : rows) {
        size_t col_idx = 0;
        for (auto& column : columns) {
            column.column->insert(row[col_idx++]);
        }
    }

    return Block(columns);
}

void StorageMySQLReplica::DoReplication() {
    for (size_t i = UnprocessedBinlogEventIdx; i < BinlogEvents.size(); ++i) {
        MySQLClickHouseEvent & event = BinlogEvents[i];
        switch (event->type) {
            case MYSQL_WRITE_ROWS_EVENT:
                auto insert = std::make_shared<ASTInsertQuery>();
                auto metadata = storage.getInMemoryMetadataPtr();
                auto block_ostream = storage.write(insert, metadata, context_);
                block_ostream->writePrefix();
                block_ostream->write(RowsToBlock(metadata, event-select_rows));
                block_ostream->writeSuffix();
                break;
            default:
                break;
        }
    }
}

StorageMySQLReplica::StorageMySQLReplica(
    const StorageID & table_id_,
    Context & context_,
    ColumnsDescription columns_description_,
    ConstraintsDescription constraints_,
    /* slave data */
    std::string & host,
    std::string & port,
    std::string & master_user,
    std::string & master_password,
    UInt32 slave_id, // non-zero unique value
    std::string & replicate_db,
    std::string & replicate_table,
    std::string & binlog_file_name,
    UInt64 binlog_pos,
    std::string & gtid_sets)
    : IStorage(table_id)
    , global_context(context_.getGlobalContext())
    , storage(table_id, columns_description_, constraints_)
    , slave_client(host, port, master_user, master_password)
{
    // Binlog stream listener task creation
    binlog_reader_task = global_context.getSchedulePool().createTask("BinlogReader", [this] { ReadBinlogStream(); });
    binlog_reader_task->deactivate();
}

void StorageMySQLReplica::startup() {
    binlog_reader_task->activateAndSchedule();
}

void StorageMySQLReplica::shutdown() {
    // TODO: may be save the binlog position?
    binlog_reader_task->deactivate();
}

void registerStorageMySQLReplica(StorageFactory & factory)
{
    factory.registerStorage("MySQLReplica", [](const StorageFactory::Arguments & args)
    {
        //TODO: copy some logic from StorageMySQL
        ASTs & engine_args = args.engine_args;
        std::string master_host = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        std::string master_port = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        std::string master_user = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        std::string master_password = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        UInt32 slave_id = engine_args[4]->as<ASTLiteral &>().value.safeGet<UInt32>();
        std::string replicate_db = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
        std::string replicate_table = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();
        std::string binlog_file_name = engine_args[7]->as<ASTLiteral &>().value.safeGet<String>();
        UInt64 binlog_pos = engine_args[8]->as<ASTLiteral &>().value.safeGet<UInt32>();
        std::string gtid_sets = engine_args[9]->as<ASTLiteral &>().value.safeGet<String>();
        return StorageMySQLReplica::create(
            args.table_id,
            args.context,
            args.columns,
            args.constraints
            master_host,
            master_port,
            master_user,
            master_password,
            slave_id,
            replicate_db,
            replicate_table,
            binlog_file_name,
            binlog_pos,
            gtid_sets);
    });
}

}
