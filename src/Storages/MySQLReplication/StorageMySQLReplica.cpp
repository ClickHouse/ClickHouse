#include <Storages/MySQLReplication/StorageMySQLReplica.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/StorageFactory.h>

#include <Processors/Pipe.h>

#include <Core/Field.h>

#include <Common/parseAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

using namespace MySQLReplication;

class MemorySource : public SourceWithProgress
{
public:
    MemorySource(
        Names column_names_,
        BlocksList::iterator begin_,
        BlocksList::iterator end_,
        const StorageMySQLReplica & storage,
        const StorageMetadataPtr & metadata_snapshot)
        : SourceWithProgress(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(std::move(column_names_))
        , begin(begin_)
        , end(end_)
        , it(begin)
    {
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (it == end)
        {
            return {};
        }
        else
        {
            Block src = *it;
            Columns columns;
            columns.reserve(column_names.size());

            /// Add only required columns to `res`.
            for (const auto & name : column_names)
                columns.emplace_back(src.getByName(name).column);

            ++it;
            return Chunk(std::move(columns), src.rows());
        }
    }
private:
    Names column_names;
    BlocksList::iterator begin;
    BlocksList::iterator end;
    BlocksList::iterator it;
};

class OneMySQLEventSource : public ISource
{
public:
    using ISource::ISource;

    OneMySQLEventSource(
        Names column_names_,
        Block & event_block_,
        const StorageMySQLReplica & storage,
        const StorageMetadataPtr & metadata_snapshot)
        : ISource(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(std::move(column_names_))
        , event_block(event_block_)
    {
    }

    String getName() const override { return "OneMySQLEvent"; }

protected:
    Chunk generate() override
    {
        if (done) {
            return {};
        }

        Columns columns;
        columns.reserve(column_names.size());
        for (const auto & name : column_names) {
            columns.emplace_back(event_block.getByName(name).column);
        }

        done = true;
        return Chunk(std::move(columns), event_block.rows());
    }

private:
    Names column_names;
    Block event_block;

    bool done = false;
};

Block StorageMySQLReplica::rowsToBlock(const StorageMetadataPtr & metadata, const std::vector<std::vector<Field>> & rows) {
    LOG_INFO(log, "Transfering MysQL event to Block");
    Block header(metadata->getSampleBlockNonMaterialized());
    MutableColumns result_columns = header.cloneEmptyColumns();
    for (const auto& row : rows) {
        size_t col_idx = 0;
        for (auto& column : result_columns) {
            LOG_INFO(log, "Column: {}", column->getName());
            LOG_INFO(log, "Column: {}", col_idx);
            column->insert(row[col_idx++]);
        }
    }

    return header.cloneWithColumns(std::move(result_columns));
}

Pipes StorageMySQLReplica::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    LOG_INFO(log, "Reading");

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        BlocksList::iterator begin = data.begin();
        BlocksList::iterator end = data.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        pipes.emplace_back(std::make_shared<MemorySource>(column_names, begin, end, *this, metadata_snapshot));
    }

    return pipes;
}


void StorageMySQLReplica::threadFunc() {
    auto metadata_snapshot = getInMemoryMetadataPtr();
    LOG_DEBUG(log, "Starting thread func");
    for (int i = 0; i < 1; ++i) {
        data.push_back(rowsToBlock(metadata_snapshot, readOneBinlogEvent().insert_rows));
        LOG_INFO(log, "Read one event");
    }
    task->scheduleAfter(500);
}

void StorageMySQLReplica::initBinlogStream() {
    if (gtid_sets.empty()) {
        slave_client.startBinlogDump(slave_id, replicate_db, binlog_filename, binlog_pos);
    } else {
        slave_client.startBinlogDumpGTID(slave_id, replicate_db, gtid_sets);
    }
}

void StorageMySQLReplica::startup() {
    LOG_DEBUG(log, "Connecting as slave client");
    slave_client.connect();
    LOG_DEBUG(log, "Connected");
    initBinlogStream();
    task->activateAndSchedule();
    LOG_DEBUG(log, "Activating task");
}

void StorageMySQLReplica::shutdown()
{
    task->deactivate();
    slave_client.disconnect();
}

MySQLClickHouseEvent StorageMySQLReplica::readOneBinlogEvent() {
    MySQLClickHouseEvent ch_event;
    LOG_INFO(log, "Reading one event");
    auto event = slave_client.readOneBinlogEvent();
    LOG_INFO(log, "Got one mysql event of type {}", event->type());
    ch_event.type = event->type();

    if (event->type() != MYSQL_WRITE_ROWS_EVENT &&
        event->type() != MYSQL_DELETE_ROWS_EVENT &&
        event->type() != MYSQL_UPDATE_ROWS_EVENT)
    {
        return ch_event;
    }

    if (std::static_pointer_cast<RowsEvent>(event)->table != replicate_table) {
       return ch_event;
    }

    switch (event->type()) {
        case MYSQL_WRITE_ROWS_EVENT: {
            auto write_event = std::static_pointer_cast<WriteRowsEvent>(event);

            ch_event.insert_rows.reserve(write_event->rows.size());
            for (auto row : write_event->rows) {
                ch_event.insert_rows.push_back(row);
            }
            break;
        }
        case MYSQL_UPDATE_ROWS_EVENT: {
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
        }
        case MYSQL_DELETE_ROWS_EVENT: {
            auto delete_event = std::static_pointer_cast<DeleteRowsEvent>(event);

            ch_event.select_rows.reserve(delete_event->rows.size());
            for (auto row : delete_event->rows) {
                ch_event.select_rows.push_back(row);
            }
            break;
        }
        default:
            break;
    }

    return ch_event;
}

StorageMySQLReplica::StorageMySQLReplica(
    const StorageID & table_id_,
    Context & context_,
    ColumnsDescription columns_description_,
    ConstraintsDescription constraints_,
    /* slave data */
    std::string & host,
    Int32 & port,
    std::string & master_user,
    std::string & master_password,
    std::string & replicate_db_,
    std::string & replicate_table_,
    std::string & binlog_filename_,
    std::string & gtid_sets_,
    UInt64 binlog_pos_,
    UInt32 slave_id_)
    : IStorage(table_id_)
    , global_context(context_.getGlobalContext())
    , slave_client(host, port, master_user, master_password)
    , slave_id(slave_id_)
    , replicate_db(replicate_db_)
    , replicate_table(replicate_table_)
    , binlog_filename(binlog_filename_)
    , binlog_pos(binlog_pos_)
    , gtid_sets(gtid_sets_)
    , log(&Poco::Logger::get("MySQLReplica (" + table_id_.table_name + ")"))
{
    LOG_INFO(log, "Host: {}, Port: {}, User: {}, Password: {}, Slave ID: {}, DB: {}, File: {}, Position: {}, GTID sets: {}", host, port, master_user, master_password, slave_id, replicate_db, binlog_filename, binlog_pos, gtid_sets);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_description_));
    storage_metadata.setConstraints(std::move(constraints_));
    setInMemoryMetadata(storage_metadata);

    task = global_context.getSchedulePool().createTask(log->name(), [this]{ threadFunc(); });
    task->deactivate();
}

void registerStorageMySQLReplica(StorageFactory & factory)
{
    factory.registerStorage("MySQLReplica", [](const StorageFactory::Arguments & args)
    {
        //TODO: copy some logic from StorageMySQL
        ASTs & engine_args = args.engine_args;
        if (engine_args.size() < 5 || engine_args.size() > 9) {
            throw Exception("StorageMySQLReplica requires 5-9 parameters"
                "MySQLReplica("
                    "'hostname:port', "
                    "'user', 'password', "
                    "'replicate_db', 'replicate_table'[, "
                    "'binlog_filename', 'gtid_sets', "
                    "binlog_pos, slave_id])",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        std::string master_host_and_port = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        const auto & [master_host, master_port] = parseAddress(master_host_and_port, DEFAULT_MYSQL_PORT);

        /*Int32 master_port = DEFAULT_MYSQL_PORT;
        {
            const auto * ast = engine_args[1]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64) {
                master_port = safeGet<UInt64>(ast->value);
            }
        }*/

        std::string master_user = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        std::string master_password = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        std::string replicate_db = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();
        std::string replicate_table = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();

        std::string binlog_filename = "";
        if (engine_args.size() > 5) {
            binlog_filename = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
        }

        std::string gtid_sets = "";
        if (engine_args.size() > 6) {
            gtid_sets = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();
        }

        UInt64 binlog_pos = BIN_LOG_HEADER_SIZE;
        if (engine_args.size() > 7) {
            const auto * ast = engine_args[7]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64) {
                binlog_pos = safeGet<UInt64>(ast->value);
            }
        }

        UInt32 slave_id = DEFAULT_SLAVE_ID;
        if (engine_args.size() > 8) {
            const auto * ast = engine_args[8]->as<ASTLiteral>();
            if (ast && ast->value.getType() == Field::Types::UInt64) {
                slave_id = safeGet<UInt64>(ast->value);
            }
        }

        return StorageMySQLReplica::create(
            args.table_id,
            args.context,
            args.columns,
            args.constraints,
            master_host,
            master_port,
            master_user,
            master_password,
            replicate_db,
            replicate_table,
            binlog_filename,
            gtid_sets,
            binlog_pos,
            slave_id);
    });
}

}
