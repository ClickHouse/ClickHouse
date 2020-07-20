#include <Storages/MySQLReplication/StorageMySQLReplica.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/StorageFactory.h>

#include <Processors/Pipe.h>

#include <Core/Field.h>

namespace DB
{

using namespace MySQLReplication;

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

Block RowsToBlock(const StorageMetadataPtr & metadata, const std::vector<std::vector<Field>> & rows) {
    Block header(metadata->getSampleBlockNonMaterialized());
    MutableColumns result_columns = header.cloneEmptyColumns();
    for (const auto& row : rows) {
        size_t col_idx = 0;
        for (auto& column : result_columns) {
            column->insert(row[col_idx++]);
        }
    }

    return header.cloneWithColumns(std::move(result_columns));
}

Pipes StorageMySQLReplica::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    Block out_block = RowsToBlock(metadata_snapshot, ReadOneBinlogEvent().insert_rows);
    Pipes pipes;
    pipes.emplace_back(std::make_shared<OneMySQLEventSource>(column_names, out_block, *this, metadata_snapshot));
    return pipes;
}

void StorageMySQLReplica::InitBinlogStream() {
    slave_client.connect();

    if (gtid_sets.empty()) {
        slave_client.startBinlogDump(slave_id, replicate_db, "", BIN_LOG_HEADER_SIZE);
    } else {
        slave_client.startBinlogDumpGTID(slave_id, replicate_db, gtid_sets);
    }
}

void StorageMySQLReplica::startup() {
    InitBinlogStream();
}

MySQLClickHouseEvent StorageMySQLReplica::ReadOneBinlogEvent() {
    MySQLClickHouseEvent ch_event;
    auto event = slave_client.readOneBinlogEvent();
    ch_event.type = event->type();
    if (event->type() != MYSQL_WRITE_ROWS_EVENT &&
        event->type() != MYSQL_DELETE_ROWS_EVENT &&
        event->type() != MYSQL_UPDATE_ROWS_EVENT &&
        event->type() != TABLE_MAP_EVENT)
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
    UInt32 slave_id, // non-zero unique value
    std::string & replicate_db,
    std::string & replicate_table,
    std::string & binlog_file_name,
    UInt64 binlog_pos,
    std::string & gtid_sets)
    : IStorage(table_id_)
    , global_context(context_.getGlobalContext())
    , slave_client(host, port, master_user, master_password)
    , slave_id(slave_id)
    , replicate_db(replicate_db)
    , replicate_table(replicate_table)
    , gtid_sets(gtid_sets)
{
}

void registerStorageMySQLReplica(StorageFactory & factory)
{
    factory.registerStorage("MySQLReplica", [](const StorageFactory::Arguments & args)
    {
        //TODO: copy some logic from StorageMySQL
        ASTs & engine_args = args.engine_args;
        std::string master_host = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        const auto * ast = engine_args[1]->as<ASTLiteral>();
        Int32 master_port = 22;
        if (ast && ast->value.getType() == Field::Types::Int64) {
            master_port = safeGet<Int64>(ast->value);
        }

        std::string master_user = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        std::string master_password = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();

        ast = engine_args[4]->as<ASTLiteral>();
        UInt32 slave_id = 5432;
        if (ast && ast->value.getType() == Field::Types::UInt64) {
            slave_id = safeGet<UInt64>(ast->value);
        }

        std::string replicate_db = engine_args[5]->as<ASTLiteral &>().value.safeGet<String>();
        std::string replicate_table = engine_args[6]->as<ASTLiteral &>().value.safeGet<String>();
        std::string binlog_file_name = engine_args[7]->as<ASTLiteral &>().value.safeGet<String>();

        ast = engine_args[8]->as<ASTLiteral>();
        UInt64 binlog_pos = BIN_LOG_HEADER_SIZE;
        if (ast && ast->value.getType() == Field::Types::UInt64) {
            binlog_pos = safeGet<UInt64>(ast->value);
        }


        std::string gtid_sets = engine_args[9]->as<ASTLiteral &>().value.safeGet<String>();
        return StorageMySQLReplica::create(
            args.table_id,
            args.context,
            args.columns,
            args.constraints,
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
