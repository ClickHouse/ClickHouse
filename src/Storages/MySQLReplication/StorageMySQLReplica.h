#pragma once

#include <ext/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMemory.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Block.h>
#include <Core/MySQLClient.h>
#include <Core/MySQLReplication.h>
#include <Core/NamesAndTypes.h>
#include <Core/Types.h>

#include <string>
#include <vector>

namespace DB
{

using namespace MySQLReplication;

namespace MySQLReplication {
    // https://github.com/mysql/mysql-server/blob/8.0/sql/rpl_binlog_sender.cc#L825
    // https://github.com/mysql/mysql-server/blob/8.0/libbinlogevents/include/binlog_event.h#L83
    const UInt64 BIN_LOG_HEADER_SIZE = 4U;
    const UInt64 DEFAULT_MYSQL_PORT = 3306;
    const UInt32 DEFAULT_SLAVE_ID = 1234;

    struct MySQLClickHouseEvent {
        MySQLEventType type;
        std::vector<std::vector<Field>> select_rows;
        std::vector<std::vector<Field>> insert_rows;
    };
}

class StorageMySQLReplica final : public  ext::shared_ptr_helper<StorageMySQLReplica>/* do I need this*/, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMySQLReplica>;
public:
    std::string getName() const override {
        return "MySQLReplica";
    }

    void startup() override;
    void shutdown() override;

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info, 
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;


protected:
    StorageMySQLReplica(
        const StorageID & table_id_,
        Context & context_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        /* slave data */
        std::string & host,
        Int32 & port,
        std::string & master_user,
        std::string & master_password,
        std::string & replicate_db,
        std::string & replicate_table,
        std::string & binlog_filename,
        std::string & gtid_sets,
        UInt64 binlog_pos=BIN_LOG_HEADER_SIZE,
        UInt32 slave_id=DEFAULT_SLAVE_ID);

private:
    Context global_context;

    MySQLClient slave_client;
    UInt32 slave_id;
    std::string replicate_db;
    std::string replicate_table;
    std::string binlog_filename;
    UInt64 binlog_pos;
    std::string gtid_sets;

    BlocksList data;

    BackgroundSchedulePool::TaskHolder task;

    void initBinlogStream();

    // listen for master events in the background
    MySQLClickHouseEvent readOneBinlogEvent();

    void threadFunc();

    Block rowsToBlock(const StorageMetadataPtr & metadata, const std::vector<std::vector<Field>> & rows);

    Poco::Logger * log;
};

}
