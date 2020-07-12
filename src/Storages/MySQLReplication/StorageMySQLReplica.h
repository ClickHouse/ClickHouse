#pragma once

#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Block.h>
#include <Core/MySQLClient.h>

using namespace MySQLReplica;

namespace MySQLReplica
{
    class EventBase;
    using BinlogEventPtr = std::shared_ptr<EventBase>;

    struct MySQLClickHouseEvent;
}

namespace DB
{

class StorageMySQLReplica final : public  ext::shared_ptr_helper<StorageMySQLReplica>/* do I need this*/, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageMySQLReplica>;
public:
    std::string getName() const override {
        return "MySQLReplica";
    }

    bool supportsSettings() const override {
        return true; /// TODO: ?
    }

    void startup() override;
    void shutdown() override;

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info, 
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;


protected:
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
        std::string & gtid_sets);

private:
    Context global_context;
    StorageMemory storage;

    std::vector<MySQLClickHouseEvent> BinlogEvents;
    /* FIXME: the binlog stream reader porcess will keep this array
        as well as all position indexes for all relplica tables for a selected DB
        and delete items already processed by all of consumers
        (perhaps also dumping the data to the disk)*/
    size_t UnprocessedBinlogEventIdx;

    BackgroundSchedulePool::TaskHolder binlog_reader_task;

    Block RowsToBlock(StorageMetadataPtr & metadata, const std::vector<Field> & rows);

    // listen for master events in the background
    void ReadBinlogStream();

    // apply received events to the underlying storage
    void DoReplication();
};

}
