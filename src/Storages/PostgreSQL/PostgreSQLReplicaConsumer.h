#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicaMetadata.h"
#include "insertPostgreSQLValue.h"

#include <Core/BackgroundSchedulePool.h>
#include <common/logger_useful.h>
#include <Storages/IStorage.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Parsers/ASTExpressionList.h>
#include "pqxx/pqxx" // Y_IGNORE


namespace DB
{

class PostgreSQLReplicaConsumer
{
public:
    using Storages = std::unordered_map<String, StoragePtr>;

    PostgreSQLReplicaConsumer(
            std::shared_ptr<Context> context_,
            PostgreSQLConnectionPtr connection_,
            const std::string & replication_slot_name_,
            const std::string & publication_name_,
            const std::string & metadata_path,
            const std::string & start_lsn,
            const size_t max_block_size_,
            Storages storages_);

    void startSynchronization();

    void stopSynchronization();

private:
    void synchronizationStream();

    bool readFromReplicationSlot();

    void syncTables(std::shared_ptr<pqxx::nontransaction> tx);

    String advanceLSN(std::shared_ptr<pqxx::nontransaction> ntx);

    void processReplicationMessage(const char * replication_message, size_t size);

    struct BufferData
    {
        ExternalResultDescription description;
        MutableColumns columns;
        std::shared_ptr<ASTExpressionList> columnsAST;
        /// Needed for insertPostgreSQLValue() method to parse array
        std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;

        BufferData(StoragePtr storage)
        {
            const auto storage_metadata = storage->getInMemoryMetadataPtr();
            description.init(storage_metadata->getSampleBlock());
            columns = description.sample_block.cloneEmptyColumns();
            const auto & storage_columns = storage_metadata->getColumns().getAllPhysical();
            auto insert_columns = std::make_shared<ASTExpressionList>();
            size_t idx = 0;
            assert(description.sample_block.columns() == storage_columns.size());

            for (const auto & column : storage_columns)
            {
                if (description.types[idx].first == ExternalResultDescription::ValueType::vtArray)
                    preparePostgreSQLArrayInfo(array_info, idx, description.sample_block.getByPosition(idx).type);
                idx++;

                insert_columns->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));
            }

            columnsAST = std::move(insert_columns);
        }
    };

    using Buffers = std::unordered_map<String, BufferData>;

    static void insertDefaultValue(BufferData & buffer, size_t column_idx);
    static void insertValue(BufferData & buffer, const std::string & value, size_t column_idx);

    enum class PostgreSQLQuery
    {
        INSERT,
        UPDATE,
        DELETE
    };

    void readTupleData(BufferData & buffer, const char * message, size_t & pos, size_t size, PostgreSQLQuery type, bool old_value = false);

    static void readString(const char * message, size_t & pos, size_t size, String & result);
    static Int64 readInt64(const char * message, size_t & pos, size_t size);
    static Int32 readInt32(const char * message, size_t & pos, size_t size);
    static Int16 readInt16(const char * message, size_t & pos, size_t size);
    static Int8 readInt8(const char * message, size_t & pos, size_t size);

    void markTableAsSkippedUntilReload(Int32 relation_id, const String & relation_name);

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string replication_slot_name, publication_name;

    PostgreSQLReplicaMetadata metadata;
    PostgreSQLConnectionPtr connection;

    std::string current_lsn, final_lsn;
    const size_t max_block_size;

    std::string table_to_insert;
    std::unordered_set<std::string> tables_to_sync;

    BackgroundSchedulePool::TaskHolder wal_reader_task;
    std::atomic<bool> stop_synchronization = false;

    Storages storages;
    Buffers buffers;

    std::unordered_map<Int32, String> relation_id_to_name;

    struct SchemaData
    {
        Int16 number_of_columns;
        /// data_type_id and type_modifier
        std::vector<std::pair<Int32, Int32>> column_identifiers;

        SchemaData(Int16 number_of_columns_) : number_of_columns(number_of_columns_) {}
    };

    std::unordered_map<Int32, SchemaData> schema_data;
    std::unordered_set<Int32> skip_until_reload;
};

}

#endif
