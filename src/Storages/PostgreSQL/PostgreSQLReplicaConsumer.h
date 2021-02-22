#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLConnection.h"
#include "PostgreSQLReplicaMetadata.h"
#include "insertPostgreSQLValue.h"

#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
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
            bool allow_minimal_ddl_,
            bool is_postgresql_replica_database_engine_,
            Storages storages_);

    void readMetadata();

    bool consume(std::vector<std::pair<Int32, String>> & skipped_tables);

    void updateNested(const String & table_name, StoragePtr nested_storage);

    void updateSkipList(const std::unordered_map<Int32, String> & tables_with_lsn);

private:
    bool readFromReplicationSlot();

    void syncTables(std::shared_ptr<pqxx::nontransaction> tx);

    String advanceLSN(std::shared_ptr<pqxx::nontransaction> ntx);

    void processReplicationMessage(const char * replication_message, size_t size);

    bool isSyncAllowed(Int32 relation_id);

    struct Buffer
    {
        ExternalResultDescription description;
        MutableColumns columns;
        std::shared_ptr<ASTExpressionList> columnsAST;
        /// Needed for insertPostgreSQLValue() method to parse array
        std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;

        Buffer(StoragePtr storage) { fillBuffer(storage); }
        void fillBuffer(StoragePtr storage);
    };

    using Buffers = std::unordered_map<String, Buffer>;

    static void insertDefaultValue(Buffer & buffer, size_t column_idx);
    static void insertValue(Buffer & buffer, const std::string & value, size_t column_idx);

    enum class PostgreSQLQuery
    {
        INSERT,
        UPDATE,
        DELETE
    };

    void readTupleData(Buffer & buffer, const char * message, size_t & pos, size_t size, PostgreSQLQuery type, bool old_value = false);

    static void readString(const char * message, size_t & pos, size_t size, String & result);
    static Int64 readInt64(const char * message, size_t & pos, size_t size);
    static Int32 readInt32(const char * message, size_t & pos, size_t size);
    static Int16 readInt16(const char * message, size_t & pos, size_t size);
    static Int8 readInt8(const char * message, size_t & pos, size_t size);

    void markTableAsSkipped(Int32 relation_id, const String & relation_name);

    /// lsn - log sequnce nuumber, like wal offset (64 bit).
    Int64 getLSNValue(const std::string & lsn)
    {
        UInt32 upper_half, lower_half;
        std::sscanf(lsn.data(), "%X/%X", &upper_half, &lower_half);
        return (static_cast<Int64>(upper_half) << 32) + lower_half;
    }

    Poco::Logger * log;
    std::shared_ptr<Context> context;
    const std::string replication_slot_name, publication_name;

    PostgreSQLReplicaMetadata metadata;
    PostgreSQLConnectionPtr connection;

    std::string current_lsn, final_lsn;
    const size_t max_block_size;
    bool allow_minimal_ddl, is_postgresql_replica_database_engine;

    std::string table_to_insert;

    /// List of tables which need to be synced after last replication stream.
    std::unordered_set<std::string> tables_to_sync;

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

    /// Cache for table schema data to be able to detect schema changes, because ddl is not
    /// replicated with postgresql logical replication protocol, but some table schema info
    /// is received if it is the first time we received dml message for given relation in current session or
    /// if relation definition has changed since the last relation definition message.
    std::unordered_map<Int32, SchemaData> schema_data;

    /// skip_list contains relation ids for tables on which ddl was performed, which can break synchronization.
    /// This breaking changes are detected in replication stream in according replication message and table is added to skip list.
    /// After it is finished, a temporary replication slot is created with 'export snapshot' option, and start_lsn is returned.
    /// Skipped tables are reloaded from snapshot (nested tables are also updated). Afterwards, if a replication message is
    /// related to a table in a skip_list, we compare current lsn with start_lsn, which was returned with according snapshot.
    /// If current_lsn >= table_start_lsn, we can safely remove table from skip list and continue its synchronization.
    /// No needed message, related to reloaded table will be missed, because messages are not consumed in the meantime,
    /// i.e. we will not miss the first start_lsn position for reloaded table.
    std::unordered_map<Int32, String> skip_list;
};

}

#endif
