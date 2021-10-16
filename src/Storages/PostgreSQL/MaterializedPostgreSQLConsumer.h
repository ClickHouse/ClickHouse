#pragma once

#include <Core/PostgreSQL/Connection.h>
#include <Core/PostgreSQL/insertPostgreSQLValue.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <common/logger_useful.h>
#include <Storages/IStorage.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Parsers/ASTExpressionList.h>


namespace DB
{

class MaterializedPostgreSQLConsumer
{
public:
    using Storages = std::unordered_map<String, StoragePtr>;

    MaterializedPostgreSQLConsumer(
            ContextPtr context_,
            std::shared_ptr<postgres::Connection> connection_,
            const String & replication_slot_name_,
            const String & publication_name_,
            const String & start_lsn,
            const size_t max_block_size_,
            bool allow_automatic_update_,
            Storages storages_);

    bool consume(std::vector<std::pair<Int32, String>> & skipped_tables);

    /// Called from reloadFromSnapshot by replication handler. This method is needed to move a table back into synchronization
    /// process if it was skipped due to schema changes.
    void updateNested(const String & table_name, StoragePtr nested_storage, Int32 table_id, const String & table_start_lsn);

private:
    /// Read approximarely up to max_block_size changes from WAL.
    bool readFromReplicationSlot();

    void syncTables();

    String advanceLSN(std::shared_ptr<pqxx::nontransaction> ntx);

    void processReplicationMessage(const char * replication_message, size_t size);

    bool isSyncAllowed(Int32 relation_id);

    struct Buffer
    {
        ExternalResultDescription description;
        MutableColumns columns;

        /// Needed to pass to insert query columns list in syncTables().
        std::shared_ptr<ASTExpressionList> columnsAST;

        /// Needed for insertPostgreSQLValue() method to parse array
        std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;

        Buffer(StoragePtr storage) { createEmptyBuffer(storage); }
        void createEmptyBuffer(StoragePtr storage);
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

    template<typename T>
    static T unhexN(const char * message, size_t pos, size_t n);
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
    ContextPtr context;
    const std::string replication_slot_name, publication_name;

    std::shared_ptr<postgres::Connection> connection;

    std::string current_lsn, final_lsn;

    /// current_lsn converted from String to Int64 via getLSNValue().
    UInt64 lsn_value;

    const size_t max_block_size;
    bool allow_automatic_update;

    String table_to_insert;

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
