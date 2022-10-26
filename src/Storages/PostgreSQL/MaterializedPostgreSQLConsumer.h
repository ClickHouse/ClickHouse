#pragma once

#include <Core/PostgreSQL/Connection.h>
#include <Core/PostgreSQL/insertPostgreSQLValue.h>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <Common/logger_useful.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTExpressionList.h>
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>


namespace DB
{
struct SettingChange;

struct StorageInfo
{
    StoragePtr storage;
    PostgreSQLTableStructure::Attributes attributes;

    StorageInfo(StoragePtr storage_, const PostgreSQLTableStructure::Attributes & attributes_)
        : storage(storage_), attributes(attributes_) {}

    StorageInfo(StoragePtr storage_, PostgreSQLTableStructure::Attributes && attributes_)
        : storage(storage_), attributes(std::move(attributes_)) {}
};
using StorageInfos = std::unordered_map<String, StorageInfo>;

class MaterializedPostgreSQLConsumer
{
private:
    struct StorageData
    {
        struct Buffer
        {
            ExternalResultDescription description;
            MutableColumns columns;

            /// Needed to pass to insert query columns list in syncTables().
            std::shared_ptr<ASTExpressionList> columns_ast;
            /// Needed for insertPostgreSQLValue() method to parse array
            std::unordered_map<size_t, PostgreSQLArrayInfo> array_info;
            /// To validate ddl.
            PostgreSQLTableStructure::Attributes attributes;

            Buffer(StorageMetadataPtr storage_metadata, const PostgreSQLTableStructure::Attributes & attributes_);

            size_t getColumnsNum() const
            {
                const auto & sample_block = description.sample_block;
                return sample_block.columns();
            }
        };

        StoragePtr storage;
        Buffer buffer;

        explicit StorageData(const StorageInfo & storage_info);
        StorageData(const StorageData & other) = delete;
    };

    using Storages = std::unordered_map<String, StorageData>;

public:
    MaterializedPostgreSQLConsumer(
            ContextPtr context_,
            std::shared_ptr<postgres::Connection> connection_,
            const String & replication_slot_name_,
            const String & publication_name_,
            const String & start_lsn,
            size_t max_block_size_,
            bool schema_as_a_part_of_table_name_,
            bool allow_automatic_update_,
            StorageInfos storages_,
            const String & name_for_logger);

    bool consume(std::vector<std::pair<Int32, String>> & skipped_tables);

    /// Called from reloadFromSnapshot by replication handler. This method is needed to move a table back into synchronization
    /// process if it was skipped due to schema changes.
    void updateNested(const String & table_name, StorageInfo nested_storage_info, Int32 table_id, const String & table_start_lsn);

    void addNested(const String & postgres_table_name, StorageInfo nested_storage_info, const String & table_start_lsn);

    void removeNested(const String & postgres_table_name);

    void setSetting(const SettingChange & setting);

private:
    /// Read approximarely up to max_block_size changes from WAL.
    bool readFromReplicationSlot();

    void syncTables();

    String advanceLSN(std::shared_ptr<pqxx::nontransaction> ntx);

    void processReplicationMessage(const char * replication_message, size_t size);

    bool isSyncAllowed(Int32 relation_id, const String & relation_name);

    static void insertDefaultValue(StorageData::Buffer & buffer, size_t column_idx);
    void insertValue(StorageData::Buffer & buffer, const std::string & value, size_t column_idx);

    enum class PostgreSQLQuery
    {
        INSERT,
        UPDATE,
        DELETE
    };

    void readTupleData(StorageData::Buffer & buffer, const char * message, size_t & pos, size_t size, PostgreSQLQuery type, bool old_value = false);

    template<typename T>
    static T unhexN(const char * message, size_t pos, size_t n);
    static void readString(const char * message, size_t & pos, size_t size, String & result);
    static Int64 readInt64(const char * message, size_t & pos, size_t size);
    static Int32 readInt32(const char * message, size_t & pos, size_t size);
    static Int16 readInt16(const char * message, size_t & pos, size_t size);
    static Int8 readInt8(const char * message, size_t & pos, size_t size);

    void markTableAsSkipped(Int32 relation_id, const String & relation_name);

    static void assertCorrectInsertion(StorageData::Buffer & buffer, size_t column_idx);

    /// lsn - log sequnce nuumber, like wal offset (64 bit).
    static Int64 getLSNValue(const std::string & lsn)
    {
        UInt32 upper_half, lower_half;
        std::sscanf(lsn.data(), "%X/%X", &upper_half, &lower_half); /// NOLINT
        return (static_cast<Int64>(upper_half) << 32) + lower_half;
    }

    Poco::Logger * log;
    ContextPtr context;
    const std::string replication_slot_name, publication_name;

    std::shared_ptr<postgres::Connection> connection;

    std::string current_lsn, final_lsn;

    /// current_lsn converted from String to Int64 via getLSNValue().
    UInt64 lsn_value;

    size_t max_block_size;

    bool schema_as_a_part_of_table_name;

    bool allow_automatic_update;

    String table_to_insert;

    /// List of tables which need to be synced after last replication stream.
    /// Holds `postgres_table_name` set.
    std::unordered_set<std::string> tables_to_sync;

    /// `postgres_table_name` -> StorageData.
    Storages storages;

    std::unordered_map<Int32, String> relation_id_to_name;

    /// `postgres_relation_id` -> `start_lsn`
    /// skip_list contains relation ids for tables on which ddl was performed, which can break synchronization.
    /// This breaking changes are detected in replication stream in according replication message and table is added to skip list.
    /// After it is finished, a temporary replication slot is created with 'export snapshot' option, and start_lsn is returned.
    /// Skipped tables are reloaded from snapshot (nested tables are also updated). Afterwards, if a replication message is
    /// related to a table in a skip_list, we compare current lsn with start_lsn, which was returned with according snapshot.
    /// If current_lsn >= table_start_lsn, we can safely remove table from skip list and continue its synchronization.
    /// No needed message, related to reloaded table will be missed, because messages are not consumed in the meantime,
    /// i.e. we will not miss the first start_lsn position for reloaded table.
    std::unordered_map<Int32, String> skip_list;

    /// `postgres_table_name` -> `start_lsn`
    /// For dynamically added tables. A new table is loaded via snapshot and we get a start lsn position.
    /// Once consumer reaches this position, it starts applying replication messages to this table.
    /// Inside replication handler we have to ensure that replication consumer does not read data from wal
    /// while the process of adding a table to replication is not finished,
    /// because we might go beyond this start lsn position before consumer knows that a new table was added.
    std::unordered_map<String, String> waiting_list;

    /// Since replication may be some time behind, we need to ensure that replication messages for deleted tables are ignored.
    std::unordered_set<String> deleted_tables;
};
}
