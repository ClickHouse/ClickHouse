#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/NamesAndAliases.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/SystemLog.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>

#include <mutex>
#include <string_view>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}


namespace DB
{

struct StorageInMemoryMetadata;

struct MetadataChangeLogElement
{
    enum Type : int8_t
    {
        TABLE_CREATE = 1,
        TABLE_ALTER = 2,
        TABLE_DROP = 3,
        TABLE_RENAME = 4,
        TABLE_UNDROP = 5,
        DATABASE_CREATE = 6,
        DATABASE_DROP = 7,
        DATABASE_RENAME = 8,
        DATABASE_SNAPSHOT = 9,
        TABLE_SNAPSHOT = 10,
    };

    time_t event_time{};
    Decimal64 event_time_microseconds{};

    Type event_type = TABLE_CREATE;
    String snapshot_id;

    String database;
    UUID database_uuid = UUIDHelpers::Nil;
    String table;
    UUID table_uuid = UUIDHelpers::Nil;

    String previous_database;
    String previous_table;

    String engine;
    String sorting_key;
    String partition_key;
    String primary_key;
    String comment;
    String create_query;

    std::vector<String> columns_name;
    std::vector<String> columns_type;
    std::vector<String> columns_default_kind;
    std::vector<String> columns_default_expression;
    std::vector<String> columns_comment;

    std::vector<String> dependencies_database;
    std::vector<String> dependencies_table;

    String query_id;
    String user;
    String initial_user;

    static std::string name() { return "MetadataChangeLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class MetadataChangeLog : public SystemLog<MetadataChangeLogElement>
{
public:
    using SystemLog<MetadataChangeLogElement>::SystemLog;
    ~MetadataChangeLog() override;

    static constexpr const char * DEFAULT_SNAPSHOT_KEEPER_PATH = "/clickhouse/metadata_change_log/snapshots";

    void shutdown() override;

    /// Reacts to `metadata_change_log.snapshot_token` (and optional `snapshot_keeper_path`)
    /// on startup and config reload. Schedules an asynchronous snapshot when the token changes.
    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

    static bool isInternalDatabase(std::string_view database_name);

    static void logTableEvent(
        const ContextPtr & local_context,
        MetadataChangeLogElement::Type event_type,
        const StorageID & table_id,
        UUID database_uuid,
        const StoragePtr & storage = nullptr,
        const StorageInMemoryMetadata * metadata = nullptr,
        const ASTPtr & create_query_ast = nullptr,
        const String & previous_database = {},
        const String & previous_table = {});

    static void logDatabaseEvent(
        const ContextPtr & local_context,
        MetadataChangeLogElement::Type event_type,
        const String & database_name,
        UUID database_uuid,
        const String & engine = {},
        const String & comment = {},
        const ASTPtr & create_query_ast = nullptr,
        const String & previous_database = {});

private:
    void stopSnapshotTask();
    void runSnapshotTask();
    bool emitSnapshot(const String & snapshot_id);
    void markSnapshotTokenCompleted(const String & token);
    void scheduleSnapshotRetry(const String & token, size_t delay_ms);
    bool isSnapshotShutdown() const;

    mutable std::mutex snapshot_mutex;
    String pending_snapshot_token TSA_GUARDED_BY(snapshot_mutex);
    String last_completed_snapshot_token TSA_GUARDED_BY(snapshot_mutex);
    String snapshot_keeper_path TSA_GUARDED_BY(snapshot_mutex) = DEFAULT_SNAPSHOT_KEEPER_PATH;
    bool snapshot_shutdown TSA_GUARDED_BY(snapshot_mutex) = false;
    BackgroundSchedulePoolTaskHolder snapshot_task TSA_GUARDED_BY(snapshot_mutex);
};

}
