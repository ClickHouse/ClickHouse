#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/SystemLog.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage_fwd.h>

#include <string_view>
#include <vector>


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
    };

    time_t event_time{};
    Decimal64 event_time_microseconds{};

    Type event_type = TABLE_CREATE;

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
};

}
