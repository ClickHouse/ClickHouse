#include <Interpreters/MetadataChangeLog.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/config_version.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnDefault.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/getFQDNOrHostName.h>

#include <chrono>


namespace DB
{

namespace
{

void insertStringArray(const std::vector<String> & data, IColumn & column)
{
    auto & arr_col = typeid_cast<ColumnArray &>(column);
    auto & str_data = typeid_cast<ColumnString &>(arr_col.getData());
    for (const auto & val : data)
        str_data.insertData(val.data(), val.size());
    arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
}

void insertLowCardinalityStringArray(const std::vector<String> & data, IColumn & column)
{
    auto & arr_col = typeid_cast<ColumnArray &>(column);
    auto & lc_data = typeid_cast<ColumnLowCardinality &>(arr_col.getData());
    for (const auto & val : data)
        lc_data.insertData(val.data(), val.size());
    arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
}

String formatKeyAST(const ASTPtr & ast)
{
    if (!ast)
        return {};
    return ast->formatForLogging();
}

void fillQueryContextFields(const ContextPtr & context, MetadataChangeLogElement & element)
{
    auto query_id = CurrentThread::getQueryId();
    if (!query_id.empty())
        element.query_id.assign(query_id.data(), query_id.size());
    else if (context)
        element.query_id = context->getCurrentQueryId();

    if (context)
    {
        const auto & client_info = context->getClientInfo();
        element.user = client_info.current_user;
        element.initial_user = client_info.initial_user;
    }
}

void fillTableMetadataFields(
    const StorageInMemoryMetadata & metadata,
    MetadataChangeLogElement & element)
{
    element.sorting_key = formatKeyAST(metadata.getSortingKeyAST());
    element.partition_key = formatKeyAST(metadata.getPartitionKeyAST());
    element.primary_key = formatKeyAST(metadata.getPrimaryKeyAST());
    element.comment = metadata.comment;

    const auto & columns = metadata.getColumns();
    element.columns_name.reserve(columns.size());
    element.columns_type.reserve(columns.size());
    element.columns_default_kind.reserve(columns.size());
    element.columns_default_expression.reserve(columns.size());
    element.columns_comment.reserve(columns.size());

    for (const auto & col : columns)
    {
        element.columns_name.push_back(col.name);
        element.columns_type.push_back(col.type ? col.type->getName() : String{});
        if (col.default_desc.expression)
        {
            element.columns_default_kind.push_back(toString(col.default_desc.kind));
            element.columns_default_expression.push_back(col.default_desc.expression->formatForLogging());
        }
        else if (col.default_desc.ephemeral_default || col.default_desc.kind == ColumnDefaultKind::Ephemeral)
        {
            element.columns_default_kind.push_back(toString(col.default_desc.kind));
            element.columns_default_expression.emplace_back();
        }
        else
        {
            element.columns_default_kind.emplace_back();
            element.columns_default_expression.emplace_back();
        }
        element.columns_comment.push_back(col.comment);
    }
}

}

ColumnsDescription MetadataChangeLogElement::getColumnsDescription()
{
    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto array_string = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    auto array_lc_string = std::make_shared<DataTypeArray>(lc_string);

    auto event_type_datatype = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"TableCreate", static_cast<Int8>(TABLE_CREATE)},
            {"TableAlter", static_cast<Int8>(TABLE_ALTER)},
            {"TableDrop", static_cast<Int8>(TABLE_DROP)},
            {"TableRename", static_cast<Int8>(TABLE_RENAME)},
            {"TableUndrop", static_cast<Int8>(TABLE_UNDROP)},
            {"DatabaseCreate", static_cast<Int8>(DATABASE_CREATE)},
            {"DatabaseDrop", static_cast<Int8>(DATABASE_DROP)},
            {"DatabaseRename", static_cast<Int8>(DATABASE_RENAME)},
        });

    return ColumnsDescription
    {
        {"hostname", lc_string, "Hostname of the server executing the query."},
        {"clickhouse_version", lc_string, "Version of the ClickHouse server that produced the row."},
        {"system_processor", lc_string, "CPU architecture of the ClickHouse server that produced the row."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds precision."},
        {"event_type", std::move(event_type_datatype),
            "Type of metadata change event: TableCreate, TableAlter, TableDrop, TableRename, TableUndrop, "
            "DatabaseCreate, DatabaseDrop, or DatabaseRename."},
        {"database", lc_string, "Database name."},
        {"database_uuid", std::make_shared<DataTypeUUID>(), "Database UUID."},
        {"table", std::make_shared<DataTypeString>(), "Table name (empty for database-level events)."},
        {"table_uuid", std::make_shared<DataTypeUUID>(), "Table UUID (Nil for database-level events or Ordinary databases)."},
        {"previous_database", lc_string, "Previous database name for rename events."},
        {"previous_table", std::make_shared<DataTypeString>(), "Previous table name for rename events."},
        {"engine", lc_string, "Table or database engine name."},
        {"sorting_key", std::make_shared<DataTypeString>(), "Table sorting key expression."},
        {"partition_key", std::make_shared<DataTypeString>(), "Table partition key expression."},
        {"primary_key", std::make_shared<DataTypeString>(), "Table primary key expression."},
        {"comment", std::make_shared<DataTypeString>(), "Table or database comment."},
        {"create_query", std::make_shared<DataTypeString>(), "CREATE statement with secrets masked."},
        {"columns_name", array_string, "Column names."},
        {"columns_type", array_string, "Column data types."},
        {"columns_default_kind", array_lc_string, "Column default kinds (DEFAULT, MATERIALIZED, ALIAS, EPHEMERAL, or empty)."},
        {"columns_default_expression", array_string, "Column default expressions with secrets masked."},
        {"columns_comment", array_string, "Column comments."},
        {"dependencies_database", array_lc_string, "Target database names of table dependencies."},
        {"dependencies_table", array_string, "Target table names of table dependencies."},
        {"query_id", std::make_shared<DataTypeString>(), "Query ID that triggered the metadata change."},
        {"user", lc_string, "Current user executing the query."},
        {"initial_user", lc_string, "Initial user that initiated the query."},
    };
}

void MetadataChangeLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(VERSION_STRING);
    columns[i++]->insert(SYSTEM_PROCESSOR);
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(static_cast<Int8>(event_type));
    columns[i++]->insert(database);
    columns[i++]->insert(database_uuid);
    columns[i++]->insert(table);
    columns[i++]->insert(table_uuid);
    columns[i++]->insert(previous_database);
    columns[i++]->insert(previous_table);
    columns[i++]->insert(engine);
    columns[i++]->insert(sorting_key);
    columns[i++]->insert(partition_key);
    columns[i++]->insert(primary_key);
    columns[i++]->insert(comment);
    columns[i++]->insert(create_query);

    insertStringArray(columns_name, *columns[i++]);
    insertStringArray(columns_type, *columns[i++]);
    insertLowCardinalityStringArray(columns_default_kind, *columns[i++]);
    insertStringArray(columns_default_expression, *columns[i++]);
    insertStringArray(columns_comment, *columns[i++]);
    insertLowCardinalityStringArray(dependencies_database, *columns[i++]);
    insertStringArray(dependencies_table, *columns[i++]);

    columns[i++]->insert(query_id);
    columns[i++]->insert(user);
    columns[i++]->insert(initial_user);
}

bool MetadataChangeLog::isInternalDatabase(std::string_view database_name)
{
    return database_name == DatabaseCatalog::SYSTEM_DATABASE
        || database_name == DatabaseCatalog::INFORMATION_SCHEMA
        || database_name == DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE
        || database_name == DatabaseCatalog::TEMPORARY_DATABASE;
}

void MetadataChangeLog::logTableEvent(
    const ContextPtr & local_context,
    MetadataChangeLogElement::Type event_type,
    const StorageID & table_id,
    UUID database_uuid,
    const StoragePtr & storage,
    const StorageInMemoryMetadata * metadata,
    const ASTPtr & create_query_ast,
    const String & previous_database,
    const String & previous_table)
{
    if (isInternalDatabase(table_id.database_name))
        return;

    if (!local_context)
        return;

    auto metadata_change_log = local_context->getMetadataChangeLog();
    if (!metadata_change_log)
        return;

    try
    {
        StorageMetadataHandle metadata_snapshot = nullptr;
        const StorageInMemoryMetadata * effective_metadata = metadata;
        if (!effective_metadata && storage)
        {
            metadata_snapshot = storage->getInMemoryMetadataPtr(local_context, false);
            if (metadata_snapshot)
                effective_metadata = &(*metadata_snapshot);
        }

        String create_query_str;
        if (create_query_ast)
            create_query_str = create_query_ast->formatForLogging();

        String engine_name;
        if (storage)
            engine_name = storage->getName();

        std::vector<QualifiedTableName> dependencies;
        if (event_type != MetadataChangeLogElement::TABLE_DROP)
        {
            if (create_query_ast)
            {
                const String current_db = local_context->getCurrentDatabase();
                auto ref_deps = getDependenciesFromCreateQuery(
                    local_context->getGlobalContext(),
                    table_id.getQualifiedName(),
                    create_query_ast,
                    current_db.empty() ? table_id.database_name : current_db,
                    /*can_throw=*/false,
                    /*validate_current_database=*/false);

                TableNamesSet unique_deps = std::move(ref_deps.dependencies);
                if (ref_deps.mv_from_dependency)
                    unique_deps.insert(ref_deps.mv_from_dependency->getQualifiedName());
                if (ref_deps.mv_to_dependency)
                    unique_deps.insert(ref_deps.mv_to_dependency->getQualifiedName());

                dependencies.assign(unique_deps.begin(), unique_deps.end());
            }
            else
            {
                for (const auto & dep : DatabaseCatalog::instance().getReferentialDependencies(table_id))
                    dependencies.push_back(dep.getQualifiedName());
            }

            std::sort(dependencies.begin(), dependencies.end());
        }

        const auto time_now = std::chrono::system_clock::now();

        metadata_change_log->add([&](MetadataChangeLogElement & element)
        {
            element.event_time = timeInSeconds(time_now);
            element.event_time_microseconds = timeInMicroseconds(time_now);
            element.event_type = event_type;

            element.database = table_id.database_name;
            element.database_uuid = database_uuid;
            element.table = table_id.table_name;
            element.table_uuid = table_id.uuid;

            element.previous_database = previous_database;
            element.previous_table = previous_table;

            element.engine = engine_name;
            element.create_query = create_query_str;

            if (effective_metadata)
                fillTableMetadataFields(*effective_metadata, element);

            element.dependencies_database.reserve(dependencies.size());
            element.dependencies_table.reserve(dependencies.size());
            for (const auto & dep : dependencies)
            {
                element.dependencies_database.push_back(dep.database);
                element.dependencies_table.push_back(dep.table);
            }

            fillQueryContextFields(local_context, element);
        });
    }
    catch (...)
    {
        tryLogCurrentException("MetadataChangeLog", "Failed to record table metadata change event");
    }
}

void MetadataChangeLog::logDatabaseEvent(
    const ContextPtr & local_context,
    MetadataChangeLogElement::Type event_type,
    const String & database_name,
    UUID database_uuid,
    const String & engine,
    const String & comment,
    const ASTPtr & create_query_ast,
    const String & previous_database)
{
    if (isInternalDatabase(database_name))
        return;

    if (!local_context)
        return;

    auto metadata_change_log = local_context->getMetadataChangeLog();
    if (!metadata_change_log)
        return;

    try
    {
        String create_query_str;
        if (create_query_ast)
            create_query_str = create_query_ast->formatForLogging();

        const auto time_now = std::chrono::system_clock::now();

        metadata_change_log->add([&](MetadataChangeLogElement & element)
        {
            element.event_time = timeInSeconds(time_now);
            element.event_time_microseconds = timeInMicroseconds(time_now);
            element.event_type = event_type;

            element.database = database_name;
            element.database_uuid = database_uuid;
            element.previous_database = previous_database;

            element.engine = engine;
            element.comment = comment;
            element.create_query = create_query_str;

            fillQueryContextFields(local_context, element);
        });
    }
    catch (...)
    {
        tryLogCurrentException("MetadataChangeLog", "Failed to record database metadata change event");
    }
}

}
