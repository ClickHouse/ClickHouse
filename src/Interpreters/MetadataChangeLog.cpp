#include <Interpreters/MetadataChangeLog.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Common/CurrentThread.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Databases/DDLDependencyVisitor.h>
#include <Databases/IDatabase.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnDefault.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/getFQDNOrHostName.h>
#include <base/scope_guard.h>

#include <config.h>
#if CLICKHOUSE_CLOUD
#    include <Interpreters/SharedDatabaseCatalog.h>
#endif

#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <chrono>
#include <ranges>


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

std::vector<QualifiedTableName> collectTableDependencies(
    const ContextPtr & local_context,
    const StorageID & table_id,
    const ASTPtr & create_query_ast)
{
    std::vector<QualifiedTableName> dependencies;
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
    return dependencies;
}

constexpr size_t SNAPSHOT_RETRY_DELAY_MS = 5000;
constexpr size_t SNAPSHOT_FLUSH_BATCH_SIZE = 1000;
constexpr size_t MAX_REMEMBERED_SNAPSHOT_TOKENS = 16;

std::vector<String> parseCompletedTokens(std::string_view data)
{
    std::vector<String> tokens;
    size_t pos = 0;
    while (pos < data.size())
    {
        const size_t newline = data.find('\n', pos);
        const auto line = (newline == std::string_view::npos) ? data.substr(pos) : data.substr(pos, newline - pos);
        if (!line.empty())
            tokens.emplace_back(line);
        if (newline == std::string_view::npos)
            break;
        pos = newline + 1;
    }
    return tokens;
}

String serializeCompletedTokens(const std::vector<String> & tokens)
{
    String out;
    for (size_t i = 0; i < tokens.size(); ++i)
    {
        if (i > 0)
            out.push_back('\n');
        out.append(tokens[i]);
    }
    return out;
}

bool isSnapshotTokenRecorded(
    const zkutil::ZooKeeperPtr & zookeeper, const String & completed_tokens_path, const String & token)
{
    String data;
    if (!zookeeper->tryGet(completed_tokens_path, data))
        return false;
    const auto tokens = parseCompletedTokens(data);
    return std::ranges::contains(tokens, token);
}

Coordination::Error recordCompletedSnapshotToken(
    const zkutil::ZooKeeperPtr & zookeeper, const String & completed_tokens_path, const String & token)
{
    constexpr size_t MAX_CAS_ATTEMPTS = 8;
    for (size_t attempt = 0; attempt < MAX_CAS_ATTEMPTS; ++attempt)
    {
        String data;
        Coordination::Stat stat;
        const bool exists = zookeeper->tryGet(completed_tokens_path, data, &stat);
        auto tokens = exists ? parseCompletedTokens(data) : std::vector<String>{};

        std::erase(tokens, token);
        tokens.push_back(token);
        if (tokens.size() > MAX_REMEMBERED_SNAPSHOT_TOKENS)
            tokens.erase(
                tokens.begin(),
                tokens.begin() + static_cast<std::ptrdiff_t>(tokens.size() - MAX_REMEMBERED_SNAPSHOT_TOKENS));

        const String new_data = serializeCompletedTokens(tokens);

        if (!exists)
        {
            const auto code = zookeeper->tryCreate(completed_tokens_path, new_data, zkutil::CreateMode::Persistent);
            if (code == Coordination::Error::ZOK)
                return code;
            if (code == Coordination::Error::ZNODEEXISTS)
                continue;
            return code;
        }

        const auto code = zookeeper->trySet(completed_tokens_path, new_data, stat.version);
        if (code == Coordination::Error::ZOK)
            return code;
        if (code == Coordination::Error::ZBADVERSION || code == Coordination::Error::ZNONODE)
            continue;
        return code;
    }

    return Coordination::Error::ZBADVERSION;
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
            {"DatabaseSnapshot", static_cast<Int8>(DATABASE_SNAPSHOT)},
            {"TableSnapshot", static_cast<Int8>(TABLE_SNAPSHOT)},
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
            "DatabaseCreate, DatabaseDrop, DatabaseRename, DatabaseSnapshot, or TableSnapshot."},
        {"snapshot_id", std::make_shared<DataTypeString>(),
            "Snapshot token identifying a catalog snapshot pass (empty for live DDL events)."},
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
    columns[i++]->insert(snapshot_id);
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
        || database_name == DatabaseCatalog::TEMPORARY_DATABASE
#if CLICKHOUSE_CLOUD
        || database_name == SharedDatabaseCatalog::INTERNAL_DATABASE_TO_DROP
#endif
        ;
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
            dependencies = collectTableDependencies(local_context, table_id, create_query_ast);

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

MetadataChangeLog::~MetadataChangeLog()
{
    stopSnapshotTask();
}

void MetadataChangeLog::shutdown()
{
    stopSnapshotTask();
    SystemLog<MetadataChangeLogElement>::shutdown();
}

void MetadataChangeLog::stopSnapshotTask()
{
    BackgroundSchedulePoolTaskHolder task_to_deactivate;
    {
        std::lock_guard lock(snapshot_mutex);
        snapshot_shutdown = true;
        task_to_deactivate = std::move(snapshot_task);
    }

    if (task_to_deactivate)
        task_to_deactivate->deactivate();
}

bool MetadataChangeLog::isSnapshotShutdown() const
{
    std::lock_guard lock(snapshot_mutex);
    return snapshot_shutdown;
}

void MetadataChangeLog::reloadConfig(const Poco::Util::AbstractConfiguration & config)
{
    const String token = Poco::trim(config.getString("metadata_change_log.snapshot_token", ""));
    const String keeper_path = Poco::trim(config.getString("metadata_change_log.snapshot_keeper_path", DEFAULT_SNAPSHOT_KEEPER_PATH));

    if (token.find('\n') != String::npos || token.find('\r') != String::npos)
    {
        LOG_WARNING(log, "Ignoring metadata_change_log.snapshot_token because it contains line breaks");
        return;
    }

    std::lock_guard lock(snapshot_mutex);
    if (snapshot_shutdown)
        return;

    snapshot_keeper_path = keeper_path;

    if (token.empty() || token == last_completed_snapshot_token || token == pending_snapshot_token)
        return;

    pending_snapshot_token = token;
    if (!snapshot_task)
        snapshot_task = getContext()->getSchedulePool()->createTask(
            getTableID(), "MetadataChangeLogSnapshot", [this] { runSnapshotTask(); });

    LOG_INFO(log, "Scheduling metadata catalog snapshot for token '{}'", token);
    snapshot_task->schedule();
}

void MetadataChangeLog::markSnapshotTokenCompleted(const String & token)
{
    std::lock_guard lock(snapshot_mutex);
    last_completed_snapshot_token = token;
    if (pending_snapshot_token == token)
    {
        pending_snapshot_token.clear();
        return;
    }

    if (!pending_snapshot_token.empty() && !snapshot_shutdown && snapshot_task)
        snapshot_task->schedule();
}

void MetadataChangeLog::scheduleSnapshotRetry(const String & token, size_t delay_ms)
{
    std::lock_guard lock(snapshot_mutex);
    if (snapshot_shutdown || pending_snapshot_token != token || !snapshot_task)
        return;
    snapshot_task->scheduleAfter(delay_ms);
}

void MetadataChangeLog::flushSnapshotBatch()
{
    const auto last_index = getLastLogIndex();
    setManualFlushTargetIndex(last_index);
    notifyFlush(last_index, /*should_prepare_tables_anyway=*/true);
    flush(last_index, /*should_prepare_tables_anyway=*/true);
}

void MetadataChangeLog::runSnapshotTask()
{
    String token;
    String keeper_path;
    {
        std::lock_guard lock(snapshot_mutex);
        if (snapshot_shutdown || pending_snapshot_token.empty() || pending_snapshot_token == last_completed_snapshot_token)
            return;
        token = pending_snapshot_token;
        keeper_path = snapshot_keeper_path;
    }

    try
    {
        auto global_context = getContext();
        const bool use_keeper = !keeper_path.empty() && global_context->hasZooKeeper();

        if (!use_keeper)
        {
            if (!emitSnapshot(token))
                return;

            markSnapshotTokenCompleted(token);
            LOG_INFO(log, "Completed metadata catalog snapshot for token '{}'", token);
            return;
        }

        while (keeper_path.size() > 1 && keeper_path.back() == '/')
            keeper_path.pop_back();

        const String completed_tokens_path = keeper_path + "/completed_tokens";
        const String lock_path = keeper_path + "/lock";

        auto zookeeper = global_context->getZooKeeper();
        zookeeper->createAncestors(completed_tokens_path);

        if (isSnapshotTokenRecorded(zookeeper, completed_tokens_path, token))
        {
            LOG_INFO(
                log,
                "Metadata catalog snapshot for token '{}' already recorded in {}",
                token,
                completed_tokens_path);
            markSnapshotTokenCompleted(token);
            return;
        }

        const String host = getFQDNOrHostName();
        const auto lock_code = zookeeper->tryCreate(lock_path, token + "\n" + host, zkutil::CreateMode::Ephemeral);
        if (lock_code == Coordination::Error::ZNODEEXISTS)
        {
            if (isSnapshotTokenRecorded(zookeeper, completed_tokens_path, token))
            {
                markSnapshotTokenCompleted(token);
                return;
            }
            LOG_INFO(log, "Metadata catalog snapshot is already in progress on another replica; will recheck token '{}'", token);
            scheduleSnapshotRetry(token, SNAPSHOT_RETRY_DELAY_MS);
            return;
        }

        if (lock_code != Coordination::Error::ZOK)
        {
            LOG_WARNING(
                log,
                "Failed to acquire Keeper lock {} for metadata snapshot token '{}' (code: {}); retrying",
                lock_path,
                token,
                Coordination::errorMessage(lock_code));
            scheduleSnapshotRetry(token, SNAPSHOT_RETRY_DELAY_MS);
            return;
        }

        SCOPE_EXIT({
            try
            {
                if (zookeeper && !zookeeper->expired())
                    zookeeper->tryRemove(lock_path);
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Failed to remove metadata snapshot lock {}", lock_path));
            }
        });

        if (isSnapshotTokenRecorded(zookeeper, completed_tokens_path, token))
        {
            markSnapshotTokenCompleted(token);
            return;
        }

        if (!emitSnapshot(token))
            return;

        const auto record_code = recordCompletedSnapshotToken(zookeeper, completed_tokens_path, token);
        if (record_code != Coordination::Error::ZOK)
        {
            LOG_WARNING(
                log,
                "Metadata snapshot for token '{}' was flushed, but updating Keeper marker {} failed (code: {})",
                token,
                completed_tokens_path,
                Coordination::errorMessage(record_code));
        }

        markSnapshotTokenCompleted(token);
        LOG_INFO(log, "Completed metadata catalog snapshot for token '{}' (Keeper marker {})", token, completed_tokens_path);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to emit metadata catalog snapshot for token '{}'; will retry", token));
        scheduleSnapshotRetry(token, SNAPSHOT_RETRY_DELAY_MS);
    }
}

bool MetadataChangeLog::emitSnapshot(const String & snapshot_id)
{
    auto global_context = getContext();
    auto databases = DatabaseCatalog::instance().getDatabases(
        {.with_datalake_catalogs = true, .with_remote_databases = true});

    size_t db_count = 0;
    size_t table_count = 0;
    size_t pending_since_flush = 0;

    auto maybe_flush_batch = [&]()
    {
        if (++pending_since_flush >= SNAPSHOT_FLUSH_BATCH_SIZE)
        {
            flushSnapshotBatch();
            pending_since_flush = 0;
        }
    };

    for (const auto & [database_name, database] : databases)
    {
        if (isSnapshotShutdown())
            return false;

        if (!database || isInternalDatabase(database_name))
            continue;

        const UUID database_uuid = database->getUUID();
        const String db_engine = database->getEngineName();
        const String db_comment = database->getDatabaseComment();

        String db_create_query;
        try
        {
            if (auto create_ast = database->getCreateDatabaseQuery())
                db_create_query = create_ast->formatForLogging();
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to format CREATE DATABASE query for snapshot of {}", database_name));
        }

        const auto db_time_now = std::chrono::system_clock::now();
        add([&](MetadataChangeLogElement & element)
        {
            element.event_time = timeInSeconds(db_time_now);
            element.event_time_microseconds = timeInMicroseconds(db_time_now);
            element.event_type = MetadataChangeLogElement::DATABASE_SNAPSHOT;
            element.snapshot_id = snapshot_id;
            element.database = database_name;
            element.database_uuid = database_uuid;
            element.engine = db_engine;
            element.comment = db_comment;
            element.create_query = db_create_query;
        });
        ++db_count;
        maybe_flush_batch();

        /// External databases (e.g. DataLakeCatalog, MySQL, PostgreSQL) require remote network
        /// requests to enumerate tables. Record only the database binding above.
        if (database->isExternal())
            continue;

        try
        {
            for (auto it = database->getTablesIterator(global_context, {}, /*skip_not_loaded=*/false); it->isValid(); it->next())
            {
                if (isSnapshotShutdown())
                    return false;

                const String table_name = it->name();
                StoragePtr storage = it->table();
                if (!storage)
                    continue;

                const StorageID table_id = storage->getStorageID();
                StorageMetadataHandle metadata_snapshot = storage->getInMemoryMetadataPtr(global_context, false);
                ASTPtr create_query_ast;
                try
                {
                    create_query_ast = database->tryGetCreateTableQuery(table_name, global_context);
                }
                catch (...)
                {
                    tryLogCurrentException(
                        log, fmt::format("Failed to get CREATE TABLE query for snapshot of {}.{}", database_name, table_name));
                }

                String create_query_str;
                if (create_query_ast)
                    create_query_str = create_query_ast->formatForLogging();

                const String engine_name = storage->getName();
                auto dependencies = collectTableDependencies(global_context, table_id, create_query_ast);

                const auto tbl_time_now = std::chrono::system_clock::now();
                add([&](MetadataChangeLogElement & element)
                {
                    element.event_time = timeInSeconds(tbl_time_now);
                    element.event_time_microseconds = timeInMicroseconds(tbl_time_now);
                    element.event_type = MetadataChangeLogElement::TABLE_SNAPSHOT;
                    element.snapshot_id = snapshot_id;
                    element.database = table_id.database_name;
                    element.database_uuid = database_uuid;
                    element.table = table_id.table_name;
                    element.table_uuid = table_id.uuid;
                    element.engine = engine_name;
                    element.create_query = create_query_str;

                    if (metadata_snapshot)
                        fillTableMetadataFields(*metadata_snapshot, element);

                    element.dependencies_database.reserve(dependencies.size());
                    element.dependencies_table.reserve(dependencies.size());
                    for (const auto & dep : dependencies)
                    {
                        element.dependencies_database.push_back(dep.database);
                        element.dependencies_table.push_back(dep.table);
                    }
                });
                ++table_count;
                maybe_flush_batch();
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to iterate tables in database {} for metadata snapshot", database_name));
        }
    }

    if (isSnapshotShutdown())
        return false;

    if (pending_since_flush > 0)
        flushSnapshotBatch();

    LOG_INFO(log, "Flushed metadata snapshot '{}' with {} databases and {} tables", snapshot_id, db_count, table_count);
    return true;
}

}
