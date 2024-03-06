#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/executeQuery.h>
#include <Optimizer/Statistics/IStatisticsStorage.h>
#include <Optimizer/Statistics/Utils.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/Stopwatch.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace
{
ContextMutablePtr createQueryContext(ContextMutablePtr context_ = nullptr, SettingsChanges setting_changes = {});

/// Load statistics into memory
std::optional<TableRowCount> loadTableStats(const StorageID & storage_id, const String & cluster_name);
std::shared_ptr<ColumnStatisticsMap> loadColumnStats(const StorageID & storage_id, const String & cluster_name);

/// Collect statistics for a table
void collectTableStats(const StorageID & storage_id, ContextMutablePtr context);
void collectColumnStats(const StorageID & storage_id, const Names & columns, ContextMutablePtr context);
}

void IStatisticsStorage::prepareTables(ContextPtr global_context)
{
    auto execute_create_table = [](ASTPtr create_query_ast, ContextMutablePtr query_context)
    {
        InterpreterCreateQuery interpreter(create_query_ast, query_context);
        interpreter.setInternal(true);
        interpreter.execute();
    };

    auto get_create_table_query = [](StatsTableDefinitionDesc & table_def, ContextMutablePtr query_context) -> ASTPtr
    {
        auto create = std::make_shared<ASTCreateQuery>();

        create->setDatabase(table_def.getDataBaseName());
        create->setTable(table_def.getTableName());

        auto columns = std::make_shared<ASTColumns>();
        columns->set(columns->columns, InterpreterCreateQuery::formatColumns(table_def.getNamesAndTypesList(), {}));
        create->set(create->columns_list, columns);

        String sql_storage = table_def.getStorage();
        ParserStorage storage_parser{ParserStorage::TABLE_ENGINE};
        ASTPtr ast_storage = parseQuery(
            storage_parser,
            sql_storage.data(),
            sql_storage.data() + sql_storage.size(),
            "Storage to create table for " + String(STATISTICS_DATABASE_NAME) + "." + TABLE_STATS_TABLE_NAME,
            0,
            DBMS_DEFAULT_MAX_PARSER_DEPTH);

        create->set(create->storage, ast_storage);

        /// Write additional (default) settings for MergeTree engine to make it make it possible to compare ASTs
        /// and recreate tables on settings changes.
        auto storage_settings = std::make_unique<MergeTreeSettings>(query_context->getMergeTreeSettings());
        storage_settings->loadFromQuery(*create->storage, query_context, false);

        return create;
    };

    Poco::Logger * log = &Poco::Logger::get("IStatisticsStorage");
    auto create_table = [&execute_create_table, &get_create_table_query, &global_context, &log](StatsTableDefinitionDesc & table_def)
    {
        auto table = DatabaseCatalog::instance().tryGetTable({table_def.getDataBaseName(), table_def.getTableName()}, global_context);
        if (!table)
        {
            LOG_DEBUG(log, "Creating new table {}", table_def.getDatabaseAndTable());

            auto query_context = Context::createCopy(global_context);
            query_context->makeQueryContext();

            auto create_query_ast = get_create_table_query(table_def, query_context);
            execute_create_table(create_query_ast, query_context);
        }
    };

    /// create table statistics table
    StatsForTableDesc table_def;
    create_table(table_def);

    /// create column statistics table
    StatsForColumnDesc column_def;
    create_table(column_def);
}

/// TODO add strategies if one node in cluster has no statistics
StatsPtr StatisticsLoader::load(const StorageID & storage_id, const String & cluster_name)
{
    /// 1. load table row count
    auto row_count = loadTableStats(storage_id, cluster_name);

    if (!row_count.has_value())
        return nullptr;

    /// 2. load column statistics
    auto column_stats = loadColumnStats(storage_id, cluster_name);

    return std::make_shared<Stats>(*row_count, *column_stats);
}

void StatisticsCollector::collect(const StorageID & storage_id, const Names & columns, ContextMutablePtr context)
{
    IStatisticsStorage::prepareTables(Context::getGlobalContextInstance());

    /// 1. update table row count
    collectTableStats(storage_id, context);

    /// 2. update column statistics
    collectColumnStats(storage_id, columns, context);
}

StatsPtr IStatisticsStorage::get(const StorageID &, const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}

void IStatisticsStorage::collect(const StorageID &, const Names &, ContextMutablePtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}

void IStatisticsStorage::loadAll()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}

void IStatisticsStorage::shutdown()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}


String StatsForTableDesc::getDataBaseName()
{
    return IStatisticsStorage::STATISTICS_DATABASE_NAME;
}

String StatsForTableDesc::getTableName()
{
    return IStatisticsStorage::TABLE_STATS_TABLE_NAME;
}

String StatsForTableDesc::getDatabaseAndTable()
{
    return String(IStatisticsStorage::STATISTICS_DATABASE_NAME) + "." + IStatisticsStorage::TABLE_STATS_TABLE_NAME;
}

NamesAndTypesList StatsForTableDesc::getNamesAndTypesList()
{
    return {
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"db", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"row_count", std::make_shared<DataTypeInt64>()}};
}

String StatsForTableDesc::getStorage()
{
    return "ENGINE = MergeTree()\n"
           "ORDER BY (db, table)";
}

String StatsForColumnDesc::getDataBaseName()
{
    return IStatisticsStorage::STATISTICS_DATABASE_NAME;
}

String StatsForColumnDesc::getTableName()
{
    return IStatisticsStorage::COLUMN_STATS_TABLE_NAME;
}

String StatsForColumnDesc::getDatabaseAndTable()
{
    return String(IStatisticsStorage::STATISTICS_DATABASE_NAME) + "." + IStatisticsStorage::COLUMN_STATS_TABLE_NAME;
}

NamesAndTypesList StatsForColumnDesc::getNamesAndTypesList()
{
    AggregateFunctionProperties properties;
    DataTypes agg_function_arg_data_types{std::make_shared<DataTypeString>()};
    auto agg_function = AggregateFunctionFactory::instance().get("uniq", NullsAction::IGNORE_NULLS, agg_function_arg_data_types, {}, properties);

    return {
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"db", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"column", std::make_shared<DataTypeString>()},
        {"ndv", std::make_shared<DataTypeAggregateFunction>(agg_function, agg_function_arg_data_types, Array{})},
        {"min_value", std::make_shared<DataTypeFloat64>()},
        {"max_value", std::make_shared<DataTypeFloat64>()},
        {"avg_row_size", std::make_shared<DataTypeFloat64>()}};
}

String StatsForColumnDesc::getStorage()
{
    return "ENGINE = MergeTree()\n"
           "ORDER BY (db, table, column)";
}

namespace
{
ContextMutablePtr createQueryContext(ContextMutablePtr context_, SettingsChanges setting_changes)
{
    auto query_context = Context::createCopy(context_ == nullptr ? Context::getGlobalContextInstance() : context_);

    query_context->makeQueryContext();
    query_context->setCurrentQueryId(""); /// Not use user query id

    setting_changes.emplace_back("allow_experimental_query_coordination", false);
    query_context->applySettingsChanges(setting_changes);

    return query_context;
}

std::optional<TableRowCount> loadTableStats(const StorageID & storage_id, const String & cluster_name)
{
    String sql = fmt::format(
        "SELECT sum(row_count) FROM cluster({}, {}, {}) WHERE db='{}' and table='{}'",
        cluster_name,
        IStatisticsStorage::STATISTICS_DATABASE_NAME,
        IStatisticsStorage::TABLE_STATS_TABLE_NAME,
        storage_id.getDatabaseName(),
        storage_id.getTableName());

    auto load_query_context = createQueryContext();
    StorageID stats_table(IStatisticsStorage::STATISTICS_DATABASE_NAME, IStatisticsStorage::TABLE_STATS_TABLE_NAME);

    auto stats_table_meta = DatabaseCatalog::instance().tryGetTable(stats_table, load_query_context);
    if (!stats_table_meta)
        return std::nullopt;

    try
    {
        auto block_io = executeQuery(sql, load_query_context, {.internal = true}).second;
        auto executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io.pipeline);

        Block block;
        executor->pull(block);

        if (!block)
            return std::nullopt;

        return block.getByPosition(0).column->getUInt(0);
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::get("IStatisticsStorage"), "Error when execute load table statistics query.");
        return std::nullopt;
    }
}

std::shared_ptr<ColumnStatisticsMap> loadColumnStats(const StorageID & storage_id, const String & cluster_name)
{
    String sql = fmt::format(
        "SELECT column, uniqMerge(ndv) as ndv, min(min_value) as min_value, max(max_value) as max_value, avg(avg_row_size) as "
        "avg_row_size FROM cluster({}, {}, {}) WHERE db='{}' and table='{}' GROUP BY column",
        cluster_name,
        IStatisticsStorage::STATISTICS_DATABASE_NAME,
        IStatisticsStorage::COLUMN_STATS_TABLE_NAME,
        storage_id.getDatabaseName(),
        storage_id.getTableName());

    auto load_query_context = createQueryContext();
    StorageID stats_table(IStatisticsStorage::STATISTICS_DATABASE_NAME, IStatisticsStorage::COLUMN_STATS_TABLE_NAME);

    auto stats_table_meta = DatabaseCatalog::instance().tryGetTable(stats_table, load_query_context);
    if (!stats_table_meta)
        return nullptr;

    auto block_io = executeQuery(sql, load_query_context, {.internal = true}).second;

    auto executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io.pipeline);
    auto table_columns = DatabaseCatalog::instance().getTable(storage_id, load_query_context)->getInMemoryMetadata().columns;

    Block block;
    std::shared_ptr<ColumnStatisticsMap> column_stats_map = std::make_shared<ColumnStatisticsMap>();

    while (!block && executor->pull(block))
    {
        for (size_t i = 0; i < block.rows(); i++)
        {
            auto column = block.getByPosition(0).column->getDataAt(i).toString();
            auto column_stats = std::make_shared<ColumnStatistics>();

            column_stats->setNdv(block.getByPosition(1).column->getFloat64(i));
            column_stats->setMinValue(block.getByPosition(2).column->getFloat64(i));
            column_stats->setMaxValue(block.getByPosition(3).column->getFloat64(i));
            column_stats->setAvgRowSize(block.getByPosition(4).column->getFloat64(i));
            column_stats->setDataType(table_columns.get(column).type);

            column_stats_map->insert({column, column_stats});
        }
    }

    return column_stats_map;
}

void collectTableStats(const StorageID & storage_id, ContextMutablePtr context)
{
    const auto time_now = std::chrono::system_clock::now();
    auto event_time = timeInSeconds(time_now);

    String delete_sql = fmt::format(
        "DELETE FROM {}.{} WHERE db='{}' and table='{}'",
        IStatisticsStorage::STATISTICS_DATABASE_NAME,
        IStatisticsStorage::TABLE_STATS_TABLE_NAME,
        storage_id.getDatabaseName(),
        storage_id.getTableName());

    executeQuery(delete_sql, createQueryContext(context), {.internal = true});

    String insert_sql = fmt::format(
        "INSERT INTO {}.{} SELECT {}, '{}', '{}', count(*) FROM {}",
        IStatisticsStorage::STATISTICS_DATABASE_NAME,
        IStatisticsStorage::TABLE_STATS_TABLE_NAME,
        event_time,
        storage_id.getDatabaseName(),
        storage_id.getTableName(),
        storage_id.getFullNameNotQuoted());

    SettingsChanges setting_changes;
    setting_changes.setSetting("optimize_trivial_count_query", 1); /// always enable trivial count optimization
    auto block_io = executeQuery(insert_sql, createQueryContext(context, setting_changes), {.internal = true}).second;
    auto executor = std::make_unique<CompletedPipelineExecutor>(block_io.pipeline);
    executor->execute();
}

void collectColumnStats(const StorageID & storage_id, const Names & columns, ContextMutablePtr context)
{
    Stopwatch watch;

    const auto time_now = std::chrono::system_clock::now();
    auto event_time = timeInSeconds(time_now);

    /// Collecting columns statistics in batch TODO add to settings
    size_t batch = 20;
    auto table_columns = DatabaseCatalog::instance().getTable(storage_id, context)->getInMemoryMetadata().columns;

    for (size_t i = 0; i < columns.size(); i += batch)
    {
        auto batch_end = std::min(i + batch, columns.size());

        /// TODO calculate avg_row_size by datatype and real dataset
        Float64 avg_row_size = 8.0;

        /// 1. deleting the old ones
        WriteBufferFromOwnString joined_columns;
        for (size_t j = i; j < batch_end; j++)
        {
            joined_columns << "'" << columns[j] << "'";
            if (j != batch_end - 1)
                joined_columns << ", ";
        }
        String delete_sql = fmt::format(
            "DELETE FROM {}.{} WHERE db='{}' and table='{}' and column in ({})",
            IStatisticsStorage::STATISTICS_DATABASE_NAME,
            IStatisticsStorage::COLUMN_STATS_TABLE_NAME,
            storage_id.getDatabaseName(),
            storage_id.getTableName(),
            joined_columns.str());
        executeQuery(delete_sql, createQueryContext(context), {.internal = true});

        /// 2. build selecting query
        WriteBufferFromOwnString select_sql;
        select_sql << "SELECT ";

        for (size_t j = i; j < batch_end; j++)
        {
            const auto & column = columns[j];

            select_sql << "toDateTime(" << toString(event_time) << "), ";
            select_sql << "'" << storage_id.getDatabaseName() << "', ";
            select_sql << "'" << storage_id.getTableName() << "', ";
            select_sql << "'" << column << "', ";
            if (table_columns.get(column).type->getTypeId() == TypeIndex::Nullable)
                select_sql << "uniqState(case when " << backQuoteIfNeed(column) << " is null then '' else  cast(" << backQuoteIfNeed(column)
                           << ", 'String') end)"
                           << ", ";
            else
                select_sql << "uniqState(cast(" << backQuoteIfNeed(column) << ", 'String'))"
                           << ", ";
            if (canConvertToFloat64(table_columns.get(column).type))
            {
                select_sql << "min(toFloat64OrDefault(" << backQuoteIfNeed(column) << "))"
                           << ", ";
                select_sql << "max(toFloat64OrDefault(" << backQuoteIfNeed(column) << "))"
                           << ", ";
            }
            else
            {
                /// Wrap function 'toString'
                select_sql << "min(toFloat64OrDefault(toString(" << backQuoteIfNeed(column) << ")))"
                           << ", ";
                select_sql << "max(toFloat64OrDefault(toString(" << backQuoteIfNeed(column) << ")))"
                           << ", ";
            }

            select_sql << "toFloat64(" << toString(avg_row_size) << ")";

            if (j != batch_end - 1)
                select_sql << ", ";
            else
                select_sql << " ";
        }
        select_sql << "FROM " << storage_id.getFullTableName();

        String select_sql_str = select_sql.str();
        LOG_TRACE(
            &Poco::Logger::get("IStatisticsStorage"),
            "Collect column statistics for {}, select sql {}",
            storage_id.getFullNameNotQuoted(),
            select_sql_str);

        /// 3. execute query
        auto block_io = executeQuery(select_sql_str, createQueryContext(context), {.internal = true}).second;
        auto executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io.pipeline);

        /// 4. create inserting executor
        std::unique_ptr<ASTInsertQuery> insert_query = std::make_unique<ASTInsertQuery>();
        insert_query->table_id = StorageID(IStatisticsStorage::STATISTICS_DATABASE_NAME, IStatisticsStorage::COLUMN_STATS_TABLE_NAME);
        ASTPtr insert_ast(insert_query.release());

        auto insert_context = Context::createCopy(Context::getGlobalContextInstance());
        insert_context->makeQueryContext();
        InterpreterInsertQuery insert_interpreter(insert_ast, insert_context);
        BlockIO io = insert_interpreter.execute();

        PushingPipelineExecutor insert_executor(io.pipeline);
        insert_executor.start();

        /// 5. insert into storage
        Block block;
        while (!block && executor->pull(block))
        {
            /// Materialize cost column to avoid failure when SquashingTransform
            /// trys to squash chunks in the following insertion.
            materializeBlockInplace(block);

            /// How many columns to collect at this time.
            size_t batch_count = batch_end - i;
            /// column count for table COLUMN_STATS_TABLE_NAME
            size_t column_count = 8;

            chassert(block.columns() == column_count * batch_count);

            /// Split block columns by rows
            auto block_columns = block.getColumns();
            Columns one_row;

            for (size_t j = 0; j < batch_count; j++)
            {
                size_t start = j * column_count;
                for (size_t n = start; n < start + column_count; n++)
                    one_row.emplace_back(std::move(block_columns[n]));
                insert_executor.push({one_row, 1});
                one_row.clear();
            }
        }
        insert_executor.finish();
    }

    LOG_DEBUG(
        &Poco::Logger::get("IStatisticsStorage"),
        "Collect column statistics for {}, cost {}s",
        storage_id.getFullNameNotQuoted(),
        watch.elapsedSeconds());
}

}

}
