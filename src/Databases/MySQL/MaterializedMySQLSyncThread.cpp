#include "config_core.h"

#if USE_MYSQL

#include <Databases/MySQL/MaterializedMySQLSyncThread.h>
#include <cstdlib>
#include <random>
#include <string_view>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnDecimal.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Databases/MySQL/DatabaseMaterializedMySQL.h>
#include <Databases/MySQL/MaterializeMetadata.h>
#include <Processors/Sources/MySQLSource.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Storages/StorageMergeTree.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <base/sleep.h>
#include <base/bit_cast.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_MYSQL_VARIABLE;
    extern const int SYNC_MYSQL_USER_ACCESS_ERROR;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_READ_ALL_DATA;
}

static constexpr auto MYSQL_BACKGROUND_THREAD_NAME = "MySQLDBSync";

static ContextMutablePtr createQueryContext(ContextPtr context)
{
    Settings new_query_settings = context->getSettings();
    new_query_settings.insert_allow_materialized_columns = true;

    /// To avoid call AST::format
    /// TODO: We need to implement the format function for MySQLAST
    new_query_settings.enable_global_with_statement = false;

    auto query_context = Context::createCopy(context);
    query_context->setSettings(new_query_settings);
    query_context->setInternalQuery(true);

    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    query_context->setCurrentQueryId(""); // generate random query_id
    return query_context;
}

static BlockIO tryToExecuteQuery(const String & query_to_execute, ContextMutablePtr query_context, const String & database, const String & comment)
{
    try
    {
        if (!database.empty())
            query_context->setCurrentDatabase(database);

        return executeQuery("/*" + comment + "*/ " + query_to_execute, query_context, true);
    }
    catch (...)
    {
        tryLogCurrentException(
            &Poco::Logger::get("MaterializedMySQLSyncThread(" + database + ")"),
            "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }
}


MaterializedMySQLSyncThread::~MaterializedMySQLSyncThread()
{
    try
    {
        stopSynchronization();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

static void checkMySQLVariables(const mysqlxx::Pool::Entry & connection, const Settings & settings)
{
    Block variables_header{
        {std::make_shared<DataTypeString>(), "Variable_name"},
        {std::make_shared<DataTypeString>(), "Value"}
    };

    const String & check_query = "SHOW VARIABLES;";

    StreamSettings mysql_input_stream_settings(settings, false, true);
    auto variables_input = std::make_unique<MySQLSource>(connection, check_query, variables_header, mysql_input_stream_settings);

    std::unordered_map<String, String> variables_error_message{
        {"log_bin", "ON"},
        {"binlog_format", "ROW"},
        {"binlog_row_image", "FULL"},
        {"default_authentication_plugin", "mysql_native_password"},
        {"log_bin_use_v1_row_events", "OFF"}
    };

    QueryPipeline pipeline(std::move(variables_input));

    PullingPipelineExecutor executor(pipeline);
    Block variables_block;
    while (executor.pull(variables_block))
    {
        ColumnPtr variable_name_column = variables_block.getByName("Variable_name").column;
        ColumnPtr variable_value_column = variables_block.getByName("Value").column;

        for (size_t index = 0; index < variables_block.rows(); ++index)
        {
            const auto & error_message_it = variables_error_message.find(variable_name_column->getDataAt(index).toString());
            const String variable_val = variable_value_column->getDataAt(index).toString();

            if (error_message_it != variables_error_message.end() && variable_val == error_message_it->second)
                variables_error_message.erase(error_message_it);
        }
    }

    if  (!variables_error_message.empty())
    {
        bool first = true;
        WriteBufferFromOwnString error_message;
        error_message << "Illegal MySQL variables, the MaterializedMySQL engine requires ";
        for (const auto & [variable_name, variable_error_val] : variables_error_message)
        {
            error_message << (first ? "" : ", ") << variable_name << "='" << variable_error_val << "'";

            if (first)
                first = false;
        }

        throw Exception(error_message.str(), ErrorCodes::ILLEGAL_MYSQL_VARIABLE);
    }
}

static std::tuple<String, String> tryExtractTableNameFromDDL(const String & ddl)
{
    String table_name;
    String database_name;
    if (ddl.empty()) return std::make_tuple(database_name, table_name);

    bool parse_failed = false;
    Tokens tokens(ddl.data(), ddl.data() + ddl.size());
    IParser::Pos pos(tokens, 0);
    Expected expected;
    ASTPtr res;
    ASTPtr table;
    if (ParserKeyword("CREATE TEMPORARY TABLE").ignore(pos, expected) || ParserKeyword("CREATE TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF NOT EXISTS").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
    }
    else if (ParserKeyword("ALTER TABLE").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
    }
    else if (ParserKeyword("DROP TABLE").ignore(pos, expected) || ParserKeyword("DROP TEMPORARY TABLE").ignore(pos, expected))
    {
        ParserKeyword("IF EXISTS").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
    }
    else if (ParserKeyword("TRUNCATE").ignore(pos, expected))
    {
        ParserKeyword("TABLE").ignore(pos, expected);
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
    }
    else if (ParserKeyword("RENAME TABLE").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(true).parse(pos, table, expected))
            parse_failed = true;
    }
    else
    {
        parse_failed = true;
    }
    if (!parse_failed)
    {
        if (auto table_id = table->as<ASTTableIdentifier>()->getTableId())
        {
            database_name = table_id.database_name;
            table_name = table_id.table_name;
        }
    }
    return std::make_tuple(database_name, table_name);
}

MaterializedMySQLSyncThread::MaterializedMySQLSyncThread(
    ContextPtr context_,
    const String & database_name_,
    const String & mysql_database_name_,
    mysqlxx::Pool && pool_,
    MySQLClient && client_,
    MaterializedMySQLSettings * settings_)
    : WithContext(context_->getGlobalContext())
    , log(&Poco::Logger::get("MaterializedMySQLSyncThread"))
    , database_name(database_name_)
    , mysql_database_name(mysql_database_name_)
    , pool(std::move(pool_)) /// NOLINT
    , client(std::move(client_))
    , settings(settings_)
{
    query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(database_name) + ", " + backQuoteIfNeed(mysql_database_name) + ") ";

    if (!settings->materialized_mysql_tables_list.value.empty())
    {
        Names tables_list;
        boost::split(tables_list, settings->materialized_mysql_tables_list.value, [](char c){ return c == ','; });
        for (String & table_name: tables_list)
        {
            boost::trim(table_name);
            materialized_tables_list.insert(table_name);
        }
    }
}

void MaterializedMySQLSyncThread::synchronization()
{
    setThreadName(MYSQL_BACKGROUND_THREAD_NAME);

    try
    {
        MaterializeMetadata metadata(
            DatabaseCatalog::instance().getDatabase(database_name)->getMetadataPath() + "/.metadata", getContext()->getSettingsRef());
        bool need_reconnect = true;

        Stopwatch watch;
        Buffers buffers(database_name);

        while (!isCancelled())
        {
            if (need_reconnect)
            {
                if (!prepareSynchronized(metadata))
                    break;
                need_reconnect = false;
            }

            /// TODO: add gc task for `sign = -1`(use alter table delete, execute by interval. need final state)
            UInt64 max_flush_time = settings->max_flush_data_time;

            try
            {
                BinlogEventPtr binlog_event = client.readOneBinlogEvent(std::max(UInt64(1), max_flush_time - watch.elapsedMilliseconds()));
                if (binlog_event)
                    onEvent(buffers, binlog_event, metadata);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::CANNOT_READ_ALL_DATA || settings->max_wait_time_when_mysql_unavailable < 0)
                    throw;

                flushBuffersData(buffers, metadata);
                LOG_INFO(log, "Lost connection to MySQL");
                need_reconnect = true;
                setSynchronizationThreadException(std::current_exception());
                sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
                continue;
            }
            if (watch.elapsedMilliseconds() > max_flush_time || buffers.checkThresholds(
                    settings->max_rows_in_buffer, settings->max_bytes_in_buffer,
                    settings->max_rows_in_buffers, settings->max_bytes_in_buffers)
                )
            {
                watch.restart();
                flushBuffersData(buffers, metadata);
            }
        }
    }
    catch (...)
    {
        client.disconnect();
        tryLogCurrentException(log);
        setSynchronizationThreadException(std::current_exception());
    }
}

void MaterializedMySQLSyncThread::stopSynchronization()
{
    if (!sync_quit && background_thread_pool)
    {
        sync_quit = true;
        if (background_thread_pool->joinable())
            background_thread_pool->join();
        client.disconnect();
    }
}

void MaterializedMySQLSyncThread::startSynchronization()
{
    background_thread_pool = std::make_unique<ThreadFromGlobalPool>([this]() { synchronization(); });
}

void MaterializedMySQLSyncThread::assertMySQLAvailable()
{
    try
    {
        checkMySQLVariables(pool.get(/* wait_timeout= */ UINT64_MAX), getContext()->getSettingsRef());
    }
    catch (const mysqlxx::ConnectionFailed & e)
    {
        if (e.errnum() == ER_ACCESS_DENIED_ERROR
            || e.errnum() == ER_DBACCESS_DENIED_ERROR)
            throw Exception("MySQL SYNC USER ACCESS ERR: mysql sync user needs "
                            "at least GLOBAL PRIVILEGES:'RELOAD, REPLICATION SLAVE, REPLICATION CLIENT' "
                            "and SELECT PRIVILEGE on Database " + mysql_database_name
                            , ErrorCodes::SYNC_MYSQL_USER_ACCESS_ERROR);
        else if (e.errnum() == ER_BAD_DB_ERROR)
            throw Exception("Unknown database '" + mysql_database_name + "' on MySQL", ErrorCodes::UNKNOWN_DATABASE);
        else
            throw;
    }
}

static inline void cleanOutdatedTables(const String & database_name, ContextPtr context)
{
    String cleaning_table_name;
    try
    {
        auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");
        const DatabasePtr & clean_database = DatabaseCatalog::instance().getDatabase(database_name);

        for (auto iterator = clean_database->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            auto query_context = createQueryContext(context);
            CurrentThread::QueryScope query_scope(query_context);

            String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
            cleaning_table_name = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(iterator->name());
            tryToExecuteQuery(" DROP TABLE " + cleaning_table_name, query_context, database_name, comment);
        }
    }
    catch (Exception & exception)
    {
        exception.addMessage("While executing " + (cleaning_table_name.empty() ? "cleanOutdatedTables" : cleaning_table_name));
        throw;
    }
}

static inline QueryPipeline
getTableOutput(const String & database_name, const String & table_name, ContextMutablePtr query_context, bool insert_materialized = false)
{
    const StoragePtr & storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);

    WriteBufferFromOwnString insert_columns_str;
    const StorageInMemoryMetadata & storage_metadata = storage->getInMemoryMetadata();
    const ColumnsDescription & storage_columns = storage_metadata.getColumns();
    const NamesAndTypesList & insert_columns_names = insert_materialized ? storage_columns.getAllPhysical() : storage_columns.getOrdinary();


    for (auto iterator = insert_columns_names.begin(); iterator != insert_columns_names.end(); ++iterator)
    {
        if (iterator != insert_columns_names.begin())
            insert_columns_str << ", ";

        insert_columns_str << backQuoteIfNeed(iterator->name);
    }


    String comment = "Materialize MySQL step 1: execute dump data";
    BlockIO res = tryToExecuteQuery("INSERT INTO " + backQuote(table_name) + " (" + insert_columns_str.str() + ")" + " VALUES",
        query_context, database_name, comment);

    return std::move(res.pipeline);
}

static inline String rewriteMysqlQueryColumn(mysqlxx::Pool::Entry & connection, const String & database_name, const String & table_name, const Settings & global_settings)
{
    Block tables_columns_sample_block
            {
                    { std::make_shared<DataTypeString>(),   "column_name" },
                    { std::make_shared<DataTypeString>(),   "column_type" }
            };

    const String & query =  "SELECT COLUMN_NAME AS column_name, COLUMN_TYPE AS column_type FROM INFORMATION_SCHEMA.COLUMNS"
                            " WHERE TABLE_SCHEMA = '"  + backQuoteIfNeed(database_name) +
                            "' AND TABLE_NAME = '" + backQuoteIfNeed(table_name) +  "' ORDER BY ORDINAL_POSITION";

    StreamSettings mysql_input_stream_settings(global_settings, false, true);
    auto mysql_source = std::make_unique<MySQLSource>(connection, query, tables_columns_sample_block, mysql_input_stream_settings);

    Block block;
    WriteBufferFromOwnString query_columns;
    QueryPipeline pipeline(std::move(mysql_source));
    PullingPipelineExecutor executor(pipeline);
    while (executor.pull(block))
    {
        const auto & column_name_col = *block.getByPosition(0).column;
        const auto & column_type_col = *block.getByPosition(1).column;
        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
        {
            String column_name = column_name_col[i].safeGet<String>();
            String column_type = column_type_col[i].safeGet<String>();
            //we can do something special conversion to guarantee select results is the same as the binlog parse results
            if (column_type.starts_with("set"))
            {
                query_columns << (backQuote(column_name) + " + 0");
            } else
                query_columns << backQuote(column_name);
            query_columns << ",";
        }
    }
    String query_columns_str = query_columns.str();
    return query_columns_str.substr(0, query_columns_str.length() - 1);
}

static inline void dumpDataForTables(
    mysqlxx::Pool::Entry & connection, const std::unordered_map<String, String> & need_dumping_tables,
    const String & query_prefix, const String & database_name, const String & mysql_database_name,
    ContextPtr context, const std::function<bool()> & is_cancelled)
{
    auto iterator = need_dumping_tables.begin();
    for (; iterator != need_dumping_tables.end() && !is_cancelled(); ++iterator)
    {
        try
        {
            const auto & table_name = iterator->first;
            auto query_context = createQueryContext(context);
            CurrentThread::QueryScope query_scope(query_context);

            String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
            tryToExecuteQuery(query_prefix + " " + iterator->second, query_context, database_name, comment); /// create table.

            auto pipeline = getTableOutput(database_name, table_name, query_context);
            StreamSettings mysql_input_stream_settings(context->getSettingsRef());
            String mysql_select_all_query = "SELECT " + rewriteMysqlQueryColumn(connection, mysql_database_name, table_name, context->getSettingsRef()) + " FROM "
                    + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name);
            LOG_INFO(&Poco::Logger::get("MaterializedMySQLSyncThread(" + database_name + ")"), "mysql_select_all_query is {}", mysql_select_all_query);
            auto input = std::make_unique<MySQLSource>(connection, mysql_select_all_query, pipeline.getHeader(), mysql_input_stream_settings);
            auto counting = std::make_shared<CountingTransform>(pipeline.getHeader());
            Pipe pipe(std::move(input));
            pipe.addTransform(counting);
            pipeline.complete(std::move(pipe));

            Stopwatch watch;
            CompletedPipelineExecutor executor(pipeline);
            executor.execute();

            const Progress & progress = counting->getProgress();
            LOG_INFO(&Poco::Logger::get("MaterializedMySQLSyncThread(" + database_name + ")"),
                "Materialize MySQL step 1: dump {}, {} rows, {} in {} sec., {} rows/sec., {}/sec."
                , table_name, formatReadableQuantity(progress.written_rows), formatReadableSizeWithBinarySuffix(progress.written_bytes)
                , watch.elapsedSeconds(), formatReadableQuantity(static_cast<size_t>(progress.written_rows / watch.elapsedSeconds()))
                , formatReadableSizeWithBinarySuffix(static_cast<size_t>(progress.written_bytes / watch.elapsedSeconds())));
        }
        catch (Exception & exception)
        {
            exception.addMessage("While executing dump MySQL {}.{} table data.", mysql_database_name, iterator->first);
            throw;
        }
    }
}

static inline UInt32 randomNumber()
{
    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max());
    return dist6(rng);
}

bool MaterializedMySQLSyncThread::prepareSynchronized(MaterializeMetadata & metadata)
{
    bool opened_transaction = false;
    mysqlxx::PoolWithFailover::Entry connection;

    while (!isCancelled())
    {
        try
        {
            connection = pool.tryGet();
            if (connection.isNull())
            {
                if (settings->max_wait_time_when_mysql_unavailable < 0)
                    throw Exception("Unable to connect to MySQL", ErrorCodes::UNKNOWN_EXCEPTION);
                sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
                continue;
            }

            opened_transaction = false;

            checkMySQLVariables(connection, getContext()->getSettingsRef());
            std::unordered_map<String, String> need_dumping_tables;
            metadata.startReplication(connection, mysql_database_name, opened_transaction, need_dumping_tables, materialized_tables_list);

            if (!need_dumping_tables.empty())
            {
                Position position;
                position.update(metadata.binlog_position, metadata.binlog_file, metadata.executed_gtid_set);

                metadata.transaction(position, [&]()
                {
                    cleanOutdatedTables(database_name, getContext());
                    dumpDataForTables(
                        connection, need_dumping_tables, query_prefix, database_name, mysql_database_name, getContext(), [this]
                        {
                            return isCancelled();
                        });
                });

                const auto & position_message = [&]()
                {
                    WriteBufferFromOwnString buf;
                    position.dump(buf);
                    return buf.str();
                };
                LOG_INFO(log, "MySQL dump database position: \n {}", position_message());
            }

            if (opened_transaction)
                connection->query("COMMIT").execute();

            client.connect();
            client.startBinlogDumpGTID(randomNumber(), mysql_database_name, materialized_tables_list, metadata.executed_gtid_set, metadata.binlog_checksum);

            setSynchronizationThreadException(nullptr);
            return true;
        }
        catch (...)
        {
            tryLogCurrentException(log);

            if (opened_transaction)
                connection->query("ROLLBACK").execute();

            try
            {
                throw;
            }
            catch (const mysqlxx::ConnectionFailed &) {}
            catch (const mysqlxx::BadQuery & e)
            {
                // Lost connection to MySQL server during query
                if (e.code() != CR_SERVER_LOST || settings->max_wait_time_when_mysql_unavailable < 0)
                    throw;
            }

            setSynchronizationThreadException(std::current_exception());
            /// Avoid busy loop when MySQL is not available.
            sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
        }
    }

    return false;
}

void MaterializedMySQLSyncThread::flushBuffersData(Buffers & buffers, MaterializeMetadata & metadata)
{
    if (buffers.data.empty())
        return;

    metadata.transaction(client.getPosition(), [&]() { buffers.commit(getContext()); });

    const auto & position_message = [&]()
    {
        WriteBufferFromOwnString buf;
        client.getPosition().dump(buf);
        return buf.str();
    };
    LOG_INFO(log, "MySQL executed position: \n {}", position_message());
}

static inline void fillSignAndVersionColumnsData(Block & data, Int8 sign_value, UInt64 version_value, size_t fill_size)
{
    MutableColumnPtr sign_mutable_column = IColumn::mutate(std::move(data.getByPosition(data.columns() - 2).column));
    MutableColumnPtr version_mutable_column = IColumn::mutate(std::move(data.getByPosition(data.columns() - 1).column));

    ColumnInt8::Container & sign_column_data = assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();
    ColumnUInt64::Container & version_column_data = assert_cast<ColumnUInt64 &>(*version_mutable_column).getData();

    for (size_t index = 0; index < fill_size; ++index)
    {
        sign_column_data.emplace_back(sign_value);
        version_column_data.emplace_back(version_value);
    }

    data.getByPosition(data.columns() - 2).column = std::move(sign_mutable_column);
    data.getByPosition(data.columns() - 1).column = std::move(version_mutable_column);
}

template <bool assert_nullable = false>
static void writeFieldsToColumn(
    IColumn & column_to, const Row & rows_data, size_t column_index, const std::vector<bool> & mask, ColumnUInt8 * null_map_column = nullptr)
{
    if (ColumnNullable * column_nullable = typeid_cast<ColumnNullable *>(&column_to))
        writeFieldsToColumn<true>(column_nullable->getNestedColumn(), rows_data, column_index, mask, &column_nullable->getNullMapColumn());
    else
    {
        const auto & write_data_to_null_map = [&](const Field & field, size_t row_index)
        {
            if (!mask.empty() && !mask.at(row_index))
                return false;

            if constexpr (assert_nullable)
            {
                if (field.isNull())
                {
                    column_to.insertDefault();
                    null_map_column->insertValue(1);
                    return false;
                }

                null_map_column->insertValue(0);
            }

            return true;
        };

        const auto & write_data_to_column = [&](auto * casted_column, auto from_type, auto to_type)
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                    casted_column->insertValue(static_cast<decltype(to_type)>(value.template get<decltype(from_type)>()));
            }
        };

        if (ColumnInt8 * casted_int8_column = typeid_cast<ColumnInt8 *>(&column_to))
            write_data_to_column(casted_int8_column, UInt64(), Int8());
        else if (ColumnInt16 * casted_int16_column = typeid_cast<ColumnInt16 *>(&column_to))
            write_data_to_column(casted_int16_column, UInt64(), Int16());
        else if (ColumnInt64 * casted_int64_column = typeid_cast<ColumnInt64 *>(&column_to))
            write_data_to_column(casted_int64_column, UInt64(), Int64());
        else if (ColumnUInt8 * casted_uint8_column = typeid_cast<ColumnUInt8 *>(&column_to))
            write_data_to_column(casted_uint8_column, UInt64(), UInt8());
        else if (ColumnUInt16 * casted_uint16_column = typeid_cast<ColumnUInt16 *>(&column_to))
            write_data_to_column(casted_uint16_column, UInt64(), UInt16());
        else if (ColumnUInt32 * casted_uint32_column = typeid_cast<ColumnUInt32 *>(&column_to))
            write_data_to_column(casted_uint32_column, UInt64(), UInt32());
        else if (ColumnUInt64 * casted_uint64_column = typeid_cast<ColumnUInt64 *>(&column_to))
            write_data_to_column(casted_uint64_column, UInt64(), UInt64());
        else if (ColumnFloat32 * casted_float32_column = typeid_cast<ColumnFloat32 *>(&column_to))
            write_data_to_column(casted_float32_column, Float64(), Float32());
        else if (ColumnFloat64 * casted_float64_column = typeid_cast<ColumnFloat64 *>(&column_to))
            write_data_to_column(casted_float64_column, Float64(), Float64());
        else if (ColumnDecimal<Decimal32> * casted_decimal_32_column = typeid_cast<ColumnDecimal<Decimal32> *>(&column_to))
            write_data_to_column(casted_decimal_32_column, Decimal32(), Decimal32());
        else if (ColumnDecimal<Decimal64> * casted_decimal_64_column = typeid_cast<ColumnDecimal<Decimal64> *>(&column_to))
            write_data_to_column(casted_decimal_64_column, Decimal64(), Decimal64());
        else if (ColumnDecimal<Decimal128> * casted_decimal_128_column = typeid_cast<ColumnDecimal<Decimal128> *>(&column_to))
            write_data_to_column(casted_decimal_128_column, Decimal128(), Decimal128());
        else if (ColumnDecimal<Decimal256> * casted_decimal_256_column = typeid_cast<ColumnDecimal<Decimal256> *>(&column_to))
            write_data_to_column(casted_decimal_256_column, Decimal256(), Decimal256());
        else if (ColumnDecimal<DateTime64> * casted_datetime_64_column = typeid_cast<ColumnDecimal<DateTime64> *>(&column_to))
            write_data_to_column(casted_datetime_64_column, DateTime64(), DateTime64());
        else if (ColumnInt32 * casted_int32_column = typeid_cast<ColumnInt32 *>(&column_to))
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                {
                    if (value.getType() == Field::Types::UInt64)
                        casted_int32_column->insertValue(value.get<Int32>());
                    else if (value.getType() == Field::Types::Int64)
                    {
                        /// For MYSQL_TYPE_INT24
                        const Int32 & num = value.get<Int32>();
                        casted_int32_column->insertValue(num & 0x800000 ? num | 0xFF000000 : num);
                    }
                    else
                        throw Exception("LOGICAL ERROR: it is a bug.", ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
        else if (ColumnString * casted_string_column = typeid_cast<ColumnString *>(&column_to))
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                {
                    const String & data = value.get<const String &>();
                    casted_string_column->insertData(data.data(), data.size());
                }
            }
        }
        else if (ColumnFixedString * casted_fixed_string_column = typeid_cast<ColumnFixedString *>(&column_to))
        {
            for (size_t index = 0; index < rows_data.size(); ++index)
            {
                const Tuple & row_data = DB::get<const Tuple &>(rows_data[index]);
                const Field & value = row_data[column_index];

                if (write_data_to_null_map(value, index))
                {
                    const String & data = value.get<const String &>();
                    casted_fixed_string_column->insertData(data.data(), data.size());
                }
            }
        }
        else
            throw Exception("Unsupported data type from MySQL.", ErrorCodes::NOT_IMPLEMENTED);
    }
}

template <Int8 sign>
static size_t onWriteOrDeleteData(const Row & rows_data, Block & buffer, size_t version)
{
    size_t prev_bytes = buffer.bytes();
    for (size_t column = 0; column < buffer.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer.getByPosition(column).column));

        writeFieldsToColumn(*col_to, rows_data, column, {});
        buffer.getByPosition(column).column = std::move(col_to);
    }

    fillSignAndVersionColumnsData(buffer, sign, version, rows_data.size());
    return buffer.bytes() - prev_bytes;
}

static inline bool differenceSortingKeys(const Tuple & row_old_data, const Tuple & row_new_data, const std::vector<size_t> sorting_columns_index)
{
    for (const auto & sorting_column_index : sorting_columns_index)
        if (row_old_data[sorting_column_index] != row_new_data[sorting_column_index])
            return true;

    return false;
}

static inline size_t onUpdateData(const Row & rows_data, Block & buffer, size_t version, const std::vector<size_t> & sorting_columns_index)
{
    if (rows_data.size() % 2 != 0)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    size_t prev_bytes = buffer.bytes();
    std::vector<bool> writeable_rows_mask(rows_data.size());

    for (size_t index = 0; index < rows_data.size(); index += 2)
    {
        writeable_rows_mask[index + 1] = true;
        writeable_rows_mask[index] = differenceSortingKeys(
            DB::get<const Tuple &>(rows_data[index]), DB::get<const Tuple &>(rows_data[index + 1]), sorting_columns_index);
    }

    for (size_t column = 0; column < buffer.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer.getByPosition(column).column));

        writeFieldsToColumn(*col_to, rows_data, column, writeable_rows_mask);
        buffer.getByPosition(column).column = std::move(col_to);
    }

    MutableColumnPtr sign_mutable_column = IColumn::mutate(std::move(buffer.getByPosition(buffer.columns() - 2).column));
    MutableColumnPtr version_mutable_column = IColumn::mutate(std::move(buffer.getByPosition(buffer.columns() - 1).column));

    ColumnInt8::Container & sign_column_data = assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();
    ColumnUInt64::Container & version_column_data = assert_cast<ColumnUInt64 &>(*version_mutable_column).getData();

    for (size_t index = 0; index < rows_data.size(); index += 2)
    {
        if (likely(!writeable_rows_mask[index]))
        {
            sign_column_data.emplace_back(1);
            version_column_data.emplace_back(version);
        }
        else
        {
            /// If the sorting keys is modified, we should cancel the old data, but this should not happen frequently
            sign_column_data.emplace_back(-1);
            sign_column_data.emplace_back(1);
            version_column_data.emplace_back(version);
            version_column_data.emplace_back(version);
        }
    }

    buffer.getByPosition(buffer.columns() - 2).column = std::move(sign_mutable_column);
    buffer.getByPosition(buffer.columns() - 1).column = std::move(version_mutable_column);
    return buffer.bytes() - prev_bytes;
}

void MaterializedMySQLSyncThread::onEvent(Buffers & buffers, const BinlogEventPtr & receive_event, MaterializeMetadata & metadata)
{
    if (receive_event->type() == MYSQL_WRITE_ROWS_EVENT)
    {
        WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(write_rows_event.table, getContext());
        size_t bytes = onWriteOrDeleteData<1>(write_rows_event.rows, buffer->first, ++metadata.data_version);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), write_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_UPDATE_ROWS_EVENT)
    {
        UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(update_rows_event.table, getContext());
        size_t bytes = onUpdateData(update_rows_event.rows, buffer->first, ++metadata.data_version, buffer->second);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), update_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_DELETE_ROWS_EVENT)
    {
        DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(delete_rows_event.table, getContext());
        size_t bytes = onWriteOrDeleteData<-1>(delete_rows_event.rows, buffer->first, ++metadata.data_version);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), delete_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_QUERY_EVENT)
    {
        QueryEvent & query_event = static_cast<QueryEvent &>(*receive_event);
        Position position_before_ddl;
        position_before_ddl.update(metadata.binlog_position, metadata.binlog_file, metadata.executed_gtid_set);
        metadata.transaction(position_before_ddl, [&]() { buffers.commit(getContext()); });
        metadata.transaction(client.getPosition(),[&](){ executeDDLAtomic(query_event); });
    }
    else
    {
        /// MYSQL_UNHANDLED_EVENT
        if (receive_event->header.type == ROTATE_EVENT)
        {
            /// Some behaviors(such as changing the value of "binlog_checksum") rotate the binlog file.
            /// To ensure that the synchronization continues, we need to handle these events
            metadata.fetchMasterVariablesValue(pool.get(/* wait_timeout= */ UINT64_MAX));
            client.setBinlogChecksum(metadata.binlog_checksum);
        }
        else if (receive_event->header.type != HEARTBEAT_EVENT)
        {
            const auto & dump_event_message = [&]()
            {
                WriteBufferFromOwnString buf;
                receive_event->dump(buf);
                return buf.str();
            };

            LOG_DEBUG(log, "Skip MySQL event: \n {}", dump_event_message());
        }
    }
}

void MaterializedMySQLSyncThread::executeDDLAtomic(const QueryEvent & query_event)
{
    try
    {
        auto query_context = createQueryContext(getContext());
        CurrentThread::QueryScope query_scope(query_context);

        String query = query_event.query;
        if (!materialized_tables_list.empty())
        {
             auto [ddl_database_name, ddl_table_name] = tryExtractTableNameFromDDL(query_event.query);

            if (!ddl_table_name.empty())
            {
                ddl_database_name =  ddl_database_name.empty() ? query_event.schema: ddl_database_name;
                if (ddl_database_name != mysql_database_name || !materialized_tables_list.contains(ddl_table_name))
                {
                    LOG_DEBUG(log, "Skip MySQL DDL: \n {}", query_event.query);
                    return;
                }
            }
        }
        String comment = "Materialize MySQL step 2: execute MySQL DDL for sync data";
        String event_database = query_event.schema == mysql_database_name ? database_name : "";
        tryToExecuteQuery(query_prefix + query, query_context, event_database, comment);
    }
    catch (Exception & exception)
    {
        exception.addMessage("While executing MYSQL_QUERY_EVENT. The query: " + query_event.query);

        tryLogCurrentException(log);

        /// If some DDL query was not successfully parsed and executed
        /// Then replication may fail on next binlog events anyway
        if (exception.code() != ErrorCodes::SYNTAX_ERROR)
            throw;
    }
}

void MaterializedMySQLSyncThread::setSynchronizationThreadException(const std::exception_ptr & exception)
{
    assert_cast<DatabaseMaterializedMySQL *>(DatabaseCatalog::instance().getDatabase(database_name).get())->setException(exception);
}

void MaterializedMySQLSyncThread::Buffers::add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes)
{
    total_blocks_rows += written_rows;
    total_blocks_bytes += written_bytes;
    max_block_rows = std::max(block_rows, max_block_rows);
    max_block_bytes = std::max(block_bytes, max_block_bytes);
}

bool MaterializedMySQLSyncThread::Buffers::checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const
{
    return max_block_rows >= check_block_rows || max_block_bytes >= check_block_bytes || total_blocks_rows >= check_total_rows
        || total_blocks_bytes >= check_total_bytes;
}

void MaterializedMySQLSyncThread::Buffers::commit(ContextPtr context)
{
    try
    {
        for (auto & table_name_and_buffer : data)
        {
            auto query_context = createQueryContext(context);
            CurrentThread::QueryScope query_scope(query_context);

            auto input = std::make_shared<SourceFromSingleChunk>(table_name_and_buffer.second->first);
            auto pipeline = getTableOutput(database, table_name_and_buffer.first, query_context, true);
            pipeline.complete(Pipe(std::move(input)));

            CompletedPipelineExecutor executor(pipeline);
            executor.execute();
        }

        data.clear();
        max_block_rows = 0;
        max_block_bytes = 0;
        total_blocks_rows = 0;
        total_blocks_bytes = 0;
    }
    catch (...)
    {
        data.clear();
        throw;
    }
}

MaterializedMySQLSyncThread::Buffers::BufferAndSortingColumnsPtr MaterializedMySQLSyncThread::Buffers::getTableDataBuffer(
    const String & table_name, ContextPtr context)
{
    const auto & iterator = data.find(table_name);
    if (iterator == data.end())
    {
        StoragePtr storage = DatabaseCatalog::instance().getTable(StorageID(database, table_name), context);

        const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
        BufferAndSortingColumnsPtr & buffer_and_soring_columns = data.try_emplace(
            table_name, std::make_shared<BufferAndSortingColumns>(metadata.getSampleBlock(), std::vector<size_t>{})).first->second;

        Names required_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

        for (const auto & required_name_for_sorting_key : required_for_sorting_key)
            buffer_and_soring_columns->second.emplace_back(
                buffer_and_soring_columns->first.getPositionByName(required_name_for_sorting_key));

        return buffer_and_soring_columns;
    }

    return iterator->second;
}

}

#endif
