#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MaterializeMySQLSyncThread.h>

#    include <cstdlib>
#    include <common/sleep.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>
#    include <Columns/ColumnTuple.h>
#    include <DataStreams/copyData.h>
#    include <DataStreams/OneBlockInputStream.h>
#    include <DataStreams/AddingVersionsBlockOutputStream.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Databases/MySQL/DatabaseMaterializeMySQL.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/executeQuery.h>
#    include <Storages/StorageMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int SYNTAX_ERROR;
}

static constexpr auto MYSQL_BACKGROUND_THREAD_NAME = "MySQLDBSync";

static BlockIO tryToExecuteQuery(const String & query_to_execute, const Context & context_, const String & database, const String & comment)
{
    try
    {
        Context context(context_);
        CurrentThread::QueryScope query_scope(context);
        context.unsafeSetCurrentDatabase(database);
        context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        context.setCurrentQueryId(""); // generate random query_id

        return executeQuery("/*" + comment + "*/ " + query_to_execute, context, true);
    }
    catch (...)
    {
        tryLogCurrentException(
            &Poco::Logger::get("MaterializeMySQLSyncThread(" + database + ")"),
            "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }
}

static inline DatabaseMaterializeMySQL & getDatabase(const String & database_name)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name);

    if (DatabaseMaterializeMySQL * database_materialize = typeid_cast<DatabaseMaterializeMySQL *>(database.get()))
        return *database_materialize;

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializeMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

MaterializeMySQLSyncThread::~MaterializeMySQLSyncThread()
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

MaterializeMySQLSyncThread::MaterializeMySQLSyncThread(
    const Context & context, const String & database_name_, const String & mysql_database_name_,
    mysqlxx::Pool && pool_, MySQLClient && client_, MaterializeMySQLSettings * settings_)
    : log(&Poco::Logger::get("MaterializeMySQLSyncThread")), global_context(context.getGlobalContext()), database_name(database_name_)
    , mysql_database_name(mysql_database_name_), pool(std::move(pool_)), client(std::move(client_)), settings(settings_)
{
    /// TODO: 做简单的check, 失败即报错
    /// binlog_format = ROW binlog_row_image = FULL
    query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(database_name) + ", " + backQuoteIfNeed(mysql_database_name) + ") ";
    startSynchronization();
}

void MaterializeMySQLSyncThread::synchronization()
{
    setThreadName(MYSQL_BACKGROUND_THREAD_NAME);

    try
    {
        if (std::optional<MaterializeMetadata> metadata = prepareSynchronized())
        {
            Stopwatch watch;
            Buffers buffers(database_name);

            while (!isCancelled())
            {
                /// TODO: add gc task for `sign = -1`(use alter table delete, execute by interval. need final state)
                UInt64 max_flush_time = settings->max_flush_data_time;
                BinlogEventPtr binlog_event = client.readOneBinlogEvent(std::max(UInt64(1), max_flush_time - watch.elapsedMilliseconds()));

                {
                    std::unique_lock<std::mutex> lock(sync_mutex);

                    if (binlog_event)
                    {
                        binlog_event->dump();
                        onEvent(buffers, binlog_event, *metadata);
                    }

                    if (watch.elapsedMilliseconds() > max_flush_time || buffers.checkThresholds(
                            settings->max_rows_in_buffer, settings->max_bytes_in_buffer,
                            settings->max_rows_in_buffers, settings->max_bytes_in_buffers)
                        )
                    {
                        watch.restart();

                        if (!buffers.data.empty())
                            flushBuffersData(buffers, *metadata);
                    }
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
        getDatabase(database_name).setException(std::current_exception());
    }
}

void MaterializeMySQLSyncThread::stopSynchronization()
{
    if (!sync_quit)
    {
        {
            sync_quit = true;
            std::lock_guard<std::mutex> lock(sync_mutex);
        }

        sync_cond.notify_one();
        background_thread_pool->join();
    }
}

void MaterializeMySQLSyncThread::startSynchronization()
{
    /// TODO: reset exception.
    background_thread_pool = std::make_unique<ThreadFromGlobalPool>([this]() { synchronization(); });
}

static inline void cleanOutdatedTables(const String & database_name, const Context & context)
{
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");
    const DatabasePtr & clean_database = DatabaseCatalog::instance().getDatabase(database_name);

    for (auto iterator = clean_database->getTablesIterator(context); iterator->isValid(); iterator->next())
    {
        String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
        String table_name = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(iterator->name());
        tryToExecuteQuery(" DROP TABLE " + table_name, context, database_name, comment);
    }
}

static inline BlockOutputStreamPtr getTableOutput(const String & database_name, const String & table_name, const Context & context)
{
    String comment = "Materialize MySQL step 1: execute dump data";
    BlockIO res = tryToExecuteQuery("INSERT INTO " + backQuoteIfNeed(table_name) + " VALUES", context, database_name, comment);

    if (!res.out)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    return res.out;
}

static inline void dumpDataForTables(
    mysqlxx::Pool::Entry & connection, MaterializeMetadata & master_info,
    const String & query_prefix, const String & database_name, const String & mysql_database_name,
    const Context & context, const std::function<bool()> & is_cancelled)
{
    auto iterator = master_info.need_dumping_tables.begin();
    for (; iterator != master_info.need_dumping_tables.end() && !is_cancelled(); ++iterator)
    {
        const auto & table_name = iterator->first;
        String comment = "Materialize MySQL step 1: execute MySQL DDL for dump data";
        tryToExecuteQuery(query_prefix + " " + iterator->second, context, mysql_database_name, comment); /// create table.

        BlockOutputStreamPtr out = std::make_shared<AddingVersionsBlockOutputStream>(master_info.version, getTableOutput(database_name, table_name, context));
        MySQLBlockInputStream input(
            connection, "SELECT * FROM " + backQuoteIfNeed(mysql_database_name) + "." + backQuoteIfNeed(table_name),
            out->getHeader(), DEFAULT_BLOCK_SIZE);
        copyData(input, *out, is_cancelled);
    }
}

std::optional<MaterializeMetadata> MaterializeMySQLSyncThread::prepareSynchronized()
{
    std::unique_lock<std::mutex> lock(sync_mutex);

    bool opened_transaction = false;
    mysqlxx::PoolWithFailover::Entry connection;

    while (!isCancelled())
    {
        try
        {
            LOG_DEBUG(log, "Checking database status.");
            while (!isCancelled() && !DatabaseCatalog::instance().isDatabaseExist(database_name))
                sync_cond.wait_for(lock, std::chrono::seconds(1));
            LOG_DEBUG(log, "Database status is OK.");

            connection = pool.get();
            opened_transaction = false;

            MaterializeMetadata metadata(connection, getDatabase(database_name).getMetadataPath() + "/.metadata", mysql_database_name, opened_transaction);

            if (!metadata.need_dumping_tables.empty())
            {
                metadata.transaction(Position(metadata.binlog_position, metadata.binlog_file), [&]()
                {
                    cleanOutdatedTables(database_name, global_context);
                    dumpDataForTables(connection, metadata, query_prefix, database_name, mysql_database_name, global_context, [this] { return isCancelled(); });
                });
            }

            if (opened_transaction)
                connection->query("COMMIT").execute();

            client.connect();
            client.startBinlogDump(std::rand(), mysql_database_name, metadata.binlog_file, metadata.binlog_position);
            return metadata;
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
            catch (mysqlxx::Exception & )
            {
                /// Avoid busy loop when MySQL is not available.
                sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
            }
        }
    }

    return {};
}

void MaterializeMySQLSyncThread::flushBuffersData(Buffers & buffers, MaterializeMetadata & metadata)
{
    metadata.transaction(client.getPosition(), [&]() { buffers.commit(global_context); });
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

template <Int8 sign>
static size_t onWriteOrDeleteData(const std::vector<Field> & rows_data, Block & buffer, size_t version)
{
    size_t prev_bytes = buffer.bytes();
    for (size_t column = 0; column < buffer.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer.getByPosition(column).column));

        for (size_t index = 0; index < rows_data.size(); ++index)
            col_to->insert(DB::get<const Tuple &>(rows_data[index])[column]);

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

static inline size_t onUpdateData(const std::vector<Field> & rows_data, Block & buffer, size_t version, const std::vector<size_t> & sorting_columns_index)
{
    if (rows_data.size() % 2 != 0)
        throw Exception("LOGICAL ERROR: ", ErrorCodes::LOGICAL_ERROR);

    size_t prev_bytes = buffer.bytes();
    std::vector<bool> difference_sorting_keys_mark(rows_data.size() / 2);

    for (size_t index = 0; index < rows_data.size(); index += 2)
        difference_sorting_keys_mark[index / 2] = differenceSortingKeys(
            DB::get<const Tuple &>(rows_data[index]), DB::get<const Tuple &>(rows_data[index + 1]), sorting_columns_index);

    for (size_t column = 0; column < buffer.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer.getByPosition(column).column));

        for (size_t index = 0; index < rows_data.size(); index += 2)
        {
            if (likely(!difference_sorting_keys_mark[index / 2]))
                col_to->insert(DB::get<const Tuple &>(rows_data[index + 1])[column]);
            else
            {
                /// If the sorting keys is modified, we should cancel the old data, but this should not happen frequently
                col_to->insert(DB::get<const Tuple &>(rows_data[index])[column]);
                col_to->insert(DB::get<const Tuple &>(rows_data[index + 1])[column]);
            }
        }

        buffer.getByPosition(column).column = std::move(col_to);
    }

    MutableColumnPtr sign_mutable_column = IColumn::mutate(std::move(buffer.getByPosition(buffer.columns() - 2).column));
    MutableColumnPtr version_mutable_column = IColumn::mutate(std::move(buffer.getByPosition(buffer.columns() - 1).column));

    ColumnInt8::Container & sign_column_data = assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();
    ColumnUInt64::Container & version_column_data = assert_cast<ColumnUInt64 &>(*version_mutable_column).getData();

    for (size_t index = 0; index < rows_data.size(); index += 2)
    {
        if (likely(!difference_sorting_keys_mark[index / 2]))
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

void MaterializeMySQLSyncThread::onEvent(Buffers & buffers, const BinlogEventPtr & receive_event, MaterializeMetadata & metadata)
{
    if (receive_event->type() == MYSQL_WRITE_ROWS_EVENT)
    {
        WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(write_rows_event.table, global_context);
        size_t bytes = onWriteOrDeleteData<1>(write_rows_event.rows, buffer->first, ++metadata.version);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), write_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_UPDATE_ROWS_EVENT)
    {
        UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(update_rows_event.table, global_context);
        size_t bytes = onUpdateData(update_rows_event.rows, buffer->first, ++metadata.version, buffer->second);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), update_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_DELETE_ROWS_EVENT)
    {
        DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(delete_rows_event.table, global_context);
        size_t bytes = onWriteOrDeleteData<-1>(delete_rows_event.rows, buffer->first, ++metadata.version);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), delete_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_QUERY_EVENT)
    {
        QueryEvent & query_event = static_cast<QueryEvent &>(*receive_event);
        flushBuffersData(buffers, metadata);

        try
        {
            String comment = "Materialize MySQL step 2: execute MySQL DDL for sync data";
            tryToExecuteQuery(query_prefix + query_event.query, global_context, query_event.schema, comment);
        }
        catch (Exception & exception)
        {
            tryLogCurrentException(log);

            if (exception.code() != ErrorCodes::SYNTAX_ERROR)
                throw;
        }
    }
}

bool MaterializeMySQLSyncThread::isMySQLSyncThread()
{
    return getThreadName() == MYSQL_BACKGROUND_THREAD_NAME;
}

void MaterializeMySQLSyncThread::Buffers::add(size_t block_rows, size_t block_bytes, size_t written_rows, size_t written_bytes)
{
    total_blocks_rows += written_rows;
    total_blocks_bytes += written_bytes;
    max_block_rows = std::max(block_rows, max_block_rows);
    max_block_bytes = std::max(block_bytes, max_block_bytes);
}

bool MaterializeMySQLSyncThread::Buffers::checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes)
{
    return max_block_rows >= check_block_rows || max_block_bytes >= check_block_bytes || total_blocks_rows >= check_total_rows
        || total_blocks_bytes >= check_total_bytes;
}

void MaterializeMySQLSyncThread::Buffers::commit(const Context & context)
{
    try
    {
        for (auto & table_name_and_buffer : data)
        {
            OneBlockInputStream input(table_name_and_buffer.second->first);
            BlockOutputStreamPtr out = getTableOutput(database, table_name_and_buffer.first, context);
            copyData(input, *out);
        }

        data.clear();
        max_block_rows = 0;
        max_block_bytes = 0;
        total_blocks_rows = 0;
        total_blocks_bytes = 0;
    }
    catch(...)
    {
        data.clear();
        throw;
    }
}

MaterializeMySQLSyncThread::Buffers::BufferAndSortingColumnsPtr MaterializeMySQLSyncThread::Buffers::getTableDataBuffer(
    const String & table_name, const Context & context)
{
    const auto & iterator = data.find(table_name);
    if (iterator == data.end())
    {
        StoragePtr storage = getDatabase(database).tryGetTable(table_name, context);

        const StorageInMemoryMetadata & metadata = storage->getInMemoryMetadata();
        BufferAndSortingColumnsPtr & buffer_and_soring_columns = data.try_emplace(
            table_name, std::make_shared<BufferAndSortingColumns>(metadata.getSampleBlockNonMaterialized(), std::vector<size_t>{})).first->second;

        if (StorageMergeTree * table_merge_tree = storage->as<StorageMergeTree>())
        {
            Names required_for_sorting_key = metadata.getColumnsRequiredForSortingKey();

            for (const auto & required_name_for_sorting_key : required_for_sorting_key)
                buffer_and_soring_columns->second.emplace_back(
                    buffer_and_soring_columns->first.getPositionByName(required_name_for_sorting_key));

        }

        return buffer_and_soring_columns;
    }

    return iterator->second;
}

}

#endif
