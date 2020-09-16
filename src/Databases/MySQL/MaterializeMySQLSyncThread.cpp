#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MaterializeMySQLSyncThread.h>

#    include <cstdlib>
#    include <random>
#    include <Columns/ColumnTuple.h>
#    include <DataStreams/CountingBlockOutputStream.h>
#    include <DataStreams/OneBlockInputStream.h>
#    include <DataStreams/copyData.h>
#    include <Databases/MySQL/DatabaseMaterializeMySQL.h>
#    include <Databases/MySQL/MaterializeMetadata.h>
#    include <Databases/MySQL/MySQLUtils.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/ReadBufferFromString.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/executeQuery.h>
#    include <Storages/StorageMergeTree.h>
#    include <Common/quoteString.h>
#    include <Common/setThreadName.h>
#    include <common/sleep.h>
#    include <ext/bit_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_MYSQL_VARIABLE;
}

static constexpr auto MYSQL_BACKGROUND_THREAD_NAME = "MySQLDBSync";

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
    const Context & context,
    const String & database_name_,
    const String & mysql_database_name_,
    mysqlxx::Pool & pool_,
    MySQLClient && client_,
    MaterializeMySQLSettings * settings_,
    DatabaseMaterializeMySQL * database_ptr_)
    : log(&Poco::Logger::get("MaterializeMySQLSyncThread"))
    , global_context(context.getGlobalContext())
    , database_name(database_name_)
    , mysql_database_name(mysql_database_name_)
    , pool(pool_)
    , client(std::move(client_))
    , settings(settings_)
    , database_ptr(database_ptr_)
{
    query_prefix = "EXTERNAL DDL FROM MySQL(" + backQuoteIfNeed(database_name) + ", " + backQuoteIfNeed(mysql_database_name) + ") ";
}

void MaterializeMySQLSyncThread::synchronization()
{
    setThreadName(MYSQL_BACKGROUND_THREAD_NAME);

    try
    {
        if (database_ptr != nullptr)
            materialize_metadata = database_ptr->dumpTablesData();

        if (materialize_metadata)
        {
            prepareSynchronized();

            Stopwatch watch;
            Buffers buffers(database_name);

            while (!isCancelled())
            {
                /// TODO: add gc task for `sign = -1`(use alter table delete, execute by interval. need final state)
                UInt64 max_flush_time = settings->max_flush_data_time;
                BinlogEventPtr binlog_event = client.readOneBinlogEvent(std::max(UInt64(1), max_flush_time - watch.elapsedMilliseconds()));

                {
                    if (binlog_event)
                        onEvent(buffers, binlog_event, *materialize_metadata);

                    if (watch.elapsedMilliseconds() > max_flush_time || buffers.checkThresholds(
                            settings->max_rows_in_buffer, settings->max_bytes_in_buffer,
                            settings->max_rows_in_buffers, settings->max_bytes_in_buffers)
                        )
                    {
                        watch.restart();

                        if (!buffers.data.empty())
                            flushBuffersData(buffers, *materialize_metadata);
                    }
                }
            }
        }
    }
    catch (...)
    {
        client.disconnect();
        tryLogCurrentException(log);
        getDatabase(database_name).setException(std::current_exception());
    }
}

void MaterializeMySQLSyncThread::stopSynchronization()
{
    if (!sync_quit && background_thread_pool)
    {
        sync_quit = true;
        background_thread_pool->join();
        client.disconnect();
    }
}

void MaterializeMySQLSyncThread::startSynchronization()
{
    background_thread_pool = std::make_unique<ThreadFromGlobalPool>(
        [this]() { synchronization(); });
}

static inline UInt32 randomNumber()
{
    std::mt19937 rng;
    rng.seed(std::random_device()());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max());
    return dist6(rng);
}

void MaterializeMySQLSyncThread::prepareSynchronized()
{
    while (!isCancelled())
    {
        try
        {
            client.connect();
            client.startBinlogDumpGTID(randomNumber(), mysql_database_name, materialize_metadata->executed_gtid_set);
        }
        catch (...)
        {
            tryLogCurrentException(log);

            try
            {
                throw;
            }
            catch (const mysqlxx::ConnectionFailed &)
            {
                /// Avoid busy loop when MySQL is not available.
                sleepForMilliseconds(settings->max_wait_time_when_mysql_unavailable);
            }
        }
    }
}

void MaterializeMySQLSyncThread::flushBuffersData(Buffers & buffers, MaterializeMetadata & metadata)
{
    metadata.transaction(client.getPosition(), [&]() { buffers.commit(global_context); });

    const auto & position_message = [&]()
    {
        std::stringstream ss;
        client.getPosition().dump(ss);
        return ss.str();
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
    IColumn & column_to, const std::vector<Field> & rows_data, size_t column_index, const std::vector<bool> & mask, ColumnUInt8 * null_map_column = nullptr)
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
static size_t onWriteOrDeleteData(const std::vector<Field> & rows_data, Block & buffer, size_t version)
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

static inline size_t onUpdateData(const std::vector<Field> & rows_data, Block & buffer, size_t version, const std::vector<size_t> & sorting_columns_index)
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

void MaterializeMySQLSyncThread::onEvent(Buffers & buffers, const BinlogEventPtr & receive_event, MaterializeMetadata & metadata)
{
    if (receive_event->type() == MYSQL_WRITE_ROWS_EVENT)
    {
        WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(write_rows_event.table, global_context);
        size_t bytes = onWriteOrDeleteData<1>(write_rows_event.rows, buffer->first, ++metadata.data_version);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), write_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_UPDATE_ROWS_EVENT)
    {
        UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(update_rows_event.table, global_context);
        size_t bytes = onUpdateData(update_rows_event.rows, buffer->first, ++metadata.data_version, buffer->second);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), update_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_DELETE_ROWS_EVENT)
    {
        DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*receive_event);
        Buffers::BufferAndSortingColumnsPtr buffer = buffers.getTableDataBuffer(delete_rows_event.table, global_context);
        size_t bytes = onWriteOrDeleteData<-1>(delete_rows_event.rows, buffer->first, ++metadata.data_version);
        buffers.add(buffer->first.rows(), buffer->first.bytes(), delete_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_QUERY_EVENT)
    {
        QueryEvent & query_event = static_cast<QueryEvent &>(*receive_event);
        flushBuffersData(buffers, metadata);

        try
        {
            Context query_context = createQueryContext(global_context);
            String comment = "Materialize MySQL step 2: execute MySQL DDL for sync data";
            String event_database = query_event.schema == mysql_database_name ? database_name : "";
            tryToExecuteQuery(query_prefix + query_event.query, query_context, event_database, comment);
        }
        catch (Exception & exception)
        {
            tryLogCurrentException(log);

            /// If some DDL query was not successfully parsed and executed
            /// Then replication may fail on next binlog events anyway
            if (exception.code() != ErrorCodes::SYNTAX_ERROR)
                throw;
        }
    }
    else if (receive_event->header.type != HEARTBEAT_EVENT)
    {
        const auto & dump_event_message = [&]()
        {
            std::stringstream ss;
            receive_event->dump(ss);
            return ss.str();
        };

        LOG_DEBUG(log, "Skip MySQL event: \n {}", dump_event_message());
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

bool MaterializeMySQLSyncThread::Buffers::checkThresholds(size_t check_block_rows, size_t check_block_bytes, size_t check_total_rows, size_t check_total_bytes) const
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
            Context query_context = createQueryContext(context);
            OneBlockInputStream input(table_name_and_buffer.second->first);
            BlockOutputStreamPtr out = getTableOutput(database, table_name_and_buffer.first, query_context, true);
            copyData(input, *out);
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

MaterializeMySQLSyncThread::Buffers::BufferAndSortingColumnsPtr MaterializeMySQLSyncThread::Buffers::getTableDataBuffer(
    const String & table_name, const Context & context)
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
