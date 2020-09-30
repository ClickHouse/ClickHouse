#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/MySQLBinlogEvent.h>

#include <Databases/MySQL/MySQLUtils.h>

namespace DB
{

using namespace MySQLReplicaConsumer;

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
}

void fillSignAndVersionColumnsData(
    Block & data,
    Int8 sign_value,
    UInt64 version_value,
    size_t fill_size)
{
    MutableColumnPtr sign_mutable_column = IColumn::mutate(
        std::move(data.getByPosition(data.columns() - 2).column));
    MutableColumnPtr version_mutable_column = IColumn::mutate(
        std::move(data.getByPosition(data.columns() - 1).column));

    ColumnInt8::Container & sign_column_data =
        assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();
    ColumnUInt64::Container & version_column_data =
        assert_cast<ColumnUInt64 &>(*version_mutable_column).getData();

    for (size_t index = 0; index < fill_size; ++index)
    {
        sign_column_data.emplace_back(sign_value);
        version_column_data.emplace_back(version_value);
    }

    data.getByPosition(data.columns() - 2).column = std::move(sign_mutable_column);
    data.getByPosition(data.columns() - 1).column = std::move(version_mutable_column);
}

bool differenceSortingKeys(
    const Tuple & row_old_data,
    const Tuple & row_new_data,
    const std::vector<size_t> sorting_columns_index)
{
    for (const auto & sorting_column_index : sorting_columns_index)
        if (row_old_data[sorting_column_index] != row_new_data[sorting_column_index])
            return true;

    return false;
}

size_t onUpdateData(
    const std::vector<Field> & rows_data,
    Block & buffer, size_t version,
    const std::vector<size_t> & sorting_columns_index)
{
    if (rows_data.size() % 2 != 0)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    size_t prev_bytes = buffer.bytes();
    std::vector<bool> writeable_rows_mask(rows_data.size());

    for (size_t index = 0; index < rows_data.size(); index += 2)
    {
        writeable_rows_mask[index + 1] = true;
        writeable_rows_mask[index] = differenceSortingKeys(
            DB::get<const Tuple &>(rows_data[index]),
            DB::get<const Tuple &>(rows_data[index + 1]),
            sorting_columns_index);
    }

    for (size_t column = 0; column < buffer.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(
            std::move(buffer.getByPosition(column).column));

        writeFieldsToColumn(*col_to, rows_data, column, writeable_rows_mask);
        buffer.getByPosition(column).column = std::move(col_to);
    }

    MutableColumnPtr sign_mutable_column = IColumn::mutate(
        std::move(buffer.getByPosition(buffer.columns() - 2).column));
    MutableColumnPtr version_mutable_column = IColumn::mutate(
        std::move(buffer.getByPosition(buffer.columns() - 1).column));

    ColumnInt8::Container & sign_column_data =
        assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();
    ColumnUInt64::Container & version_column_data =
        assert_cast<ColumnUInt64 &>(*version_mutable_column).getData();

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

void onEvent(
    const Context & global_context,
    ConsumerPtr consumer,
    const MySQLReplication::BinlogEventPtr & receive_event,
    Poco::Logger * log,
    std::function<void(ConsumerPtr)> flushBuffersData)
{
    if (receive_event->type() == MYSQL_WRITE_ROWS_EVENT)
    {
        WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*receive_event);
        MySQLBufferAndSortingColumnsPtr buffer = consumer->buffer->getTableDataBuffer(write_rows_event.table, global_context);
        if (!buffer)
        {
            return;
        }

        size_t bytes = onWriteOrDeleteData<1>(write_rows_event.rows, buffer->first, ++consumer->materialize_metadata->data_version);
        consumer->buffer->add(buffer->first.rows(), buffer->first.bytes(), write_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_UPDATE_ROWS_EVENT)
    {
        UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*receive_event);
        MySQLBufferAndSortingColumnsPtr buffer = consumer->buffer->getTableDataBuffer(update_rows_event.table, global_context);
        if (!buffer)
        {
            return;
        }

        size_t bytes = onUpdateData(update_rows_event.rows, buffer->first, ++consumer->materialize_metadata->data_version, buffer->second);
        consumer->buffer->add(buffer->first.rows(), buffer->first.bytes(), update_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_DELETE_ROWS_EVENT)
    {
        DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*receive_event);
        MySQLBufferAndSortingColumnsPtr buffer = consumer->buffer->getTableDataBuffer(delete_rows_event.table, global_context);
        if (!buffer)
        {
            return;
        }

        size_t bytes = onWriteOrDeleteData<-1>(delete_rows_event.rows, buffer->first, ++consumer->materialize_metadata->data_version);
        consumer->buffer->add(buffer->first.rows(), buffer->first.bytes(), delete_rows_event.rows.size(), bytes);
    }
    else if (receive_event->type() == MYSQL_QUERY_EVENT)
    {
        ConsumerDatabasePtr consumer_database =
            std::dynamic_pointer_cast<ConsumerDatabase>(consumer);
        if (!consumer_database) {
            return;
        }

        QueryEvent & query_event = static_cast<QueryEvent &>(*receive_event);
        flushBuffersData(consumer);

        try
        {
            Context query_context = createQueryContext(global_context);
            String comment = "Materialize MySQL step 2: execute MySQL DDL for sync data";
            String event_database = "";
            if (query_event.schema == consumer_database->mysql_database_name) {
                event_database = consumer_database->database_name;
            }
            tryToExecuteQuery(
                consumer_database->getQueryPrefix() + query_event.query,
                query_context,
                event_database,
                comment);
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

}

#endif
