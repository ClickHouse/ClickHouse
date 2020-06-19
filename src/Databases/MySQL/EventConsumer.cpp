#include <Databases/MySQL/EventConsumer.h>

#include <Columns/ColumnsNumber.h>
#include <DataStreams/copyData.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/executeQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMergeTree.h>
#include <Common/assert_cast.h>
#include <Common/setThreadName.h>

namespace DB
{

using namespace MySQLReplication;

EventConsumer::~EventConsumer()
{
    if (!quit && !background_exception)
    {
        {
            quit = true;
            std::lock_guard<std::mutex> lock(mutex);
        }

        cond.notify_one();
        background_thread_pool.wait();
    }
}

EventConsumer::EventConsumer(
    const String & database_, const Context & context_, MaterializeMetadata & metadata_, MaterializeModeSettings & settings_)
    : metadata(metadata_), context(context_), settings(settings_), database(database_), prev_version(metadata.version)
{
    background_thread_pool.scheduleOrThrowOnError([&]()
    {
        ThreadStatus thread_status;
        setThreadName("MySQLDBSync");
        std::unique_lock<std::mutex> lock(mutex);
        const auto quit_requested = [this] { return quit.load(std::memory_order_relaxed); };

        while (!quit_requested() && !background_exception)
        {
            if (!buffers.empty() && total_bytes_in_buffers)
                flushBuffers();

            cond.wait_for(lock, std::chrono::milliseconds(settings.max_flush_data_time), quit_requested);
        }
    });
}

void EventConsumer::onWriteData(const String & table_name, const std::vector<Field> & rows_data)
{
    BufferPtr buffer = getTableBuffer(table_name);

    size_t prev_bytes = buffer->data.bytes();
    for (size_t column = 0; column < buffer->data.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer->data.getByPosition(column).column));

        for (size_t index = 0; index < rows_data.size(); ++index)
            col_to->insert(DB::get<const Tuple &>(rows_data[index])[column]);
    }

    fillSignColumnsAndMayFlush(buffer->data, 1, ++metadata.version, rows_data.size(), prev_bytes);
}

static inline bool differenceSortingKeys(const Tuple & row_old_data, const Tuple & row_new_data, const std::vector<size_t> sorting_columns_index)
{
    for (const auto & sorting_column_index : sorting_columns_index)
        if (row_old_data[sorting_column_index] != row_new_data[sorting_column_index])
            return true;

    return false;
}

void EventConsumer::onUpdateData(const String & table_name, const std::vector<Field> & rows_data)
{
    if (rows_data.size() % 2 != 0)
        throw Exception("LOGICAL ERROR: ", ErrorCodes::LOGICAL_ERROR);

    BufferPtr buffer = getTableBuffer(table_name);

    size_t prev_bytes = buffer->data.bytes();
    std::vector<bool> difference_sorting_keys_mark(rows_data.size() / 2);

    for (size_t index = 0; index < rows_data.size(); index += 2)
        difference_sorting_keys_mark.emplace_back(differenceSortingKeys(
            DB::get<const Tuple &>(rows_data[index]), DB::get<const Tuple &>(rows_data[index + 1]), buffer->sorting_columns_index));

    for (size_t column = 0; column < buffer->data.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer->data.getByPosition(column).column));

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
    }

    MutableColumnPtr sign_mutable_column = IColumn::mutate(std::move(buffer->data.getByPosition(buffer->data.columns() - 2).column));
    MutableColumnPtr version_mutable_column = IColumn::mutate(std::move(buffer->data.getByPosition(buffer->data.columns() - 1).column));

    ColumnInt8::Container & sign_column_data = assert_cast<ColumnInt8 &>(*sign_mutable_column).getData();
    ColumnUInt64::Container & version_column_data = assert_cast<ColumnUInt64 &>(*version_mutable_column).getData();

    UInt64 new_version = ++metadata.version;
    for (size_t index = 0; index < rows_data.size(); index += 2)
    {
        if (likely(!difference_sorting_keys_mark[index / 2]))
        {
            sign_column_data.emplace_back(1);
            version_column_data.emplace_back(new_version);
        }
        else
        {
            /// If the sorting keys is modified, we should cancel the old data, but this should not happen frequently
            sign_column_data.emplace_back(-1);
            sign_column_data.emplace_back(1);
            version_column_data.emplace_back(new_version);
            version_column_data.emplace_back(new_version);
        }
    }

    total_bytes_in_buffers += (buffer->data.bytes() - prev_bytes);
    if (buffer->data.rows() >= settings.max_rows_in_buffer || total_bytes_in_buffers >= settings.max_bytes_in_buffers)
        flushBuffers();
}

void EventConsumer::onDeleteData(const String & table_name, const std::vector<Field> & rows_data)
{
    BufferPtr buffer = getTableBuffer(table_name);

    size_t prev_bytes = buffer->data.bytes();
    for (size_t column = 0; column < buffer->data.columns() - 2; ++column)
    {
        MutableColumnPtr col_to = IColumn::mutate(std::move(buffer->data.getByPosition(column).column));

        for (size_t index = 0; index < rows_data.size(); ++index)
            col_to->insert(DB::get<const Tuple &>(rows_data[index])[column]);
    }

    fillSignColumnsAndMayFlush(buffer->data, -1, ++metadata.version, rows_data.size(), prev_bytes);
}

EventConsumer::BufferPtr EventConsumer::getTableBuffer(const String & table_name)
{
    if (buffers.find(table_name) == buffers.end())
    {
        StoragePtr storage = DatabaseCatalog::instance().getDatabase(database)->tryGetTable(table_name, context);

        buffers[table_name] = std::make_shared<Buffer>();
        buffers[table_name]->data = storage->getSampleBlockNonMaterialized();
        if (StorageMergeTree * table_merge_tree = dynamic_cast<StorageMergeTree *>(storage.get()))
        {
            Names required_for_sorting_key = table_merge_tree->getColumnsRequiredForSortingKey();

            for (const auto & required_name_for_sorting_key : required_for_sorting_key)
                buffers[table_name]->sorting_columns_index.emplace_back(
                    buffers[table_name]->data.getPositionByName(required_name_for_sorting_key));
        }
    }

    return buffers[table_name];
}

void EventConsumer::onEvent(const BinlogEventPtr & receive_event, const MySQLReplication::Position & position)
{
    std::unique_lock<std::mutex> lock(mutex);

    if (background_exception)
        background_thread_pool.wait();

    last_position = position;
    if (receive_event->type() == MYSQL_WRITE_ROWS_EVENT)
    {
        WriteRowsEvent & write_rows_event = static_cast<WriteRowsEvent &>(*receive_event);
        write_rows_event.dump();
        onWriteData(write_rows_event.table, write_rows_event.rows);
    }
    else if (receive_event->type() == MYSQL_UPDATE_ROWS_EVENT)
    {
        UpdateRowsEvent & update_rows_event = static_cast<UpdateRowsEvent &>(*receive_event);
        update_rows_event.dump();
        onUpdateData(update_rows_event.table, update_rows_event.rows);
    }
    else if (receive_event->type() == MYSQL_DELETE_ROWS_EVENT)
    {
        DeleteRowsEvent & delete_rows_event = static_cast<DeleteRowsEvent &>(*receive_event);
        delete_rows_event.dump();
        onDeleteData(delete_rows_event.table, delete_rows_event.rows);
    }
    else if (receive_event->type() == MYSQL_QUERY_EVENT)
    {
        /// TODO: 识别, 查看是否支持的DDL, 支持的话立即刷新当前的数据, 然后执行DDL.
//        flush_function();
        /// TODO: 直接使用Interpreter执行即可
    }
}

void EventConsumer::fillSignColumnsAndMayFlush(Block & data, Int8 sign_value, UInt64 version_value, size_t fill_size, size_t prev_bytes)
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

    total_bytes_in_buffers += (data.bytes() - prev_bytes);
    if (data.rows() >= settings.max_rows_in_buffer || total_bytes_in_buffers >= settings.max_bytes_in_buffers)
        flushBuffers();
}

void EventConsumer::flushBuffers()
{
    /// TODO: 事务保证
    try
    {
        for (auto & table_name_and_buffer : buffers)
        {
            const String & table_name = table_name_and_buffer.first;
            BufferPtr & buffer = table_name_and_buffer.second;

            Context query_context = context;
            query_context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
            query_context.setCurrentQueryId(""); // generate random query_id
            String with_database_table_name = backQuoteIfNeed(database) + "." + backQuoteIfNeed(table_name);
            BlockIO res = executeQuery("INSERT INTO " + with_database_table_name + " VALUES", query_context, true);

            OneBlockInputStream input(buffer->data);
            copyData(input, *res.out);
        }

        buffers.clear();
        total_bytes_in_buffers = 0;
        prev_version = metadata.version;
    }
    catch(...)
    {
        buffers.clear();
        total_bytes_in_buffers = 0;
        metadata.version = prev_version;
        background_exception = true;
        throw;
    }
}

}
