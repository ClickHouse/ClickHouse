#include <Databases/MySQL/DataBuffers.h>

#include <Storages/IStorage.h>
#include <common/sleep.h>

namespace DB
{

DataBuffers::DataBuffers(size_t & version_, IDatabase * database_, const std::function<void(const std::unordered_map<String, Block> &)> & flush_function_)
    : version(version_), database(database_), flush_function(flush_function_)
{

    /// TODO: 定时刷新
}

void DataBuffers::flush()
{
    flush_function(buffers);
    buffers.clear();
}

void DataBuffers::writeData(const std::string & table_name, const std::vector<Field> & rows_data)
{
    Block & to = getTableBuffer(table_name, rows_data.size());
    for (size_t column = 0; column < to.columns() - 2; ++column)
    {
        /// normally columns
        MutableColumnPtr col_to = (*std::move(to.getByPosition(column).column)).mutate();

        for (size_t index = 0; index < rows_data.size(); ++index)
            col_to->insert(DB::get<const Tuple &>(rows_data[index])[column]);
    }

    Field new_version(UInt64(++version));
    MutableColumnPtr create_version_column = (*std::move(to.getByPosition(to.columns() - 2)).column).mutate();
    MutableColumnPtr delete_version_column = (*std::move(to.getByPosition(to.columns() - 1)).column).mutate();

    delete_version_column->insertManyDefaults(rows_data.size());

    for (size_t index = 0; index < rows_data.size(); ++index)
        create_version_column->insert(new_version);
}

void DataBuffers::updateData(const String & table_name, const std::vector<Field> & rows_data)
{
    if (rows_data.size() % 2 != 0)
        throw Exception("LOGICAL ERROR: ", ErrorCodes::LOGICAL_ERROR);

    Block & to = getTableBuffer(table_name, rows_data.size());
    for (size_t column = 0; column < to.columns() - 2; ++column)
    {
        /// normally columns
        MutableColumnPtr col_to = (*std::move(to.getByPosition(column).column)).mutate();

        for (size_t index = 0; index < rows_data.size(); ++index)
            col_to->insert(DB::get<const Tuple &>(rows_data[index])[column]);
    }

    Field new_version(UInt64(++version));
    MutableColumnPtr create_version_column = (*std::move(to.getByPosition(to.columns() - 2)).column).mutate();
    MutableColumnPtr delete_version_column = (*std::move(to.getByPosition(to.columns() - 1)).column).mutate();

    for (size_t index = 0; index < rows_data.size(); ++index)
    {
        if (index % 2 == 0)
        {
            create_version_column->insertDefault();
            delete_version_column->insert(new_version);
        }
        else
        {
            delete_version_column->insertDefault();
            create_version_column->insert(new_version);
        }
    }
}

void DataBuffers::deleteData(const String & table_name, const std::vector<Field> & rows_data)
{
    Block & to = getTableBuffer(table_name, rows_data.size());
    for (size_t column = 0; column < to.columns() - 2; ++column)
    {
        /// normally columns
        MutableColumnPtr col_to = (*std::move(to.getByPosition(column).column)).mutate();

        for (size_t index = 0; index < rows_data.size(); ++index)
            col_to->insert(DB::get<const Tuple &>(rows_data[index])[column]);
    }

    Field new_version(UInt64(++version));
    MutableColumnPtr create_version_column = (*std::move(to.getByPosition(to.columns() - 2)).column).mutate();
    MutableColumnPtr delete_version_column = (*std::move(to.getByPosition(to.columns() - 1)).column).mutate();

    create_version_column->insertManyDefaults(rows_data.size());

    for (size_t index = 0; index < rows_data.size(); ++index)
        delete_version_column->insert(new_version);
}

Block & DataBuffers::getTableBuffer(const String & table_name, size_t write_size)
{
    if (buffers.find(table_name) == buffers.end())
    {
        StoragePtr write_storage = database->tryGetTable(table_name);
        buffers[table_name] = write_storage->getSampleBlockNonMaterialized();
    }

    /// TODO: settings
    if (buffers[table_name].rows() + write_size > 8192)
        flush();

    return buffers[table_name];
}

[[noreturn]] void DataBuffers::scheduleFlush()
{
    while (1)
    {
        try
        {
            flush();
            sleepForSeconds(1);
        }
        catch (...)
        {
//            ++error_count;
//            sleep_time = std::min(
//                std::chrono::milliseconds{Int64(default_sleep_time.count() * std::exp2(error_count))},
//                max_sleep_time);
            tryLogCurrentException("");
        }
    }
}

}
