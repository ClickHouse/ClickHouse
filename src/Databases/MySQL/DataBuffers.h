#pragma once

#include <unordered_map>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Databases/IDatabase.h>
#include <boost/noncopyable.hpp>
#include <Common/ThreadPool.h>

namespace DB
{

class DataBuffers : private boost::noncopyable
{
public:
    DataBuffers(size_t & version_, IDatabase * database_, const std::function<void(const std::unordered_map<String, Block> &)> & flush_function_);

    void flush();

    void writeData(const std::string & table_name, const std::vector<Field> & rows_data);

    void updateData(const std::string & table_name, const std::vector<Field> & rows_data);

    void deleteData(const std::string & table_name, const std::vector<Field> & rows_data);


private:
    size_t & version;
    IDatabase * database;
    std::function<void(const std::unordered_map<String, Block> &)> flush_function;

    mutable std::mutex mutex;
    std::unordered_map<String, Block> buffers;

    [[noreturn]] void scheduleFlush();

    Block & getTableBuffer(const String & table_name, size_t write_size);

    ThreadFromGlobalPool thread{&DataBuffers::scheduleFlush, this};
};

}
