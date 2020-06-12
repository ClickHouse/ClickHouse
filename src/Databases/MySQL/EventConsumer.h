#pragma once

#include <unordered_map>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/MySQLReplication.h>
#include <Databases/IDatabase.h>
#include <Databases/MySQL/MaterializeModeSettings.h>
#include <boost/noncopyable.hpp>
#include <Common/ThreadPool.h>
#include "MaterializeMetadata.h"

namespace DB
{

class EventConsumer : private boost::noncopyable
{
public:
    ~EventConsumer();

    void onEvent(const MySQLReplication::BinlogEventPtr & event, const MySQLReplication::Position & position);

    EventConsumer(const String & database_, const Context & context, MaterializeMetadata & metadata_, MaterializeModeSettings & settings_);
private:
    MaterializeMetadata & metadata;

    const Context & context;
    const MaterializeModeSettings & settings;

    String database;
    size_t prev_version;
    size_t total_bytes_in_buffers = 0;
    MySQLReplication::Position last_position;

    struct Buffer
    {
        Block data;
        std::vector<size_t> sorting_columns_index;
    };

    using BufferPtr = std::shared_ptr<Buffer>;
    std::unordered_map<String, BufferPtr> buffers;

    void flushBuffers();

    BufferPtr getTableBuffer(const String & table_name);

    void onWriteData(const std::string & table_name, const std::vector<Field> & rows_data);

    void onUpdateData(const std::string & table_name, const std::vector<Field> & rows_data);

    void onDeleteData(const std::string & table_name, const std::vector<Field> & rows_data);

    void fillSignColumnsAndMayFlush(Block & data, Int8 sign_value, UInt64 version_value, size_t fill_size, size_t prev_bytes);

    mutable std::mutex mutex;
    std::condition_variable cond;
    std::atomic_bool quit = false;
    std::atomic_bool background_exception = false;
    ThreadPool background_thread_pool{1};
};

}
