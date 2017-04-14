#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Databases/IDatabase.h>
#include <Storages/TrivialStorageBuffer.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <common/logger_useful.h>
#include <Poco/Ext/ThreadNumber.h>

#include <ext/range.hpp>


namespace ProfileEvents
{
    extern const Event StorageBufferFlush;
    extern const Event StorageBufferErrorOnFlush;
    extern const Event StorageBufferPassedAllMinThresholds;
    extern const Event StorageBufferPassedTimeMaxThreshold;
    extern const Event StorageBufferPassedRowsMaxThreshold;
    extern const Event StorageBufferPassedBytesMaxThreshold;
}

namespace CurrentMetrics
{
    extern const Metric StorageBufferRows;
    extern const Metric StorageBufferBytes;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int INFINITE_LOOP;
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
}


StoragePtr TrivialStorageBuffer::create(const std::string & name_, NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    Context & context_, const size_t num_blocks_to_deduplicate_,
    const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
    const String & destination_database_, const String & destination_table_)
{
    return make_shared(
        name_, columns_, materialized_columns_, alias_columns_, column_defaults_,
        context_, num_blocks_to_deduplicate_, min_thresholds_, max_thresholds_,
        destination_database_, destination_table_);
}


TrivialStorageBuffer::TrivialBuffer(const std::string & name_, NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_,
    Context & context_, const size_t num_blocks_to_deduplicate_,
    const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
    const String & destination_database_, const String & destination_table_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
    name(name_), columns(columns_), context(context_),
    num_blocks_to_deduplicate(num_blocks_to_deduplicate_),
    current_hashes(std::make_unique<DeduplicationBuffer>()),
    previous_hashes(std::make_unique<DeduplicationBuffer>()),
    min_thresholds(min_thresholds_), max_thresholds(max_thresholds_),
    destination_database(destination_database_), destination_table(destination_table_),
    no_destination(destination_database.empty() && destination_table.empty()),
    log(&Logger::get("TrivialBuffer (" + name + ")")),
    flush_thread(&TrivialStorageBuffer::flushThread, this)
{
}

class TrivialBufferBlockInputStream : public IProfilingBlockInputStream
{
public:
    TrivialBufferBlockInputStream(const Names & column_names_, BlocksList::iterator begin_,
        BlocksList::iterator end_, TrivialStorageBuffer & buffer_)
        : column_names(column_names_), begin(begin_), end(end_),
        it(begin_), buffer(buffer_) {}

    String getName() const { return "TrivialStorageBuffer"; }

    String getID() const
    {
        std::stringstream res;
        res << "TrivialStorageBuffer(" << &buffer;

        for (const auto & name : column_names)
            res << ", " << name;

        res << ")";
        return res.str();
    }

protected:
    Block readImpl()
    {
        Block res;

        if (it == end)
            return res;

        for (const auto & column : column_names)
            res.insert(it->getByName(column));
        ++it;

        return res;
    }

private:
    Names column_names;
    TrivialStorageBuffer & buffer;
    BlocksList::iterator begin, end, it;
};

BlockInputStreams TrivialStorageBuffer::read(
    const Names & column_names,
    ASTPtr query,
    const Context & context,
    const Settings & settings,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned threads)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    BlockInputStreams streams;

    if (!no_destination)
    {
        auto destination = context.getTable(destination_database, destination_table);

        if (destination.get() == this)
            throw Exception("Destination table is myself. Read will cause infinite loop.",
                    ErrorCodes::INFINITE_LOOP);

        /** TrivialStorageBuffer does not support 'PREWHERE',
          * so turn off corresponding optimization.
          */
        Settings modified_settings = settings;
        modified_settings.optimize_move_to_prewhere = false;

        streams = destination->read(column_names, query, context, modified_settings,
            processed_stage, max_block_size, threads);
    }

    BlockInputStreams streams_from_buffers;
    std::lock_guard<std::mutex> lock(buffer.mutex);
    size_t size = data.size();
    if (threads > size)
        threads = size;

    for (size_t thread = 0; thread < threads; ++thread)
    {
        BlocksList::iterator begin = data.begin();
        BlocksList::iterator end = data.begin();

        std::advance(begin, thread * size / threads);
        std::advance(end, (thread + 1) * size / threads);

        streams_from_buffers.push_back(std::make_shared<TrivialBufferBlockInputStream>(column_names, begin, end, *this));
    }

    /** If sources from destination table are already processed to non-starting stage, then we should wrap
      * sources from the buffer to the same stage of processing conveyor.
      */
    if (processed_stage > QueryProcessingStage::FetchColumns)
        for (auto & stream : streams_from_buffers)
            stream = InterpreterSelectQuery(query, context, processed_stage, 0, stream).execute().in;

    streams.insert(streams.end(), streams_from_buffers.begin(), streams_from_buffers.end());
    return streams;
}

void TrivialStorageBuffer::addBlock(const Block & block)
{
    SipHash hash;
    block.updateHash(hash);
    HashType block_hash = hash.get64();

    std::lock_guard<std::mutex> lock(mutex);
    if (current_hashes->find(block_hash) == current_hashes->end()
        && previous_hashes->find(block_hash) == previous_hashes->end())
    {
        if (current_hashes->size() >= num_blocks_to_deduplicate / 2)
        {
            previous_hashes = std::move(current_hashes);
            current_hashes = std::make_unique<DeduplicationBuffer>();
        }
        current_hashes->insert(block_hash);
        current_rows += block.rows();
        current_bytes += block.bytes();
        data.push_back(block);

        CurrentMetrics::add(CurrentMetrics::StorageBufferRows, current_rows);
        CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, current_bytes);
    }
    else
    {
        auto it = previous_hashes->find(block_hash);
        if (it != previous_hashes->end())
        {
            current_hashes->insert(it);
            previous_hashes->erase(it);
        }
    }
}

void TrivialStorageBuffer::flush(bool check_thresholds)
{
    Block block_to_write;
    time_t current_time = time(0);

    time_t time_passed = 0;

    if (data.empty())
        return;

    {
        std::lock_guard<std::mutex> lock(mutex);

        if (first_write_time)
            time_passed = current_time - first_write_time;

        if (check_thresholds)
        {
            if (!checkThresholdsImpl(current_rows, current_bytes, time_passed))
                return;
        }
        else
        {
            if (current_rows == 0)
                return;
        }

        /// Collecting BlockList into single block.
        block_to_write = data.front().cloneEmpty();
        block_to_write.checkNumberOfRows();
        for (auto & block : data)
        {
            block.checkNumberOfRows();
            for (size_t column_no = 0, columns = block.columns(); column_no < columns; ++column_no)
            {
                IColumn & col_to = *block_to_write.safeGetByPosition(column_no).column.get();
                const IColumn & col_from = *block.getByName(col_to.getName()).column.get();

                col_to.insertRangeFrom(col_from, 0, block.rows());
            }

        }
        first_write_time = 0;

        ProfileEvents::increment(ProfileEvents::StorageBufferFlush);

        LOG_TRACE(log, "Flushing buffer with " << block_to_write.rows() << " rows, " << block_to_write.bytes() << " bytes, age " << time_passed << " seconds.");

        if (no_destination)
            return;

        try
        {
            writeBlockToDestination(block_to_write, context.tryGetTable(destination_database, destination_table));
            data.clear();

            CurrentMetrics::sub(CurrentMetrics::StorageBufferRows, block_to_write.rows());
            CurrentMetrics::sub(CurrentMetrics::StorageBufferBytes, block_to_write.bytes());

        }
        catch (...)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferErrorOnFlush);

            if (!first_write_time)
                first_write_time = current_time;

            /// Через некоторое время будет следующая попытка записать.
            throw;
        }
    }

}

class TrivialBufferBlockOutputStream : public IBlockOutputStream
{
public:
    TrivialBufferBlockOutputStream(TrivialStorageBuffer & buffer_) : buffer(buffer_) {}
    void write(const Block & block) override
    {
        if (!block)
            return;

        size_t rows = block.rows();
        size_t bytes = block.bytes();
        if (!rows)
            return;

        StoragePtr destination;
        if (!buffer.no_destination)
        {
            destination = buffer.context.tryGetTable(buffer.destination_database,
                buffer.destination_table);

            if (destination)
            {
                if (destination.get() == &buffer)
                    throw Exception("Destination table is myself. Write will "
                        "cause infinite loop.", ErrorCodes::INFINITE_LOOP);

                /// Проверяем структуру таблицы.
                try
                {
                    destination->check(block, true);
                }
                catch (Exception & e)
                {
                    e.addMessage("(when looking at destination table "
                        + buffer.destination_database + "."
                        + buffer.destination_table + ")");
                    throw;
                }
            }
        }

        /// Вставляем блок в список блоков.

        time_t current_time = time(0);
        if (buffer.checkThresholds(current_time, rows, bytes))
        {
            /** Если после вставки в буфер, ограничения будут превышены,
              * то будем сбрасывать буфер.
              * Это также защищает от неограниченного потребления оперативки,
              * так как в случае невозможности записать в таблицу,
              * будет выкинуто исключение, а новые данные не будут добавлены в буфер.
              */

            buffer.flush(true);
        }

        if (!buffer.first_write_time)
            buffer.first_write_time = current_time;

        buffer.addBlock(block);
    }
private:
    TrivialStorageBuffer & buffer;
};

BlockOutputStreamPtr TrivialStorageBuffer::write(ASTPtr query, const Settings & settings)
{
    return std::make_shared<TrivialBufferBlockOutputStream>(*this);
}

void TrivialStorageBuffer::shutdown()
{
    shutdown_event.set();

    if (flush_thread.joinable())
        flush_thread.join();

    try
    {
        flush(false);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

}

/** NOTE If you do OPTIMIZE after insertion,
  * it does not guarantee that all data will be in destination table at the time of
  * next SELECT just after OPTIMIZE.
  *
  * Because in case if there was already running flush method,
  *  then call to flush inside OPTIMIZE will see empty buffer and return quickly,
  *  but at the same time, the already running flush method possibly is not finished,
  *  so next SELECT will observe missing data.
  *
  * This kind of race condition make very hard to implement proper tests.
  */
bool TrivialStorageBuffer::optimize(const String & partition, bool final, const Settings & settings)
{
    if (!partition.empty())
        throw Exception("Partition cannot be specified when optimizing table of type Buffer",
            ErrorCodes::NOT_IMPLEMENTED);

    if (final)
        throw Exception("FINAL cannot be specified when optimizing table of type Buffer",
            ErrorCodes::NOT_IMPLEMENTED);

    flush(false);
    return true;
}



bool TrivialStorageBuffer::checkThresholds(const time_t current_time, const size_t additional_rows,
                    const size_t additional_bytes) const
{
    time_t time_passed = 0;
    if (first_write_time)
        time_passed = current_time - first_write_time;

    size_t rows = current_rows + additional_rows;
    size_t bytes = current_bytes + additional_bytes;

    return checkThresholdsImpl(rows, bytes, time_passed);

}

bool TrivialStorageBuffer::checkThresholdsImpl(const size_t rows, const size_t bytes,
                    const time_t time_passed) const
{
    if (time_passed > min_thresholds.time && rows > min_thresholds.rows && bytes > min_thresholds.bytes)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedAllMinThresholds);
        return true;
    }

    if (time_passed > max_thresholds.time)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedTimeMaxThreshold);
        return true;
    }

    if (rows > max_thresholds.rows)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedRowsMaxThreshold);
        return true;
    }

    if (bytes > max_thresholds.bytes)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferPassedBytesMaxThreshold);
        return true;
    }

    return false;
}

void TrivialStorageBuffer::flushThread()
{
    setThreadName("BufferFlush");

    do
    {
        try
        {
            flush(true);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    } while (!shutdown_event.tryWait(1000));
}

void TrivialStorageBuffer::writeBlockToDestination(const Block & block, StoragePtr table)
{
    if (no_destination || !block)
        return;

    if (!table)
    {
        LOG_ERROR(log, "Destination table " << destination_database << "." << destination_table << " doesn't exist. Block of data is discarded.");
        return;
    }

    auto insert = std::make_shared<ASTInsertQuery>();

    insert->database = destination_database;
    insert->table = destination_table;

    /** Будем вставлять столбцы, являющиеся пересечением множества столбцов таблицы-буфера и подчинённой таблицы.
      * Это позволит поддержать часть случаев (но не все), когда структура таблицы не совпадает.
      */
    Block structure_of_destination_table = table->getSampleBlock();
    Names columns_intersection;
    columns_intersection.reserve(block.columns());
    for (size_t i : ext::range(0, structure_of_destination_table.columns()))
    {
        auto dst_col = structure_of_destination_table.getByPosition(i);
        if (block.has(dst_col.name))
        {
            if (block.getByName(dst_col.name).type->getName() != dst_col.type->getName())
            {
                LOG_ERROR(log, "Destination table " << destination_database << "." << destination_table
                    << " have different type of column " << dst_col.name << ". Block of data is discarded.");
                return;
            }

            columns_intersection.push_back(dst_col.name);
        }
    }

    if (columns_intersection.empty())
    {
        LOG_ERROR(log, "Destination table " << destination_database << "." << destination_table << " have no common columns with block in buffer. Block of data is discarded.");
        return;
    }

    if (columns_intersection.size() != block.columns())
        LOG_WARNING(log, "Not all columns from block in buffer exist in destination table "
            << destination_database << "." << destination_table << ". Some columns are discarded.");

    auto list_of_columns = std::make_shared<ASTExpressionList>();
    insert->columns = list_of_columns;
    list_of_columns->children.reserve(columns_intersection.size());
    for (const String & column : columns_intersection)
        list_of_columns->children.push_back(std::make_shared<ASTIdentifier>(StringRange(), column, ASTIdentifier::Column));

    InterpreterInsertQuery interpreter{insert, context};

    auto block_io = interpreter.execute();
    block_io.out->writePrefix();
    block_io.out->write(block);
    block_io.out->writeSuffix();
}

void TrivialStorageBuffer::alter(const AlterCommands & params, const String & database_name,
                const String & table_name, const Context & context)
{
    for (const auto & param : params)
        if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
            throw Exception("Storage engine " + getName() + " doesn't support primary key.",
                    ErrorCodes::NOT_IMPLEMENTED);

    auto lock = lockStructureForAlter();

    /// Чтобы не осталось блоков старой структуры.
    flush(false);

    params.apply(*columns, materialized_columns, alias_columns, column_defaults);

    context.getDatabase(database_name)->alterTable(
        context, table_name,
        *columns, materialized_columns, alias_columns, column_defaults, {});
}

}
