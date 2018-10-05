#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageFactory.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/setThreadName.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Common/FieldVisitors.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <common/logger_useful.h>
#include <Poco/Ext/ThreadNumber.h>

#include <ext/range.h>


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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageBuffer::StorageBuffer(const std::string & name_, const ColumnsDescription & columns_,
    Context & context_,
    size_t num_shards_, const Thresholds & min_thresholds_, const Thresholds & max_thresholds_,
    const String & destination_database_, const String & destination_table_, bool allow_materialized_)
    : IStorage{columns_},
    name(name_), context(context_),
    num_shards(num_shards_), buffers(num_shards_),
    min_thresholds(min_thresholds_), max_thresholds(max_thresholds_),
    destination_database(destination_database_), destination_table(destination_table_),
    no_destination(destination_database.empty() && destination_table.empty()),
    allow_materialized(allow_materialized_), log(&Logger::get("StorageBuffer (" + name + ")"))
{
}

StorageBuffer::~StorageBuffer()
{
    // Should not happen if shutdown was called
    if (flush_thread.joinable())
    {
        shutdown_event.set();
        flush_thread.join();
    }
}


/// Reads from one buffer (from one block) under its mutex.
class BufferBlockInputStream : public IProfilingBlockInputStream
{
public:
    BufferBlockInputStream(const Names & column_names_, StorageBuffer::Buffer & buffer_, const StorageBuffer & storage_)
        : column_names(column_names_.begin(), column_names_.end()), buffer(buffer_), storage(storage_) {}

    String getName() const override { return "Buffer"; }

    Block getHeader() const override { return storage.getSampleBlockForColumns(column_names); }

protected:
    Block readImpl() override
    {
        Block res;

        if (has_been_read)
            return res;
        has_been_read = true;

        std::lock_guard<std::mutex> lock(buffer.mutex);

        if (!buffer.data.rows())
            return res;

        for (const auto & name : column_names)
            res.insert(buffer.data.getByName(name));

        return res;
    }

private:
    Names column_names;
    StorageBuffer::Buffer & buffer;
    const StorageBuffer & storage;
    bool has_been_read = false;
};


QueryProcessingStage::Enum StorageBuffer::getQueryProcessingStage(const Context & context) const
{
    if (!no_destination)
    {
        auto destination = context.getTable(destination_database, destination_table);

        if (destination.get() == this)
            throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

        return destination->getQueryProcessingStage(context);
    }

    return QueryProcessingStage::FetchColumns;
}

BlockInputStreams StorageBuffer::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    BlockInputStreams streams_from_dst;

    if (!no_destination)
    {
        auto destination = context.getTable(destination_database, destination_table);

        if (destination.get() == this)
            throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

        streams_from_dst = destination->read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
    }

    BlockInputStreams streams_from_buffers;
    streams_from_buffers.reserve(num_shards);
    for (auto & buf : buffers)
        streams_from_buffers.push_back(std::make_shared<BufferBlockInputStream>(column_names, buf, *this));

    /** If the sources from the table were processed before some non-initial stage of query execution,
      * then sources from the buffers must also be wrapped in the processing pipeline before the same stage.
      */
    if (processed_stage > QueryProcessingStage::FetchColumns)
        for (auto & stream : streams_from_buffers)
            stream = InterpreterSelectQuery(query_info.query, context, stream, processed_stage).execute().in;

    streams_from_dst.insert(streams_from_dst.end(), streams_from_buffers.begin(), streams_from_buffers.end());
    return streams_from_dst;
}


static void appendBlock(const Block & from, Block & to)
{
    if (!to)
        throw Exception("Cannot append to empty block", ErrorCodes::LOGICAL_ERROR);

    assertBlocksHaveEqualStructure(from, to, "Buffer");

    from.checkNumberOfRows();
    to.checkNumberOfRows();

    size_t rows = from.rows();
    size_t bytes = from.bytes();

    CurrentMetrics::add(CurrentMetrics::StorageBufferRows, rows);
    CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, bytes);

    size_t old_rows = to.rows();

    try
    {
        for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
        {
            const IColumn & col_from = *from.getByPosition(column_no).column.get();
            MutableColumnPtr col_to = (*std::move(to.getByPosition(column_no).column)).mutate();

            col_to->insertRangeFrom(col_from, 0, rows);

            to.getByPosition(column_no).column = std::move(col_to);
        }
    }
    catch (...)
    {
        /// Rollback changes.
        try
        {
            /// Avoid "memory limit exceeded" exceptions during rollback.
            auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

            for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
            {
                ColumnPtr & col_to = to.getByPosition(column_no).column;
                if (col_to->size() != old_rows)
                    col_to = (*std::move(col_to)).mutate()->cut(0, old_rows);
            }
        }
        catch (...)
        {
            /// In case when we cannot rollback, do not leave incorrect state in memory.
            std::terminate();
        }

        throw;
    }
}


class BufferBlockOutputStream : public IBlockOutputStream
{
public:
    explicit BufferBlockOutputStream(StorageBuffer & storage_) : storage(storage_) {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        if (!block)
            return;

        size_t rows = block.rows();
        if (!rows)
            return;

        StoragePtr destination;
        if (!storage.no_destination)
        {
            destination = storage.context.tryGetTable(storage.destination_database, storage.destination_table);

            if (destination)
            {
                if (destination.get() == &storage)
                    throw Exception("Destination table is myself. Write will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

                /// Check table structure.
                try
                {
                    destination->check(block, true);
                }
                catch (Exception & e)
                {
                    e.addMessage("(when looking at destination table " + storage.destination_database + "." + storage.destination_table + ")");
                    throw;
                }
            }
        }

        size_t bytes = block.bytes();

        /// If the block already exceeds the maximum limit, then we skip the buffer.
        if (rows > storage.max_thresholds.rows || bytes > storage.max_thresholds.bytes)
        {
            if (!storage.no_destination)
            {
                LOG_TRACE(storage.log, "Writing block with " << rows << " rows, " << bytes << " bytes directly.");
                storage.writeBlockToDestination(block, destination);
             }
            return;
        }

        /// We distribute the load on the shards by the stream number.
        const auto start_shard_num = Poco::ThreadNumber::get() % storage.num_shards;

        /// We loop through the buffers, trying to lock mutex. No more than one lap.
        auto shard_num = start_shard_num;

        StorageBuffer::Buffer * least_busy_buffer = nullptr;
        std::unique_lock<std::mutex> least_busy_lock;
        size_t least_busy_shard_rows = 0;

        for (size_t try_no = 0; try_no < storage.num_shards; ++try_no)
        {
            std::unique_lock<std::mutex> lock(storage.buffers[shard_num].mutex, std::try_to_lock);

            if (lock.owns_lock())
            {
                size_t num_rows = storage.buffers[shard_num].data.rows();
                if (!least_busy_buffer || num_rows < least_busy_shard_rows)
                {
                    least_busy_buffer = &storage.buffers[shard_num];
                    least_busy_lock = std::move(lock);
                    least_busy_shard_rows = num_rows;
                }
            }

            shard_num = (shard_num + 1) % storage.num_shards;
        }

        /// If you still can not lock anything at once, then we'll wait on mutex.
        if (!least_busy_buffer)
        {
            least_busy_buffer = &storage.buffers[start_shard_num];
            least_busy_lock = std::unique_lock<std::mutex>(least_busy_buffer->mutex);
        }
        insertIntoBuffer(block, *least_busy_buffer);
    }
private:
    StorageBuffer & storage;

    void insertIntoBuffer(const Block & block, StorageBuffer::Buffer & buffer)
    {
        time_t current_time = time(nullptr);

        /// Sort the columns in the block. This is necessary to make it easier to concatenate the blocks later.
        Block sorted_block = block.sortColumns();

        if (!buffer.data)
        {
            buffer.data = sorted_block.cloneEmpty();
        }
        else if (storage.checkThresholds(buffer, current_time, sorted_block.rows(), sorted_block.bytes()))
        {
            /** If, after inserting the buffer, the constraints are exceeded, then we will reset the buffer.
              * This also protects against unlimited consumption of RAM, since if it is impossible to write to the table,
              *  an exception will be thrown, and new data will not be added to the buffer.
              */

            storage.flushBuffer(buffer, true, true /* locked */);
        }

        if (!buffer.first_write_time)
            buffer.first_write_time = current_time;

        appendBlock(sorted_block, buffer.data);
    }
};


BlockOutputStreamPtr StorageBuffer::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    return std::make_shared<BufferBlockOutputStream>(*this);
}


bool StorageBuffer::mayBenefitFromIndexForIn(const ASTPtr & left_in_operand) const
{
    if (no_destination)
        return false;

    auto destination = context.getTable(destination_database, destination_table);

    if (destination.get() == this)
        throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

    return destination->mayBenefitFromIndexForIn(left_in_operand);
}


void StorageBuffer::startup()
{
    if (context.getSettingsRef().readonly)
    {
        LOG_WARNING(log, "Storage " << getName() << " is run with readonly settings, it will not be able to insert data."
            << " Set apropriate system_profile to fix this.");
    }

    flush_thread = std::thread(&StorageBuffer::flushThread, this);
}


void StorageBuffer::shutdown()
{
    shutdown_event.set();

    if (flush_thread.joinable())
        flush_thread.join();

    try
    {
        optimize(nullptr /*query*/, {} /*partition*/, false /*final*/, false /*deduplicate*/, context);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


/** NOTE If you do OPTIMIZE after insertion,
  * it does not guarantee, that all data will be in destination table at the time of next SELECT just after OPTIMIZE.
  *
  * Because in case if there was already running flushBuffer method,
  *  then call to flushBuffer inside OPTIMIZE will see empty buffer and return quickly,
  *  but at the same time, the already running flushBuffer method possibly is not finished,
  *  so next SELECT will observe missing data.
  *
  * This kind of race condition make very hard to implement proper tests.
  */
bool StorageBuffer::optimize(const ASTPtr & /*query*/, const ASTPtr & partition, bool final, bool deduplicate, const Context & /*context*/)
{
    if (partition)
        throw Exception("Partition cannot be specified when optimizing table of type Buffer", ErrorCodes::NOT_IMPLEMENTED);

    if (final)
        throw Exception("FINAL cannot be specified when optimizing table of type Buffer", ErrorCodes::NOT_IMPLEMENTED);

    if (deduplicate)
        throw Exception("DEDUPLICATE cannot be specified when optimizing table of type Buffer", ErrorCodes::NOT_IMPLEMENTED);

    flushAllBuffers(false);
    return true;
}


bool StorageBuffer::checkThresholds(const Buffer & buffer, time_t current_time, size_t additional_rows, size_t additional_bytes) const
{
    time_t time_passed = 0;
    if (buffer.first_write_time)
        time_passed = current_time - buffer.first_write_time;

    size_t rows = buffer.data.rows() + additional_rows;
    size_t bytes = buffer.data.bytes() + additional_bytes;

    return checkThresholdsImpl(rows, bytes, time_passed);
}


bool StorageBuffer::checkThresholdsImpl(size_t rows, size_t bytes, time_t time_passed) const
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


void StorageBuffer::flushAllBuffers(const bool check_thresholds)
{
    for (auto & buf : buffers)
        flushBuffer(buf, check_thresholds);
}


void StorageBuffer::flushBuffer(Buffer & buffer, bool check_thresholds, bool locked)
{
    Block block_to_write;
    time_t current_time = time(nullptr);

    size_t rows = 0;
    size_t bytes = 0;
    time_t time_passed = 0;

    std::unique_lock<std::mutex> lock(buffer.mutex, std::defer_lock);
    if (!locked)
        lock.lock();

    block_to_write = buffer.data.cloneEmpty();

    rows = buffer.data.rows();
    bytes = buffer.data.bytes();
    if (buffer.first_write_time)
        time_passed = current_time - buffer.first_write_time;

    if (check_thresholds)
    {
        if (!checkThresholdsImpl(rows, bytes, time_passed))
            return;
    }
    else
    {
        if (rows == 0)
            return;
    }

    buffer.data.swap(block_to_write);
    buffer.first_write_time = 0;

    CurrentMetrics::sub(CurrentMetrics::StorageBufferRows, block_to_write.rows());
    CurrentMetrics::sub(CurrentMetrics::StorageBufferBytes, block_to_write.bytes());

    ProfileEvents::increment(ProfileEvents::StorageBufferFlush);

    LOG_TRACE(log, "Flushing buffer with " << rows << " rows, " << bytes << " bytes, age " << time_passed << " seconds.");

    if (no_destination)
        return;

    /** For simplicity, buffer is locked during write.
        * We could unlock buffer temporary, but it would lead to too many difficulties:
        * - data, that is written, will not be visible for SELECTs;
        * - new data could be appended to buffer, and in case of exception, we must merge it with old data, that has not been written;
        * - this could lead to infinite memory growth.
        */
    try
    {
        writeBlockToDestination(block_to_write, context.tryGetTable(destination_database, destination_table));
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferErrorOnFlush);

        /// Return the block to its place in the buffer.

        CurrentMetrics::add(CurrentMetrics::StorageBufferRows, block_to_write.rows());
        CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, block_to_write.bytes());

        buffer.data.swap(block_to_write);

        if (!buffer.first_write_time)
            buffer.first_write_time = current_time;

        /// After a while, the next write attempt will happen.
        throw;
    }
}


void StorageBuffer::writeBlockToDestination(const Block & block, StoragePtr table)
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

    /** We will insert columns that are the intersection set of columns of the buffer table and the subordinate table.
      * This will support some of the cases (but not all) when the table structure does not match.
      */
    Block structure_of_destination_table = allow_materialized ? table->getSampleBlock() : table->getSampleBlockNonMaterialized();
    Names columns_intersection;
    columns_intersection.reserve(block.columns());
    for (size_t i : ext::range(0, structure_of_destination_table.columns()))
    {
        auto dst_col = structure_of_destination_table.getByPosition(i);
        if (block.has(dst_col.name))
        {
            if (!block.getByName(dst_col.name).type->equals(*dst_col.type))
            {
                LOG_ERROR(log, "Destination table " << destination_database << "." << destination_table
                    << " have different type of column " << dst_col.name << " ("
                    << block.getByName(dst_col.name).type->getName() << " != " << dst_col.type->getName()
                    << "). Block of data is discarded.");
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
        list_of_columns->children.push_back(std::make_shared<ASTIdentifier>(column));

    InterpreterInsertQuery interpreter{insert, context, allow_materialized};

    Block block_to_write;
    for (const auto & name : columns_intersection)
        block_to_write.insert(block.getByName(name));

    auto block_io = interpreter.execute();
    block_io.out->writePrefix();
    block_io.out->write(block_to_write);
    block_io.out->writeSuffix();
}


void StorageBuffer::flushThread()
{
    setThreadName("BufferFlush");

    do
    {
        try
        {
            flushAllBuffers(true);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    } while (!shutdown_event.tryWait(1000));
}


void StorageBuffer::alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context)
{
    for (const auto & param : params)
        if (param.type == AlterCommand::MODIFY_PRIMARY_KEY)
            throw Exception("Storage engine " + getName() + " doesn't support primary key.", ErrorCodes::NOT_IMPLEMENTED);

    auto lock = lockStructureForAlter(__PRETTY_FUNCTION__);

    /// So that no blocks of the old structure remain.
    optimize({} /*query*/, {} /*partition_id*/, false /*final*/, false /*deduplicate*/, context);

    ColumnsDescription new_columns = getColumns();
    params.apply(new_columns);
    context.getDatabase(database_name)->alterTable(context, table_name, new_columns, {});
    setColumns(std::move(new_columns));
}


void registerStorageBuffer(StorageFactory & factory)
{
    /** Buffer(db, table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
      *
      * db, table - in which table to put data from buffer.
      * num_buckets - level of parallelism.
      * min_time, max_time, min_rows, max_rows, min_bytes, max_bytes - conditions for flushing the buffer.
      */

    factory.registerStorage("Buffer", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 9)
            throw Exception("Storage Buffer requires 9 parameters: "
                " destination_database, destination_table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        String destination_database = static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>();
        String destination_table = static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>();

        UInt64 num_buckets = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*engine_args[2]).value);

        Int64 min_time = applyVisitor(FieldVisitorConvertToNumber<Int64>(), typeid_cast<ASTLiteral &>(*engine_args[3]).value);
        Int64 max_time = applyVisitor(FieldVisitorConvertToNumber<Int64>(), typeid_cast<ASTLiteral &>(*engine_args[4]).value);
        UInt64 min_rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*engine_args[5]).value);
        UInt64 max_rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*engine_args[6]).value);
        UInt64 min_bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*engine_args[7]).value);
        UInt64 max_bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), typeid_cast<ASTLiteral &>(*engine_args[8]).value);

        return StorageBuffer::create(
            args.table_name, args.columns,
            args.context,
            num_buckets,
            StorageBuffer::Thresholds{min_time, min_rows, min_bytes},
            StorageBuffer::Thresholds{max_time, max_rows, max_bytes},
            destination_database, destination_table,
            static_cast<bool>(args.local_context.getSettingsRef().insert_allow_materialized_columns));
    });
}

}
