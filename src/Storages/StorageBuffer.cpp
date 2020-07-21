#include <boost/range/algorithm_ext/erase.hpp>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Transforms/AddingMissedTransform.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTracker.h>
#include <Common/FieldVisitors.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <common/logger_useful.h>
#include <common/getThreadId.h>
#include <ext/range.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Sources/SourceFromInputStream.h>


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
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INFINITE_LOOP;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageBuffer::StorageBuffer(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    Context & context_,
    size_t num_shards_,
    const Thresholds & min_thresholds_,
    const Thresholds & max_thresholds_,
    const StorageID & destination_id_,
    bool allow_materialized_)
    : IStorage(table_id_)
    , global_context(context_)
    , num_shards(num_shards_), buffers(num_shards_)
    , min_thresholds(min_thresholds_)
    , max_thresholds(max_thresholds_)
    , destination_id(destination_id_)
    , allow_materialized(allow_materialized_)
    , log(&Poco::Logger::get("StorageBuffer (" + table_id_.getFullTableName() + ")"))
    , bg_pool(global_context.getBufferFlushSchedulePool())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);
}


/// Reads from one buffer (from one block) under its mutex.
class BufferSource : public SourceWithProgress
{
public:
    BufferSource(const Names & column_names_, StorageBuffer::Buffer & buffer_, const StorageBuffer & storage, const StorageMetadataPtr & metadata_snapshot)
        : SourceWithProgress(
            metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(column_names_.begin(), column_names_.end())
        , buffer(buffer_) {}

    String getName() const override { return "Buffer"; }

protected:
    Chunk generate() override
    {
        Chunk res;

        if (has_been_read)
            return res;
        has_been_read = true;

        std::lock_guard lock(buffer.mutex);

        if (!buffer.data.rows())
            return res;

        Columns columns;
        columns.reserve(column_names.size());

        for (const auto & name : column_names)
            columns.push_back(buffer.data.getByName(name).column);

        UInt64 size = columns.at(0)->size();
        res.setColumns(std::move(columns), size);

        return res;
    }

private:
    Names column_names;
    StorageBuffer::Buffer & buffer;
    bool has_been_read = false;
};


QueryProcessingStage::Enum StorageBuffer::getQueryProcessingStage(const Context & context, QueryProcessingStage::Enum to_stage, const ASTPtr & query_ptr) const
{
    if (destination_id)
    {
        auto destination = DatabaseCatalog::instance().getTable(destination_id, context);

        if (destination.get() == this)
            throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

        return destination->getQueryProcessingStage(context, to_stage, query_ptr);
    }

    return QueryProcessingStage::FetchColumns;
}


Pipes StorageBuffer::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    Pipes pipes_from_dst;

    if (destination_id)
    {
        auto destination = DatabaseCatalog::instance().getTable(destination_id, context);

        if (destination.get() == this)
            throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

        auto destination_lock = destination->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

        auto destination_metadata_snapshot = destination->getInMemoryMetadataPtr();

        const bool dst_has_same_structure = std::all_of(column_names.begin(), column_names.end(), [metadata_snapshot, destination_metadata_snapshot](const String& column_name)
        {
            const auto & dest_columns = destination_metadata_snapshot->getColumns();
            const auto & our_columns = metadata_snapshot->getColumns();
            return dest_columns.hasPhysical(column_name) &&
                   dest_columns.get(column_name).type->equals(*our_columns.get(column_name).type);
        });

        if (dst_has_same_structure)
        {
            if (query_info.order_optimizer)
                query_info.input_order_info = query_info.order_optimizer->getInputOrder(destination, destination_metadata_snapshot);

            /// The destination table has the same structure of the requested columns and we can simply read blocks from there.
            pipes_from_dst = destination->read(
                column_names, destination_metadata_snapshot, query_info,
                context, processed_stage, max_block_size, num_streams);
        }
        else
        {
            /// There is a struct mismatch and we need to convert read blocks from the destination table.
            const Block header = metadata_snapshot->getSampleBlock();
            Names columns_intersection = column_names;
            Block header_after_adding_defaults = header;
            const auto & dest_columns = destination_metadata_snapshot->getColumns();
            const auto & our_columns = metadata_snapshot->getColumns();
            for (const String & column_name : column_names)
            {
                if (!dest_columns.hasPhysical(column_name))
                {
                    LOG_WARNING(log, "Destination table {} doesn't have column {}. The default values are used.", destination_id.getNameForLogs(), backQuoteIfNeed(column_name));
                    boost::range::remove_erase(columns_intersection, column_name);
                    continue;
                }
                const auto & dst_col = dest_columns.getPhysical(column_name);
                const auto & col = our_columns.getPhysical(column_name);
                if (!dst_col.type->equals(*col.type))
                {
                    LOG_WARNING(log, "Destination table {} has different type of column {} ({} != {}). Data from destination table are converted.", destination_id.getNameForLogs(), backQuoteIfNeed(column_name), dst_col.type->getName(), col.type->getName());
                    header_after_adding_defaults.getByName(column_name) = ColumnWithTypeAndName(dst_col.type, column_name);
                }
            }

            if (columns_intersection.empty())
            {
                LOG_WARNING(log, "Destination table {} has no common columns with block in buffer. Block of data is skipped.", destination_id.getNameForLogs());
            }
            else
            {
                pipes_from_dst = destination->read(
                    columns_intersection, destination_metadata_snapshot, query_info,
                    context, processed_stage, max_block_size, num_streams);

                for (auto & pipe : pipes_from_dst)
                {
                    pipe.addSimpleTransform(std::make_shared<AddingMissedTransform>(
                            pipe.getHeader(), header_after_adding_defaults, metadata_snapshot->getColumns().getDefaults(), context));

                    pipe.addSimpleTransform(std::make_shared<ConvertingTransform>(
                            pipe.getHeader(), header, ConvertingTransform::MatchColumnsMode::Name));
                }
            }
        }

        for (auto & pipe : pipes_from_dst)
            pipe.addTableLock(destination_lock);
    }

    Pipes pipes_from_buffers;
    pipes_from_buffers.reserve(num_shards);
    for (auto & buf : buffers)
        pipes_from_buffers.emplace_back(std::make_shared<BufferSource>(column_names, buf, *this, metadata_snapshot));

    /// Convert pipes from table to structure from buffer.
    if (!pipes_from_buffers.empty() && !pipes_from_dst.empty()
        && !blocksHaveEqualStructure(pipes_from_buffers.front().getHeader(), pipes_from_dst.front().getHeader()))
    {
        for (auto & pipe : pipes_from_dst)
            pipe.addSimpleTransform(std::make_shared<ConvertingTransform>(
                    pipe.getHeader(),
                    pipes_from_buffers.front().getHeader(),
                    ConvertingTransform::MatchColumnsMode::Name));
    }

    /** If the sources from the table were processed before some non-initial stage of query execution,
      * then sources from the buffers must also be wrapped in the processing pipeline before the same stage.
      */
    if (processed_stage > QueryProcessingStage::FetchColumns)
        for (auto & pipe : pipes_from_buffers)
            pipe = InterpreterSelectQuery(query_info.query, context, std::move(pipe), SelectQueryOptions(processed_stage)).execute().pipeline.getPipe();

    if (query_info.prewhere_info)
    {
        for (auto & pipe : pipes_from_buffers)
            pipe.addSimpleTransform(std::make_shared<FilterTransform>(pipe.getHeader(), query_info.prewhere_info->prewhere_actions,
                    query_info.prewhere_info->prewhere_column_name, query_info.prewhere_info->remove_prewhere_column));

        if (query_info.prewhere_info->alias_actions)
        {
            for (auto & pipe : pipes_from_buffers)
                pipe.addSimpleTransform(std::make_shared<ExpressionTransform>(pipe.getHeader(), query_info.prewhere_info->alias_actions));

        }
    }

    for (auto & pipe : pipes_from_buffers)
        pipes_from_dst.emplace_back(std::move(pipe));

    return pipes_from_dst;
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

    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    try
    {
        for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
        {
            const IColumn & col_from = *from.getByPosition(column_no).column.get();
            MutableColumnPtr col_to = IColumn::mutate(std::move(to.getByPosition(column_no).column));

            col_to->insertRangeFrom(col_from, 0, rows);

            to.getByPosition(column_no).column = std::move(col_to);
        }
    }
    catch (...)
    {
        /// Rollback changes.
        try
        {
            for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
            {
                ColumnPtr & col_to = to.getByPosition(column_no).column;
                if (col_to->size() != old_rows)
                    col_to = col_to->cut(0, old_rows);
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
    explicit BufferBlockOutputStream(
        StorageBuffer & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {}

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        if (!block)
            return;

        // Check table structure.
        metadata_snapshot->check(block, true);

        size_t rows = block.rows();
        if (!rows)
            return;

        StoragePtr destination;
        if (storage.destination_id)
        {
            destination = DatabaseCatalog::instance().tryGetTable(storage.destination_id, storage.global_context);
            if (destination.get() == &storage)
                throw Exception("Destination table is myself. Write will cause infinite loop.", ErrorCodes::INFINITE_LOOP);
        }

        size_t bytes = block.bytes();

        storage.writes.rows += rows;
        storage.writes.bytes += bytes;

        /// If the block already exceeds the maximum limit, then we skip the buffer.
        if (rows > storage.max_thresholds.rows || bytes > storage.max_thresholds.bytes)
        {
            if (storage.destination_id)
            {
                LOG_TRACE(storage.log, "Writing block with {} rows, {} bytes directly.", rows, bytes);
                storage.writeBlockToDestination(block, destination);
            }
            return;
        }

        /// We distribute the load on the shards by the stream number.
        const auto start_shard_num = getThreadId() % storage.num_shards;

        /// We loop through the buffers, trying to lock mutex. No more than one lap.
        auto shard_num = start_shard_num;

        StorageBuffer::Buffer * least_busy_buffer = nullptr;
        std::unique_lock<std::mutex> least_busy_lock;
        size_t least_busy_shard_rows = 0;

        for (size_t try_no = 0; try_no < storage.num_shards; ++try_no)
        {
            std::unique_lock lock(storage.buffers[shard_num].mutex, std::try_to_lock);

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
            least_busy_lock = std::unique_lock(least_busy_buffer->mutex);
        }
        insertIntoBuffer(block, *least_busy_buffer);
        least_busy_lock.unlock();

        storage.reschedule();
    }
private:
    StorageBuffer & storage;
    StorageMetadataPtr metadata_snapshot;

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

            storage.flushBuffer(buffer, false /* check_thresholds */, true /* locked */);
        }

        if (!buffer.first_write_time)
            buffer.first_write_time = current_time;

        appendBlock(sorted_block, buffer.data);
    }
};


BlockOutputStreamPtr StorageBuffer::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<BufferBlockOutputStream>(*this, metadata_snapshot);
}


bool StorageBuffer::mayBenefitFromIndexForIn(
    const ASTPtr & left_in_operand, const Context & query_context, const StorageMetadataPtr & /*metadata_snapshot*/) const
{
    if (!destination_id)
        return false;

    auto destination = DatabaseCatalog::instance().getTable(destination_id, query_context);

    if (destination.get() == this)
        throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

    return destination->mayBenefitFromIndexForIn(left_in_operand, query_context, destination->getInMemoryMetadataPtr());
}


void StorageBuffer::startup()
{
    if (global_context.getSettingsRef().readonly)
    {
        LOG_WARNING(log, "Storage {} is run with readonly settings, it will not be able to insert data. Set appropriate system_profile to fix this.", getName());
    }

    flush_handle = bg_pool.createTask(log->name() + "/Bg", [this]{ flushBack(); });
    flush_handle->activateAndSchedule();
}


void StorageBuffer::shutdown()
{
    if (!flush_handle)
        return;

    flush_handle->deactivate();

    try
    {
        optimize(nullptr /*query*/, getInMemoryMetadataPtr(), {} /*partition*/, false /*final*/, false /*deduplicate*/, global_context);
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
bool StorageBuffer::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Context & /*context*/)
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

    std::unique_lock lock(buffer.mutex, std::defer_lock);
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

    LOG_TRACE(log, "Flushing buffer with {} rows, {} bytes, age {} seconds {}.", rows, bytes, time_passed, (check_thresholds ? "(bg)" : "(direct)"));

    if (!destination_id)
        return;

    /** For simplicity, buffer is locked during write.
        * We could unlock buffer temporary, but it would lead to too many difficulties:
        * - data, that is written, will not be visible for SELECTs;
        * - new data could be appended to buffer, and in case of exception, we must merge it with old data, that has not been written;
        * - this could lead to infinite memory growth.
        */
    try
    {
        writeBlockToDestination(block_to_write, DatabaseCatalog::instance().tryGetTable(destination_id, global_context));
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
    if (!destination_id || !block)
        return;

    if (!table)
    {
        LOG_ERROR(log, "Destination table {} doesn't exist. Block of data is discarded.", destination_id.getNameForLogs());
        return;
    }
    auto destination_metadata_snapshot = table->getInMemoryMetadataPtr();

    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = destination_id;

    /** We will insert columns that are the intersection set of columns of the buffer table and the subordinate table.
      * This will support some of the cases (but not all) when the table structure does not match.
      */
    Block structure_of_destination_table = allow_materialized ? destination_metadata_snapshot->getSampleBlock()
                                                              : destination_metadata_snapshot->getSampleBlockNonMaterialized();
    Block block_to_write;
    for (size_t i : ext::range(0, structure_of_destination_table.columns()))
    {
        auto dst_col = structure_of_destination_table.getByPosition(i);
        if (block.has(dst_col.name))
        {
            auto column = block.getByName(dst_col.name);
            if (!column.type->equals(*dst_col.type))
            {
                LOG_WARNING(log, "Destination table {} have different type of column {} ({} != {}). Block of data is converted.", destination_id.getNameForLogs(), backQuoteIfNeed(column.name), dst_col.type->getName(), column.type->getName());
                column.column = castColumn(column, dst_col.type);
                column.type = dst_col.type;
            }

            block_to_write.insert(column);
        }
    }

    if (block_to_write.columns() == 0)
    {
        LOG_ERROR(log, "Destination table {} have no common columns with block in buffer. Block of data is discarded.", destination_id.getNameForLogs());
        return;
    }

    if (block_to_write.columns() != block.columns())
        LOG_WARNING(log, "Not all columns from block in buffer exist in destination table {}. Some columns are discarded.", destination_id.getNameForLogs());

    auto list_of_columns = std::make_shared<ASTExpressionList>();
    insert->columns = list_of_columns;
    list_of_columns->children.reserve(block_to_write.columns());
    for (const auto & column : block_to_write)
        list_of_columns->children.push_back(std::make_shared<ASTIdentifier>(column.name));

    auto insert_context = Context(global_context);
    insert_context.makeQueryContext();

    InterpreterInsertQuery interpreter{insert, insert_context, allow_materialized};

    auto block_io = interpreter.execute();
    block_io.out->writePrefix();
    block_io.out->write(block_to_write);
    block_io.out->writeSuffix();
}


void StorageBuffer::flushBack()
{
    try
    {
        flushAllBuffers(true);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    reschedule();
}

void StorageBuffer::reschedule()
{
    time_t min_first_write_time = std::numeric_limits<time_t>::max();
    time_t rows = 0;

    for (auto & buffer : buffers)
    {
        std::lock_guard lock(buffer.mutex);
        min_first_write_time = buffer.first_write_time;
        rows += buffer.data.rows();
    }

    /// will be rescheduled via INSERT
    if (!rows)
        return;

    time_t current_time = time(nullptr);
    time_t time_passed = current_time - min_first_write_time;

    size_t min = std::max<ssize_t>(min_thresholds.time - time_passed, 1);
    size_t max = std::max<ssize_t>(max_thresholds.time - time_passed, 1);
    flush_handle->scheduleAfter(std::min(min, max) * 1000);
}

void StorageBuffer::checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) const
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN)
            throw Exception(
                "Alter of type '" + alterTypeToString(command.type) + "' is not supported by storage " + getName(),
                ErrorCodes::NOT_IMPLEMENTED);
    }
}

std::optional<UInt64> StorageBuffer::totalRows() const
{
    std::optional<UInt64> underlying_rows;
    auto underlying = DatabaseCatalog::instance().tryGetTable(destination_id, global_context);

    if (underlying)
        underlying_rows = underlying->totalRows();
    if (!underlying_rows)
        return underlying_rows;

    UInt64 rows = 0;
    for (const auto & buffer : buffers)
    {
        std::lock_guard lock(buffer.mutex);
        rows += buffer.data.rows();
    }
    return rows + *underlying_rows;
}

std::optional<UInt64> StorageBuffer::totalBytes() const
{
    UInt64 bytes = 0;
    for (const auto & buffer : buffers)
    {
        std::lock_guard lock(buffer.mutex);
        bytes += buffer.data.allocatedBytes();
    }
    return bytes;
}

void StorageBuffer::alter(const AlterCommands & params, const Context & context, TableLockHolder &)
{
    auto table_id = getStorageID();
    checkAlterIsPossible(params, context.getSettingsRef());
    auto metadata_snapshot = getInMemoryMetadataPtr();

    /// So that no blocks of the old structure remain.
    optimize({} /*query*/, metadata_snapshot, {} /*partition_id*/, false /*final*/, false /*deduplicate*/, context);

    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    params.apply(new_metadata, context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
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

        // Table and database name arguments accept expressions, evaluate them.
        engine_args[0] = evaluateConstantExpressionForDatabaseName(engine_args[0], args.local_context);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);

        // After we evaluated all expressions, check that all arguments are
        // literals.
        for (size_t i = 0; i < 9; i++)
        {
            if (!typeid_cast<ASTLiteral *>(engine_args[i].get()))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Storage Buffer expects a literal as an argument #{}, got '{}'"
                    " instead", i, engine_args[i]->formatForErrorMessage());
            }
        }

        String destination_database = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String destination_table = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();

        UInt64 num_buckets = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[2]->as<ASTLiteral &>().value);

        Int64 min_time = applyVisitor(FieldVisitorConvertToNumber<Int64>(), engine_args[3]->as<ASTLiteral &>().value);
        Int64 max_time = applyVisitor(FieldVisitorConvertToNumber<Int64>(), engine_args[4]->as<ASTLiteral &>().value);
        UInt64 min_rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[5]->as<ASTLiteral &>().value);
        UInt64 max_rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[6]->as<ASTLiteral &>().value);
        UInt64 min_bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[7]->as<ASTLiteral &>().value);
        UInt64 max_bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[8]->as<ASTLiteral &>().value);

        /// If destination_id is not set, do not write data from the buffer, but simply empty the buffer.
        StorageID destination_id = StorageID::createEmpty();
        if (!destination_table.empty())
        {
            destination_id.database_name = args.context.resolveDatabase(destination_database);
            destination_id.table_name = destination_table;
        }

        return StorageBuffer::create(
            args.table_id,
            args.columns,
            args.constraints,
            args.context,
            num_buckets,
            StorageBuffer::Thresholds{min_time, min_rows, min_bytes},
            StorageBuffer::Thresholds{max_time, max_rows, max_bytes},
            destination_id,
            static_cast<bool>(args.local_context.getSettingsRef().insert_allow_materialized_columns));
    });
}

}
