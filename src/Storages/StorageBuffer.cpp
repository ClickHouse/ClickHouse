#include <boost/range/algorithm_ext/erase.hpp>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageFactory.h>
#include <Storages/AlterCommands.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/getThreadId.h>
#include <base/range.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/Executors/PushingPipelineExecutor.h>


namespace ProfileEvents
{
    extern const Event StorageBufferFlush;
    extern const Event StorageBufferErrorOnFlush;
    extern const Event StorageBufferPassedAllMinThresholds;
    extern const Event StorageBufferPassedTimeMaxThreshold;
    extern const Event StorageBufferPassedRowsMaxThreshold;
    extern const Event StorageBufferPassedBytesMaxThreshold;
    extern const Event StorageBufferPassedTimeFlushThreshold;
    extern const Event StorageBufferPassedRowsFlushThreshold;
    extern const Event StorageBufferPassedBytesFlushThreshold;
    extern const Event StorageBufferLayerLockReadersWaitMilliseconds;
    extern const Event StorageBufferLayerLockWritersWaitMilliseconds;
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
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
}


std::unique_lock<std::mutex> StorageBuffer::Buffer::lockForReading() const
{
    return lockImpl(/* read= */true);
}
std::unique_lock<std::mutex> StorageBuffer::Buffer::lockForWriting() const
{
    return lockImpl(/* read= */false);
}
std::unique_lock<std::mutex> StorageBuffer::Buffer::tryLock() const
{
    std::unique_lock lock(mutex, std::try_to_lock);
    return lock;
}
std::unique_lock<std::mutex> StorageBuffer::Buffer::lockImpl(bool read) const
{
    std::unique_lock lock(mutex, std::defer_lock);

    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    lock.lock();
    UInt64 elapsed = watch.elapsedMilliseconds();

    if (read)
        ProfileEvents::increment(ProfileEvents::StorageBufferLayerLockReadersWaitMilliseconds, elapsed);
    else
        ProfileEvents::increment(ProfileEvents::StorageBufferLayerLockWritersWaitMilliseconds, elapsed);

    return lock;
}


StorageBuffer::StorageBuffer(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    size_t num_shards_,
    const Thresholds & min_thresholds_,
    const Thresholds & max_thresholds_,
    const Thresholds & flush_thresholds_,
    const StorageID & destination_id_,
    bool allow_materialized_)
    : IStorage(table_id_)
    , WithContext(context_->getBufferContext())
    , num_shards(num_shards_)
    , buffers(num_shards_)
    , min_thresholds(min_thresholds_)
    , max_thresholds(max_thresholds_)
    , flush_thresholds(flush_thresholds_)
    , destination_id(destination_id_)
    , allow_materialized(allow_materialized_)
    , log(&Poco::Logger::get("StorageBuffer (" + table_id_.getFullTableName() + ")"))
    , bg_pool(getContext()->getBufferFlushSchedulePool())
{
    StorageInMemoryMetadata storage_metadata;
    if (columns_.empty())
    {
        auto dest_table = DatabaseCatalog::instance().getTable(destination_id, context_);
        storage_metadata.setColumns(dest_table->getInMemoryMetadataPtr()->getColumns());
    }
    else
        storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    flush_handle = bg_pool.createTask(log->name() + "/Bg", [this]{ backgroundFlush(); });
}


/// Reads from one buffer (from one block) under its mutex.
class BufferSource : public ISource
{
public:
    BufferSource(const Names & column_names_, StorageBuffer::Buffer & buffer_, const StorageSnapshotPtr & storage_snapshot)
        : ISource(storage_snapshot->getSampleBlockForColumns(column_names_))
        , column_names_and_types(storage_snapshot->getColumnsByNames(
            GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), column_names_))
        , buffer(buffer_) {}

    String getName() const override { return "Buffer"; }

protected:
    Chunk generate() override
    {
        Chunk res;

        if (has_been_read)
            return res;
        has_been_read = true;

        std::unique_lock lock(buffer.lockForReading());

        if (!buffer.data.rows())
            return res;

        Columns columns;
        columns.reserve(column_names_and_types.size());

        for (const auto & elem : column_names_and_types)
            columns.emplace_back(getColumnFromBlock(buffer.data, elem));

        UInt64 size = columns.at(0)->size();
        res.setColumns(std::move(columns), size);

        return res;
    }

private:
    NamesAndTypesList column_names_and_types;
    StorageBuffer::Buffer & buffer;
    bool has_been_read = false;
};


QueryProcessingStage::Enum StorageBuffer::getQueryProcessingStage(
    ContextPtr local_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr &,
    SelectQueryInfo & query_info) const
{
    if (destination_id)
    {
        auto destination = DatabaseCatalog::instance().getTable(destination_id, local_context);

        if (destination.get() == this)
            throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

        /// TODO: Find a way to support projections for StorageBuffer
        query_info.ignore_projections = true;
        const auto & destination_metadata = destination->getInMemoryMetadataPtr();
        return destination->getQueryProcessingStage(local_context, to_stage, destination->getStorageSnapshot(destination_metadata, local_context), query_info);
    }

    return QueryProcessingStage::FetchColumns;
}

void StorageBuffer::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    const auto & metadata_snapshot = storage_snapshot->metadata;

    if (destination_id)
    {
        auto destination = DatabaseCatalog::instance().getTable(destination_id, local_context);

        if (destination.get() == this)
            throw Exception("Destination table is myself. Read will cause infinite loop.", ErrorCodes::INFINITE_LOOP);

        auto destination_lock = destination->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);

        auto destination_metadata_snapshot = destination->getInMemoryMetadataPtr();
        auto destination_snapshot = destination->getStorageSnapshot(destination_metadata_snapshot, local_context);

        const bool dst_has_same_structure = std::all_of(column_names.begin(), column_names.end(), [metadata_snapshot, destination_metadata_snapshot](const String& column_name)
        {
            const auto & dest_columns = destination_metadata_snapshot->getColumns();
            const auto & our_columns = metadata_snapshot->getColumns();
            auto dest_columm = dest_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name);
            return dest_columm && dest_columm->type->equals(*our_columns.getColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column_name).type);
        });

        if (dst_has_same_structure)
        {
            if (query_info.order_optimizer)
                query_info.input_order_info = query_info.order_optimizer->getInputOrder(destination_metadata_snapshot, local_context);

            /// The destination table has the same structure of the requested columns and we can simply read blocks from there.
            destination->read(
                query_plan, column_names, destination_snapshot, query_info,
                local_context, processed_stage, max_block_size, num_streams);
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
                destination->read(
                        query_plan, columns_intersection, destination_snapshot, query_info,
                        local_context, processed_stage, max_block_size, num_streams);

                if (query_plan.isInitialized())
                {

                    auto actions = addMissingDefaults(
                            query_plan.getCurrentDataStream().header,
                            header_after_adding_defaults.getNamesAndTypesList(),
                            metadata_snapshot->getColumns(),
                            local_context);

                    auto adding_missed = std::make_unique<ExpressionStep>(
                            query_plan.getCurrentDataStream(),
                            std::move(actions));

                    adding_missed->setStepDescription("Add columns missing in destination table");
                    query_plan.addStep(std::move(adding_missed));

                    auto actions_dag = ActionsDAG::makeConvertingActions(
                            query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
                            header.getColumnsWithTypeAndName(),
                            ActionsDAG::MatchColumnsMode::Name);

                    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), actions_dag);

                    converting->setStepDescription("Convert destination table columns to Buffer table structure");
                    query_plan.addStep(std::move(converting));
                }
            }
        }

        if (query_plan.isInitialized())
        {
            query_plan.addStorageHolder(destination);
            query_plan.addTableLock(std::move(destination_lock));
        }
    }

    Pipe pipe_from_buffers;
    {
        Pipes pipes_from_buffers;
        pipes_from_buffers.reserve(num_shards);
        for (auto & buf : buffers)
            pipes_from_buffers.emplace_back(std::make_shared<BufferSource>(column_names, buf, storage_snapshot));

        pipe_from_buffers = Pipe::unitePipes(std::move(pipes_from_buffers));
    }

    if (pipe_from_buffers.empty())
        return;

    QueryPlan buffers_plan;

    /** If the sources from the table were processed before some non-initial stage of query execution,
      * then sources from the buffers must also be wrapped in the processing pipeline before the same stage.
      */
    if (processed_stage > QueryProcessingStage::FetchColumns)
    {
        /// TODO: Find a way to support projections for StorageBuffer
        auto interpreter = InterpreterSelectQuery(
                query_info.query, local_context, std::move(pipe_from_buffers),
                SelectQueryOptions(processed_stage).ignoreProjections());
        interpreter.addStorageLimits(*query_info.storage_limits);
        interpreter.buildQueryPlan(buffers_plan);
    }
    else
    {
        if (query_info.prewhere_info)
        {
            auto actions_settings = ExpressionActionsSettings::fromContext(local_context);

            if (query_info.prewhere_info->row_level_filter)
            {
                pipe_from_buffers.addSimpleTransform([&](const Block & header)
                {
                    return std::make_shared<FilterTransform>(
                            header,
                            std::make_shared<ExpressionActions>(query_info.prewhere_info->row_level_filter, actions_settings),
                            query_info.prewhere_info->row_level_column_name,
                            false);
                });
            }

            pipe_from_buffers.addSimpleTransform([&](const Block & header)
            {
                return std::make_shared<FilterTransform>(
                        header,
                        std::make_shared<ExpressionActions>(query_info.prewhere_info->prewhere_actions, actions_settings),
                        query_info.prewhere_info->prewhere_column_name,
                        query_info.prewhere_info->remove_prewhere_column);
            });
        }

        for (const auto & processor : pipe_from_buffers.getProcessors())
            processor->setStorageLimits(query_info.storage_limits);

        auto read_from_buffers = std::make_unique<ReadFromPreparedSource>(std::move(pipe_from_buffers));
        read_from_buffers->setStepDescription("Read from buffers of Buffer table");
        buffers_plan.addStep(std::move(read_from_buffers));
    }

    if (!query_plan.isInitialized())
    {
        query_plan = std::move(buffers_plan);
        return;
    }

    auto result_header = buffers_plan.getCurrentDataStream().header;

    /// Convert structure from table to structure from buffer.
    if (!blocksHaveEqualStructure(query_plan.getCurrentDataStream().header, result_header))
    {
        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
                result_header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

        auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), convert_actions_dag);
        query_plan.addStep(std::move(converting));
    }

    DataStreams input_streams;
    input_streams.emplace_back(query_plan.getCurrentDataStream());
    input_streams.emplace_back(buffers_plan.getCurrentDataStream());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(buffers_plan)));
    query_plan = QueryPlan();

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
    union_step->setStepDescription("Unite sources from Buffer table");
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}


static void appendBlock(const Block & from, Block & to)
{
    size_t rows = from.rows();
    size_t old_rows = to.rows();
    size_t old_bytes = to.bytes();

    if (!to)
        to = from.cloneEmpty();

    assertBlocksHaveEqualStructure(from, to, "Buffer");

    from.checkNumberOfRows();
    to.checkNumberOfRows();

    MutableColumnPtr last_col;
    try
    {
        MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

        for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
        {
            const IColumn & col_from = *from.getByPosition(column_no).column.get();
            last_col = IColumn::mutate(std::move(to.getByPosition(column_no).column));

            /// In case of ColumnAggregateFunction aggregate states will
            /// be allocated from the query context but can be destroyed from the
            /// server context (in case of background flush), and thus memory
            /// will be leaked from the query, but only tracked memory, not
            /// memory itself.
            ///
            /// To avoid this, prohibit sharing the aggregate states.
            last_col->ensureOwnership();
            last_col->insertRangeFrom(col_from, 0, rows);

            to.getByPosition(column_no).column = std::move(last_col);
        }
        CurrentMetrics::add(CurrentMetrics::StorageBufferRows, rows);
        CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, to.bytes() - old_bytes);
    }
    catch (...)
    {
        /// Rollback changes.

        /// In case of rollback, it is better to ignore memory limits instead of abnormal server termination.
        /// So ignore any memory limits, even global (since memory tracking has drift).
        LockMemoryExceptionInThread temporarily_ignore_any_memory_limits(VariableContext::Global);

        try
        {
            for (size_t column_no = 0, columns = to.columns(); column_no < columns; ++column_no)
            {
                ColumnPtr & col_to = to.getByPosition(column_no).column;
                /// If there is no column, then the exception was thrown in the middle of append, in the insertRangeFrom()
                if (!col_to)
                {
                    col_to = std::move(last_col);
                    /// Suppress clang-tidy [bugprone-use-after-move]
                    last_col = {};
                }
                /// But if there is still nothing, abort
                if (!col_to)
                    throw Exception("No column to rollback", ErrorCodes::LOGICAL_ERROR);
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


class BufferSink : public SinkToStorage
{
public:
    explicit BufferSink(
        StorageBuffer & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {
        // Check table structure.
        metadata_snapshot->check(getHeader(), true);
    }

    String getName() const override { return "BufferSink"; }

    void consume(Chunk chunk) override
    {
        size_t rows = chunk.getNumRows();
        if (!rows)
            return;

        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        StoragePtr destination;
        if (storage.destination_id)
        {
            destination = DatabaseCatalog::instance().tryGetTable(storage.destination_id, storage.getContext());
            if (destination.get() == &storage)
                throw Exception("Destination table is myself. Write will cause infinite loop.", ErrorCodes::INFINITE_LOOP);
        }

        size_t bytes = block.bytes();

        storage.lifetime_writes.rows += rows;
        storage.lifetime_writes.bytes += bytes;

        /// If the block already exceeds the maximum limit, then we skip the buffer.
        if (rows > storage.max_thresholds.rows || bytes > storage.max_thresholds.bytes)
        {
            if (storage.destination_id)
            {
                LOG_DEBUG(storage.log, "Writing block with {} rows, {} bytes directly.", rows, bytes);
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
            std::unique_lock lock(storage.buffers[shard_num].tryLock());

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
            least_busy_lock = least_busy_buffer->lockForWriting();
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

        if (storage.checkThresholds(buffer, /* direct= */true, current_time, sorted_block.rows(), sorted_block.bytes()))
        {
            /** If, after inserting the buffer, the constraints are exceeded, then we will reset the buffer.
              * This also protects against unlimited consumption of RAM, since if it is impossible to write to the table,
              *  an exception will be thrown, and new data will not be added to the buffer.
              */

            storage.flushBuffer(buffer, false /* check_thresholds */, true /* locked */);
        }

        if (!buffer.first_write_time)
            buffer.first_write_time = current_time;

        size_t old_rows = buffer.data.rows();
        size_t old_bytes = buffer.data.allocatedBytes();

        appendBlock(sorted_block, buffer.data);

        storage.total_writes.rows += (buffer.data.rows() - old_rows);
        storage.total_writes.bytes += (buffer.data.allocatedBytes() - old_bytes);
    }
};


SinkToStoragePtr StorageBuffer::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/)
{
    return std::make_shared<BufferSink>(*this, metadata_snapshot);
}


bool StorageBuffer::mayBenefitFromIndexForIn(
    const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & /*metadata_snapshot*/) const
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
    if (getContext()->getSettingsRef().readonly)
    {
        LOG_WARNING(log, "Storage {} is run with readonly settings, it will not be able to insert data. Set appropriate buffer_profile to fix this.", getName());
    }

    flush_handle->activateAndSchedule();
}


void StorageBuffer::flush()
{
    if (!flush_handle)
        return;

    flush_handle->deactivate();

    try
    {
        optimize(nullptr /*query*/, getInMemoryMetadataPtr(), {} /*partition*/, false /*final*/, false /*deduplicate*/, {}, getContext());
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
    const Names & /* deduplicate_by_columns */,
    ContextPtr /*context*/)
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

bool StorageBuffer::supportsPrewhere() const
{
    if (!destination_id)
        return false;
    auto dest = DatabaseCatalog::instance().tryGetTable(destination_id, getContext());
    if (dest && dest.get() != this)
        return dest->supportsPrewhere();
    return false;
}

bool StorageBuffer::checkThresholds(const Buffer & buffer, bool direct, time_t current_time, size_t additional_rows, size_t additional_bytes) const
{
    time_t time_passed = 0;
    if (buffer.first_write_time)
        time_passed = current_time - buffer.first_write_time;

    size_t rows = buffer.data.rows() + additional_rows;
    size_t bytes = buffer.data.bytes() + additional_bytes;

    return checkThresholdsImpl(direct, rows, bytes, time_passed);
}


bool StorageBuffer::checkThresholdsImpl(bool direct, size_t rows, size_t bytes, time_t time_passed) const
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

    if (!direct)
    {
        if (flush_thresholds.time && time_passed > flush_thresholds.time)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferPassedTimeFlushThreshold);
            return true;
        }

        if (flush_thresholds.rows && rows > flush_thresholds.rows)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferPassedRowsFlushThreshold);
            return true;
        }

        if (flush_thresholds.bytes && bytes > flush_thresholds.bytes)
        {
            ProfileEvents::increment(ProfileEvents::StorageBufferPassedBytesFlushThreshold);
            return true;
        }
    }

    return false;
}


void StorageBuffer::flushAllBuffers(bool check_thresholds)
{
    for (auto & buf : buffers)
        flushBuffer(buf, check_thresholds, false);
}


bool StorageBuffer::flushBuffer(Buffer & buffer, bool check_thresholds, bool locked)
{
    Block block_to_write;
    time_t current_time = time(nullptr);

    std::optional<std::unique_lock<std::mutex>> lock;
    if (!locked)
        lock.emplace(buffer.lockForReading());

    time_t time_passed = 0;
    size_t rows = buffer.data.rows();
    size_t bytes = buffer.data.bytes();
    if (buffer.first_write_time)
        time_passed = current_time - buffer.first_write_time;

    if (check_thresholds)
    {
        if (!checkThresholdsImpl(/* direct= */false, rows, bytes, time_passed))
            return false;
    }

    buffer.data.swap(block_to_write);
    buffer.first_write_time = 0;

    size_t block_rows = block_to_write.rows();
    size_t block_bytes = block_to_write.bytes();
    size_t block_allocated_bytes_delta = block_to_write.allocatedBytes() - buffer.data.allocatedBytes();

    CurrentMetrics::sub(CurrentMetrics::StorageBufferRows, block_rows);
    CurrentMetrics::sub(CurrentMetrics::StorageBufferBytes, block_bytes);

    ProfileEvents::increment(ProfileEvents::StorageBufferFlush);

    if (!destination_id)
    {
        total_writes.rows -= block_rows;
        total_writes.bytes -= block_allocated_bytes_delta;

        LOG_DEBUG(log, "Flushing buffer with {} rows (discarded), {} bytes, age {} seconds {}.", rows, bytes, time_passed, (check_thresholds ? "(bg)" : "(direct)"));
        return true;
    }

    /** For simplicity, buffer is locked during write.
        * We could unlock buffer temporary, but it would lead to too many difficulties:
        * - data, that is written, will not be visible for SELECTs;
        * - new data could be appended to buffer, and in case of exception, we must merge it with old data, that has not been written;
        * - this could lead to infinite memory growth.
        */

    Stopwatch watch;
    try
    {
        writeBlockToDestination(block_to_write, DatabaseCatalog::instance().tryGetTable(destination_id, getContext()));
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::StorageBufferErrorOnFlush);

        /// Return the block to its place in the buffer.

        CurrentMetrics::add(CurrentMetrics::StorageBufferRows, block_to_write.rows());
        CurrentMetrics::add(CurrentMetrics::StorageBufferBytes, block_to_write.bytes());

        buffer.data.swap(block_to_write);

        if (!buffer.first_write_time) // -V547
            buffer.first_write_time = current_time;

        /// After a while, the next write attempt will happen.
        throw;
    }

    total_writes.rows -= block_rows;
    total_writes.bytes -= block_allocated_bytes_delta;

    UInt64 milliseconds = watch.elapsedMilliseconds();
    LOG_DEBUG(log, "Flushing buffer with {} rows, {} bytes, age {} seconds, took {} ms {}.", rows, bytes, time_passed, milliseconds, (check_thresholds ? "(bg)" : "(direct)"));
    return true;
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

    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    auto insert = std::make_shared<ASTInsertQuery>();
    insert->table_id = destination_id;

    /** We will insert columns that are the intersection set of columns of the buffer table and the subordinate table.
      * This will support some of the cases (but not all) when the table structure does not match.
      */
    Block structure_of_destination_table = allow_materialized ? destination_metadata_snapshot->getSampleBlock()
                                                              : destination_metadata_snapshot->getSampleBlockNonMaterialized();
    Block block_to_write;
    for (size_t i : collections::range(0, structure_of_destination_table.columns()))
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

    auto insert_context = Context::createCopy(getContext());
    insert_context->makeQueryContext();

    InterpreterInsertQuery interpreter{insert, insert_context, allow_materialized};

    auto block_io = interpreter.execute();
    PushingPipelineExecutor executor(block_io.pipeline);
    executor.start();
    executor.push(std::move(block_to_write));
    executor.finish();
}


void StorageBuffer::backgroundFlush()
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
        /// try_to_lock here to avoid waiting for other layers flushing to be finished,
        /// since the buffer table may:
        /// - push to Distributed table, that may take too much time,
        /// - push to table with materialized views attached,
        ///   this is also may take some time.
        ///
        /// try_to_lock is also ok for background flush, since if there is
        /// INSERT contended, then the reschedule will be done after
        /// INSERT will be done.
        std::unique_lock lock(buffer.tryLock());
        if (lock.owns_lock())
        {
            min_first_write_time = buffer.first_write_time;
            rows += buffer.data.rows();
        }
    }

    /// will be rescheduled via INSERT
    if (!rows)
        return;

    time_t current_time = time(nullptr);
    time_t time_passed = current_time - min_first_write_time;

    size_t min = std::max<ssize_t>(min_thresholds.time - time_passed, 1);
    size_t max = std::max<ssize_t>(max_thresholds.time - time_passed, 1);
    size_t flush = std::max<ssize_t>(flush_thresholds.time - time_passed, 1);
    flush_handle->scheduleAfter(std::min({min, max, flush}) * 1000);
}

void StorageBuffer::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    auto name_deps = getDependentViewsByColumn(local_context);
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());

        if (command.type == AlterCommand::Type::DROP_COLUMN && !command.clear)
        {
            const auto & deps_mv = name_deps[command.column_name];
            if (!deps_mv.empty())
            {
                throw Exception(
                    "Trying to ALTER DROP column " + backQuoteIfNeed(command.column_name) + " which is referenced by materialized view "
                        + toString(deps_mv),
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN);
            }
        }
    }
}

std::optional<UInt64> StorageBuffer::totalRows(const Settings & settings) const
{
    std::optional<UInt64> underlying_rows;
    auto underlying = DatabaseCatalog::instance().tryGetTable(destination_id, getContext());

    if (underlying)
        underlying_rows = underlying->totalRows(settings);
    if (!underlying_rows)
        return underlying_rows;

    return total_writes.rows + *underlying_rows;
}

std::optional<UInt64> StorageBuffer::totalBytes(const Settings & /*settings*/) const
{
    return total_writes.bytes;
}

void StorageBuffer::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    auto table_id = getStorageID();
    checkAlterIsPossible(params, local_context);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    /// Flush all buffers to storages, so that no non-empty blocks of the old
    /// structure remain. Structure of empty blocks will be updated during first
    /// insert.
    optimize({} /*query*/, metadata_snapshot, {} /*partition_id*/, false /*final*/, false /*deduplicate*/, {}, local_context);

    StorageInMemoryMetadata new_metadata = *metadata_snapshot;
    params.apply(new_metadata, local_context);
    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}


void registerStorageBuffer(StorageFactory & factory)
{
    /** Buffer(db, table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
      *
      * db, table - in which table to put data from buffer.
      * num_buckets - level of parallelism.
      * min_time, max_time, min_rows, max_rows, min_bytes, max_bytes - conditions for flushing the buffer,
      * flush_time, flush_rows, flush_bytes - conditions for flushing.
      */

    factory.registerStorage("Buffer", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() < 9 || engine_args.size() > 12)
            throw Exception("Storage Buffer requires from 9 to 12 parameters: "
                " destination_database, destination_table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes[, flush_time, flush_rows, flush_bytes].",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        // Table and database name arguments accept expressions, evaluate them.
        engine_args[0] = evaluateConstantExpressionForDatabaseName(engine_args[0], args.getLocalContext());
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.getLocalContext());

        // After we evaluated all expressions, check that all arguments are
        // literals.
        for (size_t i = 0; i < engine_args.size(); ++i)
        {
            if (!typeid_cast<ASTLiteral *>(engine_args[i].get()))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Storage Buffer expects a literal as an argument #{}, got '{}'"
                    " instead", i, engine_args[i]->formatForErrorMessage());
            }
        }

        size_t i = 0;

        String destination_database = engine_args[i++]->as<ASTLiteral &>().value.safeGet<String>();
        String destination_table = engine_args[i++]->as<ASTLiteral &>().value.safeGet<String>();

        UInt64 num_buckets = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);

        StorageBuffer::Thresholds min;
        StorageBuffer::Thresholds max;
        StorageBuffer::Thresholds flush;

        min.time = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        max.time = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        min.rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        max.rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        min.bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        max.bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        if (engine_args.size() > i)
            flush.time = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        if (engine_args.size() > i)
            flush.rows = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);
        if (engine_args.size() > i)
            flush.bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), engine_args[i++]->as<ASTLiteral &>().value);

        /// If destination_id is not set, do not write data from the buffer, but simply empty the buffer.
        StorageID destination_id = StorageID::createEmpty();
        if (!destination_table.empty())
        {
            destination_id.database_name = args.getContext()->resolveDatabase(destination_database);
            destination_id.table_name = destination_table;
        }

        return std::make_shared<StorageBuffer>(
            args.table_id,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            num_buckets,
            min,
            max,
            flush,
            destination_id,
            static_cast<bool>(args.getLocalContext()->getSettingsRef().insert_allow_materialized_columns));
    },
    {
        .supports_parallel_insert = true,
        .supports_schema_inference = true,
    });
}

}
