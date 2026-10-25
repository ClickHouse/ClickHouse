#include <GPU/GPUJoin.h>

#if USE_GPU

#include <GPU/CudfHashJoin.cuh>
#include <GPU/GPUDevice.h>
#include <GPU/GPUGather.cuh>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypeMapping.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Interpreters/Context.h>
#include <Processors/DeviceChunk.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <cstring>
#include <limits>

#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <chrono>
#include <utility>

namespace CurrentMetrics
{
    extern const Metric ConcurrentHashJoinPoolThreads;
    extern const Metric ConcurrentHashJoinPoolThreadsActive;
    extern const Metric ConcurrentHashJoinPoolThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace Setting
{
    extern const SettingsBool gpu_hash_join_on_device;
}

namespace
{

Block rightTableKeysOf(const TableJoin & table_join, const Block & right_sample_block, Block & payload)
{
    Block keys;
    JoinCommon::splitAdditionalColumns(table_join.getOnlyClause().key_names_right, right_sample_block, keys, payload);
    return keys;
}

std::optional<GPU::GPUElementType> integerKeyTypeOf(const IDataType & type)
{
    const auto element_type = GPU::elementTypeOf(type);
    if (!element_type || !GPU::isInteger(*element_type))
        return {};
    return element_type;
}

/// What the device holds a column of `type` as in a join on the device: its values, each of a fixed width, as they are.
std::optional<GPU::GPUElementType> deviceElementTypeOf(const IDataType & type)
{
    if (typeid_cast<const DataTypeFixedString *>(&type))
        return GPU::keyElementTypeOf(type);
    return GPU::filterElementTypeOf(type);
}

GPU::GPUElementType deviceElementTypeOrThrow(const IDataType & type)
{
    const auto element_type = deviceElementTypeOf(type);
    if (!element_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A column of {} in a GPU hash join on the device", type.getName());
    return *element_type;
}

bool allOnDevice(const Block & block)
{
    for (const auto & column : block)
    {
        if (!deviceElementTypeOf(*column.type))
            return false;
    }
    return true;
}

bool joinOnDeviceRequested()
{
    const auto context = CurrentThread::tryGetQueryContext();
    return context && context->getSettingsRef()[Setting::gpu_hash_join_on_device];
}

}

bool GPUHashJoin::isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block)
{
    if (table_join.kind() != JoinKind::Inner || table_join.strictness() != JoinStrictness::All)
        return false;

    if (!table_join.oneDisjunct() || table_join.getMixedJoinExpression())
        return false;

    const auto & clause = table_join.getOnlyClause();
    if (clause.key_names_left.size() != 1 || clause.key_names_right.size() != 1)
        return false;
    if (clause.on_filter_condition_left || clause.on_filter_condition_right)
        return false;

    if (table_join.isSpecialStorage())
        return false;

    if (!left_sample_block.has(clause.key_names_left[0]) || !right_sample_block.has(clause.key_names_right[0]))
        return false;

    /// The keys are compared as the bytes of integers of one type: a `Nullable` key has no such bytes for its nulls, and
    /// keys of two types would compare different bytes.
    const auto & left_key_type = left_sample_block.getByName(clause.key_names_left[0]).type;
    const auto & right_key_type = right_sample_block.getByName(clause.key_names_right[0]).type;
    return left_key_type->equals(*right_key_type) && integerKeyTypeOf(*left_key_type).has_value();
}

bool GPUHashJoin::runsOnDevice(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block)
{
    if (!joinOnDeviceRequested())
        return false;
    Block payload;
    rightTableKeysOf(table_join, right_sample_block, payload);
    JoinCommon::createMissedColumns(payload);
    return allOnDevice(left_sample_block) && allOnDevice(payload);
}

GPUHashJoin::GPUHashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_)
    : table_join(std::move(table_join_))
    , left_sample_block(*left_sample_block_)
    , right_sample_block(*right_sample_block_)
    , key_name_left(table_join->getOnlyClause().key_names_left[0])
    , key_name_right(table_join->getOnlyClause().key_names_right[0])
    , key_type(integerKeyTypeOf(*right_sample_block.getByName(key_name_right).type).value_or(GPU::GPUElementType::UInt8))
    , build_keys(GPU::GPUResources::get().compute)
    , log(getLogger("GPUHashJoin"))
{
    if (!isSupported(*table_join, left_sample_block, right_sample_block))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot execute this {} {} JOIN on a GPU",
            toString(table_join->strictness()),
            toString(table_join->kind()));

    const Block right_table_keys = rightTableKeysOf(*table_join, right_sample_block, build_payload_header);
    required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);

    JoinCommon::createMissedColumns(build_payload_header);

    on_device = runsOnDevice(*table_join, left_sample_block, right_sample_block);
    if (on_device)
    {
        for (const auto & column : build_payload_header)
            build_payload_types.push_back(deviceElementTypeOrThrow(*column.type));
    }

    LOG_TRACE(
        log,
        "Joining on {} with {} column(s) of the right table, {} on a GPU",
        right_sample_block.getByName(key_name_right).type->getName(),
        build_payload_header.columns(),
        on_device ? "all columns" : "keys");
}

/// The matches of one probe block, handed out as blocks of at most `slice_rows` rows each.
class GPUHashJoin::Result final : public IJoinResult
{
public:
    Result(const GPUHashJoin & join_, Block probe_block_, ColumnPtr probe_indices_, ColumnPtr build_indices_, size_t slice_rows_)
        : join(join_)
        , probe_block(std::move(probe_block_))
        , probe_indices(std::move(probe_indices_))
        , build_indices(std::move(build_indices_))
        , slice_rows(slice_rows_)
    {
    }

    JoinResultBlock next() override
    {
        const size_t total = probe_indices->size();
        const size_t rows = slice_rows == 0 ? total - offset : std::min(slice_rows, total - offset);
        Block block = join.assembleOutputBlock(probe_block, probe_indices->cut(offset, rows), build_indices->cut(offset, rows));
        offset += rows;
        return {.block = std::move(block), .next_block = nullptr, .is_last = offset >= total};
    }

    std::optional<size_t> getMatchedRightRows() const override { return probe_indices->size(); }

private:
    const GPUHashJoin & join;
    const Block probe_block;
    const ColumnPtr probe_indices;
    const ColumnPtr build_indices;
    const size_t slice_rows;
    size_t offset = 0;
};

/// The result of one probe of a join on the device, handed out as chunks of the device of at most `slice_rows` rows each.
class GPUHashJoin::DeviceResult final : public IJoinResult
{
public:
    DeviceResult(std::shared_ptr<const GPU::GPUDeviceColumns> columns_, size_t slice_rows_)
        : columns(std::move(columns_))
        , slice_rows(slice_rows_)
    {
    }

    JoinResultBlock next() override
    {
        const size_t total = columns->rows();
        const size_t rows = slice_rows == 0 ? total - offset : std::min(slice_rows, total - offset);
        last = (offset == 0 && rows == total) ? columns : columns->slice(offset, rows);
        offset += rows;
        return {.block = {}, .next_block = nullptr, .is_last = offset >= total};
    }

    std::shared_ptr<const IDeviceColumns> takeDeviceColumns() override { return std::exchange(last, nullptr); }

    std::optional<size_t> getMatchedRightRows() const override { return columns->rows(); }

private:
    const std::shared_ptr<const GPU::GPUDeviceColumns> columns;
    const size_t slice_rows;
    size_t offset = 0;
    std::shared_ptr<const GPU::GPUDeviceColumns> last;
};

GPUHashJoin::~GPUHashJoin()
{
    /// The blocks of a build that did not end, as of a query cancelled while it read the right table.
    for (BuildChunk * node = build_chunks.exchange(nullptr, std::memory_order_acquire); node;)
    {
        BuildChunk * const next = node->next;
        delete node;
        node = next;
    }
}

void GPUHashJoin::checkTypesOfKeys(const Block & block) const
{
    JoinCommon::checkTypesOfKeys(
        block, table_join->getOnlyClause().key_names_left, right_sample_block, table_join->getOnlyClause().key_names_right);
}

bool GPUHashJoin::addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context)
{
    if (num_rows == 0)
        return true;

    const ColumnPtr key_column = block.getByName(key_name_right).column->convertToFullIfWrapped()->convertToFullColumnIfSparse();

    size_t bytes = num_rows * GPU::sizeOf(key_type);
    Columns chunk;
    chunk.reserve(build_payload_header.columns());
    for (const auto & column : build_payload_header)
    {
        ColumnPtr source = block.getByName(column.name).column->convertToFullIfWrapped()->convertToFullColumnIfSparse();
        if (source->size() != num_rows)
            source = source->cut(0, num_rows);
        bytes += source->byteSize();
        chunk.push_back(std::move(source));
    }

    if (built.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of the right table after the build of a GPU hash join ended");

    if (key_column->size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of {} rows with {} keys for a GPU hash join", num_rows, key_column->size());

    return addBuildChunk(std::make_unique<BuildChunk>(BuildChunk{.keys = key_column, .payload = std::move(chunk), .device = {}, .next = nullptr}), num_rows, bytes, context);
}

bool GPUHashJoin::addDeviceChunkToJoin(const Chunk & chunk, const Block & header, JoinBuildContext context)
{
    const auto device = GPU::gpuColumnsOf(chunk);
    if (!on_device || !device)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A chunk of the right table on the device for a GPU hash join of keys on the GPU alone");

    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return true;

    /// The keys, and then the columns the result takes, as `finishBuildOnDevice` takes them.
    std::vector<size_t> positions{header.getPositionByName(key_name_right)};
    for (const auto & column : build_payload_header)
        positions.push_back(header.getPositionByName(column.name));
    auto selected = device->select(positions);

    size_t bytes = 0;
    for (size_t i = 0; i < positions.size(); ++i)
    {
        const GPU::DeviceColumnView & view = selected->views()[i];
        const GPU::GPUElementType expected = i == 0 ? key_type : build_payload_types[i - 1];
        if (view.kind != GPU::GPUColumnKind::Fixed || view.fixed.element_type != expected)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Device column {} of the right table of a GPU hash join of another type than its header's", positions[i]);
        bytes += num_rows * GPU::sizeOf(expected);
    }

    if (built.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A block of the right table after the build of a GPU hash join ended");

    return addBuildChunk(std::make_unique<BuildChunk>(BuildChunk{.keys = {}, .payload = {}, .device = std::move(selected), .next = nullptr}), num_rows, bytes, context);
}

bool GPUHashJoin::addBuildChunk(std::unique_ptr<BuildChunk> chunk, size_t num_rows, size_t bytes, JoinBuildContext context)
{
    BuildChunk * const node = chunk.release();
    node->next = build_chunks.load(std::memory_order_relaxed);
    while (!build_chunks.compare_exchange_weak(node->next, node, std::memory_order_release, std::memory_order_relaxed))
    {
    }

    build_rows.fetch_add(num_rows, std::memory_order_relaxed);
    build_bytes.fetch_add(bytes, std::memory_order_relaxed);

    if (!context.joinChecksLimits())
        return true;

    return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void GPUHashJoin::finishBuild()
{
    if (hash_join)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The build of a GPU hash join ended twice");

    Stopwatch watch;

    /// No block is added any more: every thread of the right side has finished.
    std::vector<std::unique_ptr<BuildChunk>> chunks;
    for (BuildChunk * node = build_chunks.exchange(nullptr, std::memory_order_acquire); node;)
    {
        BuildChunk * const next = node->next;
        chunks.emplace_back(node);
        node = next;
    }

    const size_t rows = build_rows.load(std::memory_order_relaxed);
    if (rows > static_cast<size_t>(std::numeric_limits<Int32>::max()))
        throw Exception(
            ErrorCodes::SET_SIZE_LIMIT_EXCEEDED, "The right table of a GPU hash join has {} rows, more than cuDF indexes", rows);

    if (on_device)
    {
        finishBuildOnDevice(chunks, rows);
        chunks.clear();
        const UInt64 moved_us = watch.elapsedMicroseconds();
        hash_join = std::make_unique<GPU::CudfHashJoin>(
            key_type, GPU::DeviceFixedColumn{.element_type = key_type, .data = build_keys.data(), .rows = rows});
        markBuilt(rows);
        LOG_DEBUG(
            log,
            "Built the hash table of {} rows of the right table, {} bytes, on the GPU with its columns: {} us to put them "
            "together on the device, {} us to build",
            rows,
            build_bytes.load(std::memory_order_relaxed),
            moved_us,
            watch.elapsedMicroseconds() - moved_us);
        return;
    }

    /// The keys in one pinned buffer, in the order of the chunks, which the payload takes as well, and to the device in one
    /// copy.
    const size_t key_size = GPU::sizeOf(key_type);
    GPU::PinnedBuffer host_keys;
    host_keys.reserve(rows * key_size);
    for (const auto & chunk : chunks)
        GPU::appendHostBytes(host_keys, GPU::rawValuesOf(*chunk->keys, chunk->keys->size(), key_size));
    if (host_keys.size() != rows * key_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of the keys of {} rows of the right table of a GPU hash join", host_keys.size(), rows);
    build_keys.append(GPU::bytesOf(host_keys));

    build_block = build_payload_header.cloneEmpty();
    const size_t num_columns = build_payload_header.columns();
    const auto put_together = [&](size_t i)
    {
        VectorWithMemoryTracking<ColumnPtr> parts;
        parts.reserve(chunks.size());
        for (const auto & chunk : chunks)
            parts.push_back(chunk->payload[i]);
        MutableColumnPtr column = build_payload_header.getByPosition(i).type->createColumn();
        column->prepareForSquashing(parts, 1);
        for (const auto & part : parts)
            column->insertRangeFrom(*part, 0, part->size());
        build_block.getByPosition(i).column = std::move(column);
    };

    if (num_columns <= 1)
    {
        for (size_t i = 0; i < num_columns; ++i)
            put_together(i);
    }
    else
    {
        ThreadPool pool(
            CurrentMetrics::ConcurrentHashJoinPoolThreads,
            CurrentMetrics::ConcurrentHashJoinPoolThreadsActive,
            CurrentMetrics::ConcurrentHashJoinPoolThreadsScheduled,
            /*max_threads_=*/ num_columns,
            /*max_free_threads_=*/ 0,
            /*queue_size_=*/ num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            pool.scheduleOrThrow([&, i, thread_group = CurrentThread::getGroup()]
            {
                ThreadGroupSwitcher switcher(thread_group, ThreadName::CONCURRENT_JOIN);
                put_together(i);
            });
        pool.wait();
    }
    chunks.clear();

    const UInt64 moved_us = watch.elapsedMicroseconds();
    hash_join = std::make_unique<GPU::CudfHashJoin>(
        key_type, GPU::DeviceFixedColumn{.element_type = key_type, .data = build_keys.data(), .rows = rows});
    markBuilt(rows);
    LOG_DEBUG(
        getLogger("GPUHashJoin"),
        "Built the hash table of {} rows of the right table, {} bytes, on the GPU: {} us to take the columns, {} us to build",
        rows,
        build_bytes.load(std::memory_order_relaxed),
        moved_us,
        watch.elapsedMicroseconds() - moved_us);
}

void GPUHashJoin::finishBuildOnDevice(const std::vector<std::unique_ptr<BuildChunk>> & chunks, size_t rows)
{
    const cuda::stream_ref stream = GPU::GPUResources::get().compute;
    const size_t num_columns = 1 + build_payload_types.size();
    const auto element_type_of = [&](size_t c) { return c == 0 ? key_type : build_payload_types[c - 1]; };

    build_columns.clear();
    for (size_t i = 0; i < build_payload_types.size(); ++i)
        build_columns.push_back(std::make_unique<GPU::DeviceBuffer>(stream));
    const auto target = [&](size_t c) -> GPU::DeviceBuffer & { return c == 0 ? build_keys : *build_columns[c - 1]; };

    /// The rows of the chunks in host memory first, a column of them in one pinned buffer and one copy, and then those
    /// on the device: the same order for every column.
    std::vector<GPU::PinnedBuffer> staged(num_columns);
    GPU::waitOnFailure(stream, [&]
    {
        for (size_t c = 0; c < num_columns; ++c)
        {
            const size_t size = GPU::sizeOf(element_type_of(c));
            target(c).reserve(rows * size);
            for (const auto & chunk : chunks)
            {
                if (chunk->device)
                    continue;
                const IColumn & column = c == 0 ? *chunk->keys : *chunk->payload[c - 1];
                GPU::appendHostBytes(staged[c], GPU::rawValuesOf(column, column.size(), size));
            }
            target(c).append(GPU::bytesOf(staged[c]));
        }

        for (const auto & chunk : chunks)
        {
            if (!chunk->device)
                continue;
            chunk->device->waitIn(stream);
            for (size_t c = 0; c < num_columns; ++c)
            {
                const GPU::DeviceColumnView & view = chunk->device->views()[c];
                const size_t bytes = view.fixed.rows * GPU::sizeOf(element_type_of(c));
                if (bytes == 0)
                    continue;
                GPU::checkCuda(
                    cudaMemcpyAsync(target(c).grow(bytes), view.fixed.data, bytes, cudaMemcpyDeviceToDevice, stream.get()),
                    "Cannot put the right table of a join together on the device");
            }
        }
    });
    GPU::checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for the right table of a join to be put together on the device");

    for (size_t c = 0; c < num_columns; ++c)
    {
        if (target(c).size() != rows * GPU::sizeOf(element_type_of(c)))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "{} bytes of column {} of {} rows of the right table of a GPU hash join", target(c).size(), c, rows);
    }
}

JoinResultPtr GPUHashJoin::probeOnDevice(
    const GPU::GPUDeviceColumns * device_input, const Block & host_block, const Block & header, size_t rows)
{
    if (!built.load(std::memory_order_acquire))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The left table of a GPU hash join is probed before the build ended");

    /// As in `joinBlock`, a stream and buffers of the thread, kept from block to block; the probe waits for its stream at
    /// the end, so they are free at the next.
    thread_local GPU::StreamPtr thread_stream = GPU::createStream();
    const cuda::stream_ref stream{thread_stream.get()};
    thread_local GPU::PinnedBuffer pinned;
    thread_local GPU::DeviceBuffer uploaded(stream);

    std::vector<GPU::DeviceColumnView> inputs;
    inputs.reserve(header.columns());
    if (device_input)
    {
        if (device_input->views().size() != header.columns() || device_input->rows() != rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{} device columns of {} rows for a GPU hash join of a header of {} columns and {} rows",
                device_input->views().size(),
                device_input->rows(),
                header.columns(),
                rows);
        device_input->waitIn(stream);
        for (size_t i = 0; i < header.columns(); ++i)
        {
            const GPU::DeviceColumnView & view = device_input->views()[i];
            if (view.kind != GPU::GPUColumnKind::Fixed || view.fixed.element_type != deviceElementTypeOrThrow(*header.getByPosition(i).type))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Device column {} of the left table of a GPU hash join of another type than its header's", i);
            inputs.push_back(view);
        }
    }
    else
    {
        /// The columns in one pinned buffer, each at a multiple of 8 bytes, and to the device in one copy.
        pinned.clear();
        std::vector<size_t> offsets;
        std::vector<GPU::GPUElementType> types;
        for (const auto & column : host_block)
        {
            const GPU::GPUElementType type = deviceElementTypeOrThrow(*column.type);
            const ColumnPtr full = column.column->convertToFullIfWrapped()->convertToFullColumnIfSparse();
            GPU::growBy(pinned, (8 - pinned.size() % 8) % 8);
            offsets.push_back(pinned.size());
            types.push_back(type);
            GPU::appendHostBytes(pinned, GPU::rawValuesOf(*full, rows, GPU::sizeOf(type)));
        }
        uploaded.clear();
        uploaded.append(GPU::bytesOf(pinned));
        for (size_t i = 0; i < offsets.size(); ++i)
            inputs.push_back(GPU::DeviceFixedColumn{.element_type = types[i], .data = uploaded.data() + offsets[i], .rows = rows});
    }

    const GPU::DeviceColumnView & probe_key = inputs[header.getPositionByName(key_name_left)];
    if (probe_key.fixed.element_type != key_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A key of the left table of a GPU hash join of another type than the right table's");
    const GPU::CudfJoinMatches matches = hash_join->probe(probe_key.fixed, stream);
    const size_t num_matches = matches.size();

    std::vector<GPU::DeviceColumnView> result;
    std::vector<std::shared_ptr<const void>> owners;
    result.reserve(inputs.size() + build_payload_types.size() + required_right_keys.columns());
    const auto gather = [&](const char * values, GPU::GPUElementType type, const int32_t * indices)
    {
        auto gathered = std::make_shared<GPU::DeviceBuffer>(stream);
        char * const data = num_matches ? gathered->grow(num_matches * GPU::sizeOf(type)) : nullptr;
        GPU::gatherFixedRows(values, GPU::sizeOf(type), indices, num_matches, data, stream);
        result.push_back(GPU::DeviceFixedColumn{.element_type = type, .data = data, .rows = num_matches});
        owners.push_back(std::move(gathered));
    };

    GPU::waitOnFailure(stream, [&]
    {
        for (const auto & input : inputs)
            gather(input.fixed.data, input.fixed.element_type, matches.probeIndices());
        for (size_t i = 0; i < build_payload_types.size(); ++i)
            gather(build_columns[i]->data(), build_payload_types[i], matches.buildIndices());
    });

    /// The right table's keys the result takes are the left table's, as `assembleOutputBlock` has them.
    for (size_t i = 0; i < required_right_keys.columns(); ++i)
    {
        const auto & right_key = required_right_keys.getByPosition(i);
        const size_t left_position = header.getPositionByName(required_right_keys_sources[i]);
        if (!right_key.type->equals(*header.getByPosition(left_position).type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The GPU hash join joined {} to {} of another type", required_right_keys_sources[i], right_key.name);
        result.push_back(result[left_position]);
    }

    GPU::checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for the result of a join on the device");

    return std::make_unique<DeviceResult>(
        std::make_shared<GPU::GPUDeviceColumns>(std::move(result), num_matches, std::move(owners), stream), table_join->maxJoinedBlockRows());
}

JoinResultPtr GPUHashJoin::joinDeviceChunk(const Chunk & chunk, const Block & header)
{
    const auto device = GPU::gpuColumnsOf(chunk);
    if (!on_device || !device)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A chunk of the left table on the device for a GPU hash join of keys on the GPU alone");
    probe_rows_total.fetch_add(chunk.getNumRows(), std::memory_order_relaxed);
    return probeOnDevice(device.get(), {}, header, chunk.getNumRows());
}

void GPUHashJoin::markBuilt(size_t rows)
{
    auto set = std::make_shared<const GPU::DeviceKeySet>(
        GPU::DeviceFixedColumn{.element_type = key_type, .data = build_keys.data(), .rows = rows}, GPU::GPUResources::get().compute);
    {
        std::lock_guard lock(built_mutex);
        key_set = std::move(set);
        built.store(true, std::memory_order_release);
    }
    built_changed.notify_all();
}

std::shared_ptr<const GPU::DeviceKeySet> GPUHashJoin::waitForKeySet(const std::function<bool()> & stop) const
{
    /// Woken by the build's end, and now and then to ask whether to go on waiting: whoever waits is stopped by its own
    /// query's end or error, which knows nothing of this join.
    std::unique_lock lock(built_mutex);
    while (!built.load(std::memory_order_acquire))
    {
        if (stop())
            return nullptr;
        built_changed.wait_for(lock, std::chrono::milliseconds(100));
    }
    return key_set;
}

void GPUHashJoin::onBuildPhaseFinish()
{
    finishBuild();
}

Block GPUHashJoin::assembleOutputBlock(const Block & probe_block, const ColumnPtr & probe_indices, const ColumnPtr & build_indices) const
{
    Block result;
    for (const auto & column : probe_block)
    {
        ColumnWithTypeAndName output = column;
        output.column = column.column->convertToFullIfWrapped()->convertToFullColumnIfSparse()->index(*probe_indices, 0);
        result.insert(std::move(output));
    }

    for (const auto & sample : build_payload_header)
    {
        ColumnPtr gathered = build_indices->empty() ? sample.column->cloneEmpty() : build_block.getByName(sample.name).column->index(*build_indices, 0);
        result.insert({std::move(gathered), sample.type, table_join->renamedRightColumnName(sample.name)});
    }

    for (size_t i = 0; i < required_right_keys.columns(); ++i)
    {
        const auto & right_key = required_right_keys.getByPosition(i);
        const ColumnWithTypeAndName & left_key = result.getByName(required_right_keys_sources[i]);

        if (!right_key.type->equals(*left_key.type))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The GPU hash join joined {} of {} to {} of {}",
                left_key.name,
                left_key.type->getName(),
                right_key.name,
                right_key.type->getName());

        result.insert({left_key.column, right_key.type, table_join->renamedRightColumnName(right_key.name)});
    }

    return result;
}

JoinResultPtr GPUHashJoin::joinBlock(Block block)
{
    const size_t probe_rows = block.rows();
    probe_rows_total.fetch_add(probe_rows, std::memory_order_relaxed);

    /// A block of no rows, as of the header, is joined as on the host: its result is the output header.
    if (on_device && probe_rows != 0)
        return probeOnDevice(nullptr, block, block, probe_rows);

    auto probe_indices = ColumnUInt32::create();
    auto build_indices = ColumnUInt32::create();

    if (probe_rows != 0)
    {
        const ColumnPtr probe_key = block.getByName(key_name_left).column->convertToFullIfWrapped()->convertToFullColumnIfSparse();

        /// `FillingRightJoinSideTransform` ends the build before the left side is probed.
        if (!built.load(std::memory_order_acquire))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The left table of a GPU hash join is probed before the build ended");

        /// Each thread probes on a stream of its own, so that the blocks of the threads are probed at once, through
        /// pinned host memory, which the copies to and from the device need to be asynchronous, and buffers of its own
        /// that it keeps from block to block. A probe waits for its stream at the end, so a buffer is free at the next.
        thread_local GPU::StreamPtr thread_stream = GPU::createStream();
        const cuda::stream_ref stream{thread_stream.get()};
        thread_local GPU::PinnedBuffer pinned_keys;
        thread_local GPU::PinnedBuffer pinned_indices;
        thread_local GPU::DeviceBuffer probe_keys(stream);

        pinned_keys.clear();
        GPU::appendHostBytes(pinned_keys, GPU::rawValuesOf(*probe_key, probe_rows, GPU::sizeOf(key_type)));
        probe_keys.clear();
        probe_keys.append(GPU::bytesOf(pinned_keys));
        const GPU::CudfJoinMatches matches
            = hash_join->probe(GPU::DeviceFixedColumn{.element_type = key_type, .data = probe_keys.data(), .rows = probe_rows}, stream);

        const size_t num_matches = matches.size();
        pinned_indices.clear();
        char * const landed = GPU::growBy(pinned_indices, 2 * num_matches * sizeof(UInt32));
        matches.copyOut(reinterpret_cast<uint32_t *>(landed), reinterpret_cast<uint32_t *>(landed + num_matches * sizeof(UInt32)), stream);

        probe_indices->getData().resize(num_matches);
        build_indices->getData().resize(num_matches);
        memcpy(probe_indices->getData().data(), landed, num_matches * sizeof(UInt32));
        memcpy(build_indices->getData().data(), landed + num_matches * sizeof(UInt32), num_matches * sizeof(UInt32));
    }

    if (!probe_indices->empty())
    {
        const UInt32 max_probe = *std::max_element(probe_indices->getData().begin(), probe_indices->getData().end());
        const UInt32 max_build = *std::max_element(build_indices->getData().begin(), build_indices->getData().end());
        const size_t build_total = build_rows.load(std::memory_order_relaxed);
        if (max_probe >= probe_rows || max_build >= build_total)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The device returned rows up to {} and {} for a left block of {} rows and a right table of {}",
                max_probe,
                max_build,
                probe_rows,
                build_total);
    }

    return std::make_unique<Result>(*this, std::move(block), std::move(probe_indices), std::move(build_indices), table_join->maxJoinedBlockRows());
}

StepAnalysisReport GPUHashJoin::getAnalysisReport() const
{
    StepAnalysisReport report;
    report.push_back({MetricGroupKey::Left, joinSideMetrics(probe_rows_total.load(std::memory_order_relaxed), std::nullopt)});
    report.push_back({MetricGroupKey::Right, joinSideMetrics(getTotalRowCount(), std::nullopt)});
    return report;
}

}

#endif
