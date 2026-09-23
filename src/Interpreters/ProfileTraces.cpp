#include <Interpreters/ProfileTraces.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileTracesBlocker.h>
#include <Common/SymbolIndex.h>
#include <Common/logger_useful.h>
#include <base/EnumReflection.h>
#include <base/demangle.h>
#include <base/getFQDNOrHostName.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <unordered_map>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char profile_traces_flush_ack_timeout[];
    extern const char profile_traces_queue_overflow[];
}

namespace
{

bool isSupportedTraceType(TraceType type)
{
    switch (type)
    {
        case TraceType::CPU:
        case TraceType::Real:
        case TraceType::Memory:
        case TraceType::MemorySample:
        case TraceType::MemoryPeak:
            return true;
        default:
            return false;
    }
}

}

struct ProfileTracesRegistry
{
    std::mutex mutex;
    std::unordered_map<UInt64, std::weak_ptr<InternalProfileTracesQueue>> subscriptions;
    std::atomic<UInt64> next_id{1};

    InternalProfileTracesQueuePtr find(UInt64 id)
    {
        std::lock_guard lock(mutex);
        auto it = subscriptions.find(id);
        return it == subscriptions.end() ? nullptr : it->second.lock();
    }
};

std::shared_ptr<ProfileTracesRegistry> InternalProfileTracesQueue::getRegistry()
{
    static auto registry = std::make_shared<ProfileTracesRegistry>();
    return registry;
}

InternalProfileTracesQueue::InternalProfileTracesQueue(
    std::shared_ptr<ProfileTracesRegistry> registry_, UInt64 id_, const String & query_id_)
    : registry(std::move(registry_)), id(id_), query_id(query_id_), host_name(getFQDNOrHostName())
{
}

InternalProfileTracesQueuePtr InternalProfileTracesQueue::create(const String & query_id)
{
    auto registry = getRegistry();
    auto id = registry->next_id.fetch_add(1, std::memory_order_relaxed);
    if (!id)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Profile trace subscription IDs exhausted");
    auto queue = InternalProfileTracesQueuePtr(new InternalProfileTracesQueue(registry, id, query_id));
    std::lock_guard lock(registry->mutex);
    registry->subscriptions.emplace(id, queue);
    return queue;
}

InternalProfileTracesQueue::~InternalProfileTracesQueue()
{
    ProfileTracesBlocker blocker;
    std::lock_guard lock(registry->mutex);
    registry->subscriptions.erase(id);
    samples.clear();
}

size_t InternalProfileTracesQueue::Sample::byteSize() const
{
    size_t bytes = sizeof(Sample) + host_name.size() + query_id.size() + trace_type.size()
        + trace.size() * sizeof(UInt64) + symbols.size() * sizeof(String);
    for (const auto & symbol : symbols)
        bytes += symbol.size();
    return bytes;
}

void InternalProfileTracesQueue::push(Sample sample)
{
    /// A slow client must not block the shared collector or retain an unbounded amount of memory.
    static constexpr size_t max_samples = 8192;
    static constexpr size_t max_bytes = 16 * 1024 * 1024;
    size_t bytes = sample.byteSize();
    std::lock_guard lock(mutex);
    if (finished)
        return;
    bool overflow = samples.size() >= max_samples || bytes > max_bytes - buffered_bytes;
    fiu_do_on(FailPoints::profile_traces_queue_overflow, { overflow = true; });
    if (overflow)
    {
        if (pending_dropped < std::numeric_limits<Int64>::max())
            ++pending_dropped;
        return;
    }
    buffered_bytes += bytes;
    samples.emplace_back(std::move(sample));
}

void InternalProfileTracesQueue::collect(
    const std::shared_ptr<ProfileTracesRegistry> & registry, UInt64 id, TraceType trace_type,
    UInt64 thread_id, UInt64 event_time_microseconds, const std::vector<UInt64> & trace, Int64 size)
{
    if (!isSupportedTraceType(trace_type))
        return;

    if (auto queue = registry->find(id))
    {
        queue->push(Sample{
            .host_name = {},
            .query_id = {},
            .trace_type = String(magic_enum::enum_name(trace_type)),
            .thread_id = thread_id,
            .event_time_microseconds = event_time_microseconds,
            .trace = trace,
            .symbols = {},
            .size = size,
        });
    }
}

void InternalProfileTracesQueue::acknowledgeFlush(const std::shared_ptr<ProfileTracesRegistry> & registry, UInt64 id)
{
    fiu_do_on(FailPoints::profile_traces_flush_ack_timeout, { return; });
    if (auto queue = registry->find(id))
    {
        {
            std::lock_guard lock(queue->mutex);
            queue->flush_acknowledged = true;
        }
        queue->flushed.notify_all();
    }
}

void InternalProfileTracesQueue::finish()
{
    ProfileTracesBlocker blocker;
    {
        std::lock_guard lock(mutex);
        if (finished)
            return;
    }

    /// This runs on the transport thread, never in a profiler signal handler. The pipe marker
    /// orders the final drain after all samples accepted before query execution finished.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    auto result = TraceSender::flushProfileTraces(id, deadline);
    {
        std::unique_lock lock(mutex);
        if (result == TraceSender::ProfileTracesFlushResult::MarkerSent
            && !flushed.wait_until(lock, deadline, [&] { return flush_acknowledged; }))
            result = TraceSender::ProfileTracesFlushResult::TimedOut;

        finished = true;
        if (result == TraceSender::ProfileTracesFlushResult::TimedOut)
        {
            pending_dropped += std::min<Int64>(samples.size(), std::numeric_limits<Int64>::max() - pending_dropped);
            pending_incomplete = true;
            samples.clear();
            buffered_bytes = 0;
        }
    }

    if (result == TraceSender::ProfileTracesFlushResult::TimedOut)
        LOG_WARNING(getLogger("ProfileTraces"), "Timed out flushing profile traces for query {}; remaining samples were discarded", query_id);
}

void InternalProfileTracesQueue::cancel()
{
    ProfileTracesBlocker blocker;
    std::lock_guard lock(mutex);
    finished = true;
    samples.clear();
    buffered_bytes = 0;
    pending_dropped = 0;
    pending_incomplete = false;
}

Block InternalProfileTracesQueue::getSampleBlock()
{
    return {
        {std::make_shared<DataTypeString>(), "host_name"},
        {std::make_shared<DataTypeString>(), "query_id"},
        {std::make_shared<DataTypeString>(), "trace_type"},
        {std::make_shared<DataTypeUInt64>(), "thread_id"},
        {std::make_shared<DataTypeUInt64>(), "event_time_microseconds"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "trace"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "symbols"},
        {std::make_shared<DataTypeInt64>(), "size"},
    };
}

Block InternalProfileTracesQueue::getBlock()
{
    /// Formatting samples must not add the delivery path to this subscription.
    ProfileTracesBlocker blocker;
    std::vector<Sample> batch;
    {
        std::lock_guard lock(mutex);
        const size_t metadata_rows = (pending_dropped != 0) + pending_incomplete;
        size_t rows = std::min(samples.size(), size_t{1024} - metadata_rows);
        batch.reserve(rows + metadata_rows);
        while (batch.size() < rows)
        {
            buffered_bytes -= samples.front().byteSize();
            batch.emplace_back(std::move(samples.front()));
            samples.pop_front();
        }
        if (pending_dropped)
        {
            Sample status;
            status.trace_type = "Dropped";
            status.size = std::exchange(pending_dropped, 0);
            batch.emplace_back(std::move(status));
        }
        if (std::exchange(pending_incomplete, false))
        {
            Sample status;
            status.trace_type = "Incomplete";
            batch.emplace_back(std::move(status));
        }
    }

    Block block = getSampleBlock();
    if (batch.empty())
        return block;

    auto columns = block.cloneEmptyColumns();
    /// Symbolization belongs to the consumer, so a client cannot stall the shared collector.
    /// Cache only this batch to bound memory independently of query duration.
    std::unordered_map<UInt64, String> symbol_cache;
    for (auto & sample : batch)
    {
        bool local = !sample.symbolized;
        if (local)
        {
            sample.symbols.reserve(sample.trace.size());
            for (UInt64 address : sample.trace)
            {
                auto [it, inserted] = symbol_cache.try_emplace(address);
                if (inserted)
                {
#if (defined(__ELF__) || defined(OS_DARWIN)) && !defined(OS_FREEBSD)
                    if (const auto * symbol = SymbolIndex::instance().findSymbol(reinterpret_cast<const void *>(address)))
                        it->second = demangle(symbol->name);
#endif
                }
                sample.symbols.push_back(it->second);
            }
        }

        columns[0]->insert(local ? host_name : sample.host_name);
        columns[1]->insert(local ? query_id : sample.query_id);
        columns[2]->insert(sample.trace_type);
        columns[3]->insert(sample.thread_id);
        columns[4]->insert(sample.event_time_microseconds);
        Array trace;
        trace.reserve(sample.trace.size());
        for (UInt64 address : sample.trace)
            trace.emplace_back(address);
        columns[5]->insert(trace);
        Array symbols;
        symbols.reserve(sample.symbols.size());
        for (auto & symbol : sample.symbols)
            symbols.emplace_back(std::move(symbol));
        columns[6]->insert(symbols);
        columns[7]->insert(sample.size);
    }
    return block.cloneWithColumns(std::move(columns));
}

void InternalProfileTracesQueue::pushBlock(const Block & block)
{
    ProfileTracesBlocker blocker;
    assertBlocksHaveEqualStructure(block, getSampleBlock(), "ProfileTraces");
    const auto & hosts = assert_cast<const ColumnString &>(*block.getByPosition(0).column);
    const auto & query_ids = assert_cast<const ColumnString &>(*block.getByPosition(1).column);
    const auto & types = assert_cast<const ColumnString &>(*block.getByPosition(2).column);
    const auto & threads = assert_cast<const ColumnUInt64 &>(*block.getByPosition(3).column).getData();
    const auto & times = assert_cast<const ColumnUInt64 &>(*block.getByPosition(4).column).getData();
    const auto & traces = assert_cast<const ColumnArray &>(*block.getByPosition(5).column);
    const auto & symbols = assert_cast<const ColumnArray &>(*block.getByPosition(6).column);
    const auto & sizes = assert_cast<const ColumnInt64 &>(*block.getByPosition(7).column).getData();
    const auto & addresses = assert_cast<const ColumnUInt64 &>(traces.getData()).getData();
    const auto & symbol_names = assert_cast<const ColumnString &>(symbols.getData());

    for (size_t row = 0; row < block.rows(); ++row)
    {
        const auto type_name = types.getDataAt(row);
        const auto trace_begin = traces.getOffsets()[static_cast<ssize_t>(row) - 1];
        const auto trace_end = traces.getOffsets()[row];
        const auto symbols_begin = symbols.getOffsets()[static_cast<ssize_t>(row) - 1];
        const auto symbols_end = symbols.getOffsets()[row];
        if (type_name == "Dropped" || type_name == "Incomplete")
        {
            if (trace_begin != trace_end || symbols_begin != symbols_end || threads[row] || times[row]
                || (type_name == "Dropped" ? sizes[row] <= 0 : sizes[row] != 0))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid profile trace delivery status");

            /// Each forwarded delta is consumed once, independently of sample queue capacity.
            /// The next status identifies this coordinator, not the original site of the loss.
            std::lock_guard lock(mutex);
            if (!finished)
            {
                if (type_name == "Dropped")
                    pending_dropped += std::min(sizes[row], std::numeric_limits<Int64>::max() - pending_dropped);
                else
                    pending_incomplete = true;
            }
            continue;
        }
        const auto type = magic_enum::enum_cast<TraceType>(type_name);
        if (!type || !isSupportedTraceType(*type))
            continue;

        Sample sample{
            .host_name = String(hosts.getDataAt(row)),
            .query_id = String(query_ids.getDataAt(row)),
            .trace_type = String(type_name),
            .thread_id = threads[row],
            .event_time_microseconds = times[row],
            .trace = {},
            .symbols = {},
            .size = sizes[row],
            .symbolized = true,
        };
        if (trace_end - trace_begin != symbols_end - symbols_begin)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Profile trace addresses and symbols have different lengths");
        sample.trace.assign(addresses.begin() + trace_begin, addresses.begin() + trace_end);
        for (size_t frame = symbols_begin; frame < symbols_end; ++frame)
            sample.symbols.emplace_back(symbol_names.getDataAt(frame));
        push(std::move(sample));
    }
}

}
