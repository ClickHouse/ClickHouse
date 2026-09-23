#include <GPU/RecordGroupBy.cuh>

#include <GPU/Cudf.cuh>
#include <GPU/GroupByKernels.cuh>

#include <cub/device/device_radix_sort.cuh>

#include <rmm/device_scalar.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/exec_policy.hpp>

#include <thrust/copy.h>
#include <thrust/iterator/counting_iterator.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

/** The host side of the `GROUP BY`: what the aggregation is over (`Shape`), the table of groups
  * and its growth (`GroupTable`), the two-pass path's buffers (`BucketedPass`), the choice between
  * the paths (`PathChoice`), and `State`, which cuts a batch into chunks and drives the kernels of
  * `GroupByKernels.cuh` over them.
  */
namespace DB::GPU
{

namespace
{

using namespace Grouping;

/// Below this many rows a chunk is not worth sorting into buckets.
constexpr size_t min_bucketed_rows = size_t{num_buckets} * 4096;

/// How many rows the two passes are measured on: little next to a query, so that one the direct
/// kernel serves better loses little to finding that out.
constexpr size_t measurement_rows = 2UL << 20;

/// The two passes are taken only where the direct kernel costs at least this many nanoseconds a
/// row. Below that the grouping takes less of the device than expanding and copying the row's
/// bytes do - a dozen bytes at the link's few gigabytes a second come to about this - so a cheaper
/// kernel gains nothing, while the sort's traffic slows the expansion that runs beside it. A
/// `sum` by a key of a hundred thousand values costs 1.4 ns a row and lost 5% in two passes; a
/// `sum`, `min` and `max` by the same key cost 2.6 ns a row and gained 15%.
constexpr double min_direct_cost_for_buckets = 2.0;

unsigned blocksFor(size_t work)
{
    return static_cast<unsigned>(std::min<size_t>((work + threads_per_block - 1) / threads_per_block, max_blocks));
}

void checkCount(size_t actual, size_t expected, const std::string & what)
{
    if (actual != expected)
        throw CudfError(std::to_string(actual) + " " + what + ", expected " + std::to_string(expected));
}

void checkLaunch(const std::string & what)
{
    checkCuda(cudaGetLastError(), "cannot launch the kernel that " + what);
}

rmm::cuda_stream_view computeStream()
{
    return StreamRegistry::get().compute;
}

bool isSigned(GPUElementType type)
{
    switch (type)
    {
        case GPUElementType::Int8:
        case GPUElementType::Int16:
        case GPUElementType::Int32:
        case GPUElementType::Int64:
            return true;
        default:
            return false;
    }
}

Fold foldOf(const GPUGroupByValue & value)
{
    const bool is_float = !isInteger(value.element_type);
    switch (value.aggregation)
    {
        case GPUAggregationKind::Sum:
            return is_float ? Fold::SumFloat : Fold::SumInt;
        case GPUAggregationKind::Min:
            return is_float ? Fold::MinFloat : (isSigned(value.element_type) ? Fold::MinSigned : Fold::MinUnsigned);
        case GPUAggregationKind::Max:
            return is_float ? Fold::MaxFloat : (isSigned(value.element_type) ? Fold::MaxSigned : Fold::MaxUnsigned);
    }
    throw CudfError("unknown aggregation " + std::to_string(static_cast<int>(value.aggregation)));
}

/// What an accumulator holds before any row is folded into it.
uint64_t identityOf(Fold fold)
{
    switch (fold)
    {
        case Fold::SumInt:
        case Fold::SumFloat:
        case Fold::MaxUnsigned:
            return 0;
        case Fold::MinSigned:
            return static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        case Fold::MaxSigned:
            return static_cast<uint64_t>(std::numeric_limits<int64_t>::min());
        case Fold::MinUnsigned:
            return std::numeric_limits<uint64_t>::max();
        case Fold::MinFloat:
            return std::bit_cast<uint64_t>(std::numeric_limits<double>::infinity());
        case Fold::MaxFloat:
            return std::bit_cast<uint64_t>(-std::numeric_limits<double>::infinity());
    }
    return 0;
}

/// What a group of this value leaves in its output column, and how wide.
std::pair<Store, uint32_t> storeOf(const GPUGroupByValue & value)
{
    if (value.aggregation == GPUAggregationKind::Sum)
        return {Store::Bits, 8};
    if (value.element_type == GPUElementType::Float32)
        return {Store::Narrow, 4};
    if (value.element_type == GPUElementType::Float64)
        return {Store::Bits, 8};
    return {Store::Truncate, static_cast<uint32_t>(sizeOf(value.element_type))};
}

GPUElementType leftIn(const GPUGroupByValue & value)
{
    return value.aggregation == GPUAggregationKind::Sum ? value.result_type : value.element_type;
}

/// What the aggregation is over: its keys and values, and what follows from them once - where
/// each key sits in the packed word, how each value folds, and what an empty record holds.
struct Shape
{
    std::vector<GPUElementType> key_element_types;
    std::vector<uint32_t> key_shifts;
    size_t key_bytes = 0;

    std::vector<GPUGroupByValue> values;
    std::vector<Fold> folds;
    Identities identities;

    Shape(GPUSpan<GPUElementType> key_element_types_, GPUSpan<GPUGroupByValue> values_)
        : key_element_types(key_element_types_.begin(), key_element_types_.end())
        , values(values_.begin(), values_.end())
    {
        if (key_element_types.empty() || key_element_types.size() > max_group_by_keys)
            throw CudfError("a `GROUP BY` of " + std::to_string(key_element_types.size()) + " keys on the device");
        if (values.empty() || values.size() > max_group_by_values)
            throw CudfError("a `GROUP BY` of " + std::to_string(values.size()) + " aggregates on the device");

        for (const GPUElementType key_element_type : key_element_types)
        {
            if (!isInteger(key_element_type))
                throw CudfError("a `GROUP BY` on a floating-point key on the device");
            key_shifts.push_back(static_cast<uint32_t>(key_bytes * 8));
            key_bytes += sizeOf(key_element_type);
        }
        if (key_bytes > max_group_by_key_bytes)
            throw CudfError("a `GROUP BY` on keys of " + std::to_string(key_bytes) + " bytes between them on the device");

        folds.reserve(values.size());
        identities.count = static_cast<uint32_t>(values.size());
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (values[i].aggregation == GPUAggregationKind::Sum && sizeOf(values[i].result_type) != 8)
                throw CudfError("a sum into element type " + std::to_string(static_cast<int>(values[i].result_type)) + ", which is not eight bytes wide");
            folds.push_back(foldOf(values[i]));
            identities.values[i] = identityOf(folds.back());
        }
    }

    uint32_t numValues() const { return static_cast<uint32_t>(values.size()); }

    /// The kernels' view of the batch's columns from row `offset` on, `rows` rows of them.
    Chunk chunkAt(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> value_views,
        GPUSpan<DeviceColumnView> filter_columns,
        const GPUFilterProgram * filter,
        size_t offset,
        size_t rows) const
    {
        Chunk chunk;
        chunk.rows = rows;

        chunk.keys.count = static_cast<uint32_t>(keys.size());
        chunk.keys.may_equal_sentinel = key_bytes == sizeof(Key);
        for (size_t i = 0; i < keys.size(); ++i)
        {
            const uint32_t size = static_cast<uint32_t>(sizeOf(key_element_types[i]));
            chunk.keys.columns[i] = {.data = keys[i].data + offset * size, .size = size, .shift = key_shifts[i]};
        }

        chunk.values.count = numValues();
        for (size_t i = 0; i < values.size(); ++i)
        {
            chunk.values.columns[i] = {
                .data = value_views[i].data + offset * sizeOf(values[i].element_type),
                .type = values[i].element_type,
                .fold = folds[i],
            };
        }

        if (filter)
        {
            chunk.filter = *filter;
            chunk.filters.present = true;
            chunk.filters.count = static_cast<uint32_t>(filter_columns.size());
            for (size_t i = 0; i < filter_columns.size(); ++i)
                chunk.filters.columns[i] = {
                    .data = filter_columns[i].data + offset * sizeOf(filter_columns[i].element_type),
                    .type = filter_columns[i].element_type,
                };
        }

        return chunk;
    }
};

/// Times what is queued on the compute stream between `begin` and `end`, apart from what it
/// waited for: the events are queued in the stream, so the first completes only when everything
/// before it has.
class KernelTimer
{
public:
    KernelTimer()
    {
        checkCuda(cudaEventCreate(&started), "cannot create an event");
        checkCuda(cudaEventCreate(&finished), "cannot create an event");
    }

    ~KernelTimer()
    {
        cudaEventDestroy(started);
        cudaEventDestroy(finished);
    }

    KernelTimer(const KernelTimer &) = delete;
    KernelTimer & operator=(const KernelTimer &) = delete;

    void begin(rmm::cuda_stream_view stream) { checkCuda(cudaEventRecord(started, stream.value()), "cannot record an event"); }
    void end(rmm::cuda_stream_view stream) { checkCuda(cudaEventRecord(finished, stream.value()), "cannot record an event"); }

    /// Waits for `end` and answers the microseconds between the two.
    double microseconds() const
    {
        checkCuda(cudaEventSynchronize(finished), "cannot wait for the kernel");
        float milliseconds = 0;
        checkCuda(cudaEventElapsedTime(&milliseconds, started, finished), "cannot time the kernel");
        return milliseconds * 1000.0;
    }

private:
    cudaEvent_t started = nullptr;
    cudaEvent_t finished = nullptr;
};

/// The table of groups on the device: the set of packed keys, and beside it a record of
/// accumulators per slot, plus the spare record at `capacity` for the sentinel key.
class GroupTable
{
public:
    GroupTable() = default;

    /// A table of at least `requested` slots, its records at their identities.
    GroupTable(size_t requested, const Shape & shape, rmm::cuda_stream_view stream)
    {
        set = std::make_unique<Set>(
            cuco::extent<size_t>{requested},
            cuco::empty_key<Key>{key_sentinel},
            cuda::std::equal_to<Key>{},
            cuco::linear_probing<1, cuco::xxhash_64<Key>>{},
            cuco::cuda_thread_scope<cuda::thread_scope_device>{},
            cuco::storage<1>{},
            rmm::mr::polymorphic_allocator<char>{},
            cuda::stream_ref{stream.value()});
        capacity = set->capacity();
        slots = set->ref(cuco::op::insert_and_find).storage_ref().data();

        if (capacity > max_capacity)
            throw CudfError("a `GROUP BY` table of " + std::to_string(capacity) + " slots on the device");

        records.emplace((capacity + 1) * shape.values.size(), stream);
        accumulators = {.records = records->data(), .num_values = shape.numValues()};

        initRecords<<<blocksFor(capacity + 1), threads_per_block, 0, stream.value()>>>(accumulators, capacity, shape.identities);
        checkLaunch("empties the accumulators of a table of groups");
    }

    bool exists() const { return set != nullptr; }

    size_t getCapacity() const { return capacity; }

    TableRef ref() const { return {.set = set->ref(cuco::op::insert_and_find), .slots = slots, .capacity = capacity, .accumulators = accumulators}; }

    /// How many more rows fit beside `groups` groups even were every one of them a new key.
    size_t room(size_t groups) const
    {
        if (!set)
            return 0;
        const size_t fits = static_cast<size_t>(max_load * static_cast<double>(capacity));
        return fits > groups ? fits - groups : 0;
    }

    /// Takes over the groups of `from`, a smaller table, sentinel record included.
    void takeGroupsOf(const GroupTable & from, const Shape & shape, rmm::cuda_stream_view stream)
    {
        checkCuda(
            cudaMemcpyAsync(
                accumulators.of(capacity),
                from.accumulators.of(from.capacity),
                shape.values.size() * sizeof(uint64_t),
                cudaMemcpyDeviceToDevice,
                stream.value()),
            "cannot carry the sentinel key's accumulators over");

        moveGroups<<<blocksFor(from.capacity), threads_per_block, 0, stream.value()>>>(from.slots, from.capacity, from.accumulators, ref());
        checkLaunch("moves the groups into a larger table");
    }

private:
    std::unique_ptr<Set> set;
    const Key * slots = nullptr;
    size_t capacity = 0;
    std::optional<rmm::device_uvector<uint64_t>> records;
    Accumulators accumulators;
};

/// The two-pass path: the sort's buffers and the overflow list, grown to the largest chunk so
/// far, and the buckets' partial groups, a shared table's worth per bucket.
class BucketedPass
{
public:
    BucketedPass(const Shape & shape, rmm::cuda_stream_view stream)
        : buckets_in(0, stream)
        , buckets_out(0, stream)
        , indices_in(0, stream)
        , indices_out(0, stream)
        , sort_storage(0, stream)
        , overflow(0, stream)
        , num_overflow(0, stream)
        , partial_keys(0, stream)
        , partial_records(0, stream)
    {
        /// The block's own few shared variables take a little of the shared memory, so a table of
        /// a whole 48 kilobytes would not launch.
        const size_t slot_bytes = sizeof(Key) + sizeof(uint64_t) * shape.values.size();
        shared_capacity = static_cast<uint32_t>(std::bit_floor((shared_table_bytes - 64) / slot_bytes));
        shared_bytes = slot_bytes * shared_capacity;

        partial_keys.resize(numPartials(), stream);
        partial_records.resize(numPartials() * shape.values.size(), stream);
    }

    /// Whether a chunk of `rows` rows can go this way beside `groups` groups: when the groups,
    /// spread over the buckets, fill less than half of a bucket's table in shared memory.
    bool fits(size_t rows, size_t groups) const
    {
        return rows >= min_bucketed_rows && rows <= std::numeric_limits<uint32_t>::max() && groups <= numPartials() / 2;
    }

    size_t numPartials() const { return size_t{num_buckets} * shared_capacity; }

    /// Sorts the chunk's rows into buckets and groups each bucket in shared memory, leaving the
    /// buckets' partial groups and the overflow list for `partialsFrom` and `overflowRows`.
    void run(const Chunk & chunk, const Shape & shape, TableRef table, Counters counters, rmm::cuda_stream_view stream)
    {
        growBuffers(chunk.rows, stream);
        checkCuda(cudaMemsetAsync(num_overflow.data(), 0, sizeof(uint32_t), stream.value()), "cannot clear the overflow count");

        bucketRows<<<blocksFor(chunk.rows), threads_per_block, 0, stream.value()>>>(chunk, buckets_in.data(), indices_in.data());
        checkLaunch("sorts a chunk's rows into buckets");

        size_t storage_bytes = sort_storage.size();
        checkCuda(
            cub::DeviceRadixSort::SortPairs(
                sort_storage.data(),
                storage_bytes,
                buckets_in.data(),
                buckets_out.data(),
                indices_in.data(),
                indices_out.data(),
                chunk.rows,
                0,
                bucket_end_bit,
                stream.value()),
            "cannot sort a chunk's rows into buckets");

        const Buckets buckets{
            .sorted_buckets = buckets_out.data(),
            .order = indices_out.data(),
            .shared_capacity = shared_capacity,
            .identities = shape.identities,
            .partial_keys = partial_keys.data(),
            .partials = {.records = partial_records.data(), .num_values = shape.numValues()},
            .overflow = overflow.data(),
            .num_overflow = num_overflow.data(),
        };
        aggregateBuckets<<<num_buckets, bucket_threads, shared_bytes, stream.value()>>>(chunk, buckets, table, counters);
        checkLaunch("groups the buckets of a chunk");
    }

    /// The partial groups from `offset` on, `count` of them.
    Partials partialsFrom(size_t offset, size_t count, const Shape & shape)
    {
        return {
            .keys = partial_keys.data() + offset,
            .records = {.records = partial_records.data() + offset * shape.values.size(), .num_values = shape.numValues()},
            .count = count,
        };
    }

    /// How many rows found no slot in their bucket's table, and the list of them.
    size_t numOverflowed(rmm::cuda_stream_view stream) const { return num_overflow.value(stream); }
    const uint32_t * overflowRows() const { return overflow.data(); }

private:
    void growBuffers(size_t rows, rmm::cuda_stream_view stream)
    {
        if (buckets_in.size() < rows)
        {
            buckets_in.resize(rows, stream);
            buckets_out.resize(rows, stream);
            indices_in.resize(rows, stream);
            indices_out.resize(rows, stream);
            overflow.resize(rows, stream);
        }

        size_t needed = 0;
        checkCuda(
            cub::DeviceRadixSort::SortPairs(
                nullptr,
                needed,
                buckets_in.data(),
                buckets_out.data(),
                indices_in.data(),
                indices_out.data(),
                rows,
                0,
                bucket_end_bit,
                stream.value()),
            "cannot size the sort of a chunk's rows into buckets");
        if (sort_storage.size() < needed)
            sort_storage.resize(needed, stream);
    }

    /// Slots of a bucket's table in shared memory: a power of two, with the records of that many
    /// groups beside the keys.
    uint32_t shared_capacity = 0;
    size_t shared_bytes = 0;

    rmm::device_uvector<uint8_t> buckets_in;
    rmm::device_uvector<uint8_t> buckets_out;
    rmm::device_uvector<uint32_t> indices_in;
    rmm::device_uvector<uint32_t> indices_out;
    rmm::device_uvector<char> sort_storage;
    rmm::device_uvector<uint32_t> overflow;
    rmm::device_scalar<uint32_t> num_overflow;
    rmm::device_uvector<Key> partial_keys;
    rmm::device_uvector<uint64_t> partial_records;
};

/** Which path a chunk takes, by what each cost on this query: the direct kernel first, until it
  * is measured, then the two passes on a small chunk until they are, then the cheaper of the two;
  * and both afresh once the groups have doubled, since the cost of each path moves with them.
  */
class PathChoice
{
public:
    /// Whether the next chunk should go in two passes, given the groups there are now.
    bool bucketed(size_t groups)
    {
        if (bucketed_cost && groups > 2 * groups_when_measured)
        {
            direct_cost.reset();
            bucketed_cost.reset();
        }

        if (!direct_cost || *direct_cost < min_direct_cost_for_buckets)
            return false;
        if (!bucketed_cost)
            return true;
        return *bucketed_cost < *direct_cost;
    }

    /// Whether the two passes are yet to be measured, in which case they take a small chunk.
    bool measuringBucketed() const { return !bucketed_cost; }

    void sawDirect(double microseconds, size_t rows) { direct_cost = microseconds * 1000.0 / static_cast<double>(rows); }

    void sawBucketed(double microseconds, size_t rows, size_t groups)
    {
        bucketed_cost = microseconds * 1000.0 / static_cast<double>(rows);
        groups_when_measured = groups;
    }

private:
    /// Nanoseconds per row each path last took, and the groups there were when both were known.
    std::optional<double> direct_cost;
    std::optional<double> bucketed_cost;
    size_t groups_when_measured = 0;
};

}

struct RecordGroupBy::State
{
    Shape shape;
    GroupTable table;
    BucketedPass buckets;
    PathChoice choice;
    KernelTimer timer;

    rmm::device_scalar<uint32_t> num_groups;
    rmm::device_scalar<uint32_t> sentinel_seen;
    size_t groups = 0;

    std::vector<rmm::device_uvector<char>> output_keys;
    std::vector<rmm::device_uvector<char>> output_values;
    size_t output_groups = 0;
    bool finalized = false;

    State(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values)
        : shape(key_element_types, values)
        , buckets(shape, computeStream())
        , num_groups(0, computeStream())
        , sentinel_seen(0, computeStream())
    {
    }

    Counters counters() { return {.num_groups = num_groups.data(), .sentinel_seen = sentinel_seen.data()}; }

    size_t room() const { return table.room(groups); }

    /// Makes room for at least `more` new groups, growing the table when it has less.
    void ensureRoom(size_t more = min_chunk_rows)
    {
        if (table.exists() && room() >= more)
            return;

        const size_t needed = groups + more;
        const size_t requested
            = std::max({static_cast<size_t>(static_cast<double>(needed) / max_load) + 1, table.getCapacity() * 2, min_capacity});
        if (requested > max_capacity)
            throw CudfError("a `GROUP BY` of " + std::to_string(needed) + " groups is too large for the device");

        GroupTable larger(requested, shape, computeStream());
        if (table.exists())
            larger.takeGroupsOf(table, shape, computeStream());
        table = std::move(larger);
    }

    /// Reads back how many groups there are, which waits for everything queued so far.
    void countGroups() { groups = num_groups.value(computeStream()); }

    /// Queues the direct kernel over the chunk and waits for it, adding its time to `microseconds`.
    void aggregateDirectly(const Chunk & chunk, double & microseconds)
    {
        const rmm::cuda_stream_view stream = computeStream();

        timer.begin(stream);
        aggregateRows<<<blocksFor(chunk.rows), threads_per_block, 0, stream.value()>>>(table.ref(), chunk, counters());
        checkLaunch("groups a chunk of rows");
        timer.end(stream);

        countGroups();
        microseconds += timer.microseconds();
    }

    /// Groups the chunk in two passes: sorted into buckets and grouped in shared memory, then the
    /// buckets' partial groups folded into the table, then whatever overflowed the buckets'
    /// tables, directly; the last two in as many at a time as the table has room for, so that
    /// the table is grown by the groups there are and not by the slots the buckets' tables have.
    void aggregateBucketed(Chunk chunk, double & microseconds)
    {
        const rmm::cuda_stream_view stream = computeStream();

        timer.begin(stream);
        buckets.run(chunk, shape, table.ref(), counters(), stream);
        timer.end(stream);

        const size_t overflowed = buckets.numOverflowed(stream);
        microseconds += timer.microseconds();

        for (size_t merged = 0; merged < buckets.numPartials();)
        {
            ensureRoom();
            const size_t some = std::min(buckets.numPartials() - merged, room());

            timer.begin(stream);
            mergePartials<<<blocksFor(some), threads_per_block, 0, stream.value()>>>(
                table.ref(), buckets.partialsFrom(merged, some, shape), chunk.values, counters());
            checkLaunch("folds the buckets' groups into the table");
            timer.end(stream);

            countGroups();
            microseconds += timer.microseconds();
            merged += some;
        }

        for (size_t taken = 0; taken < overflowed;)
        {
            ensureRoom();
            chunk.order = buckets.overflowRows() + taken;
            chunk.rows = std::min(overflowed - taken, room());
            aggregateDirectly(chunk, microseconds);
            taken += chunk.rows;
        }
    }

    /// Groups rows from `offset` on - all of them in two passes when they are few groups' worth,
    /// otherwise as many as the table has room for - and answers how many, adding the kernels'
    /// own time to `microseconds`.
    size_t aggregateChunk(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> value_views,
        GPUSpan<DeviceColumnView> filter_columns,
        const GPUFilterProgram * filter,
        size_t offset,
        size_t remaining,
        double & microseconds)
    {
        ensureRoom();

        if (buckets.fits(remaining, groups) && choice.bucketed(groups))
        {
            const size_t rows = choice.measuringBucketed() ? std::min(remaining, measurement_rows) : remaining;
            double spent = 0;
            aggregateBucketed(shape.chunkAt(keys, value_views, filter_columns, filter, offset, rows), spent);
            choice.sawBucketed(spent, rows, groups);
            microseconds += spent;
            return rows;
        }

        const size_t rows = std::min(remaining, room());
        double spent = 0;
        aggregateDirectly(shape.chunkAt(keys, value_views, filter_columns, filter, offset, rows), spent);
        choice.sawDirect(spent, rows);
        microseconds += spent;
        return rows;
    }
};

RecordGroupBy::RecordGroupBy(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values)
{
    initializeCudf();
    state = guarded("setting up a `GROUP BY`", [&] { return new State(key_element_types, values); });
}

RecordGroupBy::~RecordGroupBy()
{
    delete state;
}

double RecordGroupBy::addBatch(
    GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> value_views, GPUSpan<DeviceColumnView> filter_columns, const GPUFilterProgram * filter)
{
    if (state->finalized)
        throw CudfError("a batch arrived after the aggregation was finalized");

    const size_t num_rows = keys[0].rows;
    return guarded("grouping a batch of " + std::to_string(num_rows) + " rows", [&]
    {
        double microseconds = 0;
        for (size_t offset = 0; offset < num_rows;)
            offset += state->aggregateChunk(keys, value_views, filter_columns, filter, offset, num_rows - offset, microseconds);
        return microseconds;
    });
}

size_t RecordGroupBy::finalize()
{
    if (state->finalized)
        throw CudfError("the aggregation was finalized twice");
    state->finalized = true;

    if (!state->table.exists())
        return 0;

    return guarded("writing the groups out", [&]
    {
        const rmm::cuda_stream_view stream = computeStream();
        const Shape & shape = state->shape;
        const TableRef table = state->table.ref();

        const size_t num_regular = state->groups;
        const bool with_sentinel = state->sentinel_seen.value(stream) != 0;
        const size_t num_groups = num_regular + (with_sentinel ? 1 : 0);
        if (num_groups == 0)
            return size_t{0};

        rmm::device_uvector<uint32_t> group_slots(num_regular, stream);
        if (num_regular != 0)
        {
            const auto end = thrust::copy_if(
                rmm::exec_policy_nosync(stream),
                thrust::counting_iterator<uint32_t>(0),
                thrust::counting_iterator<uint32_t>(static_cast<uint32_t>(table.capacity)),
                table.slots,
                group_slots.begin(),
                IsOccupied{});
            checkCount(static_cast<size_t>(end - group_slots.begin()), num_regular, "occupied records");
        }

        OutputLayouts out;
        out.num_keys = static_cast<uint32_t>(shape.key_element_types.size());
        for (size_t i = 0; i < shape.key_element_types.size(); ++i)
        {
            const uint32_t size = static_cast<uint32_t>(sizeOf(shape.key_element_types[i]));
            state->output_keys.emplace_back(num_groups * size, stream);
            out.keys[i] = {.data = state->output_keys.back().data(), .size = size, .shift = shape.key_shifts[i], .store = Store::Truncate};
        }

        out.num_values = shape.numValues();
        for (size_t i = 0; i < shape.values.size(); ++i)
        {
            const auto [store, size] = storeOf(shape.values[i]);
            state->output_values.emplace_back(num_groups * size, stream);
            out.values[i] = {.data = state->output_values.back().data(), .size = size, .shift = 0, .store = store};
        }

        const GroupList groups{.slots = group_slots.data(), .num_regular = num_regular, .with_sentinel = with_sentinel};
        writeGroups<<<blocksFor(num_groups), threads_per_block, 0, stream.value()>>>(groups, table, out);
        checkLaunch("writes the groups out");

        stream.synchronize();

        state->table = GroupTable{};
        state->output_groups = num_groups;
        return num_groups;
    });
}

void RecordGroupBy::copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> value_views)
{
    const Shape & shape = state->shape;
    checkCount(keys.size(), shape.key_element_types.size(), "key destinations");
    checkCount(value_views.size(), shape.values.size(), "value destinations");

    if (state->output_groups == 0)
        return;

    const rmm::cuda_stream_view stream = computeStream();

    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (keys[i].element_type != shape.key_element_types[i])
            throw CudfError("key column " + std::to_string(i) + " is copied out into a column of another type");
        checkCount(keys[i].rows, state->output_groups, "rows of room for key column " + std::to_string(i));

        checkCuda(
            cudaMemcpyAsync(keys[i].data, state->output_keys[i].data(), state->output_keys[i].size(), cudaMemcpyDeviceToHost, stream.value()),
            "cannot copy a column of group keys back");
    }

    for (size_t i = 0; i < value_views.size(); ++i)
    {
        if (value_views[i].element_type != leftIn(shape.values[i]))
            throw CudfError("value column " + std::to_string(i) + " is copied out into a column of another type");
        checkCount(value_views[i].rows, state->output_groups, "rows of room for value column " + std::to_string(i));

        checkCuda(
            cudaMemcpyAsync(
                value_views[i].data, state->output_values[i].data(), state->output_values[i].size(), cudaMemcpyDeviceToHost, stream.value()),
            "cannot copy a column of aggregated values back");
    }

    stream.synchronize();
}

}
