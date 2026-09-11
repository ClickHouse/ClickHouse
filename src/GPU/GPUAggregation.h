#pragma once

#include "config.h"

#if USE_GPU

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray.h>

#include <mutex>
#include <optional>
#include <vector>

namespace DB::GPU
{

/// Empty when this process can use a device, and otherwise why it cannot. Probed once, on the
/// first call: bringing a CUDA context up takes long enough to be worth not repeating, and long
/// enough that this should not be called while planning a query that would not use the device.
const String & deviceProbeError();


/// The lock every device call of the keyless `sum` - this engine's `SumAccumulator` and
/// `DeviceBuffer` below - is made under.
///
/// There is one device, and everything behind the boundary runs on cuDF's default stream: one
/// resource behind one stream. cuDF does not promise that two threads may drive the same stream at
/// once, and the resident-column path makes that a real possibility rather than a theoretical one -
/// several streams fill and reduce different parts' columns at the same time, and the accumulator
/// above them reduces the partial sums they produce. So those calls are serialized here instead.
/// It costs nothing that would not have been paid anyway: the transfers share one link and the
/// reductions one set of memory controllers, and the device is busy either way. The alternative was
/// to reason about cuDF's internal state and conclude that interleaving happens to be harmless,
/// which is not a thing to bet a wrong answer on.
///
/// Take it only around a call over the boundary, and never while a cache is being looked up or
/// written. Freeing a buffer takes this lock and an eviction frees buffers, so the `GPUColumnCache`
/// is a lock taken before this one - and a thread that held this one while asking for the cache
/// would be the other order of the same two locks, which is a deadlock rather than a slow query.
///
/// The grouped sum does not take it. Not because interleaving the two is safe, but because it is
/// not reached by the path this lock was added for, and putting it under the lock is a change to
/// that operator rather than to this one.
std::unique_lock<std::mutex> lockDevice();


/// The host side's half of the boundary's type mapping. Shared by the aggregation below and by the
/// plan pass and source step that read a column out of device memory, because all of them hand the
/// device the same ten fixed-width numeric types; a second copy of these switches would be the
/// thing that drifts.
///
/// The types are `ClickHouseGPUElementType` values, kept as `int` so that this header does not have
/// to carry the boundary's enumerators.

/// What a column of `type` is sent as, or nothing when the device has no element type for it. The
/// switch is on the outermost type, so `Nullable(UInt64)`, `LowCardinality(UInt64)`, every
/// `Decimal`, and also `Date` and `DateTime` - which are stored as integers but are not integer
/// types - are all turned away by it rather than by a check of their own.
std::optional<int> elementTypeOf(const IDataType & type);

/// What a sum of `type` comes back as, or nothing when `type` is not one of the three types a sum
/// is returned in. `UInt64`, `Int64` and `Float64` are those three, which is also why they are the
/// only types `ReadFromGPUResidentColumns` accepts: for them, and only for them, a `sum` has the
/// column's own type, so a column of partial sums per part is a column of the same type as the one
/// that was summed.
std::optional<int> sumTypeOf(const IDataType & type);

/// How many bytes one value of `element_type` occupies. The same on both sides of the boundary,
/// which is what lets a column's own bytes be the thing that is copied.
size_t elementSizeOf(int element_type);

/// The bytes of `column`'s values, which is what the device is given - checking on the way that
/// the column really is a run of `num_rows` values of `element_size` bytes each. A column that is
/// constant, replicated, sparse or low-cardinality is none of that, and the caller is the one that
/// has to have made it full - see `IColumn::convertToFullIfWrapped`.
std::string_view rawValuesOf(const IColumn & column, size_t num_rows, size_t element_size);

/// Resizes `column` to `num_rows` and hands back the bytes its values occupy, so that the device
/// copies its output straight into the column the query returns instead of into a staging buffer
/// that would then be copied again. `column` has to be empty and has to be the `ColumnVector`
/// `element_type` names, which is checked rather than assumed: a column of a different width would
/// otherwise be filled with a shifted, meaningless run of bytes instead of failing.
void * resizeForElementType(IColumn & column, size_t num_rows, int element_type);

/// Whether `sum` over an argument of `argument_type` returning `result_type` is an aggregation the
/// device can do. Decided on the types alone, during planning.
///
/// Nullable, LowCardinality and Decimal arguments are all outside the prototype: a validity
/// bitmask, a dictionary and a 128-bit accumulator are each a separate piece of work, and none of
/// them is needed to see whether the path is worth having.
bool canSumOnDevice(const IDataType & argument_type, const IDataType & result_type);

/// Sums a column that arrives in pieces, on the device.
///
/// Values are staged in host memory and sent over in batches of `batch_bytes` rather than one
/// block at a time. A block is 65536 rows, which for a `UInt64` column is half a megabyte - a
/// transfer that small spends all of its time in the launch and none of it moving data, and the
/// reduction that follows cannot reach the device's memory bandwidth over so few values either.
/// The batches' sums are added up here, on the host, which is why nothing about this class is
/// proportional to the size of the table.
class SumAccumulator
{
public:
    /// `argument_type` and `result_type` must be a pair `canSumOnDevice` accepts.
    SumAccumulator(const IDataType & argument_type, const IDataType & result_type, size_t batch_bytes_);

    /// Adds every value of `column`, which has to be a full column of the argument type - see
    /// `IColumn::convertToFullIfWrapped`.
    void add(const IColumn & column);

    /// The sum of everything added so far, as a value of the result type. Sends the last batch.
    Field finalize();

private:
    /// cuDF counts the rows of a column in a signed 32-bit integer, so no batch may hold more
    /// values than that. `batch_bytes` is capped to it and a batch is sent early when the next
    /// column would cross it: the setting is a performance knob, and a large value should not
    /// turn into a failing query.
    ///
    /// The cap is also what keeps a batch's sum exact. cuDF reduces an integer column into an
    /// `Int64` unless the type asked for is the column's own - so a `UInt32` column is summed in
    /// an `Int64` and the result converted, where ClickHouse would have used a `UInt64`
    /// throughout. The two agree because no batch can overflow that `Int64`: the largest value of
    /// a type narrower than 64 bits, times this many rows, still fits (for `UInt32`, the widest
    /// of them, (2^32 - 1) * (2^31 - 1) < 2^63 - 1). A `UInt64` or `Int64` column asks for its own
    /// type back and is summed in it, wrapping exactly as it does on the CPU.
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void sumBatchOnDevice();

    const int element_type;
    const int sum_type;
    const size_t element_size;
    const size_t batch_bytes;

    PODArray<char> staged;

    /// The running sum. Integers are accumulated in their unsigned representation, so that the
    /// wraparound `sum` has on the CPU as well stays defined behaviour here.
    UInt64 integer_sum = 0;
    Float64 float_sum = 0;
};


/// One column of one `MergeTree` part, held in device memory - what a `GPUColumnCache` entry owns,
/// and the only thing on this side that keeps a device resource alive between queries.
///
/// This is where the speed of the whole GPU path comes from. On this machine, over 200 million
/// `UInt64` values: the reduction takes 6 ms at 265 GB/s when the values are already here, and
/// 334 ms at 4.8 GB/s to put them here - while the same query on sixteen cores takes 430 ms, of
/// which 390 ms is reading and decompressing the part. So a column that is here already is summed
/// seventy times faster than the CPU answers the query, and everything else this engine does is
/// spent getting the bytes across.
///
/// Filled by copying each block of the part straight out of the column's own memory into its place
/// in the buffer, never through a staging buffer of our own: a host copy of those same 1.49 GiB
/// costs 294 ms, which is as much as the transfer, and it buys nothing - the link runs at 4.8 GB/s
/// for one copy of the whole column and at 4.7 GB/s for the same bytes in half-megabyte pieces, so
/// there is nothing to gain by first making the pieces bigger.
class DeviceBuffer
{
public:
    /// Allocates `bytes_` of device memory, and throws when the device cannot give it - which is
    /// what a cache sized larger than the card looks like from here.
    ///
    /// Device memory is outside every memory limit the server knows about: it is not in
    /// `max_memory_usage`, not in `max_server_memory_usage`, and not in the memory tracker's
    /// accounting at all. `gpu_column_cache_size` is the only thing that bounds how much of this
    /// exists.
    explicit DeviceBuffer(size_t bytes_);

    ~DeviceBuffer();

    /// Holds a device resource, and there is no use for a second name for one.
    DeviceBuffer(const DeviceBuffer &) = delete;
    DeviceBuffer & operator=(const DeviceBuffer &) = delete;

    /// Copies the `bytes` at `host_data` into the buffer at `offset`. Returns once the copy has
    /// run, so the block the bytes came from may be released as soon as this does.
    void copyIn(size_t offset, const char * host_data, size_t bytes);

    /// Sums the first `num_rows` values, which have to be of `element_type`, and returns the sum as
    /// a `Field` of the type `sum_type` names. Nothing crosses the link but those eight bytes.
    Field sum(int element_type, int sum_type, size_t num_rows) const;

    /// What the cache weighs this at - device bytes, the thing `gpu_column_cache_size` limits.
    size_t size() const { return bytes; }

private:
    const size_t bytes;

    /// The device memory, behind the boundary. Owned - see the destructor.
    void * handle = nullptr;
};


/// Whether a keyed `sum` - `SELECT k, sum(x) ... GROUP BY k` - is an aggregation the device can do:
/// every key of a type it can group by, and every `sum` one `canSumOnDevice` accepts. Decided on
/// the types alone, during planning.
///
/// The keys have to be the same fixed-width numeric types the values are, and for the same reason:
/// `Nullable` needs a validity bitmask, `LowCardinality` a dictionary and `Decimal` a wider
/// accumulator, and none of the three is needed to see whether the path is worth having. A `String`
/// key, which cuDF can group by perfectly well, is left out because a variable-width column has to
/// be staged and uploaded as offsets plus characters rather than as one contiguous run of values -
/// a second layout on both sides of the boundary, not a wider `switch`.
bool canGroupBySumOnDevice(const DataTypes & key_types, const DataTypes & argument_types, const DataTypes & result_types);

/// Sums columns grouped by keys that arrive in pieces, on the device.
///
/// Batched like `SumAccumulator`, and for the same reason - a block at a time is too little to
/// occupy either the link or the device - but the partial result cannot come back after every
/// batch the way a scalar sum can. It is a whole table of groups, so it stays on the device from
/// the first batch until `finalize`, behind the handle `GPUAggregationABI.h` describes, and every
/// batch is grouped and merged into it there. Nothing here is proportional to the size of the
/// table; what it holds in host memory is one batch, and on the device one partial result.
///
/// One caveat, inherited from cuDF's groupby having no output type to ask for: an integral column is
/// summed into an `Int64` whatever it holds, so a 64-bit column's group sums are accumulated signed
/// and wrap where ClickHouse's `sum` wraps. Two's complement addition wraps modulo 2^64 either way,
/// so the bits agree with the CPU's - but this does lean on the device wrapping rather than
/// trapping on a signed overflow, which C++ leaves undefined and NVIDIA's hardware defines.
class GroupBySumAccumulator
{
public:
    /// The types must be a triple `canGroupBySumOnDevice` accepts. `argument_types[i]` is what the
    /// `i`-th `sum` reads and `result_types[i]` what it returns.
    GroupBySumAccumulator(
        const DataTypes & key_types, const DataTypes & argument_types, const DataTypes & result_types, size_t batch_bytes);

    ~GroupBySumAccumulator();

    /// Holds a device resource, and there is no use for a second name for one.
    GroupBySumAccumulator(const GroupBySumAccumulator &) = delete;
    GroupBySumAccumulator & operator=(const GroupBySumAccumulator &) = delete;

    /// Adds one row set: `key_columns` in the order the keys were given to the constructor,
    /// `value_columns` in the order the aggregates were, all of the same length and each a full
    /// column of its type - see `IColumn::convertToFullIfWrapped`.
    void add(const Columns & key_columns, const Columns & value_columns);

    /// Sends the last batch and returns the number of groups, which is how many rows the caller has
    /// to make room for. Zero when nothing was ever added: a keyed aggregation over no rows has no
    /// groups.
    size_t finalize();

    /// Copies the groups out of the device and into columns of the key and result types, which have
    /// to be empty and are resized to the number of groups `finalize` returned. Call once, after
    /// `finalize`.
    void copyGroupsTo(MutableColumns & key_columns, MutableColumns & value_columns);

private:
    /// cuDF counts the rows of a column in a signed 32-bit integer, so no batch may hold more rows
    /// than that. Here a batch is a whole row set - every key and every value column of it - so the
    /// cap is on rows rather than on one column's bytes: `gpu_aggregation_batch_bytes` is turned
    /// into a row count against it, and a batch is sent early when the next chunk would cross it.
    /// The setting is a performance knob, and a large value should not turn into a failing query.
    ///
    /// For the value columns the cap does the second job it does in `SumAccumulator`: it keeps a
    /// batch's partial sums exact. cuDF sums an integral column into an `Int64`, so a `UInt32`
    /// column is summed in an `Int64` where ClickHouse would have used a `UInt64` - and the two
    /// agree because no batch can overflow that `Int64`: the largest value of a type narrower than
    /// 64 bits, times this many rows, still fits (for `UInt32`, the widest of them,
    /// (2^32 - 1) * (2^31 - 1) < 2^63 - 1).
    ///
    /// A key column carries none of that concern - a group's key is one of the input keys whatever
    /// the batch is, and nothing is accumulated in it - so for the keys this is only cuDF's row
    /// count.
    static constexpr size_t max_batch_rows = (1UL << 31) - 1;

    void sendBatchToDevice();

    /// `ClickHouseGPUElementType` and `ClickHouseGPUSumType` values, kept as `int` so that this
    /// header does not have to carry the boundary's enumerators - the same reason
    /// `SumAccumulator` does.
    std::vector<int> key_element_types;
    std::vector<int> value_element_types;
    std::vector<int> value_sum_types;

    std::vector<size_t> key_element_sizes;
    std::vector<size_t> value_element_sizes;

    /// How many rows to gather before sending them, from `gpu_aggregation_batch_bytes` and the
    /// width of one row of keys and values.
    const size_t batch_rows;

    /// One buffer per column, values laid out one after another: what the device wants of a column,
    /// and what a `ColumnVector` holds already - so staging is an append of the column's own bytes.
    std::vector<PODArray<char>> staged_keys;
    std::vector<PODArray<char>> staged_values;
    size_t staged_rows = 0;

    /// The partial result, on the device. Owned - see the destructor.
    void * handle = nullptr;

    /// Set by `finalize`, so that `copyGroupsTo` cannot be called before it or the last batch be
    /// staged after it.
    std::optional<size_t> num_groups;
};

}

#endif
