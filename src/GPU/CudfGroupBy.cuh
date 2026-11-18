#pragma once

#include <GPU/GPUExpression.cuh>
#include <GPU/GPUTypes.cuh>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// A value column of a `GROUP BY` that the device computes from other value columns by `expression`, into a `Float64`:
/// the `columns[i]`-th value column is what it reads as its column `i`.
struct GPUDerivedValue
{
    GPUExpression expression;
    GPUSpan<uint32_t> columns;
};

/// A `GROUP BY` on the device through one `cudf::groupby::streaming_groupby`, which keeps the groups on the device across
/// the batches: of keys of integers and strings, by the aggregates of value columns, several of one column or a `count` of
/// none among them.
///
/// Without keys, the aggregates are of every row, as one group, which there is none of where no row is grouped.
///
/// Without keys and aggregates, it collects the rows of `values` that a batch's `WHERE` keeps instead, as they are, and
/// `value` is of its collected columns.
///
/// The last `derived.size()` of `values` are not in the batches: they are computed on the device from the others, the
/// `Float64` values of each `GPUDerivedValue`, over the rows of a batch that its `WHERE` keeps.
class CudfGroupBy
{
public:
    __host__ CudfGroupBy(
        GPUSpan<GPUElementType> keys,
        GPUSpan<GPUElementType> values,
        GPUSpan<GPUDerivedValue> derived,
        GPUSpan<GPUGroupByAggregate> aggregates,
        cuda::stream_ref stream);
    __host__ ~CudfGroupBy();

    CudfGroupBy(const CudfGroupBy &) = delete;
    CudfGroupBy & operator=(const CudfGroupBy &) = delete;

    /// The rows for which `filter`, when given, is false over `filter_columns` are left out. Of `values`, those of the
    /// batch, without the derived ones. The columns must be there in the order of the stream of the `CudfGroupBy`, and
    /// must stay until it has grouped them, in its order as well.
    __host__ void addBatch(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> values,
        GPUSpan<DeviceFixedColumn> filter_columns = {},
        const GPUExpression * filter = nullptr);

    __host__ size_t finalize();

    /// Whether the batches are reduced over their runs of equal keys, once the first batch with rows decided it. A
    /// `CudfGroupBy` told so before its first batch reduces as it is told, so that several of them can be merged.
    __host__ bool decided() const;
    __host__ bool reducesRuns() const;
    __host__ void reduceRuns(bool reduce);

    /// Before `finalize`, of the same keys, values and aggregates: takes the groups of `other`, or the rows it collected,
    /// into these, in the order of the stream of each; `other` has none left.
    __host__ void absorb(CudfGroupBy & other);

    /// Before the first batch, of a `GROUP BY` with keys whose aggregates combine, see `combinesPartials`: each batch is
    /// reduced into partial results by a groupby of cuDF and split by a hash of the keys into `num_parts` parts as it
    /// comes, as Sirius does, so that no hash table grows over the batches. See `partition`.
    __host__ void splitIntoParts(size_t num_parts);
    /// Instead of `finalize`, of a `GROUP BY` with keys whose aggregates combine, see `combinesPartials`: closes the groups
    /// as partial results split by a hash of their keys into `num_parts` parts, which others merge each on its own, see
    /// `absorbPart`: those split as the batches came, or else the groups so far, split now. Returns about the number
    /// of groups.
    __host__ size_t partition(size_t num_parts);
    /// Whether the partial results of every aggregate combine by an aggregate: not those of an `avg`.
    __host__ bool combinesPartials() const;
    /// Of one that has grouped no batch: takes part `part` of the groups of `other`, which `partition` split, to combine
    /// at `finalize` with the parts of the same number of others. The parts of one `other` may be taken by several at once.
    __host__ void absorbPart(const CudfGroupBy & other, size_t part);

    /// Before `finalize`: how many groups `streaming_groupby` holds so far, which waits for the device.
    __host__ size_t numGroups() const;
    /// Before `finalize`: grows `streaming_groupby` at once to hold `groups`, so that `absorb` of others of that many
    /// groups in all does not grow it again for each of them. Only of one that has grouped a batch itself.
    __host__ void reserveGroups(size_t groups);

    /// Of rows collected rather than grouped: how many are collected and not taken yet, and makes them what `value`
    /// views, until the next call, returning how many. Before `finalize` as well.
    __host__ size_t collectedRows() const;
    __host__ size_t takeCollected();

    /// After `finalize`: keeps the groups for which `expression` holds over the keys and the aggregates' results, each of a
    /// fixed width, that `columns` names, the keys first and then the aggregates. Returns the number of groups kept.
    __host__ size_t filterGroups(const GPUExpression & expression, GPUSpan<uint32_t> columns);

    __host__ DeviceColumnView key(size_t index) const;
    /// The result of aggregate `index`.
    __host__ DeviceColumnView value(size_t index) const;

private:
    struct State;
    State * state = nullptr;
};

}
